use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use ersha_core::{DeviceId, DeviceStatus, StatusId};
use tokio::sync::RwLock;

use crate::registry::{
    DeviceStatusRegistry,
    filter::{DeviceStatusFilter, DeviceStatusSortBy, Pagination, QueryOptions, SortOrder},
};

use super::InMemoryError;

#[derive(Clone)]
pub struct InMemoryDeviceStatusRegistry {
    statuses: Arc<RwLock<HashMap<StatusId, DeviceStatus>>>,
}

impl InMemoryDeviceStatusRegistry {
    pub fn new() -> Self {
        Self {
            statuses: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryDeviceStatusRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DeviceStatusRegistry for InMemoryDeviceStatusRegistry {
    type Error = InMemoryError;

    async fn store(&self, status: DeviceStatus) -> Result<(), Self::Error> {
        let mut statuses = self.statuses.write().await;
        let _ = statuses.insert(status.id, status);
        Ok(())
    }

    async fn get(&self, id: StatusId) -> Result<Option<DeviceStatus>, Self::Error> {
        let statuses = self.statuses.read().await;
        Ok(statuses.get(&id).cloned())
    }

    async fn get_latest(&self, device_id: DeviceId) -> Result<Option<DeviceStatus>, Self::Error> {
        let statuses = self.statuses.read().await;
        Ok(statuses
            .values()
            .filter(|s| s.device_id == device_id)
            .max_by_key(|s| s.timestamp)
            .cloned())
    }

    async fn batch_store(&self, statuses: Vec<DeviceStatus>) -> Result<(), Self::Error> {
        for status in statuses {
            self.store(status).await?;
        }
        Ok(())
    }

    async fn count(&self, filter: Option<DeviceStatusFilter>) -> Result<usize, Self::Error> {
        let statuses = self.statuses.read().await;
        if let Some(filter) = filter {
            return Ok(filter_statuses(&statuses, &filter).count());
        }
        Ok(statuses.len())
    }

    async fn list(
        &self,
        options: QueryOptions<DeviceStatusFilter, DeviceStatusSortBy>,
    ) -> Result<Vec<DeviceStatus>, Self::Error> {
        let statuses = self.statuses.read().await;
        let filtered: Vec<&DeviceStatus> = filter_statuses(&statuses, &options.filter).collect();
        let sorted = sort_statuses(filtered, &options.sort_by, &options.sort_order);
        let paginated = paginate_statuses(sorted, &options.pagination);
        Ok(paginated)
    }
}

fn filter_statuses<'a>(
    statuses: &'a HashMap<StatusId, DeviceStatus>,
    filter: &DeviceStatusFilter,
) -> impl Iterator<Item = &'a DeviceStatus> {
    statuses.values().filter(|status| {
        if let Some(ids) = &filter.ids
            && !ids.contains(&status.id)
        {
            return false;
        }

        if let Some(device_ids) = &filter.device_ids
            && !device_ids.contains(&status.device_id)
        {
            return false;
        }

        if let Some(dispatcher_ids) = &filter.dispatcher_ids
            && !dispatcher_ids.contains(&status.dispatcher_id)
        {
            return false;
        }

        if let Some(battery_range) = &filter.battery_range
            && !battery_range.contains(&status.battery_percent.0)
        {
            return false;
        }

        if let Some(has_errors) = &filter.has_errors {
            let status_has_errors = !status.errors.is_empty();
            if *has_errors != status_has_errors {
                return false;
            }
        }

        if let Some(error_codes) = &filter.error_codes {
            let has_matching_error = status.errors.iter().any(|e| error_codes.contains(&e.code));
            if !has_matching_error {
                return false;
            }
        }

        match (&filter.timestamp_after, &filter.timestamp_before) {
            (None, None) => (),
            (None, Some(before)) => {
                if &status.timestamp > before {
                    return false;
                }
            }
            (Some(after), None) => {
                if &status.timestamp < after {
                    return false;
                }
            }
            (Some(after), Some(before)) => {
                if &status.timestamp < after || &status.timestamp > before {
                    return false;
                }
            }
        }

        true
    })
}

fn sort_statuses<'a>(
    mut statuses: Vec<&'a DeviceStatus>,
    sort_by: &DeviceStatusSortBy,
    sort_order: &SortOrder,
) -> Vec<&'a DeviceStatus> {
    statuses.sort_by(|a, b| {
        let ord = match sort_by {
            DeviceStatusSortBy::Timestamp => a.timestamp.cmp(&b.timestamp),
            DeviceStatusSortBy::BatteryPercent => a.battery_percent.0.cmp(&b.battery_percent.0),
            DeviceStatusSortBy::DeviceId => a.device_id.0.cmp(&b.device_id.0),
        };

        match sort_order {
            SortOrder::Asc => ord,
            SortOrder::Desc => ord.reverse(),
        }
    });

    statuses
}

fn paginate_statuses(statuses: Vec<&DeviceStatus>, pagination: &Pagination) -> Vec<DeviceStatus> {
    match pagination {
        Pagination::Offset { offset, limit } => statuses
            .into_iter()
            .skip(*offset)
            .take(*limit)
            .cloned()
            .collect(),
        Pagination::Cursor { after, limit } => {
            if let Some(inner_ulid) = after {
                let id = StatusId(*inner_ulid);
                return statuses
                    .into_iter()
                    .skip_while(|status| status.id != id)
                    .skip(1)
                    .take(*limit)
                    .cloned()
                    .collect();
            }
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use jiff::Timestamp;
    use ulid::Ulid;

    use crate::registry::DeviceStatusRegistry;
    use crate::registry::filter::{
        DeviceStatusFilter, DeviceStatusSortBy, Pagination, QueryOptions, SortOrder,
    };
    use ersha_core::{
        DeviceError, DeviceErrorCode, DeviceId, DeviceStatus, DispatcherId, Percentage, StatusId,
    };

    use super::InMemoryDeviceStatusRegistry;

    fn mock_status(id: StatusId, device_id: DeviceId, battery: u8) -> DeviceStatus {
        DeviceStatus {
            id,
            device_id,
            dispatcher_id: DispatcherId(Ulid::new()),
            battery_percent: Percentage(battery),
            uptime_seconds: 3600,
            signal_rssi: -50,
            errors: vec![].into_boxed_slice(),
            timestamp: Timestamp::now(),
            sensor_statuses: vec![].into_boxed_slice(),
        }
    }

    fn mock_status_with_errors(
        id: StatusId,
        device_id: DeviceId,
        battery: u8,
        errors: Vec<DeviceError>,
    ) -> DeviceStatus {
        DeviceStatus {
            errors: errors.into_boxed_slice(),
            ..mock_status(id, device_id, battery)
        }
    }

    #[tokio::test]
    async fn test_store_and_get() {
        let registry = InMemoryDeviceStatusRegistry::new();
        let id = StatusId(Ulid::new());
        let device_id = DeviceId(Ulid::new());
        let status = mock_status(id, device_id, 85);

        registry.store(status.clone()).await.unwrap();

        let fetched = registry.get(id).await.unwrap().unwrap();
        assert_eq!(fetched.id, id);
        assert_eq!(fetched.battery_percent.0, 85);
    }

    #[tokio::test]
    async fn test_get_latest() {
        let registry = InMemoryDeviceStatusRegistry::new();
        let device_id = DeviceId(Ulid::new());

        let mut older = mock_status(StatusId(Ulid::new()), device_id, 90);
        older.timestamp = Timestamp::from_second(100).unwrap();

        let mut newer = mock_status(StatusId(Ulid::new()), device_id, 80);
        newer.timestamp = Timestamp::from_second(200).unwrap();

        registry
            .batch_store(vec![older, newer.clone()])
            .await
            .unwrap();

        let latest = registry.get_latest(device_id).await.unwrap().unwrap();
        assert_eq!(latest.battery_percent.0, 80);
    }

    #[tokio::test]
    async fn test_filter_by_battery_range() {
        let registry = InMemoryDeviceStatusRegistry::new();

        let statuses = vec![
            mock_status(StatusId(Ulid::new()), DeviceId(Ulid::new()), 20),
            mock_status(StatusId(Ulid::new()), DeviceId(Ulid::new()), 50),
            mock_status(StatusId(Ulid::new()), DeviceId(Ulid::new()), 80),
        ];

        registry.batch_store(statuses).await.unwrap();

        let filter = DeviceStatusFilter {
            battery_range: Some(40..=60),
            ..Default::default()
        };

        assert_eq!(registry.count(Some(filter)).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_filter_by_has_errors() {
        let registry = InMemoryDeviceStatusRegistry::new();

        let statuses = vec![
            mock_status(StatusId(Ulid::new()), DeviceId(Ulid::new()), 90),
            mock_status_with_errors(
                StatusId(Ulid::new()),
                DeviceId(Ulid::new()),
                50,
                vec![DeviceError {
                    code: DeviceErrorCode::LowBattery,
                    message: None,
                }],
            ),
        ];

        registry.batch_store(statuses).await.unwrap();

        let filter_with_errors = DeviceStatusFilter {
            has_errors: Some(true),
            ..Default::default()
        };
        assert_eq!(registry.count(Some(filter_with_errors)).await.unwrap(), 1);

        let filter_no_errors = DeviceStatusFilter {
            has_errors: Some(false),
            ..Default::default()
        };
        assert_eq!(registry.count(Some(filter_no_errors)).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_list_sorted_by_battery() {
        let registry = InMemoryDeviceStatusRegistry::new();

        let statuses = vec![
            mock_status(StatusId(Ulid::new()), DeviceId(Ulid::new()), 50),
            mock_status(StatusId(Ulid::new()), DeviceId(Ulid::new()), 90),
            mock_status(StatusId(Ulid::new()), DeviceId(Ulid::new()), 20),
        ];

        registry.batch_store(statuses).await.unwrap();

        let options = QueryOptions {
            filter: DeviceStatusFilter::default(),
            sort_by: DeviceStatusSortBy::BatteryPercent,
            sort_order: SortOrder::Asc,
            pagination: Pagination::Offset {
                offset: 0,
                limit: 10,
            },
        };

        let results = registry.list(options).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].battery_percent.0, 20);
        assert_eq!(results[1].battery_percent.0, 50);
        assert_eq!(results[2].battery_percent.0, 90);
    }
}
