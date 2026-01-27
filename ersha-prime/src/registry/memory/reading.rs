use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use ersha_core::{ReadingId, SensorMetric, SensorReading};
use tokio::sync::RwLock;

use crate::registry::{
    ReadingRegistry,
    filter::{Pagination, QueryOptions, ReadingFilter, ReadingSortBy, SensorMetricType, SortOrder},
};

use super::InMemoryError;

#[derive(Clone)]
pub struct InMemoryReadingRegistry {
    readings: Arc<RwLock<HashMap<ReadingId, SensorReading>>>,
}

impl InMemoryReadingRegistry {
    pub fn new() -> Self {
        Self {
            readings: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryReadingRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ReadingRegistry for InMemoryReadingRegistry {
    type Error = InMemoryError;

    async fn store(&self, reading: SensorReading) -> Result<(), Self::Error> {
        let mut readings = self.readings.write().await;
        let _ = readings.insert(reading.id, reading);
        Ok(())
    }

    async fn get(&self, id: ReadingId) -> Result<Option<SensorReading>, Self::Error> {
        let readings = self.readings.read().await;
        Ok(readings.get(&id).cloned())
    }

    async fn batch_store(&self, readings: Vec<SensorReading>) -> Result<(), Self::Error> {
        for reading in readings {
            self.store(reading).await?;
        }
        Ok(())
    }

    async fn count(&self, filter: Option<ReadingFilter>) -> Result<usize, Self::Error> {
        let readings = self.readings.read().await;
        if let Some(filter) = filter {
            return Ok(filter_readings(&readings, &filter).count());
        }
        Ok(readings.len())
    }

    async fn list(
        &self,
        options: QueryOptions<ReadingFilter, ReadingSortBy>,
    ) -> Result<Vec<SensorReading>, Self::Error> {
        let readings = self.readings.read().await;
        let filtered: Vec<&SensorReading> = filter_readings(&readings, &options.filter).collect();
        let sorted = sort_readings(filtered, &options.sort_by, &options.sort_order);
        let paginated = paginate_readings(sorted, &options.pagination);
        Ok(paginated)
    }
}

fn metric_type(metric: &SensorMetric) -> SensorMetricType {
    match metric {
        SensorMetric::SoilMoisture { .. } => SensorMetricType::SoilMoisture,
        SensorMetric::SoilTemp { .. } => SensorMetricType::SoilTemp,
        SensorMetric::AirTemp { .. } => SensorMetricType::AirTemp,
        SensorMetric::Humidity { .. } => SensorMetricType::Humidity,
        SensorMetric::Rainfall { .. } => SensorMetricType::Rainfall,
    }
}

fn filter_readings<'a>(
    readings: &'a HashMap<ReadingId, SensorReading>,
    filter: &ReadingFilter,
) -> impl Iterator<Item = &'a SensorReading> {
    readings.values().filter(|reading| {
        if let Some(ids) = &filter.ids
            && !ids.contains(&reading.id)
        {
            return false;
        }

        if let Some(device_ids) = &filter.device_ids
            && !device_ids.contains(&reading.device_id)
        {
            return false;
        }

        if let Some(sensor_ids) = &filter.sensor_ids
            && !sensor_ids.contains(&reading.sensor_id)
        {
            return false;
        }

        if let Some(dispatcher_ids) = &filter.dispatcher_ids
            && !dispatcher_ids.contains(&reading.dispatcher_id)
        {
            return false;
        }

        if let Some(metric_types) = &filter.metric_types
            && !metric_types.contains(&metric_type(&reading.metric))
        {
            return false;
        }

        if let Some(locations) = &filter.locations
            && !locations.contains(&reading.location)
        {
            return false;
        }

        if let Some(confidence_range) = &filter.confidence_range
            && !confidence_range.contains(&reading.confidence.0)
        {
            return false;
        }

        match (&filter.timestamp_after, &filter.timestamp_before) {
            (None, None) => (),
            (None, Some(before)) => {
                if &reading.timestamp > before {
                    return false;
                }
            }
            (Some(after), None) => {
                if &reading.timestamp < after {
                    return false;
                }
            }
            (Some(after), Some(before)) => {
                if &reading.timestamp < after || &reading.timestamp > before {
                    return false;
                }
            }
        }

        true
    })
}

fn sort_readings<'a>(
    mut readings: Vec<&'a SensorReading>,
    sort_by: &ReadingSortBy,
    sort_order: &SortOrder,
) -> Vec<&'a SensorReading> {
    readings.sort_by(|a, b| {
        let ord = match sort_by {
            ReadingSortBy::Timestamp => a.timestamp.cmp(&b.timestamp),
            ReadingSortBy::Confidence => a.confidence.0.cmp(&b.confidence.0),
            ReadingSortBy::DeviceId => a.device_id.0.cmp(&b.device_id.0),
        };

        match sort_order {
            SortOrder::Asc => ord,
            SortOrder::Desc => ord.reverse(),
        }
    });

    readings
}

fn paginate_readings(readings: Vec<&SensorReading>, pagination: &Pagination) -> Vec<SensorReading> {
    match pagination {
        Pagination::Offset { offset, limit } => readings
            .into_iter()
            .skip(*offset)
            .take(*limit)
            .cloned()
            .collect(),
        Pagination::Cursor { after, limit } => {
            if let Some(inner_ulid) = after {
                let id = ReadingId(*inner_ulid);
                return readings
                    .into_iter()
                    .skip_while(|reading| reading.id != id)
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
    use ordered_float::NotNan;
    use ulid::Ulid;

    use crate::registry::ReadingRegistry;
    use crate::registry::filter::{
        Pagination, QueryOptions, ReadingFilter, ReadingSortBy, SensorMetricType, SortOrder,
    };
    use ersha_core::{
        DeviceId, DispatcherId, H3Cell, Percentage, ReadingId, SensorId, SensorMetric,
        SensorReading,
    };

    use super::InMemoryReadingRegistry;

    fn mock_reading(id: ReadingId, metric: SensorMetric, confidence: u8) -> SensorReading {
        SensorReading {
            id,
            device_id: DeviceId(Ulid::new()),
            dispatcher_id: DispatcherId(Ulid::new()),
            metric,
            location: H3Cell(0x8a2a1072b59ffff),
            confidence: Percentage(confidence),
            timestamp: Timestamp::now(),
            sensor_id: SensorId(Ulid::new()),
        }
    }

    #[tokio::test]
    async fn test_store_and_get() {
        let registry = InMemoryReadingRegistry::new();
        let id = ReadingId(Ulid::new());
        let reading = mock_reading(
            id,
            SensorMetric::AirTemp {
                value: NotNan::new(25.0).unwrap(),
            },
            90,
        );

        registry.store(reading.clone()).await.unwrap();

        let fetched = registry.get(id).await.unwrap().unwrap();
        assert_eq!(fetched.id, id);
        assert_eq!(fetched.confidence.0, 90);
    }

    #[tokio::test]
    async fn test_batch_store_and_count() {
        let registry = InMemoryReadingRegistry::new();

        let readings = vec![
            mock_reading(
                ReadingId(Ulid::new()),
                SensorMetric::AirTemp {
                    value: NotNan::new(20.0).unwrap(),
                },
                80,
            ),
            mock_reading(
                ReadingId(Ulid::new()),
                SensorMetric::Humidity {
                    value: Percentage(60),
                },
                90,
            ),
            mock_reading(
                ReadingId(Ulid::new()),
                SensorMetric::AirTemp {
                    value: NotNan::new(22.0).unwrap(),
                },
                95,
            ),
        ];

        registry.batch_store(readings).await.unwrap();

        assert_eq!(registry.count(None).await.unwrap(), 3);

        let filter = ReadingFilter {
            metric_types: Some(vec![SensorMetricType::AirTemp]),
            ..Default::default()
        };
        assert_eq!(registry.count(Some(filter)).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_list_with_sorting() {
        let registry = InMemoryReadingRegistry::new();

        let readings = vec![
            mock_reading(
                ReadingId(Ulid::new()),
                SensorMetric::AirTemp {
                    value: NotNan::new(20.0).unwrap(),
                },
                80,
            ),
            mock_reading(
                ReadingId(Ulid::new()),
                SensorMetric::AirTemp {
                    value: NotNan::new(22.0).unwrap(),
                },
                95,
            ),
            mock_reading(
                ReadingId(Ulid::new()),
                SensorMetric::AirTemp {
                    value: NotNan::new(21.0).unwrap(),
                },
                70,
            ),
        ];

        registry.batch_store(readings).await.unwrap();

        let options = QueryOptions {
            filter: ReadingFilter::default(),
            sort_by: ReadingSortBy::Confidence,
            sort_order: SortOrder::Desc,
            pagination: Pagination::Offset {
                offset: 0,
                limit: 10,
            },
        };

        let results = registry.list(options).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].confidence.0, 95);
        assert_eq!(results[1].confidence.0, 80);
        assert_eq!(results[2].confidence.0, 70);
    }

    #[tokio::test]
    async fn test_filter_by_confidence_range() {
        let registry = InMemoryReadingRegistry::new();

        let readings = vec![
            mock_reading(
                ReadingId(Ulid::new()),
                SensorMetric::AirTemp {
                    value: NotNan::new(20.0).unwrap(),
                },
                50,
            ),
            mock_reading(
                ReadingId(Ulid::new()),
                SensorMetric::AirTemp {
                    value: NotNan::new(22.0).unwrap(),
                },
                75,
            ),
            mock_reading(
                ReadingId(Ulid::new()),
                SensorMetric::AirTemp {
                    value: NotNan::new(21.0).unwrap(),
                },
                90,
            ),
        ];

        registry.batch_store(readings).await.unwrap();

        let filter = ReadingFilter {
            confidence_range: Some(70..=80),
            ..Default::default()
        };

        assert_eq!(registry.count(Some(filter)).await.unwrap(), 1);
    }
}
