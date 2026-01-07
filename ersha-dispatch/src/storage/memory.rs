use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ersha_core::{
    SensorReading,
    DeviceStatus,
    ReadingId,
    StatusId,
};

use crate::storage::{
    Storage,
    StorageError,
};
use crate::storage::models::{
    StoredSensorReading,
    StoredDeviceStatus,
    StorageState,
};

/// In memory storage implementation.
/// This is primarily intended for testing and as a reference
/// implementation of the Storage trait.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    sensor_readings: Arc<Mutex<HashMap<ReadingId, StoredSensorReading>>>,
    device_statuses: Arc<Mutex<HashMap<StatusId, StoredDeviceStatus>>>,
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn store_sensor_reading(
        &self,
        reading: SensorReading,
    ) -> Result<(), StorageError> {
        let mut map = self
            .sensor_readings
            .lock()
            .map_err(|_| StorageError::Internal("sensor_readings mutex poisoned".into()))?;

        let id = reading.id;

        map.insert(
            id,
            StoredSensorReading {
                id,
                reading,
                state: StorageState::Pending,
            },
        );

        Ok(())
    }

    async fn store_device_status(
        &self,
        status: DeviceStatus,
    ) -> Result<(), StorageError> {
        let mut map = self
            .device_statuses
            .lock()
            .map_err(|_| StorageError::Internal("device_statuses mutex poisoned".into()))?;

        let id = status.id;

        map.insert(
            id,
            StoredDeviceStatus {
                id,
                status,
                state: StorageState::Pending,
            },
        );

        Ok(())
    }

    async fn fetch_pending_sensor_readings(
        &self,
    ) -> Result<Vec<SensorReading>, StorageError> {
        let map = self
            .sensor_readings
            .lock()
            .map_err(|_| StorageError::Internal("sensor_readings mutex poisoned".into()))?;

        Ok(map
            .values()
            .filter(|r| r.state == StorageState::Pending)
            .map(|r| r.reading.clone())
            .collect())
    }

    async fn fetch_pending_device_statuses(
        &self,
    ) -> Result<Vec<DeviceStatus>, StorageError> {
        let map = self
            .device_statuses
            .lock()
            .map_err(|_| StorageError::Internal("device_statuses mutex poisoned".into()))?;

        Ok(map
            .values()
            .filter(|s| s.state == StorageState::Pending)
            .map(|s| s.status.clone())
            .collect())
    }

    async fn mark_sensor_readings_uploaded(
        &self,
        ids: &[ReadingId],
    ) -> Result<(), StorageError> {
        let mut map = self
            .sensor_readings
            .lock()
            .map_err(|_| StorageError::Internal("sensor_readings mutex poisoned".into()))?;

        for id in ids {
            if let Some(entry) = map.get_mut(id) {
                entry.state = StorageState::Uploaded;
            }
        }

        Ok(())
    }

    async fn mark_device_statuses_uploaded(
        &self,
        ids: &[StatusId],
    ) -> Result<(), StorageError> {
        let mut map = self
            .device_statuses
            .lock()
            .map_err(|_| StorageError::Internal("device_statuses mutex poisoned".into()))?;

        for id in ids {
            if let Some(entry) = map.get_mut(id) {
                entry.state = StorageState::Uploaded;
            }
        }

        Ok(())
    }
}

