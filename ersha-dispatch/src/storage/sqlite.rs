use async_trait::async_trait;
use sqlx::{Error as SqlxError, Row, SqlitePool};
use std::path::Path;
use std::time::Duration;

use crate::storage::{
    CleanupStats, DeviceStatusStorage, SensorReadingsStorage, StorageMaintenance, StorageStats,
};
use ersha_core::{DeviceStatus, ReadingId, SensorReading, StatusId};

#[derive(Clone)]
pub struct SqliteStorage {
    pool: SqlitePool,
}

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqliteStorageError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] SqlxError),
    #[error("serde json error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

impl SqliteStorage {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, SqliteStorageError> {
        let database_url = format!("sqlite:{}", path.as_ref().display());
        let pool = SqlitePool::connect(&database_url).await?;

        // enable WAL for better concurrency
        sqlx::query("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")
            .execute(&pool)
            .await?;

        Self::run_migrations(&pool).await?;

        Ok(Self { pool })
    }

    async fn run_migrations(pool: &SqlitePool) -> Result<(), SqliteStorageError> {
        sqlx::migrate!("./migrations").run(pool).await?;
        Ok(())
    }

    fn serialize_reading(reading: &SensorReading) -> Result<String, SqliteStorageError> {
        Ok(serde_json::to_string(reading)?)
    }

    fn deserialize_reading(json: &str) -> Result<SensorReading, SqliteStorageError> {
        Ok(serde_json::from_str(json)?)
    }

    fn serialize_status(status: &DeviceStatus) -> Result<String, SqliteStorageError> {
        Ok(serde_json::to_string(status)?)
    }

    fn deserialize_status(json: &str) -> Result<DeviceStatus, SqliteStorageError> {
        Ok(serde_json::from_str(json)?)
    }
}

#[async_trait]
impl SensorReadingsStorage for SqliteStorage {
    type Error = SqliteStorageError;

    async fn store(&self, reading: SensorReading) -> Result<(), Self::Error> {
        let json = Self::serialize_reading(&reading)?;
        let id_str = reading.id.0.to_string();

        sqlx::query(
            "INSERT INTO sensor_readings (id, reading_json, state) VALUES (?, ?, 'pending')",
        )
        .bind(&id_str)
        .bind(&json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn store_batch(&self, readings: Vec<SensorReading>) -> Result<(), Self::Error> {
        if readings.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        for reading in readings {
            let json = Self::serialize_reading(&reading)?;
            let id_str = reading.id.0.to_string();

            sqlx::query(
                "INSERT INTO sensor_readings (id, reading_json, state) VALUES (?, ?, 'pending')",
            )
            .bind(&id_str)
            .bind(&json)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    async fn fetch_pending(&self) -> Result<Vec<SensorReading>, Self::Error> {
        let rows = sqlx::query("SELECT reading_json FROM sensor_readings WHERE state = 'pending'")
            .fetch_all(&self.pool)
            .await?;

        let mut readings = Vec::new();
        for row in rows {
            let json: String = row.try_get("reading_json")?;
            let reading = Self::deserialize_reading(&json)?;
            readings.push(reading);
        }

        Ok(readings)
    }

    async fn mark_uploaded(&self, ids: &[ReadingId]) -> Result<(), Self::Error> {
        if ids.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        for id in ids {
            let id_str = id.0.to_string();

            sqlx::query(
                "UPDATE sensor_readings SET state = 'uploaded', uploaded_at = CURRENT_TIMESTAMP WHERE id = ?",
            )
                .bind(&id_str)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;

        Ok(())
    }
}

#[async_trait]
impl DeviceStatusStorage for SqliteStorage {
    type Error = SqliteStorageError;

    async fn store(&self, status: DeviceStatus) -> Result<(), Self::Error> {
        let json = Self::serialize_status(&status)?;
        let id_str = status.id.0.to_string();

        sqlx::query(
            "INSERT INTO device_statuses (id, status_json, state) VALUES (?, ?, 'pending')",
        )
        .bind(&id_str)
        .bind(&json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn store_batch(&self, statuses: Vec<DeviceStatus>) -> Result<(), Self::Error> {
        if statuses.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        for status in statuses {
            let json = Self::serialize_status(&status)?;
            let id_str = status.id.0.to_string();

            sqlx::query(
                "INSERT INTO device_statuses (id, status_json, state) VALUES (?, ?, 'pending')",
            )
            .bind(&id_str)
            .bind(&json)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    async fn fetch_pending(&self) -> Result<Vec<DeviceStatus>, Self::Error> {
        let rows = sqlx::query("SELECT status_json FROM device_statuses WHERE state = 'pending'")
            .fetch_all(&self.pool)
            .await?;

        let mut statuses = Vec::new();
        for row in rows {
            let json: String = row.try_get("status_json")?;
            let status = Self::deserialize_status(&json)?;
            statuses.push(status);
        }

        Ok(statuses)
    }

    async fn mark_uploaded(&self, ids: &[StatusId]) -> Result<(), Self::Error> {
        if ids.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        for id in ids {
            let id_str = id.0.to_string();

            sqlx::query(
                "UPDATE device_statuses SET state = 'uploaded', uploaded_at = CURRENT_TIMESTAMP WHERE id = ?",
            )
                .bind(&id_str)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;

        Ok(())
    }
}

#[async_trait]
impl StorageMaintenance for SqliteStorage {
    type Error = SqliteStorageError;

    async fn get_stats(&self) -> Result<StorageStats, Self::Error> {
        let sensor_stats: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*) as total,
                COALESCE(SUM(CASE WHEN state = 'pending' THEN 1 ELSE 0 END), 0) as pending,
                COALESCE(SUM(CASE WHEN state = 'uploaded' THEN 1 ELSE 0 END), 0) as uploaded
             FROM sensor_readings
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let device_stats: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*) as total,
                COALESCE(SUM(CASE WHEN state = 'pending' THEN 1 ELSE 0 END), 0) as pending,
                COALESCE(SUM(CASE WHEN state = 'uploaded' THEN 1 ELSE 0 END), 0) as uploaded
             FROM device_statuses
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(StorageStats {
            sensor_readings_total: sensor_stats.0 as usize,
            sensor_readings_pending: sensor_stats.1 as usize,
            sensor_readings_uploaded: sensor_stats.2 as usize,
            device_statuses_total: device_stats.0 as usize,
            device_statuses_pending: device_stats.1 as usize,
            device_statuses_uploaded: device_stats.2 as usize,
        })
    }

    async fn cleanup_uploaded(&self, older_than: Duration) -> Result<CleanupStats, Self::Error> {
        if older_than == Duration::ZERO {
            let mut tx = self.pool.begin().await?;

            let sensor_deleted =
                sqlx::query("DELETE FROM sensor_readings WHERE state = 'uploaded'")
                    .execute(&mut *tx)
                    .await?
                    .rows_affected();

            let device_deleted =
                sqlx::query("DELETE FROM device_statuses WHERE state = 'uploaded'")
                    .execute(&mut *tx)
                    .await?
                    .rows_affected();

            tx.commit().await?;

            return Ok(CleanupStats {
                sensor_readings_deleted: sensor_deleted as usize,
                device_statuses_deleted: device_deleted as usize,
            });
        }

        let cutoff_days = older_than.as_secs_f64() / 86400.0;

        let mut tx = self.pool.begin().await?;

        let sensor_deleted = sqlx::query(
            "DELETE FROM sensor_readings WHERE state = 'uploaded' AND uploaded_at IS NOT NULL AND julianday('now') - julianday(uploaded_at) >= ?",
        )
            .bind(cutoff_days)
            .execute(&mut *tx)
            .await?
            .rows_affected();

        let device_deleted = sqlx::query(
            "DELETE FROM device_statuses WHERE state = 'uploaded' AND uploaded_at IS NOT NULL AND julianday('now') - julianday(uploaded_at) >= ?",
        )
            .bind(cutoff_days)
            .execute(&mut *tx)
            .await?
            .rows_affected();

        tx.commit().await?;

        Ok(CleanupStats {
            sensor_readings_deleted: sensor_deleted as usize,
            device_statuses_deleted: device_deleted as usize,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{SqliteStorage, SqliteStorageError};
    use crate::storage::{DeviceStatusStorage, SensorReadingsStorage, StorageMaintenance};
    use ersha_core::*;
    use std::time::Duration;
    use tempfile::NamedTempFile;
    use ulid::Ulid;

    fn dummy_reading() -> SensorReading {
        SensorReading {
            id: ReadingId(Ulid::new()),
            device_id: DeviceId(Ulid::new()),
            dispatcher_id: DispatcherId(Ulid::new()),
            metric: SensorMetric::SoilMoisture {
                value: Percentage(42),
            },
            location: H3Cell(123),
            confidence: Percentage(95),
            timestamp: jiff::Timestamp::now(),
            sensor_id: SensorId(Ulid::new()),
        }
    }

    fn dummy_status() -> DeviceStatus {
        DeviceStatus {
            id: StatusId(Ulid::new()),
            device_id: DeviceId(Ulid::new()),
            dispatcher_id: DispatcherId(Ulid::new()),
            battery_percent: Percentage(85),
            uptime_seconds: 3600,
            signal_rssi: -65,
            errors: Box::new([]),
            timestamp: jiff::Timestamp::now(),
            sensor_statuses: Box::new([]),
        }
    }

    #[tokio::test]
    async fn sqlite_sensor_reading_lifecycle() -> Result<(), SqliteStorageError> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let storage = SqliteStorage::new(db_path).await?;

        let reading = dummy_reading();
        let reading_id = reading.id;

        SensorReadingsStorage::store(&storage, reading).await?;

        let pending: Vec<SensorReading> = SensorReadingsStorage::fetch_pending(&storage).await?;
        assert_eq!(pending.len(), 1);

        SensorReadingsStorage::mark_uploaded(&storage, std::slice::from_ref(&reading_id)).await?;

        let pending: Vec<SensorReading> = SensorReadingsStorage::fetch_pending(&storage).await?;
        assert_eq!(pending.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_device_status_lifecycle() -> Result<(), SqliteStorageError> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let storage = SqliteStorage::new(db_path).await?;

        let status = dummy_status();
        let status_id = status.id;

        DeviceStatusStorage::store(&storage, status).await?;

        let pending: Vec<DeviceStatus> = DeviceStatusStorage::fetch_pending(&storage).await?;
        assert_eq!(pending.len(), 1);

        DeviceStatusStorage::mark_uploaded(&storage, std::slice::from_ref(&status_id)).await?;

        let pending: Vec<DeviceStatus> = DeviceStatusStorage::fetch_pending(&storage).await?;
        assert_eq!(pending.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_mixed_events() -> Result<(), SqliteStorageError> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let storage = SqliteStorage::new(db_path).await?;

        let reading = dummy_reading();
        let status = dummy_status();

        SensorReadingsStorage::store(&storage, reading).await?;
        DeviceStatusStorage::store(&storage, status).await?;

        let pending_readings: Vec<SensorReading> =
            SensorReadingsStorage::fetch_pending(&storage).await?;
        let pending_statuses: Vec<DeviceStatus> =
            DeviceStatusStorage::fetch_pending(&storage).await?;

        assert_eq!(pending_readings.len(), 1);
        assert_eq!(pending_statuses.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_persistence_across_instances() -> Result<(), SqliteStorageError> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        {
            let storage = SqliteStorage::new(db_path).await?;
            let reading = dummy_reading();
            SensorReadingsStorage::store(&storage, reading).await?;
        }

        {
            let storage = SqliteStorage::new(db_path).await?;
            let pending: Vec<SensorReading> =
                SensorReadingsStorage::fetch_pending(&storage).await?;
            assert_eq!(pending.len(), 1);
        }

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_batch_mark_uploaded() -> Result<(), SqliteStorageError> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let storage = SqliteStorage::new(db_path).await?;

        // create multiple readings
        let reading1 = dummy_reading();
        let reading2 = dummy_reading();
        let reading3 = dummy_reading();

        let id1 = reading1.id;
        let id2 = reading2.id;
        let id3 = reading3.id;

        SensorReadingsStorage::store(&storage, reading1).await?;
        SensorReadingsStorage::store(&storage, reading2).await?;
        SensorReadingsStorage::store(&storage, reading3).await?;

        SensorReadingsStorage::mark_uploaded(&storage, &[id1, id2][..]).await?;

        let pending: Vec<SensorReading> = SensorReadingsStorage::fetch_pending(&storage).await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, id3);

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_empty_ids_handling() -> Result<(), SqliteStorageError> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let storage = SqliteStorage::new(db_path).await?;

        // should not panic with empty slices
        let empty_readings: Vec<ReadingId> = Vec::new();
        let empty_statuses: Vec<StatusId> = Vec::new();
        SensorReadingsStorage::mark_uploaded(&storage, empty_readings.as_slice()).await?;
        DeviceStatusStorage::mark_uploaded(&storage, empty_statuses.as_slice()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_batch_sensor_readings() -> Result<(), SqliteStorageError> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let storage = SqliteStorage::new(db_path).await?;

        let readings = vec![dummy_reading(), dummy_reading(), dummy_reading()];

        SensorReadingsStorage::store_batch(&storage, readings).await?;

        let pending: Vec<SensorReading> = SensorReadingsStorage::fetch_pending(&storage).await?;
        assert_eq!(pending.len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_batch_device_statuses() -> Result<(), SqliteStorageError> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let storage = SqliteStorage::new(db_path).await?;

        let statuses = vec![dummy_status(), dummy_status()];

        DeviceStatusStorage::store_batch(&storage, statuses).await?;

        let pending: Vec<DeviceStatus> = DeviceStatusStorage::fetch_pending(&storage).await?;
        assert_eq!(pending.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_empty_batch() -> Result<(), SqliteStorageError> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let storage = SqliteStorage::new(db_path).await?;

        // should not panic with empty batches
        SensorReadingsStorage::store_batch(&storage, Vec::new()).await?;
        DeviceStatusStorage::store_batch(&storage, Vec::new()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_get_stats() -> Result<(), SqliteStorageError> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let storage = SqliteStorage::new(db_path).await?;

        let stats = storage.get_stats().await?;
        assert_eq!(stats.sensor_readings_total, 0);
        assert_eq!(stats.device_statuses_total, 0);

        SensorReadingsStorage::store(&storage, dummy_reading()).await?;
        SensorReadingsStorage::store(&storage, dummy_reading()).await?;
        DeviceStatusStorage::store(&storage, dummy_status()).await?;

        let stats = storage.get_stats().await?;
        assert_eq!(stats.sensor_readings_total, 2);
        assert_eq!(stats.sensor_readings_pending, 2);
        assert_eq!(stats.sensor_readings_uploaded, 0);
        assert_eq!(stats.device_statuses_total, 1);
        assert_eq!(stats.device_statuses_pending, 1);
        assert_eq!(stats.device_statuses_uploaded, 0);

        let reading = dummy_reading();
        let reading_id = reading.id;
        SensorReadingsStorage::store(&storage, reading).await?;
        SensorReadingsStorage::mark_uploaded(&storage, std::slice::from_ref(&reading_id)).await?;

        let stats = storage.get_stats().await?;
        assert_eq!(stats.sensor_readings_total, 3);
        assert_eq!(stats.sensor_readings_pending, 2);
        assert_eq!(stats.sensor_readings_uploaded, 1);

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_cleanup_uploaded() -> Result<(), SqliteStorageError> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let storage = SqliteStorage::new(db_path).await?;

        let reading1 = dummy_reading();
        let reading2 = dummy_reading();
        let reading3 = dummy_reading();

        let id1 = reading1.id;
        let id2 = reading2.id;

        SensorReadingsStorage::store(&storage, reading1).await?;
        SensorReadingsStorage::store(&storage, reading2).await?;
        SensorReadingsStorage::store(&storage, reading3).await?;
        DeviceStatusStorage::store(&storage, dummy_status()).await?;

        SensorReadingsStorage::mark_uploaded(&storage, &[id1, id2][..]).await?;

        // before cleanup
        let stats_before = storage.get_stats().await?;
        assert_eq!(stats_before.sensor_readings_total, 3);
        assert_eq!(stats_before.sensor_readings_uploaded, 2);

        // cleanup ALL uploaded items (duration::ZERO means delete all uploaded)
        let cleanup = storage.cleanup_uploaded(Duration::ZERO).await?;
        assert_eq!(cleanup.sensor_readings_deleted, 2);
        assert_eq!(cleanup.device_statuses_deleted, 0);

        // after cleanup
        let stats_after = storage.get_stats().await?;
        assert_eq!(stats_after.sensor_readings_total, 1);
        assert_eq!(stats_after.sensor_readings_pending, 1);
        assert_eq!(stats_after.sensor_readings_uploaded, 0);

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_time_based_cleanup() -> Result<(), SqliteStorageError> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let storage = SqliteStorage::new(db_path).await?;

        let reading1 = dummy_reading();
        let id1 = reading1.id;

        SensorReadingsStorage::store(&storage, reading1).await?;
        SensorReadingsStorage::mark_uploaded(&storage, std::slice::from_ref(&id1)).await?;

        // Wait for 2 seconds so this reading becomes \"old\"
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Create and mark another one as uploaded (recent)
        let reading2 = dummy_reading();
        let id2 = reading2.id;

        SensorReadingsStorage::store(&storage, reading2).await?;
        SensorReadingsStorage::mark_uploaded(&storage, std::slice::from_ref(&id2)).await?;

        // Cleanup items older than 1.5 seconds, should delete only the first one
        let cleanup = storage
            .cleanup_uploaded(Duration::from_millis(1500))
            .await?;
        assert_eq!(cleanup.sensor_readings_deleted, 1); // Only the old one

        let stats = storage.get_stats().await?;
        assert_eq!(stats.sensor_readings_total, 1); // The recent one remains
        assert_eq!(stats.sensor_readings_uploaded, 1); // Still marked as uploaded

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_zero_duration_cleanup() -> Result<(), SqliteStorageError> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let storage = SqliteStorage::new(db_path).await?;

        let reading = dummy_reading();
        let reading_id = reading.id;
        SensorReadingsStorage::store(&storage, reading).await?;
        SensorReadingsStorage::mark_uploaded(&storage, std::slice::from_ref(&reading_id)).await?;

        // zero duration should delete ALL uploaded items
        let cleanup = storage.cleanup_uploaded(Duration::ZERO).await?;
        assert_eq!(cleanup.sensor_readings_deleted, 1);
        assert_eq!(cleanup.device_statuses_deleted, 0);

        // data should be deleted
        let stats = storage.get_stats().await?;
        assert_eq!(stats.sensor_readings_total, 0);
        assert_eq!(stats.sensor_readings_uploaded, 0);

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_cleanup_only_affects_uploaded() -> Result<(), SqliteStorageError> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        let storage = SqliteStorage::new(db_path).await?;

        // create mixed: 2 uploaded, 1 pending, 1 device status uploaded
        let reading1 = dummy_reading();
        let reading2 = dummy_reading();
        let reading3 = dummy_reading();
        let status1 = dummy_status();

        let id1 = reading1.id;
        let id2 = reading2.id;
        let status_id1 = status1.id;

        SensorReadingsStorage::store(&storage, reading1).await?;
        SensorReadingsStorage::store(&storage, reading2).await?;
        SensorReadingsStorage::store(&storage, reading3).await?;
        DeviceStatusStorage::store(&storage, status1).await?;

        SensorReadingsStorage::mark_uploaded(&storage, &[id1, id2][..]).await?;
        DeviceStatusStorage::mark_uploaded(&storage, std::slice::from_ref(&status_id1)).await?;

        let cleanup = storage.cleanup_uploaded(Duration::ZERO).await?;
        assert_eq!(cleanup.sensor_readings_deleted, 2);
        assert_eq!(cleanup.device_statuses_deleted, 1);

        // verify pending items remain
        let stats = storage.get_stats().await?;
        assert_eq!(stats.sensor_readings_total, 1);
        assert_eq!(stats.sensor_readings_pending, 1);
        assert_eq!(stats.sensor_readings_uploaded, 0);
        assert_eq!(stats.device_statuses_total, 0);
        assert_eq!(stats.device_statuses_pending, 0);
        assert_eq!(stats.device_statuses_uploaded, 0);

        Ok(())
    }
}
