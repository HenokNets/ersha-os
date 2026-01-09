use ersha_core::*;
use ersha_dispatch::storage::Storage;
use ersha_dispatch::storage::memory::{MemoryStorage, MemoryStorageError};
use ersha_dispatch::storage::sqlite::{SqliteStorage, SqliteStorageError};
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

// Memory storage tests (original)
#[tokio::test]
async fn memory_sensor_reading_lifecycle() -> Result<(), MemoryStorageError> {
    let storage: MemoryStorage = MemoryStorage::default();

    let reading = dummy_reading();
    let reading_id = reading.id;

    storage.store_sensor_reading(reading).await?;

    let pending = storage.fetch_pending_sensor_readings().await?;
    assert_eq!(pending.len(), 1);

    storage.mark_sensor_readings_uploaded(&[reading_id]).await?;

    let pending = storage.fetch_pending_sensor_readings().await?;
    assert_eq!(pending.len(), 0);

    Ok(())
}

#[tokio::test]
async fn memory_device_status_lifecycle() -> Result<(), MemoryStorageError> {
    let storage: MemoryStorage = MemoryStorage::default();

    let status = dummy_status();
    let status_id = status.id;

    storage.store_device_status(status).await?;

    let pending = storage.fetch_pending_device_statuses().await?;
    assert_eq!(pending.len(), 1);

    storage.mark_device_statuses_uploaded(&[status_id]).await?;

    let pending = storage.fetch_pending_device_statuses().await?;
    assert_eq!(pending.len(), 0);

    Ok(())
}

#[tokio::test]
async fn memory_mixed_events() -> Result<(), MemoryStorageError> {
    let storage: MemoryStorage = MemoryStorage::default();

    let reading = dummy_reading();
    let status = dummy_status();

    storage.store_sensor_reading(reading).await?;
    storage.store_device_status(status).await?;

    let pending_readings = storage.fetch_pending_sensor_readings().await?;
    let pending_statuses = storage.fetch_pending_device_statuses().await?;

    assert_eq!(pending_readings.len(), 1);
    assert_eq!(pending_statuses.len(), 1);

    Ok(())
}

// SQLite storage tests
#[tokio::test]
async fn sqlite_sensor_reading_lifecycle() -> Result<(), SqliteStorageError> {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path();

    let storage = SqliteStorage::new(db_path).await?;

    let reading = dummy_reading();
    let reading_id = reading.id;

    storage.store_sensor_reading(reading).await?;

    let pending = storage.fetch_pending_sensor_readings().await?;
    assert_eq!(pending.len(), 1);

    storage.mark_sensor_readings_uploaded(&[reading_id]).await?;

    let pending = storage.fetch_pending_sensor_readings().await?;
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

    storage.store_device_status(status).await?;

    let pending = storage.fetch_pending_device_statuses().await?;
    assert_eq!(pending.len(), 1);

    storage.mark_device_statuses_uploaded(&[status_id]).await?;

    let pending = storage.fetch_pending_device_statuses().await?;
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

    storage.store_sensor_reading(reading).await?;
    storage.store_device_status(status).await?;

    let pending_readings = storage.fetch_pending_sensor_readings().await?;
    let pending_statuses = storage.fetch_pending_device_statuses().await?;

    assert_eq!(pending_readings.len(), 1);
    assert_eq!(pending_statuses.len(), 1);

    Ok(())
}

#[tokio::test]
async fn sqlite_persistence_across_instances() -> Result<(), SqliteStorageError> {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path();

    // First instance
    {
        let storage = SqliteStorage::new(db_path).await?;
        let reading = dummy_reading();
        storage.store_sensor_reading(reading).await?;
    }

    // New instance, same file
    {
        let storage = SqliteStorage::new(db_path).await?;
        let pending = storage.fetch_pending_sensor_readings().await?;
        assert_eq!(pending.len(), 1);
    }

    Ok(())
}

#[tokio::test]
async fn sqlite_batch_mark_uploaded() -> Result<(), SqliteStorageError> {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path();

    let storage = SqliteStorage::new(db_path).await?;

    // Create multiple readings
    let reading1 = dummy_reading();
    let reading2 = dummy_reading();
    let reading3 = dummy_reading();

    let id1 = reading1.id;
    let id2 = reading2.id;
    let id3 = reading3.id;

    storage.store_sensor_reading(reading1).await?;
    storage.store_sensor_reading(reading2).await?;
    storage.store_sensor_reading(reading3).await?;

    // Mark two as uploaded
    storage.mark_sensor_readings_uploaded(&[id1, id2]).await?;

    let pending = storage.fetch_pending_sensor_readings().await?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, id3);

    Ok(())
}

#[tokio::test]
async fn sqlite_empty_ids_handling() -> Result<(), SqliteStorageError> {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path();

    let storage = SqliteStorage::new(db_path).await?;

    // Should not panic with empty slices
    storage.mark_sensor_readings_uploaded(&[]).await?;
    storage.mark_device_statuses_uploaded(&[]).await?;

    Ok(())
}

// Batch operation tests
#[tokio::test]
async fn memory_batch_sensor_readings() -> Result<(), MemoryStorageError> {
    let storage: MemoryStorage = MemoryStorage::default();

    let readings = vec![dummy_reading(), dummy_reading(), dummy_reading()];

    storage.store_sensor_readings_batch(readings).await?;

    let pending = storage.fetch_pending_sensor_readings().await?;
    assert_eq!(pending.len(), 3);

    Ok(())
}

#[tokio::test]
async fn memory_batch_device_statuses() -> Result<(), MemoryStorageError> {
    let storage: MemoryStorage = MemoryStorage::default();

    let statuses = vec![dummy_status(), dummy_status()];

    storage.store_device_statuses_batch(statuses).await?;

    let pending = storage.fetch_pending_device_statuses().await?;
    assert_eq!(pending.len(), 2);

    Ok(())
}

#[tokio::test]
async fn sqlite_batch_sensor_readings() -> Result<(), SqliteStorageError> {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path();

    let storage = SqliteStorage::new(db_path).await?;

    let readings = vec![dummy_reading(), dummy_reading(), dummy_reading()];

    storage.store_sensor_readings_batch(readings).await?;

    let pending = storage.fetch_pending_sensor_readings().await?;
    assert_eq!(pending.len(), 3);

    Ok(())
}

#[tokio::test]
async fn sqlite_batch_device_statuses() -> Result<(), SqliteStorageError> {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path();

    let storage = SqliteStorage::new(db_path).await?;

    let statuses = vec![dummy_status(), dummy_status()];

    storage.store_device_statuses_batch(statuses).await?;

    let pending = storage.fetch_pending_device_statuses().await?;
    assert_eq!(pending.len(), 2);

    Ok(())
}

#[tokio::test]
async fn sqlite_empty_batch() -> Result<(), SqliteStorageError> {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path();

    let storage = SqliteStorage::new(db_path).await?;

    // Should not panic with empty batches
    storage.store_sensor_readings_batch(vec![]).await?;
    storage.store_device_statuses_batch(vec![]).await?;

    Ok(())
}

// Data management tests
#[tokio::test]
async fn memory_get_stats() -> Result<(), MemoryStorageError> {
    let storage: MemoryStorage = MemoryStorage::default();

    // Initial stats should be zero
    let stats = storage.get_stats().await?;
    assert_eq!(stats.sensor_readings_total, 0);
    assert_eq!(stats.device_statuses_total, 0);

    // Add some data
    storage.store_sensor_reading(dummy_reading()).await?;
    storage.store_sensor_reading(dummy_reading()).await?;
    storage.store_device_status(dummy_status()).await?;

    let stats = storage.get_stats().await?;
    assert_eq!(stats.sensor_readings_total, 2);
    assert_eq!(stats.sensor_readings_pending, 2);
    assert_eq!(stats.sensor_readings_uploaded, 0);
    assert_eq!(stats.device_statuses_total, 1);
    assert_eq!(stats.device_statuses_pending, 1);
    assert_eq!(stats.device_statuses_uploaded, 0);

    // Mark one as uploaded
    let reading = dummy_reading();
    let reading_id = reading.id;
    storage.store_sensor_reading(reading).await?;
    storage.mark_sensor_readings_uploaded(&[reading_id]).await?;

    let stats = storage.get_stats().await?;
    assert_eq!(stats.sensor_readings_total, 3);
    assert_eq!(stats.sensor_readings_pending, 2);
    assert_eq!(stats.sensor_readings_uploaded, 1);

    Ok(())
}

#[tokio::test]
async fn memory_cleanup_uploaded() -> Result<(), MemoryStorageError> {
    let storage: MemoryStorage = MemoryStorage::default();

    // Create 3 readings, mark 2 as uploaded
    let reading1 = dummy_reading();
    let reading2 = dummy_reading();
    let reading3 = dummy_reading();

    let id1 = reading1.id;
    let id2 = reading2.id;

    storage.store_sensor_reading(reading1).await?;
    storage.store_sensor_reading(reading2).await?;
    storage.store_sensor_reading(reading3).await?;
    storage.store_device_status(dummy_status()).await?;

    storage.mark_sensor_readings_uploaded(&[id1, id2]).await?;

    // Before cleanup
    let stats_before = storage.get_stats().await?;
    assert_eq!(stats_before.sensor_readings_total, 3);
    assert_eq!(stats_before.sensor_readings_uploaded, 2);

    // Cleanup uploaded (memory ignores duration, deletes all uploaded)
    let cleanup = storage.cleanup_uploaded(Duration::ZERO).await?;
    assert_eq!(cleanup.sensor_readings_deleted, 2);
    assert_eq!(cleanup.device_statuses_deleted, 0); // Not uploaded

    // After cleanup
    let stats_after = storage.get_stats().await?;
    assert_eq!(stats_after.sensor_readings_total, 1); // Only pending remains
    assert_eq!(stats_after.sensor_readings_pending, 1);
    assert_eq!(stats_after.sensor_readings_uploaded, 0);

    Ok(())
}

#[tokio::test]
async fn sqlite_get_stats() -> Result<(), SqliteStorageError> {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path();

    let storage = SqliteStorage::new(db_path).await?;

    // Initial stats should be zero
    let stats = storage.get_stats().await?;
    assert_eq!(stats.sensor_readings_total, 0);
    assert_eq!(stats.device_statuses_total, 0);

    // Add some data
    storage.store_sensor_reading(dummy_reading()).await?;
    storage.store_sensor_reading(dummy_reading()).await?;
    storage.store_device_status(dummy_status()).await?;

    let stats = storage.get_stats().await?;
    assert_eq!(stats.sensor_readings_total, 2);
    assert_eq!(stats.sensor_readings_pending, 2);
    assert_eq!(stats.sensor_readings_uploaded, 0);
    assert_eq!(stats.device_statuses_total, 1);
    assert_eq!(stats.device_statuses_pending, 1);
    assert_eq!(stats.device_statuses_uploaded, 0);

    // Mark one as uploaded
    let reading = dummy_reading();
    let reading_id = reading.id;
    storage.store_sensor_reading(reading).await?;
    storage.mark_sensor_readings_uploaded(&[reading_id]).await?;

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

    // Create 3 readings, mark 2 as uploaded
    let reading1 = dummy_reading();
    let reading2 = dummy_reading();
    let reading3 = dummy_reading();

    let id1 = reading1.id;
    let id2 = reading2.id;

    storage.store_sensor_reading(reading1).await?;
    storage.store_sensor_reading(reading2).await?;
    storage.store_sensor_reading(reading3).await?;
    storage.store_device_status(dummy_status()).await?;

    storage.mark_sensor_readings_uploaded(&[id1, id2]).await?;

    // Before cleanup
    let stats_before = storage.get_stats().await?;
    assert_eq!(stats_before.sensor_readings_total, 3);
    assert_eq!(stats_before.sensor_readings_uploaded, 2);

    // Cleanup ALL uploaded items (Duration::ZERO means delete all uploaded)
    let cleanup = storage.cleanup_uploaded(Duration::ZERO).await?;
    assert_eq!(cleanup.sensor_readings_deleted, 2);
    assert_eq!(cleanup.device_statuses_deleted, 0); // Not uploaded

    // After cleanup
    let stats_after = storage.get_stats().await?;
    assert_eq!(stats_after.sensor_readings_total, 1); // Only pending remains
    assert_eq!(stats_after.sensor_readings_pending, 1);
    assert_eq!(stats_after.sensor_readings_uploaded, 0);

    Ok(())
}

#[tokio::test]
async fn sqlite_time_based_cleanup() -> Result<(), SqliteStorageError> {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path();

    let storage = SqliteStorage::new(db_path).await?;

    // Create and mark as uploaded
    let reading1 = dummy_reading();
    let id1 = reading1.id;

    storage.store_sensor_reading(reading1).await?;
    storage.mark_sensor_readings_uploaded(&[id1]).await?;

    // Wait for 2 seconds so this reading becomes "old"
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Create and mark another one as uploaded (recent)
    let reading2 = dummy_reading();
    let id2 = reading2.id;

    storage.store_sensor_reading(reading2).await?;
    storage.mark_sensor_readings_uploaded(&[id2]).await?;

    // Cleanup items older than 1.5 seconds - should delete only the first one
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

    // Add some uploaded data
    let reading = dummy_reading();
    let reading_id = reading.id;
    storage.store_sensor_reading(reading).await?;
    storage.mark_sensor_readings_uploaded(&[reading_id]).await?;

    // Zero duration should delete ALL uploaded items
    let cleanup = storage.cleanup_uploaded(Duration::ZERO).await?;
    assert_eq!(cleanup.sensor_readings_deleted, 1);
    assert_eq!(cleanup.device_statuses_deleted, 0);

    // Data should be deleted
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

    // Create mixed: 2 uploaded, 1 pending, 1 device status uploaded
    let reading1 = dummy_reading();
    let reading2 = dummy_reading();
    let reading3 = dummy_reading();
    let status1 = dummy_status();

    let id1 = reading1.id;
    let id2 = reading2.id;
    let status_id1 = status1.id;

    storage.store_sensor_reading(reading1).await?;
    storage.store_sensor_reading(reading2).await?;
    storage.store_sensor_reading(reading3).await?;
    storage.store_device_status(status1).await?;

    // Mark some as uploaded
    storage.mark_sensor_readings_uploaded(&[id1, id2]).await?;
    storage.mark_device_statuses_uploaded(&[status_id1]).await?;

    // Cleanup should only delete uploaded items
    let cleanup = storage.cleanup_uploaded(Duration::ZERO).await?;
    assert_eq!(cleanup.sensor_readings_deleted, 2);
    assert_eq!(cleanup.device_statuses_deleted, 1);

    // Verify pending items remain
    let stats = storage.get_stats().await?;
    assert_eq!(stats.sensor_readings_total, 1);
    assert_eq!(stats.sensor_readings_pending, 1);
    assert_eq!(stats.sensor_readings_uploaded, 0);
    assert_eq!(stats.device_statuses_total, 0);
    assert_eq!(stats.device_statuses_pending, 0);
    assert_eq!(stats.device_statuses_uploaded, 0);

    Ok(())
}

#[tokio::test]
async fn memory_zero_duration_cleanup() -> Result<(), MemoryStorageError> {
    let storage: MemoryStorage = MemoryStorage::default();

    // Add some uploaded data
    let reading = dummy_reading();
    let reading_id = reading.id;
    storage.store_sensor_reading(reading).await?;
    storage.mark_sensor_readings_uploaded(&[reading_id]).await?;

    // Memory backend should also delete all uploaded with ZERO duration
    let cleanup = storage.cleanup_uploaded(Duration::ZERO).await?;
    assert_eq!(cleanup.sensor_readings_deleted, 1);
    assert_eq!(cleanup.device_statuses_deleted, 0);

    // Verify data is deleted
    let stats = storage.get_stats().await?;
    assert_eq!(stats.sensor_readings_total, 0);
    assert_eq!(stats.sensor_readings_uploaded, 0);

    Ok(())
}
