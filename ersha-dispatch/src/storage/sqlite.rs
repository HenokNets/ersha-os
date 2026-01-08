use async_trait::async_trait;
use rusqlite::{params, Connection, Row};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

use ersha_core::{DeviceStatus, ReadingId, SensorReading, StatusId};
use crate::storage::{Storage, StorageError};

/// SQLite backed storage implementation.
/// stores events as JSON blobs in two tables.
#[derive(Clone)]
pub struct SqliteStorage {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteStorage {
    /// opens or creates a SQLite database at the given path.
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let conn = Connection::open(path)
            .map_err(|e| StorageError::Internal(format!("Failed to open SQLite DB: {}", e)))?;
        
        // initialize database schema
        Self::init_schema(&conn)?;
        
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
    
    fn init_schema(conn: &Connection) -> Result<(), StorageError> {
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id TEXT PRIMARY KEY,
                reading_json TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('pending', 'uploaded')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS device_statuses (
                id TEXT PRIMARY KEY,
                status_json TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('pending', 'uploaded')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_sensor_readings_state 
            ON sensor_readings(state);
            
            CREATE INDEX IF NOT EXISTS idx_device_statuses_state 
            ON device_statuses(state);
            "#,
        )
        .map_err(|e| StorageError::Internal(format!("Failed to create tables: {}", e)))?;
        
        Ok(())
    }
    
    /// serialize SensorReading to JSON string
    fn serialize_reading(reading: &SensorReading) -> Result<String, StorageError> {
        serde_json::to_string(reading)
            .map_err(|e| StorageError::Internal(format!("JSON serialization failed: {}", e)))
    }
    
    /// deserialize JSON string to SensorReading
    fn deserialize_reading(json: &str) -> Result<SensorReading, StorageError> {
        serde_json::from_str(json)
            .map_err(|e| StorageError::Internal(format!("JSON deserialization failed: {}", e)))
    }
    
    /// serialize DeviceStatus to JSON string
    fn serialize_status(status: &DeviceStatus) -> Result<String, StorageError> {
        serde_json::to_string(status)
            .map_err(|e| StorageError::Internal(format!("JSON serialization failed: {}", e)))
    }
    
    /// deserialize JSON string to DeviceStatus
    fn deserialize_status(json: &str) -> Result<DeviceStatus, StorageError> {
        serde_json::from_str(json)
            .map_err(|e| StorageError::Internal(format!("JSON deserialization failed: {}", e)))
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    async fn store_sensor_reading(&self, reading: SensorReading) -> Result<(), StorageError> {
        let json = Self::serialize_reading(&reading)?;
        let id_str = reading.id.0.to_string();
        
        let conn: tokio::sync::MutexGuard<'_, Connection> = self.conn.lock().await;
        
        conn.execute(
            "INSERT INTO sensor_readings (id, reading_json, state) VALUES (?, ?, 'pending')",
            params![id_str, json],
        )
        .map_err(|e| StorageError::Internal(format!("Insert failed: {}", e)))?;
        
        Ok(())
    }
    
    async fn store_device_status(&self, status: DeviceStatus) -> Result<(), StorageError> {
        let json = Self::serialize_status(&status)?;
        let id_str = status.id.0.to_string();
        
        let conn: tokio::sync::MutexGuard<'_, Connection> = self.conn.lock().await;
        
        conn.execute(
            "INSERT INTO device_statuses (id, status_json, state) VALUES (?, ?, 'pending')",
            params![id_str, json],
        )
        .map_err(|e| StorageError::Internal(format!("Insert failed: {}", e)))?;
        
        Ok(())
    }
    
    async fn fetch_pending_sensor_readings(&self) -> Result<Vec<SensorReading>, StorageError> {
        let conn: tokio::sync::MutexGuard<'_, Connection> = self.conn.lock().await;
        
        let mut stmt = conn
            .prepare("SELECT reading_json FROM sensor_readings WHERE state = 'pending'")
            .map_err(|e| StorageError::Internal(format!("Prepare failed: {}", e)))?;
        
        let rows = stmt
            .query_map([], |row: &Row| {
                let json: String = row.get(0)?;
                Ok(json)
            })
            .map_err(|e| StorageError::Internal(format!("Query failed: {}", e)))?;
        
        let mut readings = Vec::new();
        for row_result in rows {
            let json: String = row_result
                .map_err(|e| StorageError::Internal(format!("Row error: {}", e)))?;
            let reading = Self::deserialize_reading(&json)?;
            readings.push(reading);
        }
        
        Ok(readings)
    }
    
    async fn fetch_pending_device_statuses(&self) -> Result<Vec<DeviceStatus>, StorageError> {
        let conn: tokio::sync::MutexGuard<'_, Connection> = self.conn.lock().await;
        
        let mut stmt = conn
            .prepare("SELECT status_json FROM device_statuses WHERE state = 'pending'")
            .map_err(|e| StorageError::Internal(format!("Prepare failed: {}", e)))?;
        
        let rows = stmt
            .query_map([], |row: &Row| {
                let json: String = row.get(0)?;
                Ok(json)
            })
            .map_err(|e| StorageError::Internal(format!("Query failed: {}", e)))?;
        
        let mut statuses = Vec::new();
        for row_result in rows {
            let json: String = row_result
                .map_err(|e| StorageError::Internal(format!("Row error: {}", e)))?;
            let status = Self::deserialize_status(&json)?;
            statuses.push(status);
        }
        
        Ok(statuses)
    }
    
    async fn mark_sensor_readings_uploaded(&self, ids: &[ReadingId]) -> Result<(), StorageError> {
        if ids.is_empty() {
            return Ok(());
        }
        
        let conn: tokio::sync::MutexGuard<'_, Connection> = self.conn.lock().await;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| StorageError::Internal(format!("Transaction failed: {}", e)))?;
        
        for id in ids {
            let id_str = id.0.to_string();
            tx.execute(
                "UPDATE sensor_readings SET state = 'uploaded' WHERE id = ?",
                params![id_str],
            )
            .map_err(|e| StorageError::Internal(format!("Update failed for {}: {}", id_str, e)))?;
        }
        
        tx.commit()
            .map_err(|e| StorageError::Internal(format!("Commit failed: {}", e)))?;
        
        Ok(())
    }
    
    async fn mark_device_statuses_uploaded(&self, ids: &[StatusId]) -> Result<(), StorageError> {
        if ids.is_empty() {
            return Ok(());
        }
        
        let conn: tokio::sync::MutexGuard<'_, Connection> = self.conn.lock().await;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| StorageError::Internal(format!("Transaction failed: {}", e)))?;
        
        for id in ids {
            let id_str = id.0.to_string();
            tx.execute(
                "UPDATE device_statuses SET state = 'uploaded' WHERE id = ?",
                params![id_str],
            )
            .map_err(|e| StorageError::Internal(format!("Update failed for {}: {}", id_str, e)))?;
        }
        
        tx.commit()
            .map_err(|e| StorageError::Internal(format!("Commit failed: {}", e)))?;
        
        Ok(())
    }
}
