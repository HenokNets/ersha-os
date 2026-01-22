use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ersha_core::{
    DeviceError, DeviceId, DeviceStatus, DispatcherId, H3Cell, Percentage, ReadingId, SensorId,
    SensorMetric, SensorReading, SensorState, SensorStatus, StatusId,
};
use ordered_float::NotNan;
use rand::Rng;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;
use ulid::Ulid;

use super::{EdgeData, EdgeReceiver};

/// Mock edge receiver that generates fake sensor data.
pub struct MockEdgeReceiver {
    /// Dispatcher ID to use in generated data.
    dispatcher_id: DispatcherId,
    /// Location to use in generated data.
    location: H3Cell,
    /// Interval between sensor readings.
    reading_interval: Duration,
    /// Interval between status updates.
    status_interval: Duration,
    /// Number of simulated devices.
    device_count: usize,
}

impl MockEdgeReceiver {
    pub fn new(
        dispatcher_id: DispatcherId,
        location: H3Cell,
        reading_interval_secs: u64,
        status_interval_secs: u64,
        device_count: usize,
    ) -> Self {
        Self {
            dispatcher_id,
            location,
            reading_interval: Duration::from_secs(reading_interval_secs),
            status_interval: Duration::from_secs(status_interval_secs),
            device_count,
        }
    }

    fn generate_devices(&self) -> Vec<MockDevice> {
        (0..self.device_count).map(|_| MockDevice::new()).collect()
    }
}

/// A simulated device with stable IDs.
struct MockDevice {
    device_id: DeviceId,
    sensor_ids: Vec<SensorId>,
}

impl MockDevice {
    fn new() -> Self {
        Self {
            device_id: DeviceId(Ulid::new()),
            sensor_ids: vec![
                SensorId(Ulid::new()), // SoilMoisture
                SensorId(Ulid::new()), // SoilTemp
                SensorId(Ulid::new()), // AirTemp
                SensorId(Ulid::new()), // Humidity
                SensorId(Ulid::new()), // Rainfall
            ],
        }
    }

    fn generate_reading(&self, dispatcher_id: DispatcherId, location: H3Cell) -> SensorReading {
        let mut rng = rand::rng();
        let sensor_idx = rng.random_range(0..self.sensor_ids.len());
        let sensor_id = self.sensor_ids[sensor_idx];

        let metric = match sensor_idx {
            0 => SensorMetric::SoilMoisture {
                value: Percentage(rng.random_range(20..80)),
            },
            1 => SensorMetric::SoilTemp {
                value: NotNan::new(rng.random_range(15.0..35.0)).unwrap(),
            },
            2 => SensorMetric::AirTemp {
                value: NotNan::new(rng.random_range(10.0..40.0)).unwrap(),
            },
            3 => SensorMetric::Humidity {
                value: Percentage(rng.random_range(30..90)),
            },
            _ => SensorMetric::Rainfall {
                value: NotNan::new(rng.random_range(0.0..50.0)).unwrap(),
            },
        };

        SensorReading {
            id: ReadingId(Ulid::new()),
            device_id: self.device_id,
            dispatcher_id,
            metric,
            location,
            confidence: Percentage(rng.random_range(85..100)),
            timestamp: jiff::Timestamp::now(),
            sensor_id,
        }
    }

    fn generate_status(&self, dispatcher_id: DispatcherId) -> DeviceStatus {
        let mut rng = rand::rng();

        let sensor_statuses: Vec<SensorStatus> = self
            .sensor_ids
            .iter()
            .map(|&sensor_id| SensorStatus {
                sensor_id,
                state: if rng.random_ratio(95, 100) {
                    SensorState::Active
                } else {
                    SensorState::Faulty
                },
                last_reading: Some(jiff::Timestamp::now()),
            })
            .collect();

        let errors: Vec<DeviceError> = if rng.random_ratio(5, 100) {
            vec![DeviceError {
                code: ersha_core::DeviceErrorCode::LowBattery,
                message: Some("Battery below 20%".into()),
            }]
        } else {
            vec![]
        };

        DeviceStatus {
            id: StatusId(Ulid::new()),
            device_id: self.device_id,
            dispatcher_id,
            battery_percent: Percentage(rng.random_range(20..100)),
            uptime_seconds: rng.random_range(3600..86400),
            signal_rssi: rng.random_range(-80..-30),
            errors: errors.into_boxed_slice(),
            timestamp: jiff::Timestamp::now(),
            sensor_statuses: sensor_statuses.into_boxed_slice(),
        }
    }
}

#[async_trait]
impl EdgeReceiver for MockEdgeReceiver {
    type Error = std::convert::Infallible;

    async fn start(
        &self,
        cancel: CancellationToken,
    ) -> Result<mpsc::Receiver<EdgeData>, Self::Error> {
        let (tx, rx) = mpsc::channel(100);

        let devices = Arc::new(self.generate_devices());
        let dispatcher_id = self.dispatcher_id;
        let location = self.location;
        let reading_interval = self.reading_interval;
        let status_interval = self.status_interval;

        info!(
            device_count = devices.len(),
            reading_interval_secs = reading_interval.as_secs(),
            status_interval_secs = status_interval.as_secs(),
            "Starting mock edge receiver"
        );

        // Spawn reading generator task
        let tx_readings = tx.clone();
        let cancel_readings = cancel.clone();
        let devices_for_readings = Arc::clone(&devices);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(reading_interval);

            loop {
                tokio::select! {
                    _ = cancel_readings.cancelled() => {
                        info!("Mock reading generator shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        for device in devices_for_readings.iter() {
                            let reading = device.generate_reading(dispatcher_id, location);
                            if tx_readings.send(EdgeData::Reading(reading)).await.is_err() {
                                info!("Channel closed, reading generator shutting down");
                                return;
                            }
                        }
                    }
                }
            }
        });

        // Spawn status generator task
        let tx_statuses = tx;
        let cancel_statuses = cancel;
        let devices_for_statuses = devices;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(status_interval);

            loop {
                tokio::select! {
                    _ = cancel_statuses.cancelled() => {
                        info!("Mock status generator shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        for device in devices_for_statuses.iter() {
                            let status = device.generate_status(dispatcher_id);
                            if tx_statuses.send(EdgeData::Status(status)).await.is_err() {
                                info!("Channel closed, status generator shutting down");
                                return;
                            }
                        }
                    }
                }
            }
        });

        Ok(rx)
    }
}
