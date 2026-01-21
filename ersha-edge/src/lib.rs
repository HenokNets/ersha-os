#![no_std]

use defmt::{Format, error, info};
use embassy_net::tcp::State;
use embassy_net::{IpAddress, IpEndpoint, Stack, tcp::TcpSocket};
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::channel::{Channel, Sender};
use embassy_time::{Duration, Timer};
use postcard::to_slice;
use serde::{Deserialize, Serialize};

const READING_QUEUE_DEPTH: usize = 16;

const SERVER_ADDR: IpEndpoint = IpEndpoint {
    addr: IpAddress::v4(10, 46, 238, 14),
    port: 9001,
};

// TODO: consolidate with ersha-core::SensorMetric after
// resolveing no_std issues.
#[derive(Serialize, Deserialize, Debug, Clone, Format)]
pub enum SensorMetric {
    /// Percentage 0-100 (1 byte in Postcard)
    SoilMoisture(u8),
    /// Degrees Celsius scaled by 100 (e.g., 25.43 -> 2543).
    /// Fits in 2 bytes instead of 4.
    SoilTemp(i16),
    AirTemp(i16),
    Humidity(u8),
    /// Rainfall in mm scaled by 100.
    Rainfall(u16),
}

static READING_CHANNEL: Channel<CriticalSectionRawMutex, SensorMetric, READING_QUEUE_DEPTH> =
    Channel::new();

pub fn sender() -> Sender<'static, CriticalSectionRawMutex, SensorMetric, READING_QUEUE_DEPTH> {
    READING_CHANNEL.sender()
}

pub struct SensorConfig {
    pub sampling_rate: Duration,
    pub calibration_offset: f32,
}

#[derive(defmt::Format)]
pub enum SensorError {
    Timeout,
    InvalidData,
}

pub trait Sensor {
    fn config(&self) -> SensorConfig;
    fn read(&self) -> impl Future<Output = Result<SensorMetric, SensorError>>;
}

#[derive(Serialize, Deserialize, Debug, Format)]
pub struct UplinkPacket {
    pub seq: u8,
    pub sensor_id: u8,
    pub metric: SensorMetric,
}

#[derive(Debug, Format)]
pub enum UplinkError {
    UnableToSend,
    SerializationFailed,
    ServerNotFound,
}

pub trait Transport {
    fn ready(&mut self) -> impl Future<Output = Result<(), UplinkError>>;
    fn send(
        &mut self,
        fport: u8,
        data: &[u8],
    ) -> impl core::future::Future<Output = Result<usize, UplinkError>>;
}

pub struct Engine<T: Transport> {
    transport: T,
    seq: u8,
}

impl<T: Transport> Engine<T> {
    pub fn new(transport: T) -> Self {
        Self { transport, seq: 0 }
    }

    pub async fn run(mut self) {
        let receiver = READING_CHANNEL.receiver();

        loop {
            loop {
                if let Err(e) = self.transport.ready().await {
                    error!("Transport not ready: {:?}", e);
                    Timer::after_secs(5).await;
                    continue;
                }
                break;
            }

            let metric = receiver.receive().await;

            let fport = match metric {
                SensorMetric::SoilMoisture(_) => 10,
                SensorMetric::AirTemp(_) => 11,
                _ => 1,
            };

            let packet = UplinkPacket {
                seq: self.seq,
                sensor_id: 0x01,
                metric,
            };

            let mut buffer = [0u8; 64];

            let encoded = match to_slice(&packet, &mut buffer) {
                Ok(used_slice) => used_slice,
                Err(_) => {
                    error!("Serialization Failed");
                    continue;
                }
            };

            match self.transport.send(fport, encoded).await {
                Ok(bytes) => info!("Sent {} bytes on FPort {}", bytes, fport),
                Err(_) => error!("Uplink failed"),
            }

            self.seq = self.seq.wrapping_add(1);
            Timer::after_millis(100).await;
        }
    }
}

#[macro_export]
macro_rules! sensor_task {
    ($task_name:ident, $sensor_ty:ty) => {
        #[embassy_executor::task]
        async fn $task_name(sensor: &'static $sensor_ty) -> ! {
            let sender = $crate::sender();

            loop {
                let config = sensor.config();

                match sensor.read().await {
                    Ok(reading) => {
                        // sender.send(reading).await;
                        if sender.try_send(reading).is_err() {
                            defmt::warn!("Sensor queue full, dropping reading");
                        }
                    }
                    Err(e) => {
                        error!("Sender Error: {:?}", e);
                    }
                }

                Timer::after(config.sampling_rate).await;
            }
        }
    };
}

pub struct Wifi<'a> {
    socket: TcpSocket<'a>,
}

impl<'a> Wifi<'a> {
    pub fn new(stack: Stack<'a>, rx_buffer: &'a mut [u8], tx_buffer: &'a mut [u8]) -> Self {
        Self {
            socket: TcpSocket::new(stack, rx_buffer, tx_buffer),
        }
    }
}

impl<'a> Transport for Wifi<'a> {
    async fn ready(&mut self) -> Result<(), UplinkError> {
        if self.socket.state() == State::Established {
            return Ok(());
        }

        self.socket.set_timeout(Some(Duration::from_secs(10)));
        self.socket
            .connect(SERVER_ADDR)
            .await
            .map_err(|_| UplinkError::ServerNotFound)?;

        Ok(())
    }

    async fn send(&mut self, _fport: u8, data: &[u8]) -> Result<usize, UplinkError> {
        let mut written = 0;
        while written < data.len() {
            written += self
                .socket
                .write(&data[written..])
                .await
                .map_err(|_| UplinkError::UnableToSend)?;
        }
        Ok(written)
    }
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::*;
    use embassy_time::Duration;

    struct MockSoilSensor;

    impl Sensor for MockSoilSensor {
        fn config(&self) -> SensorConfig {
            SensorConfig {
                sampling_rate: Duration::from_millis(10),
                calibration_offset: 0.0,
            }
        }

        fn read(&self) -> impl core::future::Future<Output = Result<SensorMetric, SensorError>> {
            async move { Ok(SensorMetric::SoilMoisture(42)) }
        }
    }

    struct MockAirSensor;

    sensor_task!(soil_task, MockSoilSensor);
}
