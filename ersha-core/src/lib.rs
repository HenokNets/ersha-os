use serde::{Deserialize, Serialize};
use ulid::Ulid;

type BoxStr = Box<str>;
type BoxList<T> = Box<[T]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeviceId(pub Ulid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReadingId(pub Ulid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StatusId(pub Ulid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DispatcherId(pub Ulid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BatchId(pub Ulid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct H3Cell(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Device {
    pub id: DeviceId,
    pub kind: DeviceKind,
    pub state: DeviceState,
    pub location: H3Cell,
    pub manufacturer: Option<BoxStr>,
    pub provisioned_at: jiff::Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeviceKind {
    Sensor,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeviceState {
    Active,
    Suspended,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorReading {
    pub id: ReadingId,
    pub device_id: DeviceId,
    pub dispatcher_id: DispatcherId,
    pub metric: SensorMetric,
    pub quality: ReadingQuality,
    pub location: H3Cell,
    pub timestamp: jiff::Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadingQuality {
    pub status: QualityStatus,
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QualityStatus {
    Ok,
    Suspect,
    Bad,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorMetric {
    pub kind: SensorMetricKind,
    pub value: f64,
    pub unit: MetricUnit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricUnit {
    Percent,
    Celsius,
    Mm,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SensorMetricKind {
    SoilMoisture,
    SoilTemp,
    AirTemp,
    Humidity,
    Rainfall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceStatus {
    pub id: StatusId,
    pub device_id: DeviceId,
    pub dispatcher_id: DispatcherId,
    pub battery_percent: u8,
    pub uptime_seconds: u64,
    pub signal_rssi: i64,
    pub errors: BoxList<DeviceError>,
    pub timestamp: jiff::Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceError {
    pub code: DeviceErrorCode,
    pub message: Option<BoxStr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeviceErrorCode {
    LowBattery,
    SensorFault,
    RadioFault,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dispatcher {
    pub id: DispatcherId,
    pub location: H3Cell,
    pub state: DispatcherState,
    pub provisioned_at: jiff::Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DispatcherState {
    Active,
    Suspended,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchUploadRequest {
    pub id: BatchId,
    pub dispatcher_id: DispatcherId,
    pub readings: BoxList<SensorReading>,
    pub statuses: BoxList<DeviceStatus>,
    pub timestamp: jiff::Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BatchUploadResponse {
    pub id: BatchId,
}
