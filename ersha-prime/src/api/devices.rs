use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use ersha_core::{
    Device, DeviceId, DeviceKind, DeviceState, H3Cell, Sensor, SensorId, SensorKind, SensorMetric,
};
use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::registry::{
    DeviceRegistry, DispatcherRegistry,
    filter::{DeviceFilter, DeviceSortBy, Pagination, QueryOptions, SortOrder},
};

use super::ApiState;

/// Request body for registering a new device.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterDeviceRequest {
    /// Optional ID. If not provided, a new ULID will be generated.
    pub id: Option<Ulid>,
    /// H3 cell location of the device.
    pub location: u64,
    /// Device kind (currently only "sensor" is supported).
    pub kind: Option<String>,
    /// Manufacturer name.
    pub manufacturer: Option<String>,
    /// Sensors attached to this device.
    #[serde(default)]
    pub sensors: Vec<SensorRequest>,
}

/// Request body for a sensor.
#[derive(Debug, Serialize, Deserialize)]
pub struct SensorRequest {
    /// Optional ID. If not provided, a new ULID will be generated.
    pub id: Option<Ulid>,
    /// Sensor kind: "soil_moisture", "soil_temp", "air_temp", "humidity", "rainfall".
    pub kind: String,
}

/// Response body for a sensor.
#[derive(Debug, Serialize, Deserialize)]
pub struct SensorResponse {
    pub id: String,
    pub kind: String,
}

impl From<&Sensor> for SensorResponse {
    fn from(s: &Sensor) -> Self {
        Self {
            id: s.id.0.to_string(),
            kind: match s.kind {
                SensorKind::SoilMoisture => "soil_moisture".to_string(),
                SensorKind::SoilTemp => "soil_temp".to_string(),
                SensorKind::AirTemp => "air_temp".to_string(),
                SensorKind::Humidity => "humidity".to_string(),
                SensorKind::Rainfall => "rainfall".to_string(),
            },
        }
    }
}

/// Response body for a device.
#[derive(Debug, Serialize, Deserialize)]
pub struct DeviceResponse {
    pub id: String,
    pub kind: String,
    pub state: String,
    pub location: u64,
    pub manufacturer: Option<String>,
    pub provisioned_at: String,
    pub sensors: Vec<SensorResponse>,
}

impl From<Device> for DeviceResponse {
    fn from(d: Device) -> Self {
        Self {
            id: d.id.0.to_string(),
            kind: match d.kind {
                DeviceKind::Sensor => "sensor".to_string(),
            },
            state: match d.state {
                DeviceState::Active => "active".to_string(),
                DeviceState::Suspended => "suspended".to_string(),
            },
            location: d.location.0,
            manufacturer: d.manufacturer.map(|s| s.to_string()),
            provisioned_at: d.provisioned_at.to_string(),
            sensors: d.sensors.iter().map(SensorResponse::from).collect(),
        }
    }
}

/// Response body for list of devices.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListDevicesResponse {
    pub devices: Vec<DeviceResponse>,
    pub total: usize,
}

// Re-export shared query enums from dispatchers
pub use super::dispatchers::{QuerySortOrder, StateFilter};

/// Sort field for device queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceQuerySortBy {
    State,
    Manufacturer,
    ProvisionedAt,
    SensorCount,
}

/// Query parameters for listing devices.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ListDevicesQuery {
    /// Filter by state
    pub state: Option<StateFilter>,
    /// Filter by location (H3 cell)
    pub location: Option<u64>,
    /// Filter by manufacturer (pattern match)
    pub manufacturer: Option<String>,
    /// Filter by provisioned after (ISO 8601 timestamp)
    pub provisioned_after: Option<String>,
    /// Filter by provisioned before (ISO 8601 timestamp)
    pub provisioned_before: Option<String>,
    /// Sort by field
    pub sort_by: Option<DeviceQuerySortBy>,
    /// Sort order
    pub sort_order: Option<QuerySortOrder>,
    /// Offset for pagination
    pub offset: Option<usize>,
    /// Limit for pagination (max 100)
    pub limit: Option<usize>,
    /// Cursor for cursor-based pagination (ULID)
    pub after: Option<String>,
}

fn parse_sensor_kind(kind: &str) -> Option<SensorKind> {
    match kind {
        "soil_moisture" => Some(SensorKind::SoilMoisture),
        "soil_temp" => Some(SensorKind::SoilTemp),
        "air_temp" => Some(SensorKind::AirTemp),
        "humidity" => Some(SensorKind::Humidity),
        "rainfall" => Some(SensorKind::Rainfall),
        _ => None,
    }
}

fn default_metric_for_kind(kind: SensorKind) -> SensorMetric {
    match kind {
        SensorKind::SoilMoisture => SensorMetric::SoilMoisture {
            value: ersha_core::Percentage(0),
        },
        SensorKind::SoilTemp => SensorMetric::SoilTemp {
            value: NotNan::new(0.0).unwrap(),
        },
        SensorKind::AirTemp => SensorMetric::AirTemp {
            value: NotNan::new(0.0).unwrap(),
        },
        SensorKind::Humidity => SensorMetric::Humidity {
            value: ersha_core::Percentage(0),
        },
        SensorKind::Rainfall => SensorMetric::Rainfall {
            value: NotNan::new(0.0).unwrap(),
        },
    }
}

/// Register a new device.
///
/// POST /api/devices
pub async fn register_device<D, Dev>(
    State(state): State<ApiState<D, Dev>>,
    Json(request): Json<RegisterDeviceRequest>,
) -> impl IntoResponse
where
    D: DispatcherRegistry,
    Dev: DeviceRegistry,
{
    let id = request.id.unwrap_or_else(Ulid::new);

    // Parse sensors
    let mut sensors = Vec::with_capacity(request.sensors.len());
    for sensor_req in request.sensors {
        let sensor_kind = match parse_sensor_kind(&sensor_req.kind) {
            Some(kind) => kind,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Invalid sensor kind: {}", sensor_req.kind),
                )
                    .into_response();
            }
        };
        sensors.push(Sensor {
            id: SensorId(sensor_req.id.unwrap_or_else(Ulid::new)),
            kind: sensor_kind.clone(),
            metric: default_metric_for_kind(sensor_kind),
        });
    }

    let device = Device {
        id: DeviceId(id),
        kind: DeviceKind::Sensor,
        state: DeviceState::Active,
        location: H3Cell(request.location),
        manufacturer: request.manufacturer.map(|s| s.into_boxed_str()),
        provisioned_at: jiff::Timestamp::now(),
        sensors: sensors.into_boxed_slice(),
    };

    match state.device_registry.register(device.clone()).await {
        Ok(()) => (StatusCode::CREATED, Json(DeviceResponse::from(device))).into_response(),
        Err(e) => {
            tracing::error!(error = ?e, "Failed to register device");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to register device",
            )
                .into_response()
        }
    }
}

/// Get a device by ID.
///
/// GET /api/devices/:id
pub async fn get_device<D, Dev>(
    State(state): State<ApiState<D, Dev>>,
    Path(id): Path<String>,
) -> impl IntoResponse
where
    D: DispatcherRegistry,
    Dev: DeviceRegistry,
{
    let ulid = match id.parse::<Ulid>() {
        Ok(ulid) => ulid,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid device ID").into_response(),
    };

    match state.device_registry.get(DeviceId(ulid)).await {
        Ok(Some(device)) => (StatusCode::OK, Json(DeviceResponse::from(device))).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "Device not found").into_response(),
        Err(e) => {
            tracing::error!(error = ?e, "Failed to get device");
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get device").into_response()
        }
    }
}

/// List all devices.
///
/// GET /api/devices
pub async fn list_devices<D, Dev>(
    State(state): State<ApiState<D, Dev>>,
    Query(query): Query<ListDevicesQuery>,
) -> impl IntoResponse
where
    D: DispatcherRegistry,
    Dev: DeviceRegistry,
{
    // Build filter
    let mut filter = DeviceFilter::default();

    if let Some(state_filter) = query.state {
        let device_state = match state_filter {
            StateFilter::Active => DeviceState::Active,
            StateFilter::Suspended => DeviceState::Suspended,
        };
        filter.states = Some(vec![device_state]);
    }

    if let Some(location) = query.location {
        filter.locations = Some(vec![H3Cell(location)]);
    }

    if let Some(ref manufacturer) = query.manufacturer {
        filter.manufacturer_pattern = Some(manufacturer.clone());
    }

    if let Some(ref ts_str) = query.provisioned_after {
        match ts_str.parse::<jiff::Timestamp>() {
            Ok(ts) => filter.provisioned_after = Some(ts),
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    "Invalid provisioned_after timestamp",
                )
                    .into_response();
            }
        }
    }

    if let Some(ref ts_str) = query.provisioned_before {
        match ts_str.parse::<jiff::Timestamp>() {
            Ok(ts) => filter.provisioned_before = Some(ts),
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    "Invalid provisioned_before timestamp",
                )
                    .into_response();
            }
        }
    }

    // Build sort options
    let sort_by = match query.sort_by {
        Some(DeviceQuerySortBy::State) => DeviceSortBy::State,
        Some(DeviceQuerySortBy::Manufacturer) => DeviceSortBy::Manufacturer,
        Some(DeviceQuerySortBy::SensorCount) => DeviceSortBy::SensorCount,
        Some(DeviceQuerySortBy::ProvisionedAt) | None => DeviceSortBy::ProvisionAt,
    };

    let sort_order = match query.sort_order {
        Some(QuerySortOrder::Asc) => SortOrder::Asc,
        Some(QuerySortOrder::Desc) | None => SortOrder::Desc,
    };

    // Build pagination
    let pagination = if let Some(ref after_str) = query.after {
        match after_str.parse::<Ulid>() {
            Ok(cursor) => Pagination::Cursor {
                after: Some(cursor),
                limit: query.limit.unwrap_or(100).min(100),
            },
            Err(_) => return (StatusCode::BAD_REQUEST, "Invalid cursor").into_response(),
        }
    } else {
        Pagination::Offset {
            offset: query.offset.unwrap_or(0),
            limit: query.limit.unwrap_or(100).min(100),
        }
    };

    let options = QueryOptions {
        filter,
        sort_by,
        sort_order,
        pagination,
    };

    match state.device_registry.list(options).await {
        Ok(devices) => {
            let total = devices.len();
            let response = ListDevicesResponse {
                devices: devices.into_iter().map(DeviceResponse::from).collect(),
                total,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            tracing::error!(error = ?e, "Failed to list devices");
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to list devices").into_response()
        }
    }
}
