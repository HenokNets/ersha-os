pub mod devices;
pub mod dispatchers;

use axum::{
    Router,
    routing::{get, post},
};

use crate::registry::{DeviceRegistry, DispatcherRegistry};

/// Shared state for API handlers.
#[derive(Clone)]
pub struct ApiState<D, Dev>
where
    D: DispatcherRegistry,
    Dev: DeviceRegistry,
{
    pub dispatcher_registry: D,
    pub device_registry: Dev,
}

/// Create the full API router with all endpoints.
pub fn api_router<D, Dev>(dispatcher_registry: D, device_registry: Dev) -> Router
where
    D: DispatcherRegistry,
    Dev: DeviceRegistry,
{
    let state = ApiState {
        dispatcher_registry,
        device_registry,
    };

    Router::new()
        .route(
            "/api/dispatchers",
            post(dispatchers::register_dispatcher::<D, Dev>),
        )
        .route(
            "/api/dispatchers",
            get(dispatchers::list_dispatchers::<D, Dev>),
        )
        .route(
            "/api/dispatchers/{id}",
            get(dispatchers::get_dispatcher::<D, Dev>),
        )
        .route(
            "/api/dispatchers/{id}/suspend",
            post(dispatchers::suspend_dispatcher::<D, Dev>),
        )
        .route("/api/devices", post(devices::register_device::<D, Dev>))
        .route("/api/devices", get(devices::list_devices::<D, Dev>))
        .route("/api/devices/{id}", get(devices::get_device::<D, Dev>))
        .with_state(state)
}
