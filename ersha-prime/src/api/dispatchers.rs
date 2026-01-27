use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use ersha_core::{Dispatcher, DispatcherId, DispatcherState, H3Cell};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::registry::{
    DeviceRegistry, DispatcherRegistry,
    filter::{DispatcherFilter, DispatcherSortBy, Pagination, QueryOptions, SortOrder},
};

use super::ApiState;

/// Request body for registering a new dispatcher.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterDispatcherRequest {
    /// Optional ID. If not provided, a new ULID will be generated.
    pub id: Option<Ulid>,
    /// H3 cell location of the dispatcher.
    pub location: u64,
}

/// Response body for a dispatcher.
#[derive(Debug, Serialize, Deserialize)]
pub struct DispatcherResponse {
    pub id: String,
    pub location: u64,
    pub state: String,
    pub provisioned_at: String,
}

impl From<Dispatcher> for DispatcherResponse {
    fn from(d: Dispatcher) -> Self {
        Self {
            id: d.id.0.to_string(),
            location: d.location.0,
            state: match d.state {
                DispatcherState::Active => "active".to_string(),
                DispatcherState::Suspended => "suspended".to_string(),
            },
            provisioned_at: d.provisioned_at.to_string(),
        }
    }
}

/// Response body for list of dispatchers.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListDispatchersResponse {
    pub dispatchers: Vec<DispatcherResponse>,
    pub total: usize,
}

/// Register a new dispatcher.
///
/// POST /api/dispatchers
pub async fn register_dispatcher<D, Dev>(
    State(state): State<ApiState<D, Dev>>,
    Json(request): Json<RegisterDispatcherRequest>,
) -> impl IntoResponse
where
    D: DispatcherRegistry,
    Dev: DeviceRegistry,
{
    let id = request.id.unwrap_or_else(Ulid::new);
    let dispatcher = Dispatcher {
        id: DispatcherId(id),
        location: H3Cell(request.location),
        state: DispatcherState::Active,
        provisioned_at: jiff::Timestamp::now(),
    };

    match state.dispatcher_registry.register(dispatcher.clone()).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(DispatcherResponse::from(dispatcher)),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = ?e, "Failed to register dispatcher");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to register dispatcher",
            )
                .into_response()
        }
    }
}

/// Get a dispatcher by ID.
///
/// GET /api/dispatchers/:id
pub async fn get_dispatcher<D, Dev>(
    State(state): State<ApiState<D, Dev>>,
    Path(id): Path<String>,
) -> impl IntoResponse
where
    D: DispatcherRegistry,
    Dev: DeviceRegistry,
{
    let ulid = match id.parse::<Ulid>() {
        Ok(ulid) => ulid,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid dispatcher ID").into_response(),
    };

    match state.dispatcher_registry.get(DispatcherId(ulid)).await {
        Ok(Some(dispatcher)) => {
            (StatusCode::OK, Json(DispatcherResponse::from(dispatcher))).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, "Dispatcher not found").into_response(),
        Err(e) => {
            tracing::error!(error = ?e, "Failed to get dispatcher");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to get dispatcher",
            )
                .into_response()
        }
    }
}

/// List all dispatchers.
///
/// GET /api/dispatchers
pub async fn list_dispatchers<D, Dev>(State(state): State<ApiState<D, Dev>>) -> impl IntoResponse
where
    D: DispatcherRegistry,
    Dev: DeviceRegistry,
{
    let options = QueryOptions {
        filter: DispatcherFilter::default(),
        sort_by: DispatcherSortBy::ProvisionAt,
        sort_order: SortOrder::Desc,
        pagination: Pagination::Offset {
            offset: 0,
            limit: 100,
        },
    };

    match state.dispatcher_registry.list(options).await {
        Ok(dispatchers) => {
            let total = dispatchers.len();
            let response = ListDispatchersResponse {
                dispatchers: dispatchers
                    .into_iter()
                    .map(DispatcherResponse::from)
                    .collect(),
                total,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            tracing::error!(error = ?e, "Failed to list dispatchers");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to list dispatchers",
            )
                .into_response()
        }
    }
}

/// Suspend a dispatcher.
///
/// POST /api/dispatchers/:id/suspend
pub async fn suspend_dispatcher<D, Dev>(
    State(state): State<ApiState<D, Dev>>,
    Path(id): Path<String>,
) -> impl IntoResponse
where
    D: DispatcherRegistry,
    Dev: DeviceRegistry,
{
    let ulid = match id.parse::<Ulid>() {
        Ok(ulid) => ulid,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid dispatcher ID").into_response(),
    };

    match state.dispatcher_registry.suspend(DispatcherId(ulid)).await {
        Ok(()) => {
            // Fetch the updated dispatcher to return
            match state.dispatcher_registry.get(DispatcherId(ulid)).await {
                Ok(Some(dispatcher)) => {
                    (StatusCode::OK, Json(DispatcherResponse::from(dispatcher))).into_response()
                }
                Ok(None) => (StatusCode::NOT_FOUND, "Dispatcher not found").into_response(),
                Err(e) => {
                    tracing::error!(error = ?e, "Failed to get suspended dispatcher");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Dispatcher suspended but failed to fetch",
                    )
                        .into_response()
                }
            }
        }
        Err(e) => {
            let err_str = e.to_string();
            if err_str.contains("not found") || err_str.contains("NotFound") {
                (StatusCode::NOT_FOUND, "Dispatcher not found").into_response()
            } else {
                tracing::error!(error = ?e, "Failed to suspend dispatcher");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to suspend dispatcher",
                )
                    .into_response()
            }
        }
    }
}
