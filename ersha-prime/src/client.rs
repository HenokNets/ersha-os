use reqwest::Client as HttpClient;
use serde::Deserialize;
use thiserror::Error;
use ulid::Ulid;

use crate::api::{
    devices::{
        DeviceResponse, ListDevicesQuery, ListDevicesResponse, RegisterDeviceRequest, SensorRequest,
    },
    dispatchers::{
        DispatcherResponse, ListDispatchersQuery, ListDispatchersResponse,
        RegisterDispatcherRequest,
    },
};

// Re-export query enums for convenience
pub use crate::api::devices::DeviceQuerySortBy;
pub use crate::api::dispatchers::{QuerySortOrder, StateFilter};

/// Error type for API client operations.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),

    #[error("Server returned error status {status}: {message}")]
    ServerError { status: u16, message: String },

    #[error("Resource not found")]
    NotFound,

    #[error("Invalid request: {0}")]
    BadRequest(String),
}

/// HTTP API client for ersha-prime.
///
/// This client provides a convenient way to interact with the ersha-prime
/// HTTP API endpoints programmatically. It is designed to be extensible
/// for future CRUD operations.
#[derive(Clone)]
pub struct Client {
    http: HttpClient,
    base_url: String,
}

impl Client {
    /// Create a new API client with the given base URL.
    ///
    /// # Example
    /// ```no_run
    /// use ersha_prime::client::Client;
    ///
    /// let client = Client::new("http://localhost:3000");
    /// ```
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            http: HttpClient::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    /// Create a new API client with a custom reqwest client.
    ///
    /// This allows you to configure timeouts, TLS settings, etc.
    pub fn with_http_client(http: HttpClient, base_url: impl Into<String>) -> Self {
        Self {
            http,
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    // -------------------------------------------------------------------------
    // Dispatcher operations
    // -------------------------------------------------------------------------

    /// Register a new dispatcher.
    ///
    /// # Arguments
    /// * `location` - H3 cell location of the dispatcher
    ///
    /// # Returns
    /// The registered dispatcher with its assigned ID.
    pub async fn register_dispatcher(
        &self,
        location: u64,
    ) -> Result<DispatcherResponse, ClientError> {
        self.register_dispatcher_with_id(None, location).await
    }

    /// Register a new dispatcher with a specific ID.
    ///
    /// # Arguments
    /// * `id` - Optional ULID for the dispatcher. If None, a new one is generated.
    /// * `location` - H3 cell location of the dispatcher
    ///
    /// # Returns
    /// The registered dispatcher.
    pub async fn register_dispatcher_with_id(
        &self,
        id: Option<Ulid>,
        location: u64,
    ) -> Result<DispatcherResponse, ClientError> {
        let request = RegisterDispatcherRequest { id, location };
        let url = format!("{}/api/dispatchers", self.base_url);

        let response = self.http.post(&url).json(&request).send().await?;

        handle_response(response).await
    }

    /// Get a dispatcher by ID.
    ///
    /// # Arguments
    /// * `id` - The dispatcher's ULID
    ///
    /// # Returns
    /// The dispatcher if found, or `ClientError::NotFound`.
    pub async fn get_dispatcher(&self, id: Ulid) -> Result<DispatcherResponse, ClientError> {
        let url = format!("{}/api/dispatchers/{}", self.base_url, id);

        let response = self.http.get(&url).send().await?;

        handle_response(response).await
    }

    /// List all dispatchers.
    ///
    /// # Returns
    /// A list of all registered dispatchers.
    pub async fn list_dispatchers(&self) -> Result<ListDispatchersResponse, ClientError> {
        self.list_dispatchers_with_query(ListDispatchersQuery::default())
            .await
    }

    /// List dispatchers with query parameters.
    ///
    /// # Arguments
    /// * `query` - Query parameters for filtering, sorting, and pagination
    ///
    /// # Returns
    /// A filtered list of dispatchers.
    pub async fn list_dispatchers_with_query(
        &self,
        query: ListDispatchersQuery,
    ) -> Result<ListDispatchersResponse, ClientError> {
        let url = format!("{}/api/dispatchers", self.base_url);

        let response = self.http.get(&url).query(&query).send().await?;

        handle_response(response).await
    }

    /// Suspend a dispatcher.
    ///
    /// Suspended dispatchers cannot upload data to ersha-prime.
    ///
    /// # Arguments
    /// * `id` - The dispatcher's ULID
    ///
    /// # Returns
    /// The updated dispatcher with suspended state.
    pub async fn suspend_dispatcher(&self, id: Ulid) -> Result<DispatcherResponse, ClientError> {
        let url = format!("{}/api/dispatchers/{}/suspend", self.base_url, id);

        let response = self.http.post(&url).send().await?;

        handle_response(response).await
    }

    // -------------------------------------------------------------------------
    // Device operations
    // -------------------------------------------------------------------------

    /// Register a new device.
    ///
    /// # Arguments
    /// * `request` - Device registration request
    ///
    /// # Returns
    /// The registered device with its assigned ID.
    pub async fn register_device(
        &self,
        request: RegisterDeviceRequest,
    ) -> Result<DeviceResponse, ClientError> {
        let url = format!("{}/api/devices", self.base_url);

        let response = self.http.post(&url).json(&request).send().await?;

        handle_response(response).await
    }

    /// Register a new device with minimal parameters.
    ///
    /// # Arguments
    /// * `location` - H3 cell location of the device
    ///
    /// # Returns
    /// The registered device.
    pub async fn register_device_simple(
        &self,
        location: u64,
    ) -> Result<DeviceResponse, ClientError> {
        self.register_device(RegisterDeviceRequest {
            id: None,
            location,
            kind: None,
            manufacturer: None,
            sensors: vec![],
        })
        .await
    }

    /// Get a device by ID.
    ///
    /// # Arguments
    /// * `id` - The device's ULID
    ///
    /// # Returns
    /// The device if found, or `ClientError::NotFound`.
    pub async fn get_device(&self, id: Ulid) -> Result<DeviceResponse, ClientError> {
        let url = format!("{}/api/devices/{}", self.base_url, id);

        let response = self.http.get(&url).send().await?;

        handle_response(response).await
    }

    /// List all devices.
    ///
    /// # Returns
    /// A list of all registered devices.
    pub async fn list_devices(&self) -> Result<ListDevicesResponse, ClientError> {
        self.list_devices_with_query(ListDevicesQuery::default())
            .await
    }

    /// List devices with query parameters.
    ///
    /// # Arguments
    /// * `query` - Query parameters for filtering, sorting, and pagination
    ///
    /// # Returns
    /// A filtered list of devices.
    pub async fn list_devices_with_query(
        &self,
        query: ListDevicesQuery,
    ) -> Result<ListDevicesResponse, ClientError> {
        let url = format!("{}/api/devices", self.base_url);

        let response = self.http.get(&url).query(&query).send().await?;

        handle_response(response).await
    }
}

/// Helper to handle HTTP responses and convert them to our Result type.
async fn handle_response<T>(response: reqwest::Response) -> Result<T, ClientError>
where
    T: for<'de> Deserialize<'de>,
{
    let status = response.status();

    if status.is_success() {
        Ok(response.json().await?)
    } else if status == reqwest::StatusCode::NOT_FOUND {
        Err(ClientError::NotFound)
    } else if status == reqwest::StatusCode::BAD_REQUEST {
        let message = response.text().await.unwrap_or_default();
        Err(ClientError::BadRequest(message))
    } else {
        let message = response.text().await.unwrap_or_default();
        Err(ClientError::ServerError {
            status: status.as_u16(),
            message,
        })
    }
}

/// Builder for creating dispatcher list queries.
#[derive(Default)]
pub struct ListDispatchersQueryBuilder {
    query: ListDispatchersQuery,
}

impl ListDispatchersQueryBuilder {
    /// Create a new query builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by state.
    pub fn state(mut self, state: StateFilter) -> Self {
        self.query.state = Some(state);
        self
    }

    /// Filter by location (H3 cell).
    pub fn location(mut self, location: u64) -> Self {
        self.query.location = Some(location);
        self
    }

    /// Set sort order.
    pub fn sort_order(mut self, order: QuerySortOrder) -> Self {
        self.query.sort_order = Some(order);
        self
    }

    /// Set pagination offset.
    pub fn offset(mut self, offset: usize) -> Self {
        self.query.offset = Some(offset);
        self
    }

    /// Set pagination limit (max 100).
    pub fn limit(mut self, limit: usize) -> Self {
        self.query.limit = Some(limit);
        self
    }

    /// Set cursor for cursor-based pagination.
    pub fn after(mut self, cursor: impl Into<String>) -> Self {
        self.query.after = Some(cursor.into());
        self
    }

    /// Build the query.
    pub fn build(self) -> ListDispatchersQuery {
        self.query
    }
}

/// Builder for creating device list queries.
#[derive(Default)]
pub struct ListDevicesQueryBuilder {
    query: ListDevicesQuery,
}

impl ListDevicesQueryBuilder {
    /// Create a new query builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by state.
    pub fn state(mut self, state: StateFilter) -> Self {
        self.query.state = Some(state);
        self
    }

    /// Filter by location (H3 cell).
    pub fn location(mut self, location: u64) -> Self {
        self.query.location = Some(location);
        self
    }

    /// Filter by manufacturer (pattern match).
    pub fn manufacturer(mut self, manufacturer: impl Into<String>) -> Self {
        self.query.manufacturer = Some(manufacturer.into());
        self
    }

    /// Filter by provisioned after timestamp (ISO 8601).
    pub fn provisioned_after(mut self, ts: impl Into<String>) -> Self {
        self.query.provisioned_after = Some(ts.into());
        self
    }

    /// Filter by provisioned before timestamp (ISO 8601).
    pub fn provisioned_before(mut self, ts: impl Into<String>) -> Self {
        self.query.provisioned_before = Some(ts.into());
        self
    }

    /// Set sort field.
    pub fn sort_by(mut self, field: DeviceQuerySortBy) -> Self {
        self.query.sort_by = Some(field);
        self
    }

    /// Set sort order.
    pub fn sort_order(mut self, order: QuerySortOrder) -> Self {
        self.query.sort_order = Some(order);
        self
    }

    /// Set pagination offset.
    pub fn offset(mut self, offset: usize) -> Self {
        self.query.offset = Some(offset);
        self
    }

    /// Set pagination limit (max 100).
    pub fn limit(mut self, limit: usize) -> Self {
        self.query.limit = Some(limit);
        self
    }

    /// Set cursor for cursor-based pagination.
    pub fn after(mut self, cursor: impl Into<String>) -> Self {
        self.query.after = Some(cursor.into());
        self
    }

    /// Build the query.
    pub fn build(self) -> ListDevicesQuery {
        self.query
    }
}

/// Builder for creating device registration requests.
///
/// This provides a fluent API for constructing `RegisterDeviceRequest`.
#[derive(Default)]
pub struct RegisterDeviceBuilder {
    id: Option<Ulid>,
    location: u64,
    kind: Option<String>,
    manufacturer: Option<String>,
    sensors: Vec<SensorRequest>,
}

impl RegisterDeviceBuilder {
    /// Create a new builder with the required location.
    pub fn new(location: u64) -> Self {
        Self {
            location,
            ..Default::default()
        }
    }

    /// Set a specific ID for the device.
    pub fn id(mut self, id: Ulid) -> Self {
        self.id = Some(id);
        self
    }

    /// Set the device kind.
    pub fn kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }

    /// Set the manufacturer name.
    pub fn manufacturer(mut self, manufacturer: impl Into<String>) -> Self {
        self.manufacturer = Some(manufacturer.into());
        self
    }

    /// Add a sensor to the device.
    pub fn sensor(mut self, kind: impl Into<String>) -> Self {
        self.sensors.push(SensorRequest {
            id: None,
            kind: kind.into(),
        });
        self
    }

    /// Add a sensor with a specific ID.
    pub fn sensor_with_id(mut self, id: Ulid, kind: impl Into<String>) -> Self {
        self.sensors.push(SensorRequest {
            id: Some(id),
            kind: kind.into(),
        });
        self
    }

    /// Build the registration request.
    pub fn build(self) -> RegisterDeviceRequest {
        RegisterDeviceRequest {
            id: self.id,
            location: self.location,
            kind: self.kind,
            manufacturer: self.manufacturer,
            sensors: self.sensors,
        }
    }
}
