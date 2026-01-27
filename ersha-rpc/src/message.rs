use ersha_core::{
    AlertRequest, AlertResponse, BatchUploadRequest, BatchUploadResponse,
    DeviceDisconnectionRequest, DeviceDisconnectionResponse, DispatcherStatusRequest,
    DispatcherStatusResponse, HelloRequest, HelloResponse,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(pub Ulid);

impl MessageId {
    pub fn new() -> Self {
        Self(Ulid::new())
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub msg_id: MessageId,
    pub reply_to: Option<MessageId>,
    pub payload: WireMessage,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum WireMessage {
    Ping,
    Pong,
    HelloRequest(HelloRequest),
    HelloResponse(HelloResponse),
    BatchUploadRequest(BatchUploadRequest),
    BatchUploadResponse(BatchUploadResponse),
    AlertRequest(AlertRequest),
    AlertResponse(AlertResponse),
    DispatcherStatusRequest(DispatcherStatusRequest),
    DispatcherStatusResponse(DispatcherStatusResponse),
    DeviceDisconnectionRequest(DeviceDisconnectionRequest),
    DeviceDisconnectionResponse(DeviceDisconnectionResponse),
    Error(WireError),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WireError {
    pub code: WireErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum WireErrorCode {
    BadRequest,
    Unsupported,
    Internal,
}
