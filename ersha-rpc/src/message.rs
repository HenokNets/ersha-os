use ersha_core::{BatchUploadRequest, BatchUploadResponse, HelloRequest, HelloResponse};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct MessageId(pub Ulid);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub msg_id: MessageId,
    pub reply_to: Option<MessageId>,
    pub payload: WireMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireMessage {
    Ping,
    Pong,
    HelloRequest(HelloRequest),
    HelloResponse(HelloResponse),
    BatchUploadRequest(BatchUploadRequest),
    BatchUploadResponse(BatchUploadResponse),
    Error(WireError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireError {
    pub code: WireErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireErrorCode {
    BadRequest,
    Unsupported,
    Internal,
}
