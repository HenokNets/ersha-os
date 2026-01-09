use ersha_core::{BatchUploadRequest, BatchUploadResponse, DispatcherId, H3Cell};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub const PROTO_VER: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(pub Ulid);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub proto: u16,
    pub msg_id: MessageId,
    pub reply_to: Option<MessageId>,
    pub payload: WireMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireMessage {
    Hello(Hello),
    Ping,
    Pong,
    BatchUploadRequest(BatchUploadRequest),
    BatchUploadResponse(BatchUploadResponse),
    Error(WireError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub dispatcher_id: DispatcherId,
    pub location: H3Cell,
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
