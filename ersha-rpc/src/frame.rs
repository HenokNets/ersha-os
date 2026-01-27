use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::Envelope;

pub const MAX_FRAME_BYTES: u32 = 2_000_000; // 2 MB

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("postcard error: {0}")]
    Postcard(#[from] postcard::Error),
    #[error("frame too large")]
    FrameTooLarge,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub async fn write_frame<W>(w: &mut W, msg: &Envelope) -> Result<(), FrameError>
where
    W: AsyncWriteExt + Unpin,
{
    let bytes = postcard::to_stdvec(msg)?;
    let len = bytes.len() as u32;

    if len > MAX_FRAME_BYTES {
        return Err(FrameError::FrameTooLarge);
    }

    w.write_u32(len).await?;
    w.write_all(&bytes).await?;
    w.flush().await?;

    Ok(())
}

pub async fn read_frame<R>(r: &mut R) -> Result<Envelope, FrameError>
where
    R: AsyncReadExt + Unpin,
{
    let len = r.read_u32().await?;
    if len > MAX_FRAME_BYTES {
        return Err(FrameError::FrameTooLarge);
    }

    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    let msg = postcard::from_bytes(&buf)?;

    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MessageId, WireError, WireErrorCode, WireMessage};
    use ersha_core::{DispatcherId, H3Cell, HelloRequest, HelloResponse};
    use tokio::io::duplex;

    fn create_envelope(payload: WireMessage) -> Envelope {
        Envelope {
            msg_id: MessageId::new(),
            reply_to: None,
            payload,
        }
    }

    #[tokio::test]
    async fn test_roundtrip_ping() {
        let (mut writer, mut reader) = duplex(1024);
        let original = create_envelope(WireMessage::Ping);

        write_frame(&mut writer, &original).await.unwrap();
        let read = read_frame(&mut reader).await.unwrap();

        assert_eq!(read, original);
    }

    #[tokio::test]
    async fn test_roundtrip_pong() {
        let (mut writer, mut reader) = duplex(1024);
        let original = create_envelope(WireMessage::Pong);

        write_frame(&mut writer, &original).await.unwrap();
        let read = read_frame(&mut reader).await.unwrap();

        assert_eq!(read, original);
    }

    #[tokio::test]
    async fn test_roundtrip_hello_request() {
        let (mut writer, mut reader) = duplex(1024);
        let request = HelloRequest {
            dispatcher_id: DispatcherId(ulid::Ulid::new()),
            location: H3Cell(0x8a2a1072b59ffff),
        };
        let original = create_envelope(WireMessage::HelloRequest(request.clone()));

        write_frame(&mut writer, &original).await.unwrap();
        let read = read_frame(&mut reader).await.unwrap();

        assert_eq!(read, original);
    }

    #[tokio::test]
    async fn test_roundtrip_hello_response_accepted() {
        let (mut writer, mut reader) = duplex(1024);
        let response = HelloResponse::Accepted {
            dispatcher_id: DispatcherId(ulid::Ulid::new()),
        };
        let original = create_envelope(WireMessage::HelloResponse(response.clone()));

        write_frame(&mut writer, &original).await.unwrap();
        let read = read_frame(&mut reader).await.unwrap();

        assert_eq!(read, original);
    }

    #[tokio::test]
    async fn test_roundtrip_hello_response_rejected() {
        let (mut writer, mut reader) = duplex(1024);
        let response = HelloResponse::Rejected {
            reason: ersha_core::HelloRejectionReason::UnknownDispatcher,
        };
        let original = create_envelope(WireMessage::HelloResponse(response.clone()));

        write_frame(&mut writer, &original).await.unwrap();
        let read = read_frame(&mut reader).await.unwrap();

        assert_eq!(read, original);
    }

    #[tokio::test]
    async fn test_roundtrip_error() {
        let (mut writer, mut reader) = duplex(1024);
        let error = WireError {
            code: WireErrorCode::BadRequest,
            message: "Test error message".to_string(),
        };
        let original = create_envelope(WireMessage::Error(error.clone()));

        write_frame(&mut writer, &original).await.unwrap();
        let read = read_frame(&mut reader).await.unwrap();

        assert_eq!(read, original);
    }

    #[tokio::test]
    async fn test_roundtrip_with_reply_to() {
        let (mut writer, mut reader) = duplex(1024);
        let reply_to = MessageId::new();
        let original = Envelope {
            msg_id: MessageId::new(),
            reply_to: Some(reply_to),
            payload: WireMessage::Ping,
        };

        write_frame(&mut writer, &original).await.unwrap();
        let read = read_frame(&mut reader).await.unwrap();

        assert_eq!(read, original);
    }

    #[tokio::test]
    async fn test_frame_too_large_write() {
        let (mut writer, _reader) = duplex(1024);

        let error = WireError {
            code: WireErrorCode::Internal,
            message: "x".repeat(MAX_FRAME_BYTES as usize + 1),
        };
        let envelope = create_envelope(WireMessage::Error(error));

        let result = write_frame(&mut writer, &envelope).await;

        assert!(matches!(result, Err(FrameError::FrameTooLarge)));
    }

    #[tokio::test]
    async fn test_frame_too_large_read() {
        let (mut writer, mut reader) = duplex(1024);

        let oversized_len = MAX_FRAME_BYTES + 1;
        writer.write_u32(oversized_len).await.unwrap();
        writer.flush().await.unwrap();

        let result = read_frame(&mut reader).await;
        assert!(matches!(result, Err(FrameError::FrameTooLarge)));
    }

    #[tokio::test]
    async fn test_multiple_frames() {
        let (mut writer, mut reader) = duplex(4096);
        let frame1 = create_envelope(WireMessage::Ping);
        let frame2 = create_envelope(WireMessage::Pong);
        let frame3 = create_envelope(WireMessage::Error(WireError {
            code: WireErrorCode::Unsupported,
            message: "Test".to_string(),
        }));

        // Write all frames
        write_frame(&mut writer, &frame1).await.unwrap();
        write_frame(&mut writer, &frame2).await.unwrap();
        write_frame(&mut writer, &frame3).await.unwrap();

        // Read all frames back
        let read1 = read_frame(&mut reader).await.unwrap();
        let read2 = read_frame(&mut reader).await.unwrap();
        let read3 = read_frame(&mut reader).await.unwrap();

        assert_eq!(read1, frame1);
        assert_eq!(read2, frame2);
        assert_eq!(read3, frame3);
    }

    #[tokio::test]
    async fn test_various_error_codes() {
        let error_codes = vec![
            WireErrorCode::BadRequest,
            WireErrorCode::Unsupported,
            WireErrorCode::Internal,
        ];

        for code in error_codes {
            let (mut writer, mut reader) = duplex(1024);
            let error = WireError {
                code: code.clone(),
                message: format!("Error with {:?}", code),
            };
            let original = create_envelope(WireMessage::Error(error.clone()));

            write_frame(&mut writer, &original).await.unwrap();
            let read = read_frame(&mut reader).await.unwrap();

            assert_eq!(read, original);
        }
    }
}
