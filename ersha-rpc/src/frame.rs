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
