use crate::DeviceId;
use crate::Error;
use crate::ReadingPacket;
use crate::SensorCapability;

use embassy_net::{
    IpAddress, IpEndpoint, Stack,
    tcp::{State, TcpSocket},
};

const SERVER_ADDR: IpEndpoint = IpEndpoint {
    addr: IpAddress::v4(10, 46, 238, 14),
    port: 9001,
};

pub trait Transport {
    /// Called once after network join / connect
    fn provision(&mut self) -> impl Future<Output = Result<DeviceId, Error>>;

    /// Send device capabilities to the server / network
    fn announce_sensors(
        &mut self,
        device_id: DeviceId,
        sensors: &[SensorCapability],
    ) -> impl Future<Output = Result<(), Error>>;

    /// Send a single sensor reading
    fn send_reading(&mut self, packet: &ReadingPacket) -> impl Future<Output = Result<(), Error>>;
}

pub struct Wifi<'a> {
    socket: TcpSocket<'a>,
    device_id: Option<DeviceId>,
}

impl<'a> Wifi<'a> {
    pub fn new(stack: Stack<'a>, rx: &'a mut [u8], tx: &'a mut [u8]) -> Self {
        Self {
            socket: TcpSocket::new(stack, rx, tx),
            device_id: None,
        }
    }
}

impl<'a> Transport for Wifi<'a> {
    async fn provision(&mut self) -> Result<DeviceId, Error> {
        if self.socket.state() != State::Established {
            self.socket
                .connect(SERVER_ADDR)
                .await
                .map_err(|_| Error::ServerNotFound)?;
        }

        self.socket
            .write(b"HELLO")
            .await
            .map_err(|_| Error::UnableToSend)?;

        let mut buf = [0u8; 4];
        read_exact(&mut self.socket, &mut buf)
            .await
            .map_err(|_| Error::UnableToSend)?;

        let id = u32::from_be_bytes(buf);
        self.device_id = Some(id);
        Ok(id)
    }

    async fn announce_sensors(
        &mut self,
        device_id: DeviceId,
        sensors: &[SensorCapability],
    ) -> Result<(), Error> {
        let mut buf = [0u8; 64];
        let used = postcard::to_slice(&(device_id, sensors), &mut buf)
            .map_err(|_| Error::SerializationFailed)?;

        self.socket
            .write(used)
            .await
            .map_err(|_| Error::UnableToSend)?;
        Ok(())
    }

    async fn send_reading(&mut self, packet: &ReadingPacket) -> Result<(), Error> {
        let mut buf = [0u8; 64];
        let used = postcard::to_slice(packet, &mut buf).map_err(|_| Error::SerializationFailed)?;

        self.socket
            .write(used)
            .await
            .map_err(|_| Error::UnableToSend)?;
        Ok(())
    }
}

async fn read_exact(socket: &mut TcpSocket<'_>, mut buf: &mut [u8]) -> Result<(), Error> {
    while !buf.is_empty() {
        let n = socket.read(buf).await.map_err(|_| Error::UnableToSend)?;

        if n == 0 {
            return Err(Error::ServerNotFound);
        }

        buf = &mut buf[n..];
    }
    Ok(())
}
