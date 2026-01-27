mod device;
mod device_status;
mod dispatcher;
mod reading;

pub use device::InMemoryDeviceRegistry;
pub use device_status::InMemoryDeviceStatusRegistry;
pub use dispatcher::InMemoryDispatcherRegistry;
pub use reading::InMemoryReadingRegistry;

#[derive(Debug, thiserror::Error)]
pub enum InMemoryError {
    #[error("not found")]
    NotFound,
}
