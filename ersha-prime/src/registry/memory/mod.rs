mod device;
mod dispatcher;

pub use device::InMemoryDeviceRegistry;
pub use dispatcher::InMemoryDispatcherRegistry;

#[derive(Debug, thiserror::Error)]
pub enum InMemoryError {
    #[error("not found")]
    NotFound,
}
