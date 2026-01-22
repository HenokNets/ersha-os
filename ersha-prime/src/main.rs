use std::net::SocketAddr;
use std::path::PathBuf;

use axum::{Router, routing::get};
use clap::Parser;
use ersha_core::{Dispatcher, DispatcherState, HelloRequest, HelloResponse};
use ersha_prime::{
    config::{Config, RegistryConfig},
    registry::{
        DispatcherRegistry, memory::InMemoryDispatcherRegistry, sqlite::SqliteDispatcherRegistry,
    },
};
use ersha_rpc::Server;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Parser)]
#[command(name = "ersha-prime")]
#[command(about = "Ersha Prime")]
struct Cli {
    /// Path to the configuration file
    #[arg(short, long, default_value = "ersha-prime.toml")]
    config: PathBuf,
}

struct AppState<R: DispatcherRegistry> {
    dispatcher_registry: R,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let config = if cli.config.exists() {
        info!(path = ?cli.config, "Loading configuration");
        Config::load(&cli.config)?
    } else {
        info!("No configuration file found, using defaults");
        Config::default()
    };

    info!(rpc_addr = %config.server.rpc_addr, http_addr = %config.server.http_addr, "Starting servers");

    match config.registry {
        RegistryConfig::Memory => {
            info!("Using in-memory dispatcher registry");
            let registry = InMemoryDispatcherRegistry::new();
            run_server(registry, config.server.rpc_addr, config.server.http_addr).await?;
        }
        RegistryConfig::Sqlite { path } => {
            info!(path = ?path, "Using SQLite dispatcher registry");
            let registry = SqliteDispatcherRegistry::new(path.to_string_lossy()).await?;
            run_server(registry, config.server.rpc_addr, config.server.http_addr).await?;
        }
    }

    Ok(())
}

async fn run_server<R>(
    registry: R,
    rpc_addr: SocketAddr,
    http_addr: SocketAddr,
) -> color_eyre::Result<()>
where
    R: DispatcherRegistry,
{
    let state = AppState {
        dispatcher_registry: registry,
    };

    let cancel = CancellationToken::new();

    let rpc_listener = TcpListener::bind(rpc_addr).await?;
    info!(%rpc_addr, "RPC server listening");

    let rpc_server = Server::new(rpc_listener, state).on_hello(
        |hello: HelloRequest, _msg_id, _rpc, state: &AppState<R>| {
            let dispatcher_registry = state.dispatcher_registry.clone();
            async move {
                info!(
                    dispatcher_id = ?hello.dispatcher_id,
                    location = ?hello.location,
                    "received hello request"
                );

                let dispatcher = Dispatcher {
                    id: hello.dispatcher_id,
                    location: hello.location,
                    state: DispatcherState::Active,
                    provisioned_at: jiff::Timestamp::now(),
                };

                if let Err(e) = dispatcher_registry.register(dispatcher).await {
                    tracing::error!(error = ?e, "failed to register dispatcher");
                } else {
                    info!(dispatcher_id = ?hello.dispatcher_id, "dispatcher registered");
                }

                HelloResponse {
                    dispatcher_id: hello.dispatcher_id,
                }
            }
        },
    );

    let axum_app = Router::new().route("/health", get(health_handler));

    let axum_listener = TcpListener::bind(http_addr).await?;
    info!(%http_addr, "HTTP server listening");

    let cancel_clone = cancel.clone();
    tokio::select! {
        _ = rpc_server.serve(cancel.clone()) => {
            info!("RPC server shut down");
        }
        result = axum::serve(axum_listener, axum_app).with_graceful_shutdown(async move {
            cancel_clone.cancelled().await;
        }) => {
            if let Err(e) = result {
                tracing::error!(error = ?e, "HTTP server error");
            }
            info!("HTTP server shut down");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down...");
            cancel.cancel();
        }
    }

    Ok(())
}

async fn health_handler() -> &'static str {
    "OK"
}
