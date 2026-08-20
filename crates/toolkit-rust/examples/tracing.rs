use std::path::PathBuf;

use dee_bugee_rust::{LoggerConfig, non_blocking_layer};
use tracing_subscriber::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("ToolkitRustExample.jsonl"));
    let (layer, _guard) = non_blocking_layer(LoggerConfig::new(&path, "backend"))?;
    tracing_subscriber::registry().with(layer).init();

    tracing::info!(
        subsystem = "example",
        event = "example.started",
        duration_ms = 12.5,
        status = 200,
        "Rust adapter example started"
    );
    println!("Wrote {}", path.display());
    Ok(())
}
