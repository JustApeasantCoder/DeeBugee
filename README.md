# Debug Logging Toolkit

A native, reusable structured-log viewer and producer toolkit for desktop applications. The viewer is written in Rust with `eframe`/`egui` and `wgpu`; it does not run a browser, WebView, HTTP server, or local listening port.

## MVP status

Implemented:

- Schema-versioned JSONL contract compatible with Streamee's current structured logs.
- Native Windows viewer with GPU rendering.
- Live polling of append-only files with partial-record buffering and truncation recovery.
- Multiple simultaneous files and drag-and-drop.
- Bounded 250,000-record in-memory window.
- Virtualized fixed and variable-height tables.
- Drag-and-drop table columns with persistent order and resizable widths.
- Compact or wrapped messages plus a full event-details pane.
- Facets for level, source, subsystem, tracing target, event, provider, status and correlation.
- Automatic facets for scalar values in `fields`.
- Left-click include-only, Ctrl+left-click additive include, and right-click exclusion.
- Persistent filter bookmarks with full search/facet/level/correlation state.
- Minimum-level, fuzzy multi-term text and correlation filtering.
- Deterministic color grouping by source, subsystem, target, event, provider or correlation.
- Pause, configurable latest-at-top/bottom tailing, saved TOML workspaces, and filtered JSONL export.
- Automatic native persistence for column layout, panel sizing, bookmarks, color grouping, message wrapping, and tail preferences.
- Explicit invalid-record and discarded-history counters.
- Rust `tracing`, Electron main/renderer and .NET `ILoggerProvider` adapters.
- Non-blocking bounded producer queues and full-record rotation.

Planned after the MVP is exercised with real applications:

- Timestamp range controls and richer query expressions.
- Persistent on-disk indexes for histories larger than the memory window.
- Optional named-pipe transport while retaining JSONL as the durable record.
- Signed installers and published Cargo/npm/NuGet packages.

## Run the viewer

```powershell
cargo run --release -p debug-logging-toolkit -- "C:\path\to\Application.jsonl"
```

For Streamee:

```powershell
cargo run --release -p debug-logging-toolkit -- "$env:TEMP\streamee_logs\Streamee.jsonl"
```

You can also launch without arguments and use **Open JSONL**, drag files onto the window, or open a saved workspace.

## Facet controls

- Left-click a value to show only that value within the facet.
- Ctrl+left-click to include another value using OR semantics.
- Right-click a value to exclude it.
- Click an already exclusive value again to return it to neutral.
- Different facets combine with AND semantics.
- Save the active combination from the bookmark bar above the table; left-click a bookmark to restore it and right-click to remove it.

For example, left-clicking `backend` under Source and right-clicking `normalizer` under Subsystem produces:

```text
source == backend AND subsystem != normalizer
```

## Table layout

- Drag any column header and drop it before or after another header to rearrange the table.
- The blue insertion marker shows where the column will land.
- Use the horizontal scrollbar below the table when the columns exceed the viewport.
- Hold the middle mouse button over the table and drag to pan vertically and horizontally.
- Column order, resized widths, panel sizes, message wrapping, color grouping, bookmarks, and latest position are saved automatically and restored on the next launch.
- Saved workspaces include their sources, filters, bookmarks, color grouping, latest position, and column order.
- Tail following can place the latest record at the top or bottom. Manual scrolling pauses following until the latest edge is reached again.

## Search

Search terms use AND semantics across the message, event, source, subsystem, correlation, optional schema fields, and structured `fields` values. Terms do not need to be adjacent, and small typos are tolerated. For example, both `Segment Local` and `segmant locl` match `[Segment Detection][Local]`.

## Workspace layout

```text
crates/toolkit-schema   Shared Rust event types
crates/toolkit-core     Bounded event store and bitmap facet indexes
crates/toolkit-viewer   Native egui viewer and JSONL follower
crates/toolkit-rust     Non-blocking tracing adapter and rotating writer
adapters/electron       TypeScript Electron main/renderer adapter
adapters/dotnet         Microsoft.Extensions.Logging provider
schemas                 Language-neutral JSON Schema
examples                Integration examples
tests/fixtures          Viewer and adapter fixtures
```

## Rust/Tauri integration

```rust
use debug_logging_toolkit_rust::{LoggerConfig, non_blocking_layer};
use tracing_subscriber::prelude::*;

let config = LoggerConfig::new(log_path, "backend");
let (toolkit_layer, logging_guard) = non_blocking_layer(config)?;
tracing_subscriber::registry()
    .with(toolkit_layer)
    .init();

tracing::info!(
    subsystem = "torrent",
    event = "torrent.ready",
    request_id = request_id,
    duration_ms = elapsed.as_secs_f64() * 1000.0,
    "Torrent ready"
);

// Keep logging_guard alive until application shutdown.
```

The same layer works in a Tauri backend. A WebView renderer should batch events through a Tauri command, following the same pattern as the Electron renderer bridge.

The viewer accepts Streamee's schema-v1 Unix-millisecond timestamps and numeric process/session IDs. It also reads early adapter records that used RFC 3339 timestamp text, so existing JSONL files remain usable.

## Electron integration

Install the handler in the main process:

```ts
import { installElectronLogging } from "@debug-logging-toolkit/electron";

const writer = installElectronLogging(ipcMain, logPath);
```

Create the renderer logger in preload or the renderer bridge:

```ts
import { createRendererLogger } from "@debug-logging-toolkit/electron/renderer";

const logger = createRendererLogger(ipcRenderer, { appSessionId });
logger.captureConsole();
logger.log("info", "player.ready", "Player ready", { duration_ms: 42.5 }, "player");
```

## .NET integration

```csharp
using DebugLoggingToolkit.Extensions.Logging;
using Microsoft.Extensions.Logging;

using var factory = LoggerFactory.Create(builder =>
    builder.AddDebugLoggingToolkit(new ToolkitLoggerOptions
    {
        Path = logPath,
        Source = "app"
    }));

var logger = factory.CreateLogger("playback");
logger.LogInformation(
    "Playback started with request {request_id} in {duration_ms} ms",
    requestId,
    elapsed.TotalMilliseconds);
```

## Validation

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

cd adapters\electron
npm install
npm test

cd ..\dotnet\DebugLoggingToolkit.Extensions.Logging
dotnet build -c Release
```

## License

MIT
