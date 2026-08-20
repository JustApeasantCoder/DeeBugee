# DeeBugee

DeeBugee is a fast, native structured-log viewer for Windows. It follows JSONL log files in real time and turns large event streams into a searchable, filterable workspace that stays responsive during active development and production diagnostics.

The viewer is built with Rust, `egui`, and `wgpu`. Logs remain on the local machine: DeeBugee does not require a browser, WebView, HTTP server, or listening port.

## Highlights

- Follow one or more append-only JSONL files in real time.
- Open files from the picker, command line, or drag and drop.
- Search messages and structured fields with fuzzy, multi-term matching.
- Filter by level, source, feature tag, subsystem, target, event, provider, status, correlation, and custom scalar fields.
- Keep up to 250,000 recent records in a bounded in-memory window.
- Reorder and resize columns, wrap messages, and inspect complete event details.
- Save filter bookmarks and reusable TOML workspaces.
- Color related events by source, feature tag, subsystem, target, event, provider, or correlation.
- Pause live following, choose whether the newest event appears at the top or bottom, and export filtered results.
- Preserve layout and viewing preferences automatically between launches.
- Produce compatible logs with the included Rust, Electron, and .NET adapters.

## Quick start

### Requirements

- Windows 10 or Windows 11
- A current stable Rust toolchain with Cargo

Node.js/npm and the .NET 8 SDK are only required when building their respective adapters or examples.

### Clone and build

```powershell
git clone https://github.com/JustApeasantCoder/DeeBugee.git
cd DeeBugee
.\BUILD.bat
```

The release executable is created at:

```text
target\release\dee-bugee.exe
```

You can also build directly with Cargo:

```powershell
cargo build --release -p dee-bugee
```

### Open a log file

Pass a JSONL file when launching DeeBugee:

```powershell
.\RUN.bat "C:\Logs\Application.jsonl"
```

Or run it directly through Cargo:

```powershell
cargo run --release -p dee-bugee -- "C:\Logs\Application.jsonl"
```

Launch without a path to start with an empty workspace:

```powershell
.\RUN.bat
```

Use **Open JSONL** to select files, drag files onto the window, or open a previously saved workspace. Multiple files can be followed at the same time.

## Using the viewer

### Search and filtering

Search terms use AND semantics across messages, event names, sources, subsystems, correlation values, optional schema properties, and structured `fields`. Terms do not need to be adjacent, and small typing mistakes are tolerated.

Facet controls provide fast structured filtering:

Feature tags are derived consistently from bracketed message prefixes and subsystem names. Context prefixes such as `Settings`, `Frontend`, `Backend`, `Local`, and `Remote` do not replace the underlying feature, so `[Segment Detection][Local]` and `[Settings][WhisperLive]` remain grouped under `Segment Detection` and `WhisperLive` respectively.

- Left-click a value to include only that value in its facet.
- Ctrl+left-click additional values to combine them with OR semantics.
- Right-click a value to exclude it.
- Click an exclusive value again to return it to neutral.
- Filters from different facets combine with AND semantics.
- Use the minimum-level selector to hide lower-severity events.

Save frequently used searches and facet combinations as bookmarks from the bar above the table. Left-click a bookmark to restore it and right-click it to remove it.

### Table and live-follow controls

- Drag column headers to reorder them.
- Drag column edges to resize them.
- Use the horizontal scrollbar when the table is wider than the window.
- Hold the middle mouse button over the table and drag to pan vertically or horizontally.
- Switch between compact and wrapped messages.
- Select a row to inspect the complete structured event.
- Pause and resume live following at any time.
- Place the latest record at the top or bottom of the table.
- Set **Keep latest** to cap the in-memory view. Committing a new limit reloads the open logs immediately so the newest window is filled without waiting for another event. Pruning pauses while you are scrolled away from the latest record so the older rows remain stable, then catches up when you return to the latest edge. Source log files are never changed.

Manual vertical scrolling pauses automatic following until the latest edge is reached again. Horizontal scrolling does not interrupt vertical following. Column order, widths, panel sizes, bookmarks, message wrapping, colors, and tail preferences are restored automatically on the next launch.

### Workspaces and export

A saved TOML workspace records the open sources, filters, bookmarks, column order, color grouping, latest-record position, and log limit. Use workspaces to return to the same diagnostic setup later or share a repeatable view with another developer.

Filtered records can be exported to a new JSONL file without modifying the source logs.

## JSONL event format

DeeBugee reads one JSON object per line. Each event must contain the following fields:

- `schema_version`
- `timestamp`
- `level`
- `source`
- `subsystem`
- `event`
- `message`
- `app_session_id`

Example:

```json
{"schema_version":1,"timestamp":1787202000000,"level":"info","source":"desktop_app","subsystem":"worker","event":"job.completed","message":"Background job completed","app_session_id":"session-42","request_id":"request-108","duration_ms":36.4,"status":"ok","fields":{"items_processed":24}}
```

Supported levels are `trace`, `debug`, `info`, `warn`, `error`, and `fatal`. Timestamps may be Unix epoch milliseconds or RFC 3339 text. Additional properties are accepted, and scalar values inside `fields` automatically become filterable facets.

The complete contract is defined in [`schemas/event-v1.schema.json`](schemas/event-v1.schema.json).

## Application integrations

The adapters are included in this repository and currently support local path-based integration.

### Rust and Tauri

Add the Rust adapter as a path dependency:

```toml
[dependencies]
dee-bugee-rust = { path = "path/to/DeeBugee/crates/toolkit-rust" }
```

Install the non-blocking `tracing` layer:

```rust
use dee_bugee_rust::{LoggerConfig, non_blocking_layer};
use tracing_subscriber::prelude::*;

let config = LoggerConfig::new(log_path, "backend");
let (deebugee_layer, logging_guard) = non_blocking_layer(config)?;

tracing_subscriber::registry()
    .with(deebugee_layer)
    .init();

tracing::info!(
    subsystem = "worker",
    event = "job.completed",
    request_id = request_id,
    duration_ms = elapsed.as_secs_f64() * 1000.0,
    "Background job completed"
);

// Keep logging_guard alive until application shutdown.
```

The same layer can be installed in a Tauri backend. Renderer events can be batched through a Tauri command and written through the shared adapter.

### Electron

Build the adapter before referencing it from an Electron application:

```powershell
cd adapters\electron
npm install
npm run build
```

Install the writer in the main process:

```ts
import { installElectronLogging } from "@deebugee/electron";

const writer = installElectronLogging(ipcMain, logPath);
```

Create a batched renderer logger in preload or another renderer bridge:

```ts
import { createRendererLogger } from "@deebugee/electron/renderer";

const logger = createRendererLogger(ipcRenderer, {
  appSessionId,
  source: "desktop_app",
});

logger.captureConsole();
logger.log(
  "info",
  "window.ready",
  "Main window is ready",
  { duration_ms: 42.5 },
  "interface",
);
```

### .NET

Reference `adapters/dotnet/DeeBugee.Extensions.Logging/DeeBugee.Extensions.Logging.csproj` from the consuming project, then register the provider:

```csharp
using DeeBugee.Extensions.Logging;
using Microsoft.Extensions.Logging;

using var factory = LoggerFactory.Create(builder =>
    builder.AddDeeBugee(new ToolkitLoggerOptions
    {
        Path = logPath,
        Source = "desktop_app"
    }));

var logger = factory.CreateLogger("worker");
logger.LogInformation(
    "Completed request {request_id} in {duration_ms} ms",
    requestId,
    elapsed.TotalMilliseconds);
```

## Development and validation

Run the complete Rust validation suite from the repository root:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Validate the Electron adapter:

```powershell
cd adapters\electron
npm install
npm test
```

Build the .NET adapter and example:

```powershell
dotnet build adapters\dotnet\DeeBugee.Extensions.Logging\DeeBugee.Extensions.Logging.csproj -c Release
dotnet build examples\csharp\CSharpExample.csproj -c Release
```

## Repository layout

```text
crates/toolkit-schema   Shared Rust event types
crates/toolkit-core     Bounded event store and bitmap facet indexes
crates/toolkit-viewer   Native viewer and JSONL follower
crates/toolkit-rust     Non-blocking tracing adapter and rotating writer
adapters/electron       Electron main-process and renderer adapter
adapters/dotnet         Microsoft.Extensions.Logging provider
schemas                 Language-neutral JSON Schema
examples                Integration examples
tests/fixtures          Test data
```

## License

DeeBugee is available under the [MIT License](LICENSE).
