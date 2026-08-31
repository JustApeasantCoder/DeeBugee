# DeeBugee instrumentation guide

Use this reference when adding, improving, or reviewing application logging for DeeBugee.

## Design from the debugging question

Inspect the current logger, log paths, rotation, process boundaries, startup/shutdown flow, and fresh output before editing. Decide which question the events must answer, then model the smallest coherent trail.

- `source`: stable producer boundary such as `renderer`, `backend`, or `sidecar`; do not use a class name or per-instance value.
- `subsystem`: stable owned area such as `sync`, `storage`, or `worker.queue`.
- `target`: optional code/module/category origin, useful when it differs from the subsystem.
- `event`: stable dotted fact such as `sync.refresh.started`, `sync.refresh.completed`, or `sync.refresh.failed`; do not embed IDs or outcomes in the event name.
- `message`: concise human summary, optionally beginning with a stable `[Feature]` prefix for Tag grouping. Do not make prose the only place where identity, outcome, or error data lives.

For an operation worth tracing, emit at meaningful transitions rather than every line of code. A useful pattern is accepted/started, a branch or external boundary when diagnostic, then completed/failed/cancelled. Put `duration_ms` on a terminal event measured from a real start. Use the application's existing bounded `status` vocabulary, and pair failures with a stable `error_kind` plus safe context.

Use levels consistently: trace for very high-volume internals, debug for diagnostic state, info for normal lifecycle/outcome facts, warn for degraded but continuing behavior, error for failed operations, and fatal only when the process cannot continue. Level is severity, not outcome; do not invent `status` from it.

## Make trails joinable and filterable

Generate one `app_session_id` at process/application startup and reuse it until shutdown. Carry the most specific shared correlation ID across process and async boundaries:

```text
playback_session_id > request_id > session_id > app_session_id
```

DeeBugee selects only the first available value in that order for its Correlation facet. If two related events carry different higher-priority IDs, correlation filtering will separate them even when a lower-priority ID matches. Propagate the same applicable higher-priority ID through the whole trail rather than generating a new one per layer.

Promote `provider`, `status`, `duration_ms`, and `error_kind` when applicable. Put other data in `fields`. Scalar strings, numbers, and booleans become discovered facets; arrays and objects remain searchable and visible in details. Prefer bounded categorical scalars such as `cache_state`, `attempt`, `route`, `result_count`, or `status_code`. Avoid unbounded field keys, large payloads, stack dumps on successful events, and values that cannot answer a debugging question.

Preserve user privacy and operational safety. Never emit secrets or private payloads in messages, field values, exception text, URLs, or headers. Redact at construction when possible and review capture-console/exception behavior because error objects and prose may contain sensitive values even when sensitive keys are scrubbed.

## Use the native adapter correctly

### Rust and Tauri

Use `dee-bugee-rust::non_blocking_layer(LoggerConfig)` with the existing `tracing` subscriber. `LoggerConfig::new` creates an app session ID and defaults to a bounded 16,384-event queue, 50 MiB active file, and four archives. For new application integrations, prefer a 50 MiB active file with one archive (two JSONL files, about 100 MiB total) unless measured diagnostic needs justify retaining more history. Keep `LoggerGuard` alive through shutdown so the worker drains.

The tracing layer promotes `playback_session_id`, `request_id`, `session_id`, `provider`, `duration_ms`, `error_kind`, and `status`; remaining tracing fields become `fields`. It reports queue pressure as `logger.events_dropped`. Use a narrow Tauri command or existing bridge for renderer batches, then write them through the backend-owned path. Do not grant renderer filesystem access.

### Electron

Install `installElectronLogging` in the main process and expose only the constrained renderer logging operation through preload/context isolation. Keep one `appSessionId` shared by the related main/renderer run, flush at important lifecycle boundaries, and retain the unhook function returned by `captureConsole()` when console interception is temporary.

The current renderer logger's fourth `log` argument becomes `fields`. Values such as `request_id`, `duration_ms`, or `status` passed there are not promoted to top-level properties and therefore do not participate in the built-in top-level correlation/status behavior. If the integration requires promoted metadata, coordinate the adapter input type, schema mapping, tests, and examples rather than duplicating fields silently.

The main writer validates events, bounds batch count/bytes, rotates the active file, and emits `logger.events_rejected` when it can report rejected renderer entries. Renderer queue overflow emits `logger.events_dropped`. Treat either warning as an instrumentation-health signal. `captureConsole()` is opt-in and can duplicate an existing logger; use it only when that tradeoff is intentional.

### .NET

Register `AddDeeBugee(ToolkitLoggerOptions)` on the existing `Microsoft.Extensions.Logging` builder. Dispose the provider or containing `LoggerFactory` so the bounded channel drains. Defaults match the Rust adapter's queue and rotation policy; apply the same one-archive recommendation for a roughly 100 MiB application log budget unless the user requests otherwise.

Use structured template properties named `subsystem`, `event`, `playback_session_id`, `request_id`, `session_id`, `provider`, `duration_ms`, `error_kind`, and `status` for promotion. The logger category becomes `target` and is the subsystem fallback; `EventId` supplies the event fallback. Exceptions supply `error_kind` when it was not provided. Remaining structured properties become redacted `fields`.

### Custom runtime

Create the parent directory and append one serialized event plus `\n` at complete-record boundaries. Serialize writers per file or otherwise guarantee records cannot interleave. Use a bounded non-blocking queue, emit a safe dropped/rejected counter when possible, rotate between complete records, retain a finite archive set, and keep write failures non-fatal to the application's primary work. Preserve file sharing compatible with a concurrent reader on Windows.

## Review and validation checklist

- One application run has stable session identity, and async/process handoffs preserve the intended correlation.
- Event names and categorical values are bounded; messages remain readable; useful scalars become facets.
- Success, failure, cancellation, timeout, retry, and fallback paths use accurate severity/outcome semantics where they exist.
- Queues, retention, rotation, shutdown flush, and write-failure behavior fit the application's volume and lifecycle. Unless the application has a measured need for deeper history, confirm retention is one active JSONL plus one archive rather than accepting a larger adapter default implicitly.
- Fresh JSONL contains complete valid v1 lines, no secrets, no avoidable duplicates, and no unexplained dropped/rejected-event warnings.
- DeeBugee can isolate the intended trail by Tag/correlation/facets and inspect complete details; filtered export contains the underlying matching events.
