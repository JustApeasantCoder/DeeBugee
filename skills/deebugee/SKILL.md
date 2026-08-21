---
name: deebugee
description: Develop DeeBugee or use it to design, integrate, inspect, and diagnose structured local JSONL logging in Rust/Tauri, Electron, .NET, and custom applications. Use for the native viewer, its project/workspace workflow, adapters, schema, or application instrumentation intended for DeeBugee; not for unrelated remote observability systems.
license: MIT
---

# DeeBugee

DeeBugee is a native Windows viewer for structured, local, append-only JSONL diagnostics. It reads files directly. Do not introduce an HTTP service, listener, database, cloud pipeline, or renderer file access merely to support the viewer.

## Select the work mode

- **Investigate a problem with existing logs:** read [references/viewer-workflows.md](references/viewer-workflows.md). Inspect fresh events and use the viewer's structured workflow before proposing instrumentation or code changes.
- **Add or improve application logging:** read [references/instrumentation.md](references/instrumentation.md). Inspect the application's existing logging path, process boundaries, lifecycle, and representative logs first; extend the central path instead of creating a parallel logger.
- **Configure a repository for DeeBugee:** use project mode and the project-manifest guidance in [references/viewer-workflows.md](references/viewer-workflows.md). Keep the viewer installed once per developer rather than copying its executable into each repository.
- **Change the DeeBugee repository:** read [references/repository-work.md](references/repository-work.md) before changing the viewer, core, schema, adapters, documentation, versioning, or release flow.
- **Support another runtime:** follow the event contract below and the custom-writer guidance in [references/instrumentation.md](references/instrumentation.md). Reuse v1 instead of inventing a competing format.

When the checkout is available, treat its current `README.md`, `schemas/event-v1.schema.json`, shared Rust `LogEvent`, and relevant adapter as the source of truth. The usual checkout is `C:\@My APPs\DeeBugee`. Verify live files when cheap because viewer behavior and adapter APIs can evolve.

## Work evidence-first

For diagnosis, establish the failing time window, app run, feature, and expected event trail. Reproduce once when safe, then narrow by session/correlation, feature Tag, subsystem, event, level, provider/status, and useful `fields.*` facets. Distinguish these outcomes:

- the expected event never occurred;
- it occurred but carried the wrong state or identity;
- it failed and exposes a queryable cause;
- the evidence is insufficient and a specific boundary needs instrumentation.

Do not infer a root cause from severity or message prose alone. Correlate the structured trail and inspect the complete event details. If evidence is missing, add the smallest diagnostic event at the boundary that can separate the remaining hypotheses.

## Preserve the v1 event contract

Each record is one complete JSON object followed by one newline. Required properties are:

```text
schema_version: 1
timestamp: Unix epoch milliseconds or RFC 3339 text
level: trace | debug | info | warn | error | fatal
source, subsystem, event, message, app_session_id
```

Keep `app_session_id` stable for one application run. Use stable dotted event names and a bounded vocabulary for `source`, `subsystem`, `provider`, `status`, and `error_kind`.

Promote recognized diagnostic values such as `playback_session_id`, `request_id`, `session_id`, `provider`, `duration_ms`, `error_kind`, and `status` to top-level properties. Put other structured values in `fields`; scalar field values become viewer facets, while nested values remain searchable and visible in details.

The viewer selects one correlation value in this order: `playback_session_id`, `request_id`, `session_id`, then `app_session_id`. Carry the same most-specific applicable identifier across related frontend, backend, worker, and provider events.

`tag` is derived by the viewer, not written by producers. Start `message` with a consistent bracketed feature prefix such as `[Sync] Refresh completed` when cross-process feature grouping is useful. Context prefixes such as `Settings`, `Frontend`, `Backend`, `Local`, `Remote`, `Renderer`, and `Sidecar` are skipped when a more specific bracket follows; otherwise Tag falls back to `subsystem`.

Never log secrets, tokens, credentials, cookies, raw authorization headers, private keys, magnet links, or sensitive payloads. Inspect message construction as well as structured fields. Adapter redaction is a backstop, not permission to emit sensitive data.

## Validate proportionally

Use a temporary or explicitly designated log path when practical.

1. Exercise a representative success and the relevant failure or cancellation path.
2. Confirm every non-empty line is one valid v1 event and incomplete records are never exposed as complete lines.
3. Confirm session identity, correlation, duration, status, provider, and errors are queryable at the intended top level; confirm useful custom scalars appear as `fields.*` facets.
4. Open the file in DeeBugee and verify Tag grouping, filters, correlation, details, rotation/replacement, and live tailing when those behaviors are in scope.
5. Run the consuming application's focused checks and the relevant adapter checks. If interactive verification is unavailable, state that rather than claiming it.

When the DeeBugee checkout is present, validate a JSONL file with:

```powershell
cargo run -p dee-bugee-schema --example validate -- <path-to-jsonl>
```

Preserve JSONL v1 unless the user explicitly requests a coordinated schema-version change across the schema, shared types, adapters, examples, fixtures, indexing, and viewer.
