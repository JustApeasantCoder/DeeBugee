# DeeBugee viewer workflows

Use this reference when configuring DeeBugee or diagnosing an application from existing JSONL logs.

## Choose the right launch surface

- **Direct logs:** open one or more `.jsonl` files, drag them into DeeBugee, open a folder containing JSONL files, or run `dee-bugee.exe <path>`. Use this for a quick one-off investigation. Adding sources merges them into the current view; **New** clears the loaded set.
- **Project mode:** run `dee-bugee.exe --project <repository>` or `dee-bugee.exe <repository>` when `.deebugee/project.toml` exists. Use this for a repository's normal log-source definition. Keep `.deebugee/` in the repository's applicable Git ignore file by default so the manifest remains developer-local. Track the manifest only when the user explicitly requests a shared definition. Each developer's filters, bookmarks, layout, and workspace state remain private under Local AppData.
- **Standalone workspace:** use `--workspace <path>` or the Workspace menu when the exact sources, filters, bookmarks, column order, colors, timestamps, repeat grouping, live-follow settings, and memory limit should be reopened together. Do not combine `--workspace` and `--project`.

In project mode, `--logs <path>` may be repeated to override manifest sources for that launch. Relative manifest sources resolve from the repository root and Windows `%NAME%` variables are supported. Prefer `%LOCALAPPDATA%` or relative paths over machine-specific absolute paths. A source can name a file that has not been created yet. Folder sources cover JSONL files directly inside that folder; do not assume recursive discovery.

Install or update one shared portable viewer per developer. Do not commit the executable, `.deebugee/` directory, personal workspace, or launcher boilerplate merely to make a project discoverable unless the user explicitly asks to share the project manifest.

## Run a structured investigation

1. Start from the named project/source set and note the loaded/invalid-record counters. Reproduce the symptom in a fresh application run when safe so `app_session_id` separates it from stale history.
2. Narrow the time and feature with fuzzy multi-term search, minimum severity, Tag, source, subsystem, target, event, provider, and status. Search terms use AND semantics and tolerate small misspellings.
3. Select a decisive row and inspect the full event details. Use **Filter By Correlation** to isolate the selected event's correlation trail; the selected value follows DeeBugee's correlation precedence.
4. Use scalar `fields.*` facets to test specific hypotheses such as a mode, attempt, route, cache state, item ID, or response code. Nested objects and arrays are searchable/details-only, so ask for a promoted scalar when repeated filtering matters.
5. Left-click a facet value to include only it, Ctrl+left-click to OR additional values in that same facet, and right-click to exclude noise. Different facets combine with AND semantics. Reset filters before concluding that events are absent from the source.
6. Pause when the visible evidence must remain stable. Pause freezes the displayed view while ingestion continues in the background; Resume refreshes it. Manual vertical scrolling suspends follow until the latest edge is reached again.
7. Turn on **Group Repeats** when repeated matching events hide the sequence. It collapses the table presentation only; level filtering and filtered export still operate on the underlying events.
8. Save a bookmark for a reusable question, a workspace for the whole investigation setup, or export the filtered events to a new JSONL evidence file. Export never modifies the source logs.

Report the evidence trail, not merely a matching message: include the app/correlation identity, relevant ordered events, missing expected transition, decisive structured values, and whether older events may have fallen outside the in-memory window.

## Use the viewer intentionally

- **Tags:** group the same feature across renderer/backend/sidecar sources when messages use a stable bracketed feature prefix.
- **Color rows by:** reveal interleaving by Source, Tag, Subsystem, Target, Event, Provider, or Correlation without changing filters.
- **Saved views:** retain complete search, severity, facet, exclusion, and correlation state for the current source set. Click to apply; Ctrl+Alt+click to replace with current filters; middle-click to rename; right-click to remove.
- **Event details and Copy:** inspect and copy the complete normalized JSON together with the message before making claims about optional fields.
- **Latest at top/bottom and Follow Latest Events:** choose the reading direction without changing event timestamps or source order.
- **Keep latest / maximum events:** bound viewer memory without touching source files. Pruning pauses while inspecting older rows, but very old events may already be outside the loaded window.
- **Timestamp display/format:** use local time for comparison with local UI/system events or UTC for cross-machine/provider analysis; the underlying timestamp is unchanged.
- **Invalid records:** treat a nonzero count as an evidence-quality problem. Identify the source and line, then check partial writes, mixed formats, unsupported schema versions, and invalid required values.

## Turn gaps into targeted instrumentation

Only add logging after the existing trail cannot distinguish the remaining hypotheses. Place the new event at a real decision or process boundary and include the identifier and state that make the branches separable. Avoid adding broad debug noise, duplicating a central logger, or logging the same payload at every layer.
