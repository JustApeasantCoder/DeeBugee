# DeeBugee repository work

Use this reference only for work in the DeeBugee checkout or coordinated changes to its published surfaces.

## Scope and source of truth

The usual root is `C:\@My APPs\DeeBugee`. Current Rust packages are `dee-bugee`, `dee-bugee-core`, `dee-bugee-schema`, and `dee-bugee-rust`; Electron uses `@deebugee/electron` and the `dee-bugee:batch` IPC channel; .NET uses `DeeBugee.Extensions.Logging` and `AddDeeBugee`.

Inspect the current worktree before editing and preserve unrelated changes. Use the repository's current README and source over names or commands in this reference if they differ.

Keep cross-surface changes coordinated:

- Schema changes must be reflected in the language-neutral schema, shared Rust type/validation, affected adapters, examples/fixtures, and viewer indexing/rendering.
- Product identity changes must include executable/package metadata, UI text, adapters, docs, scripts, and schema metadata rather than only the window title.
- Tag behavior belongs in the shared `LogEvent::tag()` derivation. Keep facet indexing, table/filter/color consumers, and cross-source tests aligned; do not require producers to write a `tag` property.
- Tail-follow changes must preserve the user's vertical intent. Retain a scroll request until the viewport reaches the actual latest edge and settles; horizontal scrolling or middle-button panning must not disable vertical following.
- Bounded retention must not modify source logs, and older visible rows should remain stable while the user is inspecting history away from the latest edge.
- Repeat grouping is a table-presentation feature. Keep filters, severity semantics, event details, and filtered export operating on the underlying events rather than the collapsed group rows.
- Project mode keeps the committed `.deebugee/project.toml` definition separate from per-developer workspace state under Local AppData. Preserve relative and `%NAME%` source expansion, stable project identity, explicit `--logs` overrides, and the `--project`/`--workspace` conflict check together.

## Development and versioning

Use `RUN.bat [path-to-jsonl]` for the repository's debug rebuild/restart loop. It must not change the product version.

Do not bump versions merely to build, test, or run the app. For an explicitly requested release/version bump, use `scripts\Bump-PatchVersion.ps1` so the Rust workspace, Electron package and lockfile, and .NET package remain synchronized. Review the resulting manifest changes and regenerated lock data before committing.

## Validation

Run checks appropriate to the changed surfaces. The complete repository validation is:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

Push-Location adapters\electron
npm install
npm test
Pop-Location

dotnet build adapters\dotnet\DeeBugee.Extensions.Logging\DeeBugee.Extensions.Logging.csproj -c Release
dotnet build examples\csharp\CSharpExample.csproj -c Release
```

Run the two .NET builds sequentially because shared outputs can be temporarily locked. When only one surface changed, focused checks may be sufficient, but schema or shared-contract changes require all affected language surfaces.

For viewer behavior, supplement automated checks with a representative JSONL file such as `tests\fixtures\sample.jsonl` or `examples\showcase.jsonl`. Verify actual interaction when the change concerns dragging, scrolling, follow state, persistence, file rotation/replacement, or live tailing; compilation alone does not prove those behaviors.

Before a requested commit or release, also inspect ignored/untracked files and credential-like content, run `git diff --check` (and staged checks when applicable), and make sure the versioned surfaces are coherent. Do not publish, tag, or push unless the user requested that external action.

