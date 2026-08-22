---
name: deebugee-project-install
description: Install or update the portable DeeBugee viewer once for a Windows developer and configure a consuming repository with a local, Git-ignored .deebugee/project.toml manifest. Use for DeeBugee onboarding or project discovery; not for developing DeeBugee itself or changing application logging instrumentation.
license: MIT
---

# DeeBugee Project Install

Keep viewer installation and project configuration separate. A developer keeps
one portable executable outside application repositories. Each application may
keep one local project manifest, while filters, bookmarks, and layout stay
private under Local AppData. Ignore `.deebugee/` in Git by default. Track the
manifest only when the user explicitly requests a shared project definition.

## Gather the real inputs

Before configuring a project, identify the exact repository root and real JSONL
files or directories from its logging code or saved configuration. Do not guess
a generic log path. Inspect an existing `.deebugee/project.toml` before changing
it. In a Git repository, inspect the applicable ignore rules and tracked state.

Before creating or updating the manifest, ensure the repository's applicable
Git ignore file contains `.deebugee/`; prefer the root `.gitignore` when no more
specific convention applies, and do not add a duplicate rule. An ignore rule
does not untrack a manifest already in Git. Do not remove it from the index or
rewrite an intentional sharing policy unless the user explicitly asks; report
the tracked state instead. If the user explicitly requests a shared manifest,
that instruction overrides the default ignore policy.

## Install the viewer

Application developers do not clone the DeeBugee source repository. Use the
official `dee-bugee.exe` release asset and keep one copy at a stable user-level
location such as `%LOCALAPPDATA%\Programs\DeeBugee\dee-bugee.exe`. Do not copy
the executable, Cargo files, installer scripts, or a DeeBugee source checkout
into the consuming repository.

Prefer the repository's official `install.ps1` for installation and updates. It
selects the latest published version by default, accepts `-Version <version>`
for a specific release, verifies the asset against the SHA-256 digest returned
by the GitHub release API, and installs it under
`%LOCALAPPDATA%\Programs\DeeBugee`. Pass `-AddToPath` only when the user wants
that directory added to their user `PATH`, and report that change explicitly.
Updating replaces this shared user-level executable; it does not edit project
manifests or personal workspace state. If the official installer is unavailable,
use the release asset manually while preserving the same digest verification.

Use the documented bootstrap flow from the DeeBugee README: download
`https://raw.githubusercontent.com/JustApeasantCoder/DeeBugee/main/install.ps1`
to a temporary file, then run it in a new `powershell.exe` process with
`-NoProfile -ExecutionPolicy Bypass -File <installer>`. This process-scoped
bypass supports first-time Windows installations without changing the user's
execution policy.

## Configure a project

After installing the viewer, configure the consuming repository before opening
the viewer. Prefer the non-interactive command after gathering the real inputs;
it writes the manifest without opening a window or moving the user's mouse:

```powershell
dee-bugee.exe --configure-project . --project-id "com.example.my-application" --project-name "My Application" --source "%LOCALAPPDATA%/MyApplication/logs" --source "logs/development"
```

Supply `--source` once for every JSONL file or directory. The command refuses
to replace an existing manifest unless `--force` is supplied. Use a stable,
unique ID suitable for sharing across clones. Use Windows `%NAME%` environment
variables for machine-dependent roots and paths relative to the repository for
project-local logs. Every source must resolve to a JSONL file or a directory
containing JSONL files.

For interactive developer onboarding, launch from the repository root with
`dee-bugee.exe --project .`. When no manifest exists, the native project setup
screen suggests a name and stable ID, accepts JSONL files or folders, normalizes
selected repository paths to relative paths and Local AppData paths to
`%LOCALAPPDATA%`, then creates the manifest and opens the project. Existing
manifests can be edited through **Project > Configure Project** and require an
explicit overwrite confirmation.

When asked to configure the repository directly without launching the viewer,
create `.deebugee/project.toml` only after gathering the real inputs. Use this
v1 shape:

```toml
version = 1
id = "com.example.my-application"
name = "My Application"
sources = [
  "%LOCALAPPDATA%/MyApplication/logs",
  "logs/development",
]
```

Choose a stable, unique ID suitable for sharing across clones. Use Windows
`%NAME%` environment variables for machine-dependent roots and paths relative
to the repository for project-local logs. Every source must resolve to a JSONL
file or a directory containing JSONL files.

Launch from the repository root with `dee-bugee.exe .` or explicitly with
`dee-bugee.exe --project <root>`. `--logs <path>` is a temporary source override.
`--workspace <path>` opens a standalone saved workspace and must not be combined
with `--project`.

## Verify

Verify that the executable remains outside the application repository, the
manifest contains the intended ID/name/sources, environment variables resolve,
and `dee-bugee.exe --project <root>` loads the manifest. Unless the user
explicitly requested a shared manifest, verify that Git ignores `.deebugee/`
and that the manifest is not tracked. Personal project state belongs under
`%LOCALAPPDATA%\DeeBugee\projects`, not in Git. Launch a window only when it is
within the user's request; otherwise report the command.
