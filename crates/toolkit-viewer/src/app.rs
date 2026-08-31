use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io::{BufWriter, Write},
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use chrono::{DateTime, Local, Utc};
use crossbeam_channel::{Receiver, Sender, TryRecvError, unbounded};
use dee_bugee_core::{
    DEFAULT_MAX_EVENTS, EventStore, FacetSelection, FilterState, SearchSnapshot,
    parse_structured_query, status_text,
};
use dee_bugee_schema::{Level, LogEvent};
use eframe::egui::{
    self, Color32, FontFamily, FontId, Label, PointerButton, PopupCloseBehavior, RichText, Sense,
    Stroke, TextStyle,
    text::{LayoutJob, TextFormat},
};
use egui_extras::{Column, TableBuilder};
use serde::{Deserialize, Serialize};

use crate::follower::{ReaderCommand, ReaderHandle, ReaderMessage, spawn_reader};
use crate::update::{self, AvailableUpdate, CheckResult};

const DISPLAYED_FACETS: [&str; 9] = [
    "level",
    "source",
    "tag",
    "subsystem",
    "target",
    "event",
    "provider",
    "status",
    "correlation",
];
const PREFERENCES_KEY: &str = "dee_bugee.viewer_preferences.v1";
const TAIL_HEADROOM_ROWS: f32 = 2.5;
const LATEST_SETTLE_FRAMES: u8 = 2;
const SEARCH_DEBOUNCE: Duration = Duration::from_millis(200);
const MAX_CONFIGURABLE_EVENTS: usize = 5_000_000;
const DEFAULT_TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f %:z";
const DEFAULT_UI_SCALE: f32 = 1.0;
const MIN_UI_SCALE: f32 = 0.75;
const MAX_UI_SCALE: f32 = 1.50;

const SURFACE_0: Color32 = Color32::from_rgb(13, 16, 22);
const SURFACE_1: Color32 = Color32::from_rgb(18, 22, 29);
const SURFACE_2: Color32 = Color32::from_rgb(25, 30, 39);
const BORDER: Color32 = Color32::from_rgb(43, 50, 63);
const TEXT_PRIMARY: Color32 = Color32::from_rgb(225, 230, 238);
const TEXT_MUTED: Color32 = Color32::from_rgb(139, 149, 166);
const ACCENT: Color32 = Color32::from_rgb(92, 139, 255);
const ACCENT_SOFT: Color32 = Color32::from_rgb(34, 53, 91);
const SUCCESS: Color32 = Color32::from_rgb(87, 205, 148);
const WARNING: Color32 = Color32::from_rgb(240, 184, 82);
const DANGER: Color32 = Color32::from_rgb(242, 108, 122);

/// Launch inputs keep the shared project definition separate from private
/// workspace state and backwards-compatible direct log paths.
#[derive(Debug, Clone, Default)]
pub struct LaunchRequest {
    pub workspace_path: Option<PathBuf>,
    pub project_root: Option<PathBuf>,
    pub log_paths: Vec<PathBuf>,
}

impl LaunchRequest {
    pub fn new(
        workspace_path: Option<PathBuf>,
        project_root: Option<PathBuf>,
        log_paths: Vec<PathBuf>,
    ) -> Self {
        Self {
            workspace_path,
            project_root,
            log_paths,
        }
    }

    pub fn window_title(&self) -> String {
        self.project_root
            .as_deref()
            .and_then(project_display_name)
            .or_else(|| {
                self.workspace_path
                    .as_deref()
                    .and_then(workspace_display_name)
            })
            .map(|name| format!("DeeBugee — {name}"))
            .unwrap_or_else(|| "DeeBugee".to_string())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ProjectConfig {
    version: u16,
    id: String,
    name: String,
    sources: Vec<String>,
}

/// Inputs for creating or replacing the shareable project manifest without
/// launching the native setup window.
pub struct ProjectConfiguration {
    pub root: PathBuf,
    pub id: String,
    pub name: String,
    pub sources: Vec<String>,
    pub overwrite: bool,
}

pub fn configure_project(configuration: ProjectConfiguration) -> Result<PathBuf, String> {
    let root = configuration.root.canonicalize().map_err(|error| {
        format!(
            "Unable to resolve project root {}: {error}",
            configuration.root.display()
        )
    })?;
    if !root.is_dir() {
        return Err(format!(
            "Project root {} is not a directory",
            root.display()
        ));
    }

    let sources = configuration
        .sources
        .iter()
        .map(|source| manifest_source_argument(&root, source))
        .collect();
    let config = ProjectConfig {
        version: 1,
        id: configuration.id.trim().to_string(),
        name: configuration.name.trim().to_string(),
        sources,
    };
    write_project_manifest(&root, &config, configuration.overwrite)
}

struct ProjectLaunch {
    workspace_path: PathBuf,
    sources: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
struct ProjectSetup {
    root: PathBuf,
    name: String,
    id: String,
    sources: Vec<String>,
    editing_existing: bool,
    confirm_overwrite: bool,
    error: Option<String>,
}

impl ProjectSetup {
    fn new(root: PathBuf) -> Self {
        let root = root.canonicalize().unwrap_or(root);
        let name = root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("My Application")
            .to_string();
        let id = suggested_project_id(&root, &name);
        Self {
            root,
            name,
            id,
            sources: vec![String::new()],
            editing_existing: false,
            confirm_overwrite: false,
            error: None,
        }
    }

    fn from_config(root: PathBuf, config: ProjectConfig) -> Self {
        Self {
            root,
            name: config.name,
            id: config.id,
            sources: config.sources,
            editing_existing: true,
            confirm_overwrite: false,
            error: None,
        }
    }

    fn config(&self) -> ProjectConfig {
        ProjectConfig {
            version: 1,
            id: self.id.trim().to_string(),
            name: self.name.trim().to_string(),
            sources: self
                .sources
                .iter()
                .map(|source| source.trim().to_string())
                .filter(|source| !source.is_empty())
                .collect(),
        }
    }
}

fn project_display_name(path: &Path) -> Option<String> {
    let manifest_path = project_manifest_path(path);
    if let Ok(text) = std::fs::read_to_string(&manifest_path)
        && let Ok(config) = toml::from_str::<ProjectConfig>(&text)
        && !config.name.trim().is_empty()
    {
        return Some(config.name.trim().to_owned());
    }
    let root = if path.is_file() {
        let parent = path.parent()?;
        if parent.file_name().is_some_and(|name| name == ".deebugee") {
            parent.parent()?
        } else {
            parent
        }
    } else {
        path
    };
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    root.file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
}

fn project_manifest_path(path: &Path) -> PathBuf {
    if path.is_file() {
        path.to_path_buf()
    } else {
        path.join(".deebugee").join("project.toml")
    }
}

fn project_root(path: &Path) -> PathBuf {
    let manifest_path = project_manifest_path(path);
    let root = project_root_from_manifest(&manifest_path).unwrap_or_else(|_| path.to_path_buf());
    root.canonicalize().unwrap_or(root)
}

fn project_root_from_manifest(manifest_path: &Path) -> Result<PathBuf, String> {
    let manifest_parent = manifest_path
        .parent()
        .ok_or_else(|| format!("Project manifest {} has no parent", manifest_path.display()))?;
    if manifest_parent
        .file_name()
        .is_some_and(|name| name == ".deebugee")
    {
        manifest_parent
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| {
                format!(
                    "Project manifest {} has no project root",
                    manifest_path.display()
                )
            })
    } else {
        Ok(manifest_parent.to_path_buf())
    }
}

fn load_project(path: &Path) -> Result<ProjectLaunch, String> {
    let manifest_path = project_manifest_path(path);
    let project_root = project_root_from_manifest(&manifest_path)?;
    let config = load_project_config(&manifest_path)?;
    project_launch_from_config(&project_root, &manifest_path, &config)
}

fn load_project_config(manifest_path: &Path) -> Result<ProjectConfig, String> {
    let text = std::fs::read_to_string(manifest_path).map_err(|error| {
        format!(
            "Unable to read project manifest {}: {error}",
            manifest_path.display()
        )
    })?;
    toml::from_str::<ProjectConfig>(&text).map_err(|error| {
        format!(
            "Unable to parse project manifest {}: {error}",
            manifest_path.display()
        )
    })
}

fn project_launch_from_config(
    project_root: &Path,
    manifest_path: &Path,
    config: &ProjectConfig,
) -> Result<ProjectLaunch, String> {
    if config.version != 1 {
        return Err(format!(
            "Unsupported project manifest version {} in {}",
            config.version,
            manifest_path.display()
        ));
    }
    if config.id.trim().is_empty() || config.name.trim().is_empty() {
        return Err(format!(
            "Project manifest {} requires non-empty id and name values",
            manifest_path.display()
        ));
    }
    if config.sources.is_empty() {
        return Err(format!(
            "Project manifest {} requires at least one source",
            manifest_path.display()
        ));
    }
    let sources = config
        .sources
        .iter()
        .map(|source| resolve_project_source(project_root, source))
        .collect::<Result<Vec<_>, String>>()?;
    let local_app_data = std::env::var_os("LOCALAPPDATA")
        .ok_or_else(|| "LOCALAPPDATA is unavailable; cannot store project workspace".to_string())?;
    let workspace_path = PathBuf::from(local_app_data)
        .join("DeeBugee")
        .join("projects")
        .join(stable_project_key(config.id.trim()))
        .join("workspace.toml");
    Ok(ProjectLaunch {
        workspace_path,
        sources,
    })
}

fn resolve_project_source(project_root: &Path, source: &str) -> Result<PathBuf, String> {
    let expanded = expand_environment_variables(source, |name| std::env::var_os(name))?;
    let path = PathBuf::from(expanded);
    Ok(if path.is_absolute() {
        path
    } else {
        project_root.join(path)
    })
}

fn write_project_manifest(
    root: &Path,
    config: &ProjectConfig,
    overwrite: bool,
) -> Result<PathBuf, String> {
    let manifest_path = project_manifest_path(root);
    project_launch_from_config(root, &manifest_path, config)?;
    let parent = manifest_path
        .parent()
        .ok_or_else(|| format!("Project manifest {} has no parent", manifest_path.display()))?;
    std::fs::create_dir_all(parent).map_err(|error| {
        format!(
            "Unable to create project configuration folder {}: {error}",
            parent.display()
        )
    })?;
    let text = toml::to_string_pretty(config)
        .map_err(|error| format!("Unable to serialize project manifest: {error}"))?;
    let mut options = OpenOptions::new();
    options.write(true);
    if overwrite {
        options.create(true).truncate(true);
    } else {
        options.create_new(true);
    }
    let mut file = options.open(&manifest_path).map_err(|error| {
        format!(
            "Unable to create project manifest {}: {error}",
            manifest_path.display()
        )
    })?;
    file.write_all(text.as_bytes()).map_err(|error| {
        format!(
            "Unable to write project manifest {}: {error}",
            manifest_path.display()
        )
    })?;
    file.flush().map_err(|error| {
        format!(
            "Unable to flush project manifest {}: {error}",
            manifest_path.display()
        )
    })?;
    Ok(manifest_path)
}

fn manifest_source_path(project_root: &Path, selected_path: &Path) -> String {
    let selected = selected_path
        .canonicalize()
        .unwrap_or_else(|_| selected_path.to_path_buf());
    let root = project_root
        .canonicalize()
        .unwrap_or_else(|_| project_root.to_path_buf());
    if let Ok(relative) = selected.strip_prefix(&root) {
        return if relative.as_os_str().is_empty() {
            ".".to_string()
        } else {
            portable_path(relative)
        };
    }
    if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") {
        let local_app_data = PathBuf::from(local_app_data);
        let local_app_data = local_app_data.canonicalize().unwrap_or(local_app_data);
        if let Ok(relative) = selected.strip_prefix(local_app_data) {
            let relative = portable_path(relative);
            return if relative.is_empty() {
                "%LOCALAPPDATA%".to_string()
            } else {
                format!("%LOCALAPPDATA%/{relative}")
            };
        }
    }
    portable_path(&selected)
}

fn manifest_source_argument(project_root: &Path, source: &str) -> String {
    let source = source.trim();
    if source.starts_with('%') {
        return source.replace('\\', "/");
    }
    let path = PathBuf::from(source);
    if path.is_absolute() {
        manifest_source_path(project_root, &path)
    } else {
        portable_path(&path)
    }
}

fn portable_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn display_path(path: &Path) -> String {
    let path = path.display().to_string();
    if let Some(network_path) = path.strip_prefix(r"\\?\UNC\") {
        format!(r"\\{network_path}")
    } else {
        path.strip_prefix(r"\\?\").unwrap_or(&path).to_string()
    }
}

fn add_project_source(sources: &mut Vec<String>, source: String) {
    if let Some(empty_source) = sources.iter_mut().find(|source| source.trim().is_empty()) {
        *empty_source = source;
    } else {
        sources.push(source);
    }
}

fn suggested_project_id(root: &Path, project_name: &str) -> String {
    git_origin_url(root)
        .as_deref()
        .and_then(project_id_from_remote)
        .unwrap_or_else(|| format!("local.{}", id_component(project_name)))
}

fn git_origin_url(root: &Path) -> Option<String> {
    let text = std::fs::read_to_string(root.join(".git").join("config")).ok()?;
    let mut in_origin = false;
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_origin = line.eq_ignore_ascii_case("[remote \"origin\"]");
        } else if in_origin
            && let Some((key, value)) = line.split_once('=')
            && key.trim().eq_ignore_ascii_case("url")
        {
            return Some(value.trim().to_string());
        }
    }
    None
}

fn project_id_from_remote(remote: &str) -> Option<String> {
    let remote = remote.trim().trim_end_matches('/').trim_end_matches(".git");
    let (host, path) = if let Some((_, remainder)) = remote.split_once("://") {
        let (authority, path) = remainder.split_once('/')?;
        (
            authority.rsplit('@').next()?.split(':').next()?,
            path.to_string(),
        )
    } else {
        let (authority, path) = remote.split_once(':')?;
        (authority.rsplit('@').next()?, path.to_string())
    };
    let parts = host
        .split('.')
        .rev()
        .chain(path.split('/'))
        .map(id_component)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    (!parts.is_empty()).then(|| parts.join("."))
}

fn id_component(value: &str) -> String {
    let mut result = String::new();
    let mut previous_was_separator = false;
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            result.push(character);
            previous_was_separator = false;
        } else if !previous_was_separator && !result.is_empty() {
            result.push('-');
            previous_was_separator = true;
        }
    }
    result.trim_matches('-').to_string()
}

fn expand_environment_variables<F>(value: &str, mut lookup: F) -> Result<String, String>
where
    F: FnMut(&str) -> Option<std::ffi::OsString>,
{
    let mut expanded = String::with_capacity(value.len());
    let mut remainder = value;
    while let Some(start) = remainder.find('%') {
        expanded.push_str(&remainder[..start]);
        let variable = &remainder[start + 1..];
        let Some(end) = variable.find('%') else {
            return Err(format!(
                "Unclosed environment variable in project source: {value}"
            ));
        };
        let name = &variable[..end];
        if name.is_empty() {
            return Err(format!(
                "Empty environment variable in project source: {value}"
            ));
        }
        let replacement =
            lookup(name).ok_or_else(|| format!("Environment variable %{name}% is unavailable"))?;
        expanded.push_str(&replacement.to_string_lossy());
        remainder = &variable[end + 1..];
    }
    expanded.push_str(remainder);
    Ok(expanded)
}

fn stable_project_key(id: &str) -> String {
    let hash = id
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    format!("{hash:016x}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MessageTone {
    Positive,
    Negative,
    Warning,
    Activity,
}

impl MessageTone {
    const fn color(self) -> Color32 {
        match self {
            Self::Positive => SUCCESS,
            Self::Negative => DANGER,
            Self::Warning => WARNING,
            Self::Activity => ACCENT,
        }
    }
}

// Longer phrases deliberately come first. This prevents a positive substring such as
// "found" from winning inside a negative status such as "not found".
const MESSAGE_HIGHLIGHTS: &[(MessageTone, &str)] = &[
    (MessageTone::Negative, "not found"),
    (MessageTone::Negative, "not configured"),
    (MessageTone::Negative, "not initialized"),
    (MessageTone::Negative, "not connected"),
    (MessageTone::Negative, "not ready"),
    (MessageTone::Negative, "not authorized"),
    (MessageTone::Negative, "not supported"),
    (MessageTone::Negative, "not implemented"),
    (MessageTone::Negative, "no results"),
    (MessageTone::Negative, "no response"),
    (MessageTone::Negative, "no data"),
    (MessageTone::Negative, "no connection"),
    (MessageTone::Negative, "no match"),
    (MessageTone::Negative, "not available"),
    (MessageTone::Negative, "access denied"),
    (MessageTone::Negative, "permission denied"),
    (MessageTone::Negative, "connection refused"),
    (MessageTone::Negative, "connection failed"),
    (MessageTone::Negative, "network error"),
    (MessageTone::Negative, "request failed"),
    (MessageTone::Negative, "operation failed"),
    (MessageTone::Negative, "validation failed"),
    (MessageTone::Negative, "authentication failed"),
    (MessageTone::Negative, "authentication required"),
    (MessageTone::Negative, "service unavailable"),
    (MessageTone::Negative, "out of memory"),
    (MessageTone::Negative, "out of space"),
    (MessageTone::Negative, "timed out"),
    (MessageTone::Negative, "could not"),
    (MessageTone::Negative, "unable to"),
    (MessageTone::Negative, "failed"),
    (MessageTone::Negative, "failure"),
    (MessageTone::Negative, "error"),
    (MessageTone::Negative, "fatal"),
    (MessageTone::Negative, "exception"),
    (MessageTone::Negative, "panic"),
    (MessageTone::Negative, "crashed"),
    (MessageTone::Negative, "aborted"),
    (MessageTone::Negative, "cancelled"),
    (MessageTone::Negative, "canceled"),
    (MessageTone::Negative, "denied"),
    (MessageTone::Negative, "forbidden"),
    (MessageTone::Negative, "unauthorized"),
    (MessageTone::Negative, "rejected"),
    (MessageTone::Negative, "invalid"),
    (MessageTone::Negative, "missing"),
    (MessageTone::Negative, "unavailable"),
    (MessageTone::Negative, "disconnected"),
    (MessageTone::Negative, "offline"),
    (MessageTone::Negative, "corrupted"),
    (MessageTone::Negative, "mismatch"),
    (MessageTone::Negative, "blocked"),
    (MessageTone::Negative, "exceeded"),
    (MessageTone::Negative, "expired"),
    (MessageTone::Negative, "timeout"),
    (MessageTone::Negative, "unsupported"),
    (MessageTone::Negative, "unreachable"),
    (MessageTone::Negative, "overloaded"),
    (MessageTone::Negative, "malformed"),
    (MessageTone::Negative, "duplicate"),
    (MessageTone::Warning, "warning"),
    (MessageTone::Warning, "warn"),
    (MessageTone::Warning, "retrying"),
    (MessageTone::Warning, "retry"),
    (MessageTone::Warning, "delayed"),
    (MessageTone::Warning, "pending"),
    (MessageTone::Warning, "partial"),
    (MessageTone::Warning, "skipped"),
    (MessageTone::Warning, "deprecated"),
    (MessageTone::Warning, "rate limited"),
    (MessageTone::Warning, "throttled"),
    (MessageTone::Warning, "in progress"),
    (MessageTone::Warning, "waiting"),
    (MessageTone::Warning, "paused"),
    (MessageTone::Warning, "degraded"),
    (MessageTone::Warning, "limited"),
    (MessageTone::Warning, "fallback"),
    (MessageTone::Warning, "ignored"),
    (MessageTone::Warning, "unchanged"),
    (MessageTone::Warning, "slow"),
    (MessageTone::Positive, "successfully"),
    (MessageTone::Positive, "successful"),
    (MessageTone::Positive, "succeeded"),
    (MessageTone::Positive, "completed"),
    (MessageTone::Positive, "complete"),
    (MessageTone::Positive, "connected"),
    (MessageTone::Positive, "available"),
    (MessageTone::Positive, "validated"),
    (MessageTone::Positive, "verified"),
    (MessageTone::Positive, "detected"),
    (MessageTone::Positive, "resolved"),
    (MessageTone::Positive, "recovered"),
    (MessageTone::Positive, "accepted"),
    (MessageTone::Positive, "authorized"),
    (MessageTone::Positive, "enabled"),
    (MessageTone::Positive, "ready"),
    (MessageTone::Positive, "healthy"),
    (MessageTone::Positive, "initialized"),
    (MessageTone::Positive, "configured"),
    (MessageTone::Positive, "registered"),
    (MessageTone::Positive, "synchronized"),
    (MessageTone::Positive, "synced"),
    (MessageTone::Positive, "imported"),
    (MessageTone::Positive, "exported"),
    (MessageTone::Positive, "uploaded"),
    (MessageTone::Positive, "downloaded"),
    (MessageTone::Positive, "installed"),
    (MessageTone::Positive, "cleared"),
    (MessageTone::Positive, "removed"),
    (MessageTone::Positive, "released"),
    (MessageTone::Positive, "found"),
    (MessageTone::Positive, "success"),
    (MessageTone::Positive, "passed"),
    (MessageTone::Positive, "done"),
    (MessageTone::Positive, "created"),
    (MessageTone::Positive, "saved"),
    (MessageTone::Positive, "loaded"),
    (MessageTone::Positive, "started"),
    (MessageTone::Activity, "triggered"),
    (MessageTone::Activity, "trigger"),
    (MessageTone::Activity, "attempting"),
    (MessageTone::Activity, "initiated"),
    (MessageTone::Activity, "scheduled"),
    (MessageTone::Activity, "dispatching"),
    (MessageTone::Activity, "dispatched"),
    (MessageTone::Activity, "enqueued"),
    (MessageTone::Activity, "processing"),
    (MessageTone::Activity, "running"),
    (MessageTone::Activity, "queued"),
    (MessageTone::Activity, "received"),
    (MessageTone::Activity, "sending"),
    (MessageTone::Activity, "fetching"),
    (MessageTone::Activity, "searching"),
    (MessageTone::Activity, "scanning"),
    (MessageTone::Activity, "updating"),
    (MessageTone::Activity, "reloading"),
    (MessageTone::Activity, "listening"),
    (MessageTone::Activity, "watching"),
    (MessageTone::Activity, "discovering"),
    (MessageTone::Activity, "connecting"),
];

fn configure_ui(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();
    install_system_font(
        &mut fonts,
        FontFamily::Proportional,
        "dee_bugee_ui",
        &["Inter-Regular.ttf", "segoeui.ttf"],
    );
    install_system_font(
        &mut fonts,
        FontFamily::Monospace,
        "dee_bugee_mono",
        &["CascadiaMono.ttf", "consola.ttf"],
    );
    ctx.set_fonts(fonts);

    let mut visuals = egui::Visuals::dark();
    visuals.panel_fill = SURFACE_0;
    visuals.window_fill = SURFACE_1;
    visuals.extreme_bg_color = Color32::from_rgb(10, 13, 18);
    visuals.text_edit_bg_color = Some(Color32::from_rgb(11, 14, 20));
    visuals.faint_bg_color = Color32::from_rgb(16, 20, 27);
    visuals.code_bg_color = Color32::from_rgb(11, 14, 19);
    visuals.override_text_color = Some(TEXT_PRIMARY);
    visuals.weak_text_color = Some(TEXT_MUTED);
    visuals.selection.bg_fill = ACCENT_SOFT;
    visuals.selection.stroke = Stroke::new(1.0, Color32::from_rgb(177, 197, 255));
    visuals.warn_fg_color = WARNING;
    visuals.error_fg_color = DANGER;
    visuals.window_stroke = Stroke::new(1.0, BORDER);
    visuals.widgets.noninteractive.bg_fill = SURFACE_1;
    visuals.widgets.noninteractive.weak_bg_fill = SURFACE_1;
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, BORDER);
    visuals.widgets.noninteractive.fg_stroke = Stroke::new(1.0, TEXT_PRIMARY);
    visuals.widgets.inactive.bg_fill = SURFACE_2;
    visuals.widgets.inactive.weak_bg_fill = SURFACE_2;
    visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, BORDER);
    visuals.widgets.inactive.fg_stroke = Stroke::new(1.0, TEXT_PRIMARY);
    visuals.widgets.hovered.bg_fill = Color32::from_rgb(35, 42, 54);
    visuals.widgets.hovered.weak_bg_fill = Color32::from_rgb(35, 42, 54);
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.0, Color32::from_rgb(68, 80, 101));
    visuals.widgets.hovered.fg_stroke = Stroke::new(1.0, Color32::WHITE);
    visuals.widgets.active.bg_fill = ACCENT_SOFT;
    visuals.widgets.active.weak_bg_fill = ACCENT_SOFT;
    visuals.widgets.active.bg_stroke = Stroke::new(1.0, ACCENT);
    visuals.widgets.active.fg_stroke = Stroke::new(1.0, Color32::WHITE);
    visuals.widgets.open = visuals.widgets.active;

    ctx.set_visuals(visuals);
    ctx.all_styles_mut(|style| {
        style.animation_time = 0.10;
        style.spacing.item_spacing = egui::vec2(7.0, 6.0);
        style.spacing.button_padding = egui::vec2(10.0, 5.0);
        style.spacing.interact_size = egui::vec2(38.0, 28.0);
        style.spacing.indent = 16.0;
        style.text_styles = [
            (
                TextStyle::Heading,
                FontId::new(20.0, FontFamily::Proportional),
            ),
            (TextStyle::Body, FontId::new(14.0, FontFamily::Proportional)),
            (
                TextStyle::Button,
                FontId::new(13.0, FontFamily::Proportional),
            ),
            (
                TextStyle::Small,
                FontId::new(11.0, FontFamily::Proportional),
            ),
            (
                TextStyle::Monospace,
                FontId::new(13.0, FontFamily::Monospace),
            ),
        ]
        .into();
    });
}

fn install_system_font(
    fonts: &mut egui::FontDefinitions,
    family: FontFamily,
    name: &str,
    candidates: &[&str],
) {
    let Some(bytes) = load_windows_font(candidates) else {
        return;
    };
    fonts.font_data.insert(
        name.to_string(),
        Arc::new(egui::FontData::from_owned(bytes)),
    );
    fonts
        .families
        .entry(family)
        .or_default()
        .insert(0, name.to_string());
}

fn load_windows_font(candidates: &[&str]) -> Option<Vec<u8>> {
    let mut font_directories = Vec::new();
    if let Some(windows_dir) = std::env::var_os("WINDIR") {
        font_directories.push(PathBuf::from(windows_dir).join("Fonts"));
    }
    if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") {
        font_directories.push(
            PathBuf::from(local_app_data)
                .join("Microsoft")
                .join("Windows")
                .join("Fonts"),
        );
    }

    font_directories.iter().find_map(|directory| {
        candidates
            .iter()
            .find_map(|candidate| std::fs::read(directory.join(candidate)).ok())
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TableColumn {
    Timestamp,
    RelativeTime,
    Level,
    Source,
    Tag,
    Subsystem,
    Event,
    Provider,
    Correlation,
    Duration,
    Status,
    Message,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
enum TimestampDisplay {
    #[default]
    Local,
    Utc,
}

impl TimestampDisplay {
    const ALL: [Self; 2] = [Self::Local, Self::Utc];

    const fn title(self) -> &'static str {
        match self {
            Self::Local => "Local Time",
            Self::Utc => "UTC",
        }
    }

    fn format(self, event: &LogEvent, format: &str) -> String {
        match self {
            Self::Local => event
                .timestamp
                .with_timezone(&Local)
                .format(format)
                .to_string(),
            Self::Utc => event.timestamp.format(format).to_string(),
        }
    }

    fn format_timestamp(self, timestamp: DateTime<Utc>, format: &str) -> String {
        match self {
            Self::Local => timestamp.with_timezone(&Local).format(format).to_string(),
            Self::Utc => timestamp.format(format).to_string(),
        }
    }
}

fn default_timestamp_format() -> String {
    DEFAULT_TIMESTAMP_FORMAT.to_string()
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ColorBy {
    #[default]
    Off,
    Source,
    Tag,
    Subsystem,
    Target,
    Event,
    Provider,
    Correlation,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LatestAt {
    #[default]
    Bottom,
    Top,
}

#[derive(Clone, Copy)]
enum LatestNavigationAction {
    JumpButton,
    FollowEnabled,
}

impl LatestNavigationAction {
    fn event_value(self) -> &'static str {
        match self {
            Self::JumpButton => "jump_button",
            Self::FollowEnabled => "follow_enabled",
        }
    }
}

impl LatestAt {
    const ALL: [Self; 2] = [Self::Bottom, Self::Top];

    const fn title(self) -> &'static str {
        match self {
            Self::Bottom => "Bottom",
            Self::Top => "Top",
        }
    }
}

impl ColorBy {
    const ALL: [Self; 8] = [
        Self::Off,
        Self::Source,
        Self::Tag,
        Self::Subsystem,
        Self::Target,
        Self::Event,
        Self::Provider,
        Self::Correlation,
    ];

    fn title(self) -> &'static str {
        match self {
            Self::Off => "Off",
            Self::Source => "Source",
            Self::Tag => "Tag",
            Self::Subsystem => "Subsystem",
            Self::Target => "Target",
            Self::Event => "Event",
            Self::Provider => "Provider",
            Self::Correlation => "Correlation",
        }
    }

    fn event_value(self, event: &LogEvent) -> Option<String> {
        match self {
            Self::Off => None,
            Self::Source => Some(event.source.clone()),
            Self::Tag => Some(event.tag()),
            Self::Subsystem => Some(event.subsystem.clone()),
            Self::Target => event.target.clone(),
            Self::Event => Some(event.event.clone()),
            Self::Provider => event.provider.clone(),
            Self::Correlation => Some(event.correlation_id().to_string()),
        }
        .filter(|value| !value.is_empty() && value != "-")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FilterBookmark {
    name: String,
    filter: FilterState,
}

#[derive(Debug, Clone)]
struct BookmarkRename {
    index: usize,
    name: String,
}

/// A collapsed run of equivalent events in the current filtered view.
/// The newest occurrence remains the table representative, so a live event burst
/// stays at the newest point in the log rather than being stranded at its first row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ErrorGroup {
    count: usize,
    first_index: usize,
    previous_index: Option<usize>,
    latest_index: usize,
}

#[derive(Debug, Clone, Default)]
struct SessionEventSummary {
    count: usize,
    warning_count: usize,
    error_count: usize,
    duration_total_ms: f64,
    duration_count: usize,
    max_duration_ms: Option<f64>,
    last_status: Option<(DateTime<Utc>, String)>,
}

impl SessionEventSummary {
    fn observe(&mut self, event: &LogEvent) {
        self.count += 1;
        match event.level {
            Level::Warn => self.warning_count += 1,
            Level::Error | Level::Fatal => self.error_count += 1,
            Level::Trace | Level::Debug | Level::Info => {}
        }
        if let Some(duration_ms) = event.duration_ms {
            self.duration_total_ms += duration_ms;
            self.duration_count += 1;
            self.max_duration_ms = Some(
                self.max_duration_ms
                    .map_or(duration_ms, |current| current.max(duration_ms)),
            );
        }
        let status = status_text(event.status.as_ref());
        if status != "-"
            && self
                .last_status
                .as_ref()
                .is_none_or(|(timestamp, _)| event.timestamp >= *timestamp)
        {
            self.last_status = Some((event.timestamp, status));
        }
    }

    fn average_duration_ms(&self) -> Option<f64> {
        (self.duration_count > 0).then(|| self.duration_total_ms / self.duration_count as f64)
    }

    fn status(&self) -> Option<&str> {
        self.last_status.as_ref().map(|(_, status)| status.as_str())
    }
}

#[derive(Debug, Clone)]
struct SessionSummary {
    id: String,
    first_timestamp: DateTime<Utc>,
    last_timestamp: DateTime<Utc>,
    event_count: usize,
    warning_count: usize,
    error_count: usize,
    providers: BTreeSet<String>,
    correlations: BTreeSet<String>,
    events: BTreeMap<String, SessionEventSummary>,
    last_status: Option<(DateTime<Utc>, String)>,
}

impl SessionSummary {
    fn new(event: &LogEvent) -> Self {
        let mut summary = Self {
            id: event.app_session_id.clone(),
            first_timestamp: event.timestamp,
            last_timestamp: event.timestamp,
            event_count: 0,
            warning_count: 0,
            error_count: 0,
            providers: BTreeSet::new(),
            correlations: BTreeSet::new(),
            events: BTreeMap::new(),
            last_status: None,
        };
        summary.observe(event);
        summary
    }

    fn observe(&mut self, event: &LogEvent) {
        self.first_timestamp = self.first_timestamp.min(event.timestamp);
        self.last_timestamp = self.last_timestamp.max(event.timestamp);
        self.event_count += 1;
        match event.level {
            Level::Warn => self.warning_count += 1,
            Level::Error | Level::Fatal => self.error_count += 1,
            Level::Trace | Level::Debug | Level::Info => {}
        }
        if let Some(provider) = event.provider.as_deref().filter(|value| !value.is_empty()) {
            self.providers.insert(provider.to_string());
        }
        let correlation = event.correlation_id();
        if correlation != event.app_session_id && !correlation.is_empty() {
            self.correlations.insert(correlation.to_string());
        }
        let status = status_text(event.status.as_ref());
        if status != "-"
            && self
                .last_status
                .as_ref()
                .is_none_or(|(timestamp, _)| event.timestamp >= *timestamp)
        {
            self.last_status = Some((event.timestamp, status));
        }
        self.events
            .entry(event.event.clone())
            .or_default()
            .observe(event);
    }

    fn elapsed_ms(&self) -> i64 {
        (self.last_timestamp - self.first_timestamp)
            .num_milliseconds()
            .max(0)
    }

    fn status(&self) -> Option<&str> {
        self.last_status.as_ref().map(|(_, status)| status.as_str())
    }
}

#[derive(Debug, Default)]
struct SessionExplorerState {
    open: bool,
    summaries: BTreeMap<String, SessionSummary>,
    compare_a: Option<String>,
    compare_b: Option<String>,
}

impl SessionExplorerState {
    fn observe_many(&mut self, events: &[LogEvent]) {
        for event in events {
            match self.summaries.get_mut(&event.app_session_id) {
                Some(summary) => summary.observe(event),
                None => {
                    self.summaries
                        .insert(event.app_session_id.clone(), SessionSummary::new(event));
                }
            }
        }
    }

    fn rebuild(&mut self, store: &EventStore) {
        self.summaries.clear();
        for index in 0..store.len() {
            if let Some(event) = store.get(index) {
                match self.summaries.get_mut(&event.app_session_id) {
                    Some(summary) => summary.observe(event),
                    None => {
                        self.summaries
                            .insert(event.app_session_id.clone(), SessionSummary::new(event));
                    }
                }
            }
        }
        self.retain_valid_selection();
    }

    fn reset(&mut self) {
        self.summaries.clear();
        self.compare_a = None;
        self.compare_b = None;
    }

    fn retain_valid_selection(&mut self) {
        if self
            .compare_a
            .as_ref()
            .is_some_and(|id| !self.summaries.contains_key(id))
        {
            self.compare_a = None;
        }
        if self
            .compare_b
            .as_ref()
            .is_some_and(|id| !self.summaries.contains_key(id))
        {
            self.compare_b = None;
        }
    }

    fn sorted_summaries(&self) -> Vec<SessionSummary> {
        let mut summaries: Vec<_> = self.summaries.values().cloned().collect();
        summaries.sort_by(|left, right| {
            right
                .last_timestamp
                .cmp(&left.last_timestamp)
                .then_with(|| left.id.cmp(&right.id))
        });
        summaries
    }
}

#[derive(Debug, Clone, PartialEq)]
struct SessionComparisonRow {
    event: String,
    count_a: usize,
    count_b: usize,
    warnings_a: usize,
    warnings_b: usize,
    errors_a: usize,
    errors_b: usize,
    average_duration_a: Option<f64>,
    average_duration_b: Option<f64>,
    max_duration_a: Option<f64>,
    max_duration_b: Option<f64>,
    status_a: Option<String>,
    status_b: Option<String>,
}

impl SessionComparisonRow {
    fn differs(&self) -> bool {
        self.count_a != self.count_b
            || self.warnings_a != self.warnings_b
            || self.errors_a != self.errors_b
            || self.status_a != self.status_b
            || duration_changed(self.average_duration_a, self.average_duration_b)
            || duration_changed(self.max_duration_a, self.max_duration_b)
    }

    fn significance(&self) -> (bool, usize, usize) {
        (
            self.count_a == 0 || self.count_b == 0,
            self.errors_a.abs_diff(self.errors_b),
            self.count_a.abs_diff(self.count_b),
        )
    }
}

fn duration_changed(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => (left - right).abs() >= 1.0,
        (None, None) => false,
        _ => true,
    }
}

fn compare_sessions(left: &SessionSummary, right: &SessionSummary) -> Vec<SessionComparisonRow> {
    let event_names: BTreeSet<_> = left
        .events
        .keys()
        .chain(right.events.keys())
        .cloned()
        .collect();
    let mut rows: Vec<_> = event_names
        .into_iter()
        .map(|event| {
            let left = left.events.get(&event);
            let right = right.events.get(&event);
            SessionComparisonRow {
                event,
                count_a: left.map_or(0, |summary| summary.count),
                count_b: right.map_or(0, |summary| summary.count),
                warnings_a: left.map_or(0, |summary| summary.warning_count),
                warnings_b: right.map_or(0, |summary| summary.warning_count),
                errors_a: left.map_or(0, |summary| summary.error_count),
                errors_b: right.map_or(0, |summary| summary.error_count),
                average_duration_a: left.and_then(SessionEventSummary::average_duration_ms),
                average_duration_b: right.and_then(SessionEventSummary::average_duration_ms),
                max_duration_a: left.and_then(|summary| summary.max_duration_ms),
                max_duration_b: right.and_then(|summary| summary.max_duration_ms),
                status_a: left
                    .and_then(SessionEventSummary::status)
                    .map(str::to_string),
                status_b: right
                    .and_then(SessionEventSummary::status)
                    .map(str::to_string),
            }
        })
        .collect();
    rows.sort_by(|left, right| {
        right
            .differs()
            .cmp(&left.differs())
            .then_with(|| right.significance().cmp(&left.significance()))
            .then_with(|| left.event.cmp(&right.event))
    });
    rows
}

fn session_start_times(
    summaries: &BTreeMap<String, SessionSummary>,
) -> BTreeMap<String, DateTime<Utc>> {
    summaries
        .iter()
        .map(|(id, summary)| (id.clone(), summary.first_timestamp))
        .collect()
}

impl TableColumn {
    const ALL: [Self; 12] = [
        Self::Timestamp,
        Self::RelativeTime,
        Self::Level,
        Self::Source,
        Self::Tag,
        Self::Subsystem,
        Self::Event,
        Self::Provider,
        Self::Correlation,
        Self::Duration,
        Self::Status,
        Self::Message,
    ];

    const fn title(self) -> &'static str {
        match self {
            Self::Timestamp => "Timestamp",
            Self::RelativeTime => "Relative",
            Self::Level => "Level",
            Self::Source => "Source",
            Self::Tag => "Tag",
            Self::Subsystem => "Subsystem",
            Self::Event => "Event",
            Self::Provider => "Provider",
            Self::Correlation => "Correlation",
            Self::Duration => "Duration",
            Self::Status => "Status",
            Self::Message => "Message",
        }
    }

    fn layout(self) -> Column {
        match self {
            Self::Timestamp => Column::initial(225.0),
            Self::RelativeTime => Column::initial(92.0),
            Self::Level => Column::initial(76.0),
            Self::Source => Column::initial(90.0),
            Self::Tag => Column::initial(130.0),
            Self::Subsystem => Column::initial(110.0),
            Self::Event => Column::initial(150.0),
            Self::Provider => Column::initial(90.0),
            Self::Correlation => Column::initial(145.0),
            Self::Duration => Column::initial(82.0),
            Self::Status => Column::initial(70.0),
            Self::Message => Column::remainder(),
        }
    }
}

fn default_column_order() -> Vec<TableColumn> {
    TableColumn::ALL.to_vec()
}

fn normalize_column_order(columns: Vec<TableColumn>) -> Vec<TableColumn> {
    let had_relative_time = columns.contains(&TableColumn::RelativeTime);
    let mut normalized = Vec::with_capacity(TableColumn::ALL.len());
    for column in columns.into_iter().chain(TableColumn::ALL) {
        if !normalized.contains(&column) {
            normalized.push(column);
        }
    }
    if !had_relative_time {
        normalized.retain(|column| *column != TableColumn::RelativeTime);
        let position = normalized
            .iter()
            .position(|column| *column == TableColumn::Timestamp)
            .map_or(0, |position| position + 1);
        normalized.insert(position, TableColumn::RelativeTime);
    }
    normalized
}

fn normalize_saved_column_order(
    mut columns: Vec<TableColumn>,
    relative_time_column: bool,
) -> Vec<TableColumn> {
    if !relative_time_column {
        columns.retain(|column| *column != TableColumn::RelativeTime);
    }
    normalize_column_order(columns)
}

fn default_facet_order() -> Vec<String> {
    DISPLAYED_FACETS
        .iter()
        .map(|facet| (*facet).to_owned())
        .collect()
}

fn normalize_facet_order(mut facets: Vec<String>) -> Vec<String> {
    for facet in DISPLAYED_FACETS {
        if !facets.iter().any(|existing| existing == facet) {
            facets.push(facet.to_owned());
        }
    }
    let mut seen = BTreeSet::new();
    facets.retain(|facet| seen.insert(facet.clone()));
    facets
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PinnedEvent {
    key: String,
    #[serde(default)]
    note: String,
    event_json: String,
}

impl PinnedEvent {
    fn event(&self) -> Option<LogEvent> {
        serde_json::from_str(&self.event_json).ok()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceHealthState {
    Loading,
    Tailing,
    Rotated,
    Missing,
    Error,
}

#[derive(Debug, Clone)]
struct SourceHealth {
    state: SourceHealthState,
    parse_errors: u64,
    rotations: u64,
    detail: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
struct ViewerPreferences {
    version: u16,
    #[serde(default)]
    sources: Vec<PathBuf>,
    #[serde(default)]
    filter: FilterState,
    column_order: Vec<TableColumn>,
    #[serde(default)]
    relative_time_column: bool,
    wrapped_messages: bool,
    semantic_highlighting: bool,
    stick_to_bottom: bool,
    color_by: ColorBy,
    bookmarks: Vec<FilterBookmark>,
    #[serde(default)]
    bookmarks_by_source: BTreeMap<String, Vec<FilterBookmark>>,
    #[serde(default)]
    pins: Vec<PinnedEvent>,
    #[serde(default)]
    pins_by_source: BTreeMap<String, Vec<PinnedEvent>>,
    #[serde(default = "default_facet_order")]
    facet_order: Vec<String>,
    #[serde(default)]
    hidden_facets: BTreeSet<String>,
    latest_at: LatestAt,
    max_events: usize,
    timestamp_display: TimestampDisplay,
    timestamp_format: String,
    group_errors: bool,
    #[serde(default)]
    show_fields: bool,
    #[serde(default = "default_ui_scale")]
    ui_scale: f32,
}

impl Default for ViewerPreferences {
    fn default() -> Self {
        Self {
            version: 1,
            sources: Vec::new(),
            filter: FilterState::default(),
            column_order: default_column_order(),
            relative_time_column: true,
            wrapped_messages: true,
            semantic_highlighting: false,
            stick_to_bottom: true,
            color_by: ColorBy::Off,
            bookmarks: Vec::new(),
            bookmarks_by_source: BTreeMap::new(),
            pins: Vec::new(),
            pins_by_source: BTreeMap::new(),
            facet_order: default_facet_order(),
            hidden_facets: BTreeSet::new(),
            latest_at: LatestAt::Bottom,
            max_events: DEFAULT_MAX_EVENTS,
            timestamp_display: TimestampDisplay::default(),
            timestamp_format: default_timestamp_format(),
            group_errors: false,
            show_fields: false,
            ui_scale: DEFAULT_UI_SCALE,
        }
    }
}

const fn default_ui_scale() -> f32 {
    DEFAULT_UI_SCALE
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkspaceConfig {
    version: u16,
    sources: Vec<PathBuf>,
    filter: FilterState,
    wrapped_messages: bool,
    #[serde(default)]
    semantic_highlighting: bool,
    stick_to_bottom: bool,
    #[serde(default)]
    color_by: ColorBy,
    #[serde(default)]
    bookmarks: Vec<FilterBookmark>,
    #[serde(default)]
    pins: Vec<PinnedEvent>,
    #[serde(default = "default_facet_order")]
    facet_order: Vec<String>,
    #[serde(default)]
    hidden_facets: BTreeSet<String>,
    #[serde(default)]
    latest_at: LatestAt,
    #[serde(default = "default_column_order")]
    column_order: Vec<TableColumn>,
    #[serde(default)]
    relative_time_column: bool,
    #[serde(default = "default_max_events")]
    max_events: usize,
    #[serde(default)]
    timestamp_display: TimestampDisplay,
    #[serde(default = "default_timestamp_format")]
    timestamp_format: String,
    #[serde(default)]
    group_errors: bool,
    #[serde(default)]
    show_fields: bool,
}

impl WorkspaceConfig {
    fn new(sources: Vec<PathBuf>) -> Self {
        Self {
            version: 1,
            sources,
            filter: FilterState::default(),
            wrapped_messages: true,
            semantic_highlighting: false,
            stick_to_bottom: true,
            color_by: ColorBy::Off,
            bookmarks: Vec::new(),
            pins: Vec::new(),
            facet_order: default_facet_order(),
            hidden_facets: BTreeSet::new(),
            latest_at: LatestAt::Bottom,
            column_order: default_column_order(),
            relative_time_column: true,
            max_events: DEFAULT_MAX_EVENTS,
            timestamp_display: TimestampDisplay::default(),
            timestamp_format: default_timestamp_format(),
            group_errors: false,
            show_fields: false,
        }
    }
}

const fn default_max_events() -> usize {
    DEFAULT_MAX_EVENTS
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchMode {
    Full,
    Incremental,
}

#[derive(Debug)]
struct SearchRequest {
    generation: u64,
    query: String,
    snapshot: SearchSnapshot,
    mode: SearchMode,
    start_index: usize,
    event_count: usize,
    discarded_events: u64,
}

#[derive(Debug)]
struct SearchResponse {
    generation: u64,
    query: String,
    matches: Vec<usize>,
    mode: SearchMode,
    start_index: usize,
    event_count: usize,
    discarded_events: u64,
}

struct SearchWorker {
    requests: Sender<SearchRequest>,
    responses: Receiver<SearchResponse>,
    latest_generation: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Default)]
enum UpdateState {
    #[default]
    Idle,
    Checking,
    Available(AvailableUpdate),
    Failed(String),
}

impl SearchWorker {
    fn spawn(ctx: egui::Context) -> Self {
        let (request_sender, request_receiver) = unbounded::<SearchRequest>();
        let (response_sender, response_receiver) = unbounded();
        let latest_generation = Arc::new(AtomicU64::new(0));
        let worker_generation = Arc::clone(&latest_generation);
        thread::Builder::new()
            .name("debug-log-search".to_string())
            .spawn(move || {
                while let Ok(mut request) = request_receiver.recv() {
                    for newer in request_receiver.try_iter() {
                        request = newer;
                    }
                    if worker_generation.load(Ordering::Acquire) != request.generation {
                        continue;
                    }
                    let generation = request.generation;
                    let Some(matches) = request.snapshot.search(&request.query, || {
                        worker_generation.load(Ordering::Relaxed) != generation
                    }) else {
                        continue;
                    };
                    if worker_generation.load(Ordering::Acquire) != generation {
                        continue;
                    }
                    let response = SearchResponse {
                        generation,
                        query: request.query,
                        matches,
                        mode: request.mode,
                        start_index: request.start_index,
                        event_count: request.event_count,
                        discarded_events: request.discarded_events,
                    };
                    if response_sender.send(response).is_err() {
                        break;
                    }
                    ctx.request_repaint();
                }
            })
            .expect("failed to start log search worker");
        Self {
            requests: request_sender,
            responses: response_receiver,
            latest_generation,
        }
    }

    fn next_generation(&self) -> u64 {
        self.latest_generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn send(&self, request: SearchRequest) -> bool {
        self.requests.send(request).is_ok()
    }
}

pub struct ViewerApp {
    store: EventStore,
    filter: FilterState,
    visible_rows: Vec<usize>,
    facet_counts: BTreeMap<String, Vec<(String, u64)>>,
    reader: ReaderHandle,
    sources: Vec<PathBuf>,
    workspace_path: Option<PathBuf>,
    project_root: Option<PathBuf>,
    project_setup: Option<ProjectSetup>,
    selected_row: Option<usize>,
    invalid_records: u64,
    source_load_started_at: BTreeMap<PathBuf, Instant>,
    last_error: Option<String>,
    last_notice: Option<String>,
    settings_open: bool,
    paused: bool,
    filters_dirty: bool,
    wrapped_messages: bool,
    semantic_highlighting: bool,
    stick_to_bottom: bool,
    color_by: ColorBy,
    bookmarks: Vec<FilterBookmark>,
    bookmarks_by_source: BTreeMap<String, Vec<FilterBookmark>>,
    bookmark_scope: Option<String>,
    bookmark_rename: Option<BookmarkRename>,
    pins: Vec<PinnedEvent>,
    pins_by_source: BTreeMap<String, Vec<PinnedEvent>>,
    pin_note_edit: Option<(String, String)>,
    facet_search: String,
    facet_sections_expanded: bool,
    facet_sections_request: Option<bool>,
    facet_order: Vec<String>,
    hidden_facets: BTreeSet<String>,
    source_health: BTreeMap<PathBuf, SourceHealth>,
    latest_at: LatestAt,
    column_order: Vec<TableColumn>,
    middle_pan_active: bool,
    tail_was_at_bottom: bool,
    scroll_to_bottom_requested: bool,
    scroll_settle_frames: u8,
    latest_navigation_diagnostic: Option<LatestNavigationAction>,
    scroll_to_selected_requested: bool,
    last_discarded_events: u64,
    search_worker: SearchWorker,
    search_generation: u64,
    search_due_at: Option<Instant>,
    search_in_flight: bool,
    text_matches: Vec<usize>,
    text_matches_query: String,
    text_matches_event_count: usize,
    text_matches_discarded_events: u64,
    max_events: usize,
    timestamp_display: TimestampDisplay,
    timestamp_format: String,
    group_errors: bool,
    show_fields: bool,
    ui_scale: f32,
    table_rows: Vec<usize>,
    error_groups: BTreeMap<usize, ErrorGroup>,
    event_limit_reload_pending: bool,
    update_receiver: Option<std::sync::mpsc::Receiver<CheckResult>>,
    update_state: UpdateState,
    update_check_was_requested: bool,
    session_explorer: SessionExplorerState,
}

impl ViewerApp {
    pub fn new(creation_context: &eframe::CreationContext<'_>, launch: LaunchRequest) -> Self {
        configure_ui(&creation_context.egui_ctx);

        let mut preferences = creation_context
            .storage
            .and_then(|storage| eframe::get_value::<ViewerPreferences>(storage, PREFERENCES_KEY))
            .filter(|preferences| preferences.version == 1)
            .unwrap_or_default();
        preferences.column_order = normalize_saved_column_order(
            preferences.column_order,
            preferences.relative_time_column,
        );
        preferences.max_events = preferences.max_events.clamp(1, MAX_CONFIGURABLE_EVENTS);
        preferences.ui_scale = preferences.ui_scale.clamp(MIN_UI_SCALE, MAX_UI_SCALE);
        creation_context
            .egui_ctx
            .set_zoom_factor(preferences.ui_scale);

        let active_project_root = launch.project_root.as_deref().map(project_root);
        let (project_workspace_path, project_sources, project_error, project_setup) =
            match active_project_root.as_deref() {
                Some(root) if project_manifest_path(root).is_file() => match load_project(root) {
                    Ok(project) => (
                        Some(project.workspace_path),
                        Some(project.sources),
                        None,
                        None,
                    ),
                    Err(error) => (None, None, Some(error), None),
                },
                Some(root) if root.is_dir() => {
                    let mut setup = ProjectSetup::new(root.to_path_buf());
                    if !launch.log_paths.is_empty() {
                        setup.sources = launch
                            .log_paths
                            .iter()
                            .map(|path| manifest_source_path(root, path))
                            .collect();
                    }
                    (None, None, None, Some(setup))
                }
                Some(root) => (
                    None,
                    None,
                    Some(format!(
                        "Project root {} is not an existing directory",
                        root.display()
                    )),
                    None,
                ),
                None => (None, None, None, None),
            };
        let conflicting_targets = launch.project_root.is_some() && launch.workspace_path.is_some();
        let requested_workspace_path = project_workspace_path.or(launch.workspace_path);
        let (workspace_path, workspace, workspace_error) = match requested_workspace_path {
            Some(path) if path.exists() => match load_workspace(&path) {
                Ok(workspace) => (Some(path), Some(workspace), None),
                Err(error) => (None, None, Some(error)),
            },
            Some(path) => (
                Some(path),
                Some(WorkspaceConfig::new(launch.log_paths.clone())),
                None,
            ),
            None => (None, None, None),
        };
        let startup_error = if conflicting_targets {
            Some("Use either --project or --workspace, not both".to_string())
        } else {
            project_error.or(workspace_error)
        };
        let paths_to_open = if !launch.log_paths.is_empty() {
            launch.log_paths
        } else if let Some(project_sources) = project_sources {
            project_sources
        } else {
            workspace
                .as_ref()
                .map(|workspace| workspace.sources.clone())
                .unwrap_or_default()
        };
        let paths_to_open = normalize_source_paths(paths_to_open);
        let filter = workspace
            .as_ref()
            .map(|workspace| workspace.filter.clone())
            .unwrap_or_default();
        let wrapped_messages = workspace
            .as_ref()
            .map(|workspace| workspace.wrapped_messages)
            .unwrap_or(preferences.wrapped_messages);
        let semantic_highlighting = workspace
            .as_ref()
            .map(|workspace| workspace.semantic_highlighting)
            .unwrap_or(preferences.semantic_highlighting);
        let stick_to_bottom = workspace
            .as_ref()
            .map(|workspace| workspace.stick_to_bottom)
            .unwrap_or(preferences.stick_to_bottom);
        let color_by = workspace
            .as_ref()
            .map(|workspace| workspace.color_by)
            .unwrap_or(preferences.color_by);
        let latest_at = workspace
            .as_ref()
            .map(|workspace| workspace.latest_at)
            .unwrap_or(preferences.latest_at);
        let column_order = workspace
            .as_ref()
            .map_or(preferences.column_order, |workspace| {
                normalize_saved_column_order(
                    workspace.column_order.clone(),
                    workspace.relative_time_column,
                )
            });
        let max_events = workspace
            .as_ref()
            .map(|workspace| workspace.max_events)
            .unwrap_or(preferences.max_events)
            .clamp(1, MAX_CONFIGURABLE_EVENTS);
        let timestamp_display = workspace
            .as_ref()
            .map(|workspace| workspace.timestamp_display)
            .unwrap_or(preferences.timestamp_display);
        let timestamp_format = workspace
            .as_ref()
            .map(|workspace| workspace.timestamp_format.clone())
            .unwrap_or(preferences.timestamp_format);
        let group_errors = workspace
            .as_ref()
            .map(|workspace| workspace.group_errors)
            .unwrap_or(preferences.group_errors);
        let show_fields = workspace
            .as_ref()
            .map(|workspace| workspace.show_fields)
            .unwrap_or(preferences.show_fields);
        let bookmark_scope = bookmark_scope_key(&paths_to_open);
        let mut bookmarks_by_source = preferences.bookmarks_by_source;
        let bookmarks = workspace.as_ref().map_or_else(
            || {
                bookmark_scope
                    .as_ref()
                    .map(|scope| {
                        bookmarks_by_source
                            .entry(scope.clone())
                            .or_insert_with(|| preferences.bookmarks.clone())
                            .clone()
                    })
                    .unwrap_or_default()
            },
            |workspace| workspace.bookmarks.clone(),
        );
        let mut pins_by_source = preferences.pins_by_source;
        let pins = workspace.as_ref().map_or_else(
            || {
                bookmark_scope
                    .as_ref()
                    .map(|scope| {
                        pins_by_source
                            .entry(scope.clone())
                            .or_insert_with(|| preferences.pins.clone())
                            .clone()
                    })
                    .unwrap_or_default()
            },
            |workspace| workspace.pins.clone(),
        );
        let facet_order = normalize_facet_order(
            workspace
                .as_ref()
                .map(|workspace| workspace.facet_order.clone())
                .unwrap_or(preferences.facet_order),
        );
        let hidden_facets = workspace
            .as_ref()
            .map(|workspace| workspace.hidden_facets.clone())
            .unwrap_or(preferences.hidden_facets);
        let reader = spawn_reader(paths_to_open.clone());
        let search_worker = SearchWorker::spawn(creation_context.egui_ctx.clone());
        let mut app = Self {
            store: EventStore::new(max_events),
            filter,
            visible_rows: Vec::new(),
            facet_counts: BTreeMap::new(),
            reader,
            // Keep explicit workspace sources even if the reader has not yet
            // completed its first poll, so a newly created workspace is durable.
            sources: paths_to_open,
            workspace_path,
            project_root: active_project_root,
            project_setup,
            selected_row: None,
            invalid_records: 0,
            source_load_started_at: BTreeMap::new(),
            last_error: startup_error,
            last_notice: None,
            settings_open: false,
            paused: false,
            filters_dirty: true,
            wrapped_messages,
            semantic_highlighting,
            stick_to_bottom,
            color_by,
            bookmarks,
            bookmarks_by_source,
            bookmark_scope,
            bookmark_rename: None,
            pins,
            pins_by_source,
            pin_note_edit: None,
            facet_search: String::new(),
            facet_sections_expanded: false,
            facet_sections_request: None,
            facet_order,
            hidden_facets,
            source_health: BTreeMap::new(),
            latest_at,
            column_order,
            middle_pan_active: false,
            tail_was_at_bottom: true,
            scroll_to_bottom_requested: true,
            scroll_settle_frames: LATEST_SETTLE_FRAMES,
            latest_navigation_diagnostic: None,
            scroll_to_selected_requested: false,
            last_discarded_events: 0,
            search_worker,
            search_generation: 0,
            search_due_at: None,
            search_in_flight: false,
            text_matches: Vec::new(),
            text_matches_query: String::new(),
            text_matches_event_count: 0,
            text_matches_discarded_events: 0,
            max_events,
            timestamp_display,
            timestamp_format,
            group_errors,
            show_fields,
            ui_scale: preferences.ui_scale,
            table_rows: Vec::new(),
            error_groups: BTreeMap::new(),
            event_limit_reload_pending: false,
            update_receiver: Some(update::check_for_update_async()),
            update_state: UpdateState::Checking,
            update_check_was_requested: false,
            session_explorer: SessionExplorerState::default(),
        };
        app.refresh_filters();
        if !app.filter.text.trim().is_empty() {
            app.schedule_text_search(Duration::ZERO);
        }
        if let Err(error) = app.save_active_workspace() {
            app.last_error = Some(format!("Unable to create active workspace: {error}"));
        }
        app
    }

    fn drain_reader(&mut self) {
        let previous_latest_visible =
            latest_visible_id(&self.store, &self.visible_rows, self.latest_at);
        // Capture the visible tail before this can release deferred pruning. Otherwise
        // a pruning pass can shift indexes before the change detector sees it.
        self.store
            .set_pruning_paused(should_pause_pruning(self.paused, self.tail_was_at_bottom));
        let mut received_events = false;
        loop {
            match self.reader.messages.try_recv() {
                Ok(ReaderMessage::Batch(events)) => {
                    self.session_explorer.observe_many(&events);
                    self.store.extend(events);
                    received_events = true;
                }
                Ok(ReaderMessage::InitialLoadStarted {
                    path,
                    file_size_bytes,
                }) => {
                    let health = self
                        .source_health
                        .entry(path.clone())
                        .or_insert(SourceHealth {
                            state: SourceHealthState::Loading,
                            parse_errors: 0,
                            rotations: 0,
                            detail: None,
                        });
                    health.state = SourceHealthState::Loading;
                    self.source_load_started_at
                        .insert(path.clone(), Instant::now());
                    tracing::info!(
                        target: "deebugee.diagnostics",
                        subsystem = "ingestion",
                        event = "viewer.source_load.started",
                        status = "started",
                        source_path = %path.display(),
                        file_size_bytes,
                        "[Load] Initial source load started"
                    );
                }
                Ok(ReaderMessage::InitialLoadCompleted {
                    path,
                    file_size_bytes,
                    event_count,
                    invalid_count,
                    duration,
                }) => {
                    let health = self
                        .source_health
                        .entry(path.clone())
                        .or_insert(SourceHealth {
                            state: SourceHealthState::Tailing,
                            parse_errors: 0,
                            rotations: 0,
                            detail: None,
                        });
                    health.state = SourceHealthState::Tailing;
                    health.parse_errors = health.parse_errors.max(invalid_count);
                    let end_to_end_duration = self
                        .source_load_started_at
                        .remove(&path)
                        .map(|started_at| started_at.elapsed())
                        .map_or(duration, |app_duration| app_duration.max(duration));
                    tracing::info!(
                        target: "deebugee.diagnostics",
                        subsystem = "ingestion",
                        event = "viewer.source_load.completed",
                        status = "completed",
                        duration_ms = end_to_end_duration.as_secs_f64() * 1_000.0,
                        reader_duration_ms = duration.as_secs_f64() * 1_000.0,
                        source_path = %path.display(),
                        file_size_bytes,
                        event_count,
                        invalid_count,
                        "[Load] Initial source load completed"
                    );
                }
                Ok(ReaderMessage::InvalidRecord { path, line, error }) => {
                    self.invalid_records += 1;
                    let health = self
                        .source_health
                        .entry(path.clone())
                        .or_insert(SourceHealth {
                            state: SourceHealthState::Tailing,
                            parse_errors: 0,
                            rotations: 0,
                            detail: None,
                        });
                    health.parse_errors += 1;
                    health.detail = Some(format!("line {line}: {error}"));
                    self.last_error = Some(format!("{}:{line}: {error}", path.display()));
                }
                Ok(ReaderMessage::SourceOpened(path)) => {
                    if !self.sources.contains(&path) {
                        self.sources.push(path.clone());
                        self.sources.sort();
                    }
                    self.source_health.entry(path).or_insert(SourceHealth {
                        state: SourceHealthState::Tailing,
                        parse_errors: 0,
                        rotations: 0,
                        detail: None,
                    });
                }
                Ok(ReaderMessage::SourceRecovered(path)) => {
                    let health = self.source_health.entry(path).or_insert(SourceHealth {
                        state: SourceHealthState::Tailing,
                        parse_errors: 0,
                        rotations: 0,
                        detail: None,
                    });
                    health.state = SourceHealthState::Tailing;
                    health.detail = Some("Reader recovered after a transient error".to_owned());
                }
                Ok(ReaderMessage::SourceReplaced(path)) => {
                    let health = self.source_health.entry(path).or_insert(SourceHealth {
                        state: SourceHealthState::Rotated,
                        parse_errors: 0,
                        rotations: 0,
                        detail: None,
                    });
                    health.state = SourceHealthState::Rotated;
                    health.rotations += 1;
                    health.detail =
                        Some("File was replaced or truncated; tail restarted".to_owned());
                }
                Ok(ReaderMessage::SourceError { path, error }) => {
                    let state = if path.exists() {
                        SourceHealthState::Error
                    } else {
                        SourceHealthState::Missing
                    };
                    self.source_health.insert(
                        path.clone(),
                        SourceHealth {
                            state,
                            parse_errors: 0,
                            rotations: 0,
                            detail: Some(error.clone()),
                        },
                    );
                    self.last_error = Some(format!("{}: {error}", path.display()));
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.last_error = Some("JSONL reader stopped unexpectedly".to_string());
                    break;
                }
            }
        }

        if self.store.discarded_events() != self.last_discarded_events {
            self.last_discarded_events = self.store.discarded_events();
            self.session_explorer.rebuild(&self.store);
            self.selected_row = None;
            received_events = true;
            if !self.filter.text.trim().is_empty() && !self.paused {
                self.schedule_text_search(Duration::ZERO);
            }
        }
        let refresh_decision =
            ingestion_refresh_decision(received_events, self.paused, self.tail_was_at_bottom);
        if refresh_decision.mark_filters_dirty {
            // Keep newly ingested rows pending while the user reads history. Jumping
            // or manually returning to Latest will rebuild the table on the next frame,
            // even if the source becomes quiet before then.
            self.filters_dirty = true;
        }
        if refresh_decision.refresh_anchored_view {
            // A hidden record or an older-row prune must not move the log viewport.
            // When the current filter can be evaluated synchronously, compare the
            // visible tail before and after ingestion and follow only if the newest
            // visible event itself changed.
            let visible_tail_changed = can_refresh_filtered_rows_immediately(
                !self.filter.text.trim().is_empty(),
                self.cached_search_is_current(),
            ) && {
                self.refresh_filters();
                visible_tail_changed(
                    previous_latest_visible,
                    &self.store,
                    &self.visible_rows,
                    self.latest_at,
                )
            };
            if visible_tail_changed && self.stick_to_bottom {
                self.request_scroll_to_latest();
            }
        }
    }

    fn begin_update_check(&mut self) {
        if matches!(self.update_state, UpdateState::Checking) {
            return;
        }
        self.update_receiver = Some(update::check_for_update_async());
        self.update_state = UpdateState::Checking;
        self.update_check_was_requested = true;
    }

    fn drain_update_check(&mut self) {
        let Some(receiver) = self.update_receiver.as_ref() else {
            return;
        };
        let Ok(result) = receiver.try_recv() else {
            return;
        };
        self.update_receiver = None;
        match result {
            CheckResult::Available(update) => self.update_state = UpdateState::Available(update),
            CheckResult::Current => {
                self.update_state = UpdateState::Idle;
                if self.update_check_was_requested {
                    self.last_notice = Some("DeeBugee is up to date".to_string());
                }
            }
            CheckResult::Failed(error) => self.update_state = UpdateState::Failed(error),
        }
        self.update_check_was_requested = false;
    }

    fn update_dialog(&mut self, context: &egui::Context) {
        let UpdateState::Available(update) = &self.update_state else {
            return;
        };
        let update = update.clone();
        let mut update_now = false;
        let mut later = false;
        egui::Window::new("Update available")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(context, |ui| {
                ui.set_min_width(360.0);
                ui.label(
                    RichText::new(format!("DeeBugee {} is available", update.version))
                        .strong()
                        .color(TEXT_PRIMARY),
                );
                ui.add_space(4.0);
                ui.label("The update is downloaded from the official GitHub release, verified, then installed after DeeBugee closes.");
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    update_now = ui.button("Update and Restart").clicked();
                    later = ui.button("Later").clicked();
                });
            });
        if later {
            self.update_state = UpdateState::Idle;
        }
        if update_now {
            let arguments: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
            match update::start_update(&arguments) {
                Ok(()) => context.send_viewport_cmd(egui::ViewportCommand::Close),
                Err(error) => {
                    self.last_error = Some(format!("Unable to start update: {error}"));
                    self.update_state = UpdateState::Idle;
                }
            }
        }
    }

    fn request_scroll_to_latest(&mut self) {
        self.scroll_to_bottom_requested = true;
        self.scroll_settle_frames = LATEST_SETTLE_FRAMES;
    }

    fn filter_changed(&mut self) {
        let was_at_latest = self.tail_was_at_bottom;
        self.filters_dirty = true;
        // Facet hide/show changes are synchronous, but the table is rendered later in
        // this same frame. Rebuild now so a concurrent Jump to latest targets the
        // filtered row list, not the list from before the facet change. Text searches
        // still wait for their worker result when its cache is stale.
        if can_refresh_filtered_rows_immediately(
            !self.filter.text.trim().is_empty(),
            self.cached_search_is_current(),
        ) {
            self.refresh_filters();
        }
        // Filtering is a view change, not an instruction to jump. Preserve an
        // existing latest anchor, but leave readers who are inspecting older rows
        // exactly where they are. Jump to latest remains an explicit action.
        if was_at_latest {
            self.request_scroll_to_latest();
        }
    }

    fn query_text_changed(&mut self) {
        self.schedule_text_search(SEARCH_DEBOUNCE);
        self.filters_dirty = true;
        self.tail_was_at_bottom = true;
        self.request_scroll_to_latest();
    }

    fn displayed_facets(&self) -> Vec<String> {
        let discovered: Vec<String> = self
            .store
            .facet_names()
            .filter(|facet| facet.starts_with("fields."))
            .take(20)
            .map(str::to_string)
            .collect();
        let mut facets = self.facet_order.clone();
        facets.extend(discovered);
        facets.retain(|facet| !self.hidden_facets.contains(facet));
        let mut seen = BTreeSet::new();
        facets.retain(|facet| seen.insert(facet.clone()));
        facets
    }

    fn refresh_filters(&mut self) {
        let facets = self.displayed_facets();
        let text_matches =
            (!self.filter.text.trim().is_empty()).then_some(self.text_matches.as_slice());
        let results = self
            .store
            .query_with_facets(&self.filter, &facets, text_matches);
        self.visible_rows = results.rows;
        order_visible_rows(&mut self.visible_rows, self.latest_at);
        self.rebuild_table_rows();
        self.facet_counts = results.facet_counts;
        self.filters_dirty = false;
    }

    fn rebuild_table_rows(&mut self) {
        self.table_rows.clear();
        self.error_groups.clear();

        if !self.group_errors {
            self.table_rows.clone_from(&self.visible_rows);
            return;
        }

        let mut groups = BTreeMap::<String, ErrorGroup>::new();
        for index in &self.visible_rows {
            let Some(event) = self.store.get(*index) else {
                continue;
            };
            if !is_groupable_event(event) {
                self.table_rows.push(*index);
                continue;
            }
            let key = error_group_key(event);
            let group = groups.entry(key).or_insert(ErrorGroup {
                count: 0,
                first_index: *index,
                previous_index: None,
                latest_index: *index,
            });
            group.count += 1;
            // `visible_rows` uses the configured display order. Keep the first entry
            // it encounters (the newest occurrence) as the representative.
            if group.count == 1 {
                self.table_rows.push(*index);
            } else {
                group.first_index = *index;
                if group.count == 2 {
                    group.previous_index = Some(*index);
                }
            }
        }

        for group in groups.into_values() {
            self.error_groups.insert(group.latest_index, group);
        }

        // In a bottom-latest view the first encountered record is older. Rebuild in
        // reverse so each group is represented by its newest occurrence, then restore
        // the user's requested table order.
        if self.latest_at == LatestAt::Bottom {
            self.table_rows.clear();
            self.error_groups.clear();
            let mut reverse_groups = BTreeMap::<String, ErrorGroup>::new();
            for index in self.visible_rows.iter().rev() {
                let Some(event) = self.store.get(*index) else {
                    continue;
                };
                if !is_groupable_event(event) {
                    continue;
                }
                let group = reverse_groups
                    .entry(error_group_key(event))
                    .or_insert(ErrorGroup {
                        count: 0,
                        first_index: *index,
                        previous_index: None,
                        latest_index: *index,
                    });
                group.count += 1;
                group.first_index = *index;
                if group.count == 2 {
                    group.previous_index = Some(*index);
                }
            }
            for group in reverse_groups.into_values() {
                self.error_groups.insert(group.latest_index, group);
            }
            for index in &self.visible_rows {
                let Some(event) = self.store.get(*index) else {
                    continue;
                };
                if !is_groupable_event(event) || self.error_groups.contains_key(index) {
                    self.table_rows.push(*index);
                }
            }
        }

        if self
            .selected_row
            .is_some_and(|selected| !self.table_rows.contains(&selected))
        {
            self.selected_row = None;
        }
    }

    fn cached_search_is_current(&self) -> bool {
        self.search_cache_matches_store_generation()
            && self.text_matches_event_count == self.store.len()
    }

    fn search_cache_matches_store_generation(&self) -> bool {
        self.text_matches_query == self.filter.text
            && self.text_matches_discarded_events == self.store.discarded_events()
    }

    fn schedule_text_search(&mut self, delay: Duration) {
        self.search_generation = self.search_worker.next_generation();
        self.search_due_at = Some(Instant::now() + delay);
        self.search_in_flight = false;
    }

    fn start_scheduled_search(&mut self) {
        let Some(due_at) = self.search_due_at else {
            return;
        };
        if Instant::now() < due_at || self.paused {
            return;
        }
        self.search_due_at = None;
        let query = self.filter.text.clone();
        if query.trim().is_empty() {
            self.text_matches.clear();
            self.text_matches_query.clear();
            self.text_matches_event_count = self.store.len();
            self.text_matches_discarded_events = self.store.discarded_events();
            self.filters_dirty = true;
            return;
        }

        let can_extend = self.search_cache_matches_store_generation()
            && self.text_matches_event_count <= self.store.len();
        let (mode, start_index) = if can_extend {
            (SearchMode::Incremental, self.text_matches_event_count)
        } else {
            (SearchMode::Full, 0)
        };
        if mode == SearchMode::Incremental && start_index == self.store.len() {
            self.filters_dirty = true;
            return;
        }
        let event_count = self.store.len();
        let request = SearchRequest {
            generation: self.search_generation,
            query,
            snapshot: self.store.search_snapshot(start_index),
            mode,
            start_index,
            event_count,
            discarded_events: self.store.discarded_events(),
        };
        self.search_in_flight = self.search_worker.send(request);
        if !self.search_in_flight {
            self.last_error = Some("Log search worker is unavailable".to_string());
        }
    }

    fn drain_search_results(&mut self) {
        while let Ok(response) = self.search_worker.responses.try_recv() {
            if response.generation != self.search_generation {
                continue;
            }
            self.search_in_flight = false;
            if response.query != self.filter.text
                || response.discarded_events != self.store.discarded_events()
            {
                self.schedule_text_search(Duration::ZERO);
                continue;
            }
            match response.mode {
                SearchMode::Full => self.text_matches = response.matches,
                SearchMode::Incremental
                    if self.search_cache_matches_store_generation()
                        && self.text_matches_event_count == response.start_index =>
                {
                    self.text_matches.extend(response.matches);
                }
                SearchMode::Incremental => {
                    self.schedule_text_search(Duration::ZERO);
                    continue;
                }
            }
            self.text_matches_query = response.query;
            self.text_matches_event_count = response.event_count;
            self.text_matches_discarded_events = response.discarded_events;
            self.filters_dirty = true;
            if self.tail_was_at_bottom && self.cached_search_is_current() {
                let previous_latest =
                    latest_visible_id(&self.store, &self.visible_rows, self.latest_at);
                self.refresh_filters();
                if visible_tail_changed(
                    previous_latest,
                    &self.store,
                    &self.visible_rows,
                    self.latest_at,
                ) && self.stick_to_bottom
                {
                    self.request_scroll_to_latest();
                }
            }
            if response.event_count < self.store.len() {
                self.schedule_text_search(Duration::ZERO);
            }
        }
    }

    fn add_paths(&mut self, paths: Vec<PathBuf>) {
        let paths = normalize_source_paths(paths);
        if paths.is_empty() {
            return;
        }
        let mut requested_paths = self.sources.clone();
        requested_paths.extend(paths.iter().cloned());
        self.switch_bookmark_scope(&requested_paths);
        if self
            .reader
            .commands
            .send(ReaderCommand::AddPaths(paths))
            .is_err()
        {
            self.last_error = Some("JSONL reader is unavailable".to_string());
        }
    }

    fn switch_bookmark_scope(&mut self, paths: &[PathBuf]) {
        let next_scope = bookmark_scope_key(paths);
        if self.bookmark_scope == next_scope {
            return;
        }

        if let Some(scope) = self.bookmark_scope.take() {
            self.bookmarks_by_source
                .insert(scope.clone(), std::mem::take(&mut self.bookmarks));
            self.pins_by_source
                .insert(scope, std::mem::take(&mut self.pins));
        }
        self.bookmarks = next_scope
            .as_ref()
            .and_then(|scope| self.bookmarks_by_source.get(scope))
            .cloned()
            .unwrap_or_default();
        self.pins = next_scope
            .as_ref()
            .and_then(|scope| self.pins_by_source.get(scope))
            .cloned()
            .unwrap_or_default();
        self.bookmark_scope = next_scope;
        self.bookmark_rename = None;
    }

    fn apply_event_limit(&mut self, max_events: usize) {
        self.max_events = max_events.clamp(1, MAX_CONFIGURABLE_EVENTS);
        let previous_discarded = self.store.discarded_events();
        self.store.set_max_events(self.max_events);
        if self.store.discarded_events() == previous_discarded {
            return;
        }

        self.session_explorer.rebuild(&self.store);
        self.selected_row = None;
        self.last_discarded_events = self.store.discarded_events();
        self.search_generation = self.search_worker.next_generation();
        self.search_due_at = None;
        self.search_in_flight = false;
        self.text_matches.clear();
        self.text_matches_query.clear();
        self.text_matches_event_count = 0;
        self.text_matches_discarded_events = 0;
        if !self.filter.text.trim().is_empty() && !self.paused {
            self.schedule_text_search(Duration::ZERO);
        }
        self.filters_dirty = true;
        self.tail_was_at_bottom = true;
        self.request_scroll_to_latest();
    }

    fn replace_paths(&mut self, paths: Vec<PathBuf>) {
        let paths = normalize_source_paths(paths);
        self.switch_bookmark_scope(&paths);
        self.reader = spawn_reader(paths.clone());
        self.store = EventStore::new(self.max_events);
        self.session_explorer.reset();
        self.sources = paths;
        self.selected_row = None;
        self.invalid_records = 0;
        self.source_load_started_at.clear();
        self.source_health.clear();
        self.last_error = None;
        self.last_discarded_events = 0;
        self.search_generation = self.search_worker.next_generation();
        self.search_due_at = None;
        self.search_in_flight = false;
        self.text_matches.clear();
        self.text_matches_query.clear();
        self.text_matches_event_count = 0;
        self.text_matches_discarded_events = 0;
        self.filters_dirty = true;
        self.tail_was_at_bottom = true;
        self.event_limit_reload_pending = false;
        self.request_scroll_to_latest();
    }

    fn reload_current_sources(&mut self) {
        let paths = self.sources.clone();
        if paths.is_empty() {
            self.event_limit_reload_pending = false;
            return;
        }
        self.replace_paths(paths);
    }

    fn new_log_set(&mut self) {
        self.replace_paths(Vec::new());
        self.last_notice = Some("Cleared loaded logs. Open a JSONL file to begin.".to_string());
    }

    fn active_workspace_config(&self) -> WorkspaceConfig {
        WorkspaceConfig {
            version: 1,
            sources: self.sources.clone(),
            filter: self.filter.clone(),
            wrapped_messages: self.wrapped_messages,
            semantic_highlighting: self.semantic_highlighting,
            stick_to_bottom: self.stick_to_bottom,
            color_by: self.color_by,
            bookmarks: self.bookmarks.clone(),
            pins: self.pins.clone(),
            facet_order: self.facet_order.clone(),
            hidden_facets: self.hidden_facets.clone(),
            latest_at: self.latest_at,
            column_order: self.column_order.clone(),
            relative_time_column: true,
            max_events: self.max_events,
            timestamp_display: self.timestamp_display,
            timestamp_format: self.timestamp_format.clone(),
            group_errors: self.group_errors,
            show_fields: self.show_fields,
        }
    }

    fn save_active_workspace(&self) -> Result<(), String> {
        let Some(path) = &self.workspace_path else {
            return Ok(());
        };
        let parent = path
            .parent()
            .ok_or_else(|| format!("Workspace path {} has no parent folder", path.display()))?;
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        let text = toml::to_string_pretty(&self.active_workspace_config())
            .map_err(|error| error.to_string())?;
        std::fs::write(path, text).map_err(|error| error.to_string())
    }

    fn open_workspace(&mut self) {
        let Some(path) = rfd::FileDialog::new()
            .add_filter("Toolkit workspace", &["toml"])
            .pick_file()
        else {
            return;
        };
        let result = std::fs::read_to_string(&path)
            .map_err(|error| error.to_string())
            .and_then(|text| {
                toml::from_str::<WorkspaceConfig>(&text).map_err(|error| error.to_string())
            });
        match result {
            Ok(workspace) if workspace.version == 1 => {
                self.filter = workspace.filter;
                self.wrapped_messages = workspace.wrapped_messages;
                self.semantic_highlighting = workspace.semantic_highlighting;
                self.stick_to_bottom = workspace.stick_to_bottom;
                self.color_by = workspace.color_by;
                self.latest_at = workspace.latest_at;
                self.column_order = normalize_saved_column_order(
                    workspace.column_order,
                    workspace.relative_time_column,
                );
                self.apply_event_limit(workspace.max_events);
                self.timestamp_display = workspace.timestamp_display;
                self.timestamp_format = workspace.timestamp_format;
                self.group_errors = workspace.group_errors;
                self.show_fields = workspace.show_fields;
                self.facet_order = normalize_facet_order(workspace.facet_order);
                self.hidden_facets = workspace.hidden_facets;
                self.replace_paths(workspace.sources);
                self.workspace_path = Some(path.clone());
                if let Some(scope) = &self.bookmark_scope {
                    self.bookmarks_by_source
                        .insert(scope.clone(), workspace.bookmarks.clone());
                }
                self.bookmarks = workspace.bookmarks;
                if let Some(scope) = &self.bookmark_scope {
                    self.pins_by_source
                        .insert(scope.clone(), workspace.pins.clone());
                }
                self.pins = workspace.pins;
                self.last_notice = Some(format!("Opened workspace {}", path.display()));
            }
            Ok(workspace) => {
                self.last_error = Some(format!(
                    "Unsupported workspace version {}",
                    workspace.version
                ));
            }
            Err(error) => self.last_error = Some(format!("Unable to open workspace: {error}")),
        }
    }

    fn save_workspace(&mut self) {
        let Some(path) = rfd::FileDialog::new()
            .add_filter("Toolkit workspace", &["toml"])
            .set_file_name("logging-workspace.toml")
            .save_file()
        else {
            return;
        };
        self.workspace_path = Some(path.clone());
        match self.save_active_workspace() {
            Ok(()) => self.last_notice = Some(format!("Saved workspace {}", path.display())),
            Err(error) => self.last_error = Some(format!("Unable to save workspace: {error}")),
        }
    }

    fn open_project_configuration(&mut self) {
        let Some(root) = self.project_root.clone() else {
            return;
        };
        let manifest_path = project_manifest_path(&root);
        match load_project_config(&manifest_path) {
            Ok(config) => self.project_setup = Some(ProjectSetup::from_config(root, config)),
            Err(error) => {
                self.last_error = Some(format!("Unable to configure project: {error}"));
            }
        }
    }

    fn save_project_setup(&mut self, setup: &ProjectSetup) -> Result<(), String> {
        let config = setup.config();
        let manifest_path = write_project_manifest(&setup.root, &config, setup.editing_existing)?;
        let project = load_project(&setup.root)?;
        self.project_root = Some(setup.root.clone());
        self.workspace_path = Some(project.workspace_path);
        self.replace_paths(project.sources);
        if let Err(error) = self.save_active_workspace() {
            self.last_error = Some(format!(
                "Project was created, but its workspace failed: {error}"
            ));
        }
        self.last_notice = Some(format!(
            "Configured {} with {} log source{}",
            manifest_path.display(),
            config.sources.len(),
            if config.sources.len() == 1 { "" } else { "s" }
        ));
        Ok(())
    }

    fn project_setup_screen(&mut self, root: &mut egui::Ui) {
        let Some(mut setup) = self.project_setup.take() else {
            return;
        };
        let mut save_now = false;
        let mut cancel = false;

        egui::CentralPanel::default()
            .frame(egui::Frame::new().fill(SURFACE_0))
            .show(root, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    ui.add_space(28.0);
                    ui.vertical_centered(|ui| {
                        ui.label(RichText::new("{  }").monospace().size(32.0).color(ACCENT));
                        ui.heading(if setup.editing_existing {
                            "Configure Project"
                        } else {
                            "Set Up This Project"
                        });
                        ui.label(
                            RichText::new(display_path(&setup.root))
                                .small()
                                .color(TEXT_MUTED),
                        );
                    });
                    ui.add_space(20.0);

                    let available = ui.available_width();
                    let form_width = available.min(780.0);
                    let left_margin = ((available - form_width) / 2.0).max(0.0);
                    ui.horizontal(|ui| {
                        ui.add_space(left_margin);
                        ui.vertical(|ui| {
                            ui.set_width(form_width);
                            egui::Frame::new()
                                .fill(SURFACE_1)
                                .stroke(Stroke::new(1.0, BORDER))
                                .corner_radius(8.0)
                                .inner_margin(egui::Margin::same(20))
                                .show(ui, |ui| {
                                    ui.label(RichText::new("Project name").strong());
                                    if ui
                                        .add_sized(
                                            [ui.available_width(), 30.0],
                                            egui::TextEdit::singleline(&mut setup.name),
                                        )
                                        .changed()
                                    {
                                        setup.confirm_overwrite = false;
                                        setup.error = None;
                                    }
                                    ui.add_space(12.0);

                                    ui.label(RichText::new("Project ID").strong());
                                    if ui
                                        .add_sized(
                                            [ui.available_width(), 30.0],
                                            egui::TextEdit::singleline(&mut setup.id)
                                                .font(TextStyle::Monospace),
                                        )
                                        .changed()
                                    {
                                        setup.confirm_overwrite = false;
                                        setup.error = None;
                                    }
                                    ui.label(
                                        RichText::new(
                                            "Keep this stable across clones; it identifies private workspace state.",
                                        )
                                        .small()
                                        .color(TEXT_MUTED),
                                    );
                                    ui.add_space(18.0);

                                    ui.horizontal(|ui| {
                                        ui.label(RichText::new("Log sources").strong());
                                        ui.with_layout(
                                            egui::Layout::right_to_left(egui::Align::Center),
                                            |ui| {
                                                if ui.button("Add Folder…").clicked()
                                                    && let Some(path) = rfd::FileDialog::new()
                                                        .set_directory(&setup.root)
                                                        .pick_folder()
                                                {
                                                    add_project_source(
                                                        &mut setup.sources,
                                                        manifest_source_path(&setup.root, &path),
                                                    );
                                                    setup.confirm_overwrite = false;
                                                    setup.error = None;
                                                }
                                                if ui.button("Add File…").clicked()
                                                    && let Some(path) = rfd::FileDialog::new()
                                                        .set_directory(&setup.root)
                                                        .add_filter(
                                                            "JSON Lines",
                                                            &["jsonl", "log"],
                                                        )
                                                        .pick_file()
                                                {
                                                    add_project_source(
                                                        &mut setup.sources,
                                                        manifest_source_path(&setup.root, &path),
                                                    );
                                                    setup.confirm_overwrite = false;
                                                    setup.error = None;
                                                }
                                            },
                                        );
                                    });
                                    ui.label(
                                        RichText::new(
                                            "Use a JSONL file or a folder containing JSONL files. Paths may point to logs that are created later.",
                                        )
                                        .small()
                                        .color(TEXT_MUTED),
                                    );
                                    ui.add_space(8.0);

                                    let mut remove_source = None;
                                    for (index, source) in setup.sources.iter_mut().enumerate() {
                                        ui.horizontal(|ui| {
                                            if ui
                                                .add_sized(
                                                    [ui.available_width() - 72.0, 28.0],
                                                    egui::TextEdit::singleline(source)
                                                        .font(TextStyle::Monospace)
                                                        .hint_text(
                                                            "%LOCALAPPDATA%/MyApplication/logs",
                                                        ),
                                                )
                                                .changed()
                                            {
                                                setup.confirm_overwrite = false;
                                                setup.error = None;
                                            }
                                            if ui.button("Remove").clicked() {
                                                remove_source = Some(index);
                                            }
                                        });
                                        if !source.trim().is_empty() {
                                            match resolve_project_source(&setup.root, source.trim()) {
                                                Ok(path) if path.exists() => {
                                                    ui.label(
                                                        RichText::new(format!(
                                                            "Ready: {}",
                                                            display_path(&path)
                                                        ))
                                                        .small()
                                                        .color(SUCCESS),
                                                    );
                                                }
                                                Ok(path) => {
                                                    ui.label(
                                                        RichText::new(format!(
                                                            "Not created yet: {}",
                                                            display_path(&path)
                                                        ))
                                                        .small()
                                                        .color(WARNING),
                                                    );
                                                }
                                                Err(error) => {
                                                    ui.label(
                                                        RichText::new(error).small().color(DANGER),
                                                    );
                                                }
                                            }
                                        }
                                        ui.add_space(6.0);
                                    }
                                    if let Some(index) = remove_source {
                                        setup.sources.remove(index);
                                        setup.confirm_overwrite = false;
                                        setup.error = None;
                                    }
                                    if ui.button("+ Add another source").clicked() {
                                        setup.sources.push(String::new());
                                        setup.confirm_overwrite = false;
                                        setup.error = None;
                                    }

                                    if let Some(error) = &setup.error {
                                        ui.add_space(12.0);
                                        ui.label(RichText::new(error).color(DANGER));
                                    }
                                    ui.add_space(18.0);
                                    ui.separator();
                                    ui.add_space(12.0);

                                    let can_save = !setup.name.trim().is_empty()
                                        && !setup.id.trim().is_empty()
                                        && setup.sources.iter().any(|source| !source.trim().is_empty());
                                    ui.horizontal(|ui| {
                                        if setup.confirm_overwrite {
                                            ui.label(
                                                RichText::new(
                                                    "Replace the existing project manifest?",
                                                )
                                                .color(WARNING),
                                            );
                                            if ui
                                                .add_enabled(
                                                    can_save,
                                                    egui::Button::new("Overwrite Manifest"),
                                                )
                                                .clicked()
                                            {
                                                save_now = true;
                                            }
                                            if ui.button("Keep Existing").clicked() {
                                                setup.confirm_overwrite = false;
                                            }
                                        } else {
                                            let save_label = if setup.editing_existing {
                                                "Save Changes"
                                            } else {
                                                "Create Project Configuration"
                                            };
                                            if ui
                                                .add_enabled(can_save, egui::Button::new(save_label))
                                                .clicked()
                                            {
                                                if setup.editing_existing {
                                                    setup.confirm_overwrite = true;
                                                } else {
                                                    save_now = true;
                                                }
                                            }
                                        }
                                        if ui.button("Cancel").clicked() {
                                            cancel = true;
                                        }
                                    });
                                });
                        });
                    });
                });
            });

        if cancel {
            if !setup.editing_existing {
                self.project_root = None;
                self.last_notice = Some("Project setup cancelled".to_string());
            }
            return;
        }
        if save_now {
            match self.save_project_setup(&setup) {
                Ok(()) => {
                    root.ctx()
                        .send_viewport_cmd(egui::ViewportCommand::Title(format!(
                            "DeeBugee — {}",
                            setup.name.trim()
                        )));
                    return;
                }
                Err(error) => setup.error = Some(error),
            }
        }
        self.project_setup = Some(setup);
    }

    fn export_filtered(&mut self) {
        let Some(path) = rfd::FileDialog::new()
            .add_filter("JSON Lines", &["jsonl"])
            .set_file_name("filtered.jsonl")
            .save_file()
        else {
            return;
        };
        let events: Vec<LogEvent> = self
            .visible_rows
            .iter()
            .filter_map(|index| self.store.get(*index).cloned())
            .collect();
        let count = events.len();
        let export_path = path.clone();
        thread::Builder::new()
            .name("debug-log-export".to_string())
            .spawn(move || {
                let result = (|| -> std::io::Result<()> {
                    let file = std::fs::File::create(export_path)?;
                    let mut writer = BufWriter::new(file);
                    for event in events {
                        serde_json::to_writer(&mut writer, &event)?;
                        writer.write_all(b"\n")?;
                    }
                    writer.flush()
                })();
                if let Err(error) = result {
                    eprintln!("filtered log export failed: {error}");
                }
            })
            .ok();
        self.last_notice = Some(format!("Exporting {count} records to {}", path.display()));
    }

    fn export_investigation(&mut self) {
        let Some(path) = rfd::FileDialog::new()
            .add_filter("DeeBugee investigation", &["json"])
            .set_file_name("deebugee-investigation.json")
            .save_file()
        else {
            return;
        };
        let pinned: Vec<serde_json::Value> = self
            .pins
            .iter()
            .map(|pin| {
                serde_json::json!({
                    "note": pin.note,
                    "event": serde_json::from_str::<serde_json::Value>(&pin.event_json)
                        .unwrap_or_else(|_| serde_json::Value::String(pin.event_json.clone())),
                })
            })
            .collect();
        let bundle = serde_json::json!({
            "format": "deebugee-investigation-v1",
            "created_at": Utc::now(),
            "sources": self.sources,
            "filter": self.filter,
            "pins": pinned,
        });
        match std::fs::write(
            &path,
            serde_json::to_vec_pretty(&bundle).unwrap_or_default(),
        ) {
            Ok(()) => {
                self.last_notice = Some(format!(
                    "Exported {} pinned events to {}",
                    self.pins.len(),
                    path.display()
                ))
            }
            Err(error) => {
                self.last_error = Some(format!("Unable to export investigation: {error}"))
            }
        }
    }

    fn selected_pin_key(&self) -> Option<String> {
        self.selected_row
            .and_then(|row| self.store.get(row))
            .map(event_pin_key)
    }

    fn toggle_selected_pin(&mut self) {
        let Some(event) = self
            .selected_row
            .and_then(|row| self.store.get(row))
            .cloned()
        else {
            return;
        };
        let key = event_pin_key(&event);
        if let Some(index) = self.pins.iter().position(|pin| pin.key == key) {
            self.pins.remove(index);
            self.pin_note_edit = None;
            self.last_notice = Some("Event unpinned".to_owned());
        } else {
            let event_json = serde_json::to_string(&event).unwrap_or_default();
            self.pins.push(PinnedEvent {
                key,
                note: String::new(),
                event_json,
            });
            self.pins
                .sort_by_key(|pin| pin.event().map(|event| event.timestamp));
            self.last_notice = Some("Event pinned".to_owned());
        }
    }

    fn select_event_key(&mut self, key: &str) -> bool {
        let Some(index) = event_index_by_key(&self.store, key) else {
            self.last_error =
                Some("Pinned event is no longer retained in the loaded logs".to_owned());
            return false;
        };
        if !self.table_rows.contains(&index) {
            self.filter.clear();
            self.refresh_filters();
            if self.group_errors && !self.table_rows.contains(&index) {
                self.group_errors = false;
                self.rebuild_table_rows();
            }
        }
        self.selected_row = Some(index);
        self.scroll_to_selected_requested = true;
        self.scroll_to_bottom_requested = false;
        true
    }

    fn navigate_matching(&mut self, direction: i32, predicate: impl Fn(&LogEvent) -> bool) {
        if self.table_rows.is_empty() {
            return;
        }
        let current = self
            .selected_row
            .and_then(|selected| self.table_rows.iter().position(|row| *row == selected));
        let len = self.table_rows.len() as i32;
        let start = current.map_or(if direction > 0 { -1 } else { 0 }, |value| value as i32);
        for step in 1..=len {
            let position = (start + direction * step).rem_euclid(len) as usize;
            let index = self.table_rows[position];
            if self.store.get(index).is_some_and(&predicate) {
                self.selected_row = Some(index);
                self.scroll_to_selected_requested = true;
                self.scroll_to_bottom_requested = false;
                break;
            }
        }
    }

    fn navigate_pin(&mut self, direction: i32) {
        let keys: Vec<String> = self
            .pins
            .iter()
            .filter(|pin| event_index_by_key(&self.store, &pin.key).is_some())
            .map(|pin| pin.key.clone())
            .collect();
        if keys.is_empty() {
            self.last_error = Some("No pinned events remain in the loaded logs".to_owned());
            return;
        }
        let current_key = self.selected_pin_key();
        let current = current_key
            .as_ref()
            .and_then(|key| keys.iter().position(|candidate| candidate == key));
        let len = keys.len() as i32;
        let start = current.map_or(if direction > 0 { -1 } else { 0 }, |value| value as i32);
        let position = (start + direction).rem_euclid(len) as usize;
        self.select_event_key(&keys[position]);
    }

    fn handle_keyboard_navigation(&mut self, context: &egui::Context) {
        if context.egui_wants_keyboard_input() {
            return;
        }
        let (up, down, next_error, previous_error, toggle_pin, edit_note, next_pin, previous_pin) =
            context.input(|input| {
                (
                    input.key_pressed(egui::Key::ArrowUp) && !input.modifiers.ctrl,
                    input.key_pressed(egui::Key::ArrowDown) && !input.modifiers.ctrl,
                    input.key_pressed(egui::Key::F8) && !input.modifiers.shift,
                    input.key_pressed(egui::Key::F8) && input.modifiers.shift,
                    input.key_pressed(egui::Key::P),
                    input.key_pressed(egui::Key::N),
                    input.key_pressed(egui::Key::ArrowDown) && input.modifiers.ctrl,
                    input.key_pressed(egui::Key::ArrowUp) && input.modifiers.ctrl,
                )
            });
        if up {
            self.navigate_matching(-1, |_| true);
        }
        if down {
            self.navigate_matching(1, |_| true);
        }
        if next_error {
            self.navigate_matching(1, is_error_event);
        }
        if previous_error {
            self.navigate_matching(-1, is_error_event);
        }
        if next_pin {
            self.navigate_pin(1);
        }
        if previous_pin {
            self.navigate_pin(-1);
        }
        if toggle_pin {
            self.toggle_selected_pin();
        }
        if edit_note
            && let Some(key) = self.selected_pin_key()
            && let Some(pin) = self.pins.iter().find(|pin| pin.key == key)
        {
            self.pin_note_edit = Some((key, pin.note.clone()));
        }
    }

    fn top_bar(&mut self, root: &mut egui::Ui) {
        let toolbar_frame = egui::Frame::new()
            .fill(SURFACE_1)
            .inner_margin(egui::Margin::symmetric(14, 9))
            .stroke(Stroke::new(1.0, BORDER));
        egui::Panel::top("toolbar")
            .frame(toolbar_frame)
            .show(root, |ui| {
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(
                            !self.sources.is_empty() || !self.store.is_empty(),
                            egui::Button::new("New"),
                        )
                        .on_hover_text("Clear the loaded logs and start with a new JSONL file")
                        .clicked()
                    {
                        self.new_log_set();
                    }
                    if ui.button("Open Logs").clicked()
                        && let Some(paths) = rfd::FileDialog::new()
                            .add_filter("JSON Lines", &["jsonl", "log"])
                            .pick_files()
                    {
                        self.add_paths(paths);
                    }
                    if ui.button("Open Folder").clicked()
                        && let Some(path) = rfd::FileDialog::new().pick_folder()
                    {
                        self.add_paths(vec![path]);
                    }
                    ui.menu_button("Workspace", |ui| {
                        if ui.button("Open Workspace…").clicked() {
                            self.open_workspace();
                            ui.close();
                        }
                        if ui.button("Save Workspace As…").clicked() {
                            self.save_workspace();
                            ui.close();
                        }
                        ui.separator();
                        if ui
                            .add_enabled(
                                !self.visible_rows.is_empty(),
                                egui::Button::new("Export Filtered Logs…"),
                            )
                            .clicked()
                        {
                            self.export_filtered();
                            ui.close();
                        }
                        if ui
                            .add_enabled(
                                !self.pins.is_empty(),
                                egui::Button::new("Export Investigation…"),
                            )
                            .on_hover_text("Export pinned events, notes, sources, and the active filter")
                            .clicked()
                        {
                            self.export_investigation();
                            ui.close();
                        }
                    });
                    if self.project_root.is_some() {
                        ui.menu_button("Project", |ui| {
                            if ui.button("Configure Project…").clicked() {
                                self.open_project_configuration();
                                ui.close();
                            }
                        });
                    }
                    if ui
                        .selectable_label(self.session_explorer.open, "Sessions")
                        .on_hover_text("Explore application runs and compare their outcomes")
                        .clicked()
                    {
                        self.session_explorer.open = !self.session_explorer.open;
                    }
                    self.settings_menu(ui);
                    self.source_health_menu(ui);
                    ui.menu_button("Navigate", |ui| {
                        if ui.button("First Error").clicked() {
                            if let Some(index) = self.table_rows.iter().copied().find(|index| self.store.get(*index).is_some_and(is_error_event)) {
                                self.selected_row = Some(index);
                                self.scroll_to_selected_requested = true;
                                self.scroll_to_bottom_requested = false;
                            }
                            ui.close();
                        }
                        if ui.button("Previous Error   Shift+F8").clicked() {
                            self.navigate_matching(-1, is_error_event);
                            ui.close();
                        }
                        if ui.button("Next Error   F8").clicked() {
                            self.navigate_matching(1, is_error_event);
                            ui.close();
                        }
                    });

                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let total = self.store.len();
                        let shown = self.visible_rows.len();
                        let table_rows = self.table_rows.len();
                        let shown_label = if self.group_errors && table_rows != shown {
                            format!("{table_rows} groups · {shown} events")
                        } else {
                            format!("{shown} shown")
                        };
                        ui.label(
                            RichText::new(format!("{shown_label}  ·  {total} loaded"))
                                .color(TEXT_MUTED),
                        );
                        if self.paused {
                            ui.label(RichText::new("PAUSED").strong().small().color(WARNING));
                        } else if total > 0 {
                            ui.label(RichText::new("LIVE").strong().small().color(SUCCESS));
                        }
                    });
                });

                ui.add_space(6.0);
                ui.horizontal_wrapped(|ui| {
                    let search_width = (ui.available_width() * 0.34).clamp(260.0, 520.0);
                    let search = ui.add_sized(
                        [search_width, 30.0],
                        egui::TextEdit::singleline(&mut self.filter.text)
                            .vertical_align(egui::Align::Center)
                            .hint_text("Search or enter field expressions…"),
                    );
                    search.clone().on_hover_text(
                        "Examples: provider=remote · duration_ms>1000 · fields.retry_count>=3 · timestamp:last-5m",
                    );
                    if search.changed() {
                        self.query_text_changed();
                    }
                    if ui
                        .add_enabled(!self.filter.text.is_empty(), egui::Button::new("Clear"))
                        .on_hover_text("Clear the search text")
                        .clicked()
                    {
                        self.filter.text.clear();
                        self.filter_changed();
                    }
                    if self.search_due_at.is_some() || self.search_in_flight {
                        ui.spinner();
                        ui.label(RichText::new("Searching…").small().color(TEXT_MUTED));
                    }

                    egui::ComboBox::from_id_salt("minimum_level")
                        .selected_text(
                            self.filter
                                .minimum_level
                                .map(|level| format!("{level} +"))
                                .unwrap_or_else(|| "All Levels".to_string()),
                        )
                        .width(96.0)
                        .show_ui(ui, |ui| {
                            if ui
                                .selectable_value(
                                    &mut self.filter.minimum_level,
                                    None,
                                    "All Levels",
                                )
                                .changed()
                            {
                                self.filter_changed();
                            }
                            for level in Level::ALL {
                                if ui
                                    .selectable_value(
                                        &mut self.filter.minimum_level,
                                        Some(level),
                                        format!("{level} +"),
                                    )
                                    .changed()
                                {
                                    self.filter_changed();
                                }
                            }
                        });

                    ui.separator();
                    if ui
                        .add(
                            egui::Button::new(if self.paused { "Resume" } else { "Pause" })
                                .selected(self.paused),
                        )
                        .on_hover_text(
                            "Pause freezes the current view; ingestion continues in the background",
                        )
                        .clicked()
                    {
                        self.paused = !self.paused;
                        if !self.paused {
                            self.filters_dirty = true;
                        }
                    }
                    if ui
                        .add(
                            egui::Button::new("Group Repeats").selected(self.group_errors),
                        )
                        .on_hover_text(
                            "Collapse matching repeated events into one row. Level filters and exports remain unchanged.",
                        )
                        .clicked()
                    {
                        self.group_errors = !self.group_errors;
                        self.rebuild_table_rows();
                        if self.tail_was_at_bottom {
                            self.request_scroll_to_latest();
                        }
                    }
                    if ui
                        .add(egui::Button::new("Show Fields").selected(self.show_fields))
                        .on_hover_text(
                            "Show structured args and remaining fields beneath each message.",
                        )
                        .clicked()
                    {
                        self.show_fields = !self.show_fields;
                        if self.tail_was_at_bottom {
                            self.request_scroll_to_latest();
                        }
                    }
                });

                self.structured_query_bar(ui);

                if let Some(error) = self.last_error.clone() {
                    ui.add_space(5.0);
                    egui::Frame::new()
                        .fill(Color32::from_rgb(56, 27, 34))
                        .inner_margin(egui::Margin::symmetric(9, 5))
                        .corner_radius(4.0)
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.label(RichText::new("Error").strong().color(DANGER));
                                ui.label(&error);
                                if ui.small_button("Dismiss").clicked() {
                                    self.last_error = None;
                                }
                            });
                        });
                } else if self.invalid_records > 0 {
                    ui.add_space(5.0);
                    ui.label(
                        RichText::new(format!("{} invalid records", self.invalid_records))
                            .color(WARNING),
                    );
                }
                if let Some(notice) = self.last_notice.clone() {
                    ui.add_space(5.0);
                    egui::Frame::new()
                        .fill(Color32::from_rgb(23, 50, 42))
                        .inner_margin(egui::Margin::symmetric(9, 5))
                        .corner_radius(4.0)
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.label(RichText::new("Done").strong().color(SUCCESS));
                                ui.label(&notice);
                                if ui.small_button("Dismiss").clicked() {
                                    self.last_notice = None;
                                }
                            });
                        });
                }
            });
    }

    fn structured_query_bar(&mut self, ui: &mut egui::Ui) {
        let parsed = parse_structured_query(&self.filter.text, Utc::now());
        if parsed.predicates.is_empty() && parsed.errors.is_empty() {
            return;
        }

        let mut remove_range = None;
        ui.add_space(5.0);
        ui.horizontal_wrapped(|ui| {
            if !parsed.predicates.is_empty() {
                ui.label(RichText::new("QUERY").strong().small().color(TEXT_MUTED));
            }
            for predicate in &parsed.predicates {
                if ui
                    .add(
                        egui::Button::new(format!("{}  ×", predicate.label))
                            .fill(ACCENT_SOFT)
                            .stroke(Stroke::new(1.0, ACCENT)),
                    )
                    .on_hover_text("Remove this structured expression")
                    .clicked()
                {
                    remove_range = Some(predicate.range.clone());
                }
            }
        });

        if !parsed.errors.is_empty() {
            ui.add_space(4.0);
            egui::Frame::new()
                .fill(Color32::from_rgb(56, 40, 24))
                .inner_margin(egui::Margin::symmetric(9, 5))
                .corner_radius(4.0)
                .show(ui, |ui| {
                    ui.horizontal_wrapped(|ui| {
                        ui.label(RichText::new("Query").strong().color(WARNING));
                        ui.label(
                            RichText::new(
                                parsed
                                    .errors
                                    .iter()
                                    .map(|error| error.message.as_str())
                                    .collect::<Vec<_>>()
                                    .join(" · "),
                            )
                            .color(WARNING),
                        );
                    });
                });
        }

        if let Some(range) = remove_range {
            self.filter.text = remove_query_expression(&self.filter.text, range);
            self.query_text_changed();
        }
    }

    fn source_health_menu(&mut self, ui: &mut egui::Ui) {
        let mut source_paths: Vec<PathBuf> = self.source_health.keys().cloned().collect();
        let configured_files: Vec<PathBuf> = self
            .sources
            .iter()
            .filter(|path| !path.is_dir() && !source_paths.contains(path))
            .cloned()
            .collect();
        source_paths.extend(configured_files);
        source_paths.sort();
        source_paths.dedup();
        let problems = self
            .source_health
            .values()
            .filter(|health| {
                health.parse_errors > 0
                    || health.rotations > 0
                    || !matches!(health.state, SourceHealthState::Tailing)
            })
            .count();
        let label = if problems == 0 {
            format!("Sources {}", source_paths.len())
        } else {
            format!("Sources {} · {problems}!", source_paths.len())
        };
        ui.menu_button(
            RichText::new(label).color(if problems == 0 { SUCCESS } else { WARNING }),
            |ui| {
                ui.set_min_width(420.0);
                if source_paths.is_empty() {
                    ui.label(RichText::new("No sources loaded").color(TEXT_MUTED));
                }
                for path in &source_paths {
                    let health = self.source_health.get(path);
                    let (state, color) = match health.map(|health| health.state) {
                        Some(SourceHealthState::Loading) => ("Loading", ACCENT),
                        Some(SourceHealthState::Tailing) => ("Tailing", SUCCESS),
                        Some(SourceHealthState::Rotated) => ("Rotated", WARNING),
                        Some(SourceHealthState::Missing) => ("Missing", DANGER),
                        Some(SourceHealthState::Error) => ("Error", DANGER),
                        None => ("Waiting", TEXT_MUTED),
                    };
                    ui.horizontal(|ui| {
                        ui.label(RichText::new(state).strong().color(color));
                        ui.label(path.display().to_string());
                    });
                    if let Some(health) = health {
                        if health.parse_errors > 0 {
                            ui.label(
                                RichText::new(format!("{} parse errors", health.parse_errors))
                                    .small()
                                    .color(WARNING),
                            );
                        }
                        if health.rotations > 0 {
                            ui.label(
                                RichText::new(format!("{} rotations detected", health.rotations))
                                    .small()
                                    .color(WARNING),
                            );
                        }
                        if let Some(detail) = &health.detail {
                            ui.label(RichText::new(detail).small().color(TEXT_MUTED));
                        }
                    }
                    ui.separator();
                }
            },
        );
    }

    fn settings_menu(&mut self, ui: &mut egui::Ui) {
        let button_response = ui.button("Settings");
        let settings_button_clicked = button_response.clicked();
        let mut settings_open = self.settings_open;
        if settings_button_clicked {
            settings_open = !settings_open;
        }

        // egui memory permits only one ordinary popup at a time. Keep Settings on
        // explicit state so opening a ComboBox does not replace its parent popup,
        // then exclude the nested popup rectangles from our outside-click handling.
        let mut nested_popup_rects = Vec::new();
        let popup_response = egui::Popup::menu(&button_response)
            .open_bool(&mut settings_open)
            .close_behavior(PopupCloseBehavior::IgnoreClicks)
            .show(|ui| {
            ui.set_min_width(340.0);

            ui.label(RichText::new("Interface").strong());
            ui.horizontal(|ui| {
                let scale_response = ui.add(
                    egui::Slider::new(&mut self.ui_scale, MIN_UI_SCALE..=MAX_UI_SCALE)
                        .text("Scale")
                        .suffix("×")
                        .step_by(0.05),
                );
                if scale_response.changed() {
                    ui.ctx().set_zoom_factor(self.ui_scale);
                }
                if ui.small_button("Reset").clicked() {
                    self.ui_scale = DEFAULT_UI_SCALE;
                    ui.ctx().set_zoom_factor(self.ui_scale);
                }
            });
            ui.label(
                RichText::new("Adjust the size of the entire DeeBugee interface.")
                    .small()
                    .color(TEXT_MUTED),
            );

            ui.separator();

            ui.label(RichText::new("Live View").strong());
            if ui
                .checkbox(&mut self.stick_to_bottom, "Follow Latest Events")
                .changed()
            {
                if self.stick_to_bottom {
                    self.tail_was_at_bottom = true;
                    self.latest_navigation_diagnostic =
                        Some(LatestNavigationAction::FollowEnabled);
                    tracing::info!(
                        target: "deebugee.diagnostics",
                        subsystem = "navigation",
                        event = "viewer.latest_navigation.requested",
                        status = "requested",
                        action = LatestNavigationAction::FollowEnabled.event_value(),
                        latest_at = self.latest_at.title(),
                        visible_row_count = self.table_rows.len(),
                        "[Navigation] Latest-event navigation requested"
                    );
                    self.request_scroll_to_latest();
                } else {
                    self.scroll_to_bottom_requested = false;
                    self.scroll_settle_frames = 0;
                    self.latest_navigation_diagnostic = None;
                }
            }
            let previous_latest_at = self.latest_at;
            egui::ComboBox::from_id_salt("settings_latest_at")
                .selected_text(format!("Place Latest Events: {}", self.latest_at.title()))
                .width(220.0)
                .show_ui(ui, |ui| {
                    for option in LatestAt::ALL {
                        ui.selectable_value(&mut self.latest_at, option, option.title());
                    }
                });
            if let Some(response) = ui.ctx().read_response(
                ui.make_persistent_id("settings_latest_at").with("popup"),
            ) {
                nested_popup_rects.push(response.interact_rect);
            }
            if self.latest_at != previous_latest_at {
                self.filter_changed();
            }

            ui.separator();
            ui.label(RichText::new("Table Appearance").strong());
            let wrap_changed = ui
                .checkbox(&mut self.wrapped_messages, "Wrap Message Text")
                .changed();
            if should_reanchor_after_wrap(
                wrap_changed,
                self.stick_to_bottom,
                self.tail_was_at_bottom,
                self.scroll_to_bottom_requested,
            ) {
                // Wrapping changes the virtual height of every message row. Preserve
                // the current latest anchor instead of retaining an offset calculated
                // for the old layout.
                self.request_scroll_to_latest();
            }
            ui.checkbox(&mut self.semantic_highlighting, "Highlight Meaningful Terms")
                .on_hover_text(
                    "Color positive, negative, warning, and activity phrases in log messages",
                );
            egui::ComboBox::from_id_salt("settings_color_by")
                .selected_text(format!("Color Rows By: {}", self.color_by.title()))
                .width(220.0)
                .show_ui(ui, |ui| {
                    for option in ColorBy::ALL {
                        ui.selectable_value(&mut self.color_by, option, option.title());
                    }
                });
            if let Some(response) = ui.ctx().read_response(
                ui.make_persistent_id("settings_color_by").with("popup"),
            ) {
                nested_popup_rects.push(response.interact_rect);
            }

            ui.separator();
            ui.label(RichText::new("Timestamps").strong());
            egui::ComboBox::from_id_salt("settings_timestamp_display")
                .selected_text(format!("Display: {}", self.timestamp_display.title()))
                .width(220.0)
                .show_ui(ui, |ui| {
                    for option in TimestampDisplay::ALL {
                        ui.selectable_value(&mut self.timestamp_display, option, option.title());
                    }
                })
                .response
                .on_hover_text(
                    "Display UTC log timestamps in your computer's local time, or keep UTC",
                );
            if let Some(response) = ui.ctx().read_response(
                ui.make_persistent_id("settings_timestamp_display")
                    .with("popup"),
            ) {
                nested_popup_rects.push(response.interact_rect);
            }
            ui.label(RichText::new("Format").small().color(TEXT_MUTED));
            ui.add_sized(
                [310.0, 26.0],
                egui::TextEdit::singleline(&mut self.timestamp_format)
                    .hint_text("%Y-%m-%d %H:%M:%S%.3f %:z"),
            );
            ui.label(
                RichText::new(
                    "Tokens: %Y year  %m month  %d day  %H hour  %M minute  %S second  %.3f ms  %:z offset",
                )
                .small()
                .color(TEXT_MUTED),
            );
            ui.horizontal_wrapped(|ui| {
                for (label, format) in [
                    ("Full", DEFAULT_TIMESTAMP_FORMAT),
                    ("Date + Time", "%d/%m/%Y %H:%M:%S"),
                    ("Time Only", "%H:%M:%S%.3f"),
                    ("US 12-Hour", "%m/%d/%Y %I:%M:%S %p"),
                ] {
                    if ui.small_button(label).clicked() {
                        self.timestamp_format = format.to_string();
                    }
                }
            });
            if ui.small_button("Reset Timestamp Format").clicked() {
                self.timestamp_format = default_timestamp_format();
            }

            ui.separator();
            ui.label(RichText::new("Data Retention").strong());
            ui.label(
                RichText::new("Maximum Events Kept in Memory").small().color(TEXT_MUTED),
            );
            let mut max_events = self.max_events;
            let limit_response = ui
                .add(
                    egui::DragValue::new(&mut max_events)
                        .range(1..=MAX_CONFIGURABLE_EVENTS)
                        .speed(100.0),
                )
                .on_hover_text(
                    "Pruning pauses while you read older entries. Changing this reloads the current sources.",
                );
            if limit_response.changed() {
                self.apply_event_limit(max_events);
                self.event_limit_reload_pending = true;
            }
            let commit_limit = self.event_limit_reload_pending
                && (limit_response.drag_stopped()
                    || limit_response.lost_focus()
                    || (!limit_response.has_focus() && !limit_response.dragged())
                    || (limit_response.has_focus()
                        && ui.input(|input| input.key_pressed(egui::Key::Enter))));
            if commit_limit {
                self.reload_current_sources();
            }

            ui.separator();
            ui.label(RichText::new("Facet Sections").strong());
            ui.label(RichText::new("Show, hide, and order the filter sections.").small().color(TEXT_MUTED));
            let facets = self.facet_order.clone();
            let mut move_facet = None;
            for (index, facet) in facets.iter().enumerate() {
                ui.horizontal(|ui| {
                    let mut visible = !self.hidden_facets.contains(facet);
                    if ui.checkbox(&mut visible, facet_title(facet)).changed() {
                        if visible { self.hidden_facets.remove(facet); } else { self.hidden_facets.insert(facet.clone()); }
                        self.filters_dirty = true;
                    }
                    if ui.small_button("↑").on_hover_text("Move section up").clicked() && index > 0 {
                        move_facet = Some((index, index - 1));
                    }
                    if ui.small_button("↓").on_hover_text("Move section down").clicked() && index + 1 < facets.len() {
                        move_facet = Some((index, index + 1));
                    }
                });
            }
            if let Some((from, to)) = move_facet {
                self.facet_order.swap(from, to);
            }

            ui.separator();
            ui.label(RichText::new("About").strong());
            ui.label(format!("DeeBugee v{}", env!("CARGO_PKG_VERSION")));
            if let UpdateState::Available(update) = &self.update_state {
                ui.label(
                    RichText::new(format!("Update {} Available", update.version)).color(SUCCESS),
                );
            }
            let update_label = if matches!(self.update_state, UpdateState::Checking) {
                "Checking for Updates…"
            } else {
                "Check for Updates"
            };
            if ui
                .add_enabled(
                    !matches!(self.update_state, UpdateState::Checking),
                    egui::Button::new(update_label),
                )
                .clicked()
            {
                self.begin_update_check();
            }
            if let UpdateState::Failed(error) = &self.update_state {
                ui.label(RichText::new(error).small().color(WARNING));
            }
        });

        if let Some(popup_response) = popup_response {
            let pointer_pos = ui.ctx().pointer_interact_pos();
            let pointer_in_nested_popup = pointer_pos.is_some_and(|position| {
                nested_popup_rects
                    .iter()
                    .any(|rect| rect.contains(position))
            });
            let clicked_outside = should_close_settings_popup(
                settings_button_clicked,
                ui.ctx().input(|input| input.pointer.any_click()),
                popup_response.response.clicked_elsewhere(),
                pointer_in_nested_popup,
            );
            if clicked_outside {
                settings_open = false;
            }
        }

        self.settings_open = settings_open;
    }

    fn sidebar(&mut self, root: &mut egui::Ui) {
        let active_filter_count = usize::from(!self.filter.text.trim().is_empty())
            + usize::from(self.filter.minimum_level.is_some())
            + usize::from(self.filter.correlation.is_some())
            + self
                .filter
                .facets
                .values()
                .filter(|selection| !selection.is_empty())
                .count();
        egui::Panel::left("facets")
            .default_size(248.0)
            .min_size(200.0)
            .resizable(true)
            .frame(
                egui::Frame::new()
                    .fill(SURFACE_1)
                    .inner_margin(egui::Margin::symmetric(10, 8))
                    .stroke(Stroke::new(1.0, BORDER)),
            )
            .show(root, |ui| {
                ui.spacing_mut().item_spacing.y = 3.0;
                ui.spacing_mut().interact_size.y = 22.0;

                ui.horizontal(|ui| {
                    ui.heading("Filters");
                    if active_filter_count > 0 {
                        ui.label(
                            RichText::new(active_filter_count.to_string())
                                .strong()
                                .color(ACCENT),
                        );
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let button_height = ui.spacing().interact_size.y.max(26.0);
                        if ui
                            .add_enabled_ui(active_filter_count > 0, |ui| {
                                ui.add_sized([52.0, button_height], egui::Button::new("Reset"))
                            })
                            .inner
                            .clicked()
                        {
                            self.filter.clear();
                            self.filter_changed();
                        }
                        let label = if self.facet_sections_expanded {
                            "Collapse All"
                        } else {
                            "Expand All"
                        };
                        if ui
                            .add_sized([82.0, button_height], egui::Button::new(label))
                            .clicked()
                        {
                            self.facet_sections_expanded = !self.facet_sections_expanded;
                            self.facet_sections_request = Some(self.facet_sections_expanded);
                        }
                    });
                });
                ui.separator();

                ui.horizontal(|ui| {
                    let control_height = ui.spacing().interact_size.y.max(26.0);
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui
                            .add_enabled_ui(!self.facet_search.is_empty(), |ui| {
                                ui.add_sized([48.0, control_height], egui::Button::new("Clear"))
                            })
                            .inner
                            .clicked()
                        {
                            self.facet_search.clear();
                        }
                        let search_width = ui.available_width().max(80.0);
                        let search_response = ui.add_sized(
                            [search_width, control_height],
                            egui::TextEdit::singleline(&mut self.facet_search)
                                .vertical_align(egui::Align::Center),
                        );
                        if self.facet_search.is_empty() {
                            ui.painter().text(
                                search_response.rect.left_center() + egui::vec2(4.0, 0.0),
                                egui::Align2::LEFT_CENTER,
                                "Search facet values…",
                                egui::TextStyle::Body.resolve(ui.style()),
                                ui.visuals().weak_text_color(),
                            );
                        }
                    });
                });
                ui.add_space(3.0);

                let facet_sections_request = self.facet_sections_request.take();
                egui::ScrollArea::vertical().show(ui, |ui| {
                    let search = self.facet_search.trim().to_ascii_lowercase();
                    let facets = self.displayed_facets();
                    for facet in facets {
                        let counts: Vec<(String, u64)> = self
                            .facet_counts
                            .get(&facet)
                            .into_iter()
                            .flatten()
                            .filter(|(value, _)| facet_value_matches_search(value, &search))
                            .take(100)
                            .cloned()
                            .collect();
                        let has_search_result = !search.is_empty() && !counts.is_empty();
                        let active = self
                            .filter
                            .facets
                            .get(&facet)
                            .is_some_and(|selection| !selection.is_empty());
                        let title = if active {
                            RichText::new(facet_title(&facet)).strong().color(ACCENT)
                        } else {
                            RichText::new(facet_title(&facet)).strong()
                        };

                        egui::CollapsingHeader::new(title)
                            .default_open(matches!(
                                facet.as_str(),
                                "level" | "source" | "subsystem"
                            ))
                            .open(facet_section_open_request(
                                has_search_result,
                                facet_sections_request,
                            ))
                            .show(ui, |ui| {
                                for (value, count) in counts {
                                    self.facet_value_row(ui, &facet, &value, count);
                                }
                            });
                    }
                });
            });
    }

    fn facet_value_row(&mut self, ui: &mut egui::Ui, facet: &str, value: &str, count: u64) {
        let state = self.filter.facets.get(facet);
        let included = state.is_some_and(|selection| selection.included.contains(value));
        let excluded = state.is_some_and(|selection| selection.excluded.contains(value));
        let label = format!("{value}   {count}");
        let text = if excluded {
            RichText::new(label).color(DANGER).strikethrough()
        } else if included {
            RichText::new(label)
                .strong()
                .color(Color32::from_rgb(190, 207, 255))
        } else {
            RichText::new(label).color(TEXT_PRIMARY)
        };
        let response = ui
            .selectable_label(included, text)
            .on_hover_text("Left-click to show only; Ctrl+left-click to add; right-click to hide");

        if response.clicked_by(PointerButton::Primary) {
            let selection = self.filter.facets.entry(facet.to_string()).or_default();
            apply_primary_facet_click(selection, value, ui.input(|input| input.modifiers.ctrl));
            self.filter_changed();
        }
        if response.clicked_by(PointerButton::Secondary) {
            self.filter
                .facets
                .entry(facet.to_string())
                .or_default()
                .toggle_exclude(value);
            self.filter_changed();
        }
    }

    fn bookmark_bar(&mut self, ui: &mut egui::Ui) {
        if !self.filter.is_active() && self.bookmarks.is_empty() {
            return;
        }

        let already_saved = self
            .bookmarks
            .iter()
            .any(|bookmark| bookmark.filter == self.filter);
        let bookmarks = self.bookmarks.clone();
        let mut apply_filter = None;
        let mut overwrite_index = None;
        let mut remove_index = None;

        ui.horizontal_wrapped(|ui| {
            ui.label(
                RichText::new("SAVED VIEWS")
                    .strong()
                    .small()
                    .color(TEXT_MUTED),
            );
            if ui
                .add_enabled(
                    self.filter.is_active() && !already_saved,
                    egui::Button::new("＋ Save Current"),
                )
                .on_hover_text("Save the current search and facet filters")
                .clicked()
            {
                self.bookmarks.push(FilterBookmark {
                    name: bookmark_name(&self.filter),
                    filter: self.filter.clone(),
                });
            }

            for (index, bookmark) in bookmarks.iter().enumerate() {
                let response = ui
                    .selectable_label(bookmark.filter == self.filter, bookmark.name.clone())
                    .on_hover_text(
                        "Click to apply; Ctrl+Alt+click to update from the current filters; \
                         middle-click to rename; right-click to remove",
                    );
                if response.clicked() {
                    let modifiers = ui.input(|input| input.modifiers);
                    if modifiers.ctrl && modifiers.alt {
                        overwrite_index = Some(index);
                    } else {
                        apply_filter = Some(bookmark.filter.clone());
                    }
                }
                if response.clicked_by(PointerButton::Middle) {
                    self.bookmark_rename = Some(BookmarkRename {
                        index,
                        name: bookmark.name.clone(),
                    });
                }
                if response.clicked_by(PointerButton::Secondary) {
                    remove_index = Some(index);
                }
            }
        });
        ui.separator();

        if let Some(filter) = apply_filter {
            self.filter = filter;
            self.filter_changed();
        }
        if let Some(index) = overwrite_index
            && let Some(bookmark) = self.bookmarks.get_mut(index)
        {
            bookmark.filter = self.filter.clone();
        }
        if let Some(index) = remove_index {
            self.bookmarks.remove(index);
        }

        self.bookmark_rename_dialog(ui.ctx());
    }

    fn bookmark_rename_dialog(&mut self, context: &egui::Context) {
        let Some(rename) = self.bookmark_rename.as_mut() else {
            return;
        };

        let mut save = false;
        let mut cancel = false;
        egui::Window::new("Rename saved view")
            .collapsible(false)
            .resizable(false)
            .show(context, |ui| {
                ui.label("Name");
                let name = ui.add(
                    egui::TextEdit::singleline(&mut rename.name)
                        .desired_width(260.0)
                        .hint_text("Saved view name"),
                );
                name.request_focus();

                ui.horizontal(|ui| {
                    let can_save = !rename.name.trim().is_empty();
                    save = ui
                        .add_enabled(can_save, egui::Button::new("Save"))
                        .clicked()
                        || (can_save && ui.input(|input| input.key_pressed(egui::Key::Enter)));
                    cancel = ui.button("Cancel").clicked()
                        || ui.input(|input| input.key_pressed(egui::Key::Escape));
                });
            });

        if save {
            if let Some(rename) = self.bookmark_rename.take()
                && let Some(bookmark) = self.bookmarks.get_mut(rename.index)
            {
                bookmark.name = rename.name.trim().to_owned();
            }
        } else if cancel {
            self.bookmark_rename = None;
        }
    }

    fn pin_bar(&mut self, ui: &mut egui::Ui) {
        if self.pins.is_empty() {
            return;
        }
        let mut previous_timestamp = None;
        let summaries: Vec<(String, String)> = self
            .pins
            .iter()
            .map(|pin| {
                let label = pin.event().map_or_else(
                    || "Unavailable event".to_owned(),
                    |event| {
                        let delta = previous_timestamp
                            .map(|previous| {
                                format!(
                                    "{} · ",
                                    format_relative_elapsed(event.timestamp - previous)
                                )
                            })
                            .unwrap_or_default();
                        previous_timestamp = Some(event.timestamp);
                        format!(
                            "{delta}{} · {}",
                            event.event,
                            compact_identifier(event.correlation_id(), 14)
                        )
                    },
                );
                (pin.key.clone(), label)
            })
            .collect();
        egui::Frame::new()
            .fill(SURFACE_2)
            .inner_margin(egui::Margin::symmetric(8, 5))
            .show(ui, |ui| {
                ui.horizontal_wrapped(|ui| {
                    ui.label(
                        RichText::new(format!("Pins {}", self.pins.len()))
                            .strong()
                            .color(ACCENT),
                    );
                    if ui
                        .small_button("← Previous")
                        .on_hover_text("Ctrl+Up")
                        .clicked()
                    {
                        self.navigate_pin(-1);
                    }
                    if ui
                        .small_button("Next →")
                        .on_hover_text("Ctrl+Down")
                        .clicked()
                    {
                        self.navigate_pin(1);
                    }
                    if self.pins.len() >= 2
                        && let (Some(first), Some(last)) = (
                            self.pins.first().and_then(PinnedEvent::event),
                            self.pins.last().and_then(PinnedEvent::event),
                        )
                    {
                        ui.label(
                            RichText::new(format!(
                                "span {}",
                                format_relative_elapsed(last.timestamp - first.timestamp)
                            ))
                            .small()
                            .color(TEXT_MUTED),
                        );
                    }
                    for (key, label) in &summaries {
                        if ui.small_button(label).clicked() {
                            self.select_event_key(key);
                        }
                    }
                });
            });
        ui.add_space(4.0);
    }

    fn pin_note_window(&mut self, context: &egui::Context) {
        let Some((key, mut note)) = self.pin_note_edit.take() else {
            return;
        };
        let mut open = true;
        let mut save = false;
        let mut cancel = false;
        egui::Window::new("Investigation Note")
            .collapsible(false)
            .resizable(true)
            .default_width(430.0)
            .open(&mut open)
            .show(context, |ui| {
                ui.label(
                    RichText::new("Saved with this pinned event.")
                        .small()
                        .color(TEXT_MUTED),
                );
                ui.add(
                    egui::TextEdit::multiline(&mut note)
                        .desired_rows(6)
                        .desired_width(f32::INFINITY),
                );
                ui.horizontal(|ui| {
                    save = ui.button("Save Note").clicked();
                    cancel = ui.button("Cancel").clicked()
                        || ui.input(|input| input.key_pressed(egui::Key::Escape));
                });
            });
        if save {
            if let Some(pin) = self.pins.iter_mut().find(|pin| pin.key == key) {
                pin.note = note;
            }
        } else if open && !cancel {
            self.pin_note_edit = Some((key, note));
        }
    }

    fn details_panel(&mut self, root: &mut egui::Ui) {
        let Some(event) = self
            .selected_row
            .and_then(|row| self.store.get(row))
            .cloned()
        else {
            return;
        };
        let semantic_highlighting = self.semantic_highlighting;
        let group_detail = self.selected_row.and_then(|row| {
            self.error_groups.get(&row).and_then(|group| {
                error_group_summary(
                    &self.store,
                    *group,
                    self.timestamp_display,
                    &self.timestamp_format,
                )
            })
        });

        egui::Panel::bottom("details")
            .resizable(true)
            .default_size(220.0)
            .min_size(120.0)
            .frame(
                egui::Frame::new()
                    .fill(SURFACE_1)
                    .inner_margin(egui::Margin::symmetric(14, 10))
                    .stroke(Stroke::new(1.0, BORDER)),
            )
            .show(root, |ui| {
                ui.horizontal(|ui| {
                    ui.heading("Event Details");
                    ui.label(
                        RichText::new(event.level.to_string().to_uppercase())
                            .strong()
                            .small()
                            .color(level_color(event.level)),
                    );
                    ui.label(
                        RichText::new(
                            self.timestamp_display
                                .format(&event, &self.timestamp_format),
                        )
                        .small()
                        .color(TEXT_MUTED),
                    );
                    if ui
                        .button("Copy")
                        .on_hover_text("Copy all event details")
                        .clicked()
                    {
                        let raw = serde_json::to_string_pretty(&event)
                            .unwrap_or_else(|error| format!("Unable to serialize event: {error}"));
                        ui.ctx().copy_text(format!("{}\n\n{}", event.message, raw));
                    }
                    if ui.button("Copy Correlation").clicked() {
                        ui.ctx().copy_text(event.correlation_id().to_owned());
                    }
                    let pinned = self
                        .selected_pin_key()
                        .is_some_and(|key| self.pins.iter().any(|pin| pin.key == key));
                    if ui
                        .button(if pinned { "Unpin" } else { "Pin" })
                        .on_hover_text("P")
                        .clicked()
                    {
                        self.toggle_selected_pin();
                    }
                    if pinned
                        && ui.button("Note…").on_hover_text("N").clicked()
                        && let Some(key) = self.selected_pin_key()
                        && let Some(pin) = self.pins.iter().find(|pin| pin.key == key)
                    {
                        self.pin_note_edit = Some((key, pin.note.clone()));
                    }
                    if ui.button("Filter By Correlation").clicked() {
                        self.filter.correlation = Some(event.correlation_id().to_string());
                        self.filter_changed();
                    }
                    if self.filter.correlation.is_some() && ui.button("Clear Correlation").clicked()
                    {
                        self.filter.correlation = None;
                        self.filter_changed();
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.button("× Close").clicked() {
                            self.selected_row = None;
                        }
                    });
                });
                ui.add_space(4.0);
                egui::ScrollArea::vertical().show(ui, |ui| {
                    if semantic_highlighting {
                        let mut message =
                            highlighted_message(&event.message, ui.visuals().text_color());
                        for section in &mut message.sections {
                            section.format.font_id = FontId::proportional(15.0);
                        }
                        ui.add(Label::new(message));
                    } else {
                        ui.label(RichText::new(&event.message).strong().size(15.0));
                    }
                    if let Some(detail) = console_argument_summary(&event) {
                        ui.label(RichText::new(detail).color(TEXT_MUTED));
                    }
                    if let Some(detail) = &group_detail {
                        ui.label(RichText::new(detail).small().color(TEXT_MUTED));
                    }
                    ui.separator();
                    if !event.fields.is_empty() {
                        ui.label(RichText::new("Fields").strong());
                        egui::Grid::new("event_field_paths")
                            .striped(true)
                            .show(ui, |ui| {
                                for (name, value) in &event.fields {
                                    let path = format!("fields.{name}");
                                    ui.monospace(&path);
                                    ui.label(compact_identifier(&value.to_string(), 80));
                                    if ui.small_button("Copy Path").clicked() {
                                        ui.ctx().copy_text(path);
                                    }
                                    ui.end_row();
                                }
                            });
                        ui.separator();
                    }
                    let mut raw = serde_json::to_string_pretty(&event)
                        .unwrap_or_else(|error| format!("Unable to serialize event: {error}"));
                    ui.add(
                        egui::TextEdit::multiline(&mut raw)
                            .font(TextStyle::Monospace)
                            .desired_width(f32::INFINITY)
                            .interactive(false),
                    );
                });
            });
    }

    fn session_explorer_window(&mut self, context: &egui::Context) {
        if !self.session_explorer.open {
            return;
        }

        let summaries = self.session_explorer.sorted_summaries();
        let mut open = true;
        let mut compare_a = self.session_explorer.compare_a.clone();
        let mut compare_b = self.session_explorer.compare_b.clone();
        let mut filter_session = None;
        let timestamp_display = self.timestamp_display;
        let timestamp_format = self.timestamp_format.clone();

        egui::Window::new("Session Explorer")
            .id(egui::Id::new("session_explorer"))
            .open(&mut open)
            .default_size(egui::vec2(920.0, 620.0))
            .min_size(egui::vec2(680.0, 380.0))
            .resizable(true)
            .show(context, |ui| {
                if summaries.is_empty() {
                    ui.centered_and_justified(|ui| {
                        ui.label(
                            RichText::new("No application sessions are loaded yet.")
                                .color(TEXT_MUTED),
                        );
                    });
                    return;
                }

                ui.horizontal(|ui| {
                    ui.heading("Application Runs");
                    ui.label(
                        RichText::new(format!("{} sessions", summaries.len()))
                            .small()
                            .color(TEXT_MUTED),
                    );
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui
                            .add_enabled(
                                compare_a.is_some() || compare_b.is_some(),
                                egui::Button::new("Clear Comparison"),
                            )
                            .clicked()
                        {
                            compare_a = None;
                            compare_b = None;
                        }
                    });
                });
                ui.label(
                    RichText::new(
                        "Mark two runs as A and B to compare event coverage, outcomes, errors, and timing.",
                    )
                    .small()
                    .color(TEXT_MUTED),
                );
                ui.separator();

                ui.columns(2, |columns| {
                    columns[0].set_min_width(300.0);
                    egui::ScrollArea::vertical()
                        .id_salt("session_list")
                        .show(&mut columns[0], |ui| {
                            for summary in &summaries {
                                session_summary_card(
                                    ui,
                                    summary,
                                    timestamp_display,
                                    &timestamp_format,
                                    &mut compare_a,
                                    &mut compare_b,
                                    &mut filter_session,
                                );
                                ui.add_space(6.0);
                            }
                        });

                    columns[1].set_min_width(360.0);
                    session_comparison_ui(
                        &mut columns[1],
                        &summaries,
                        compare_a.as_deref(),
                        compare_b.as_deref(),
                    );
                });
            });

        self.session_explorer.open = open;
        self.session_explorer.compare_a = compare_a;
        self.session_explorer.compare_b = compare_b;
        if let Some(session_id) = filter_session {
            apply_session_filter(&mut self.filter, session_id);
            self.filter_changed();
        }
    }

    fn central_table(&mut self, root: &mut egui::Ui) {
        egui::CentralPanel::default().show(root, |ui| {
            if self.store.is_empty() {
                ui.centered_and_justified(|ui| {
                    ui.vertical_centered(|ui| {
                        ui.label(RichText::new("{  }").monospace().size(34.0).color(ACCENT));
                        ui.add_space(10.0);
                        ui.heading("Open Logs to Start Exploring");
                        ui.label(
                            RichText::new(
                                "Choose JSONL files, open a folder, or drop files anywhere in this window.",
                            )
                            .color(TEXT_MUTED),
                        );
                        ui.add_space(4.0);
                        ui.label(
                            RichText::new("New records are followed automatically.")
                                .small()
                                .color(TEXT_MUTED),
                        );
                    });
                });
                return;
            }

            self.bookmark_bar(ui);
            self.pin_bar(ui);

            let row_height = ui.text_style_height(&TextStyle::Body) + 8.0;
            // Capture this before the nested table consumes it. We apply it only after
            // knowing the pointer is over the table and only when it moves away from
            // the latest edge; a wheel attempt at the edge must not disable following.
            let (pointer_position, raw_vertical_scroll_delta) = ui.input(|input| {
                let raw_vertical_scroll_delta = raw_mouse_wheel_delta_y(&input.raw.events);
                (input.pointer.interact_pos(), raw_vertical_scroll_delta)
            });
            let column_order = self.column_order.clone();
            let table_rows = &self.table_rows;
            let error_groups = &self.error_groups;
            let store = &self.store;
            let wrapped = self.wrapped_messages;
            let semantic_highlighting = self.semantic_highlighting;
            let show_fields = self.show_fields;
            let color_by = self.color_by;
            let latest_at = self.latest_at;
            let timestamp_display = self.timestamp_display;
            let timestamp_format = &self.timestamp_format;
            let scroll_to_bottom_requested = self.scroll_to_bottom_requested;
            let latest_view_is_current = latest_view_is_current(
                self.filters_dirty,
                !self.filter.text.trim().is_empty(),
                self.cached_search_is_current(),
            );
            let was_at_latest_before_table = self.tail_was_at_bottom;
            let scroll_to_selected_requested = self.scroll_to_selected_requested;
            let selected_scroll_row = self.selected_row.and_then(|selected| {
                self.table_rows.iter().position(|row| *row == selected)
            });
            let session_starts = session_start_times(&self.session_explorer.summaries);
            // Labels are normally selectable so log text can be copied. egui's
            // selection handler treats every pointer button as a drag source,
            // including the middle button we reserve for table panning.
            let middle_pan_down = ui.input(|input| input.pointer.middle_down());
            let table_pan_id = ui.make_persistent_id("events_middle_pan");
            let middle_pan_started_here = ui.input(|input| {
                input.pointer.button_pressed(PointerButton::Middle)
                    && input.pointer.interact_pos().is_some_and(|position| {
                        ui.available_rect_before_wrap().contains(position)
                    })
            });
            if middle_pan_started_here {
                // Claim the drag before TableBuilder creates resize handles or
                // other draggable widgets. This makes the wheel button exclusive
                // to panning for the full duration of the gesture.
                ui.ctx().set_dragged_id(table_pan_id);
            }
            let mut selected = self.selected_row;
            let mut requested_move = None;
            let table_content_width = ui.available_width().max(1_400.0);
            let mut horizontal_output = ui
                .scope(|ui| {
                    if middle_pan_down {
                        ui.style_mut().interaction.selectable_labels = false;
                    }
                    egui::ScrollArea::horizontal()
                    .id_salt("events_horizontal_scroll")
                    .scroll_bar_visibility(egui::scroll_area::ScrollBarVisibility::AlwaysVisible)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                    ui.set_min_width(table_content_width);
                    let mut table = TableBuilder::new(ui)
                        .id_salt("events")
                        .striped(true)
                        .resizable(true)
                        .sense(Sense::click());
                    for column in &column_order {
                        table = table.column(column.layout());
                    }
                    if scroll_to_bottom_requested && !table_rows.is_empty() {
                        table = match latest_at {
                            LatestAt::Top => table.vertical_scroll_offset(0.0),
                            // The final spacer row supplies deliberate breathing room below the
                            // newest record, so it is the true latest scroll target.
                            LatestAt::Bottom => table
                                .scroll_to_row(table_rows.len(), Some(egui::Align::BOTTOM)),
                        }
                        .animate_scrolling(false);
                    } else if scroll_to_selected_requested
                        && let Some(row) = selected_scroll_row
                    {
                        table = table.scroll_to_row(row, Some(egui::Align::Center)).animate_scrolling(false);
                    }

                    table
                        .header(row_height + 4.0, |mut header| {
                            for column in &column_order {
                                header.col(|ui| {
                                    ui.painter().rect_filled(ui.max_rect(), 0.0, SURFACE_2);
                                    let (drop_zone, dropped) = ui.dnd_drop_zone::<TableColumn, _>(
                                        egui::Frame::NONE,
                                        |ui| {
                                            let drag_source = ui.dnd_drag_source(
                                                ui.make_persistent_id((
                                                    "table_column_drag",
                                                    *column,
                                                )),
                                                *column,
                                                |ui| {
                                                    ui.add_sized(
                                                        ui.available_size(),
                                                        Label::new(
                                                            RichText::new(column.title())
                                                            .strong()
                                                            .color(Color32::from_rgb(190, 198, 211)),
                                                        ),
                                                    )
                                                },
                                            );
                                            // The helper publishes on the next frame. Also
                                            // publish on drag start so a quick drag can finish.
                                            drag_source.response.dnd_set_drag_payload(*column);
                                            if drag_source.response.drag_started()
                                                || drag_source.response.dragged()
                                            {
                                                ui.ctx().request_repaint();
                                            }
                                            drag_source.response.on_hover_text(
                                                "Hold and drag to rearrange this column",
                                            );
                                        },
                                    );

                                    if let Some(source) =
                                        drop_zone.response.dnd_hover_payload::<TableColumn>()
                                        && *source != *column
                                    {
                                        let insert_after = ui
                                            .ctx()
                                            .pointer_interact_pos()
                                            .is_some_and(|position| {
                                                position.x >= drop_zone.response.rect.center().x
                                            });
                                        let marker_x = if insert_after {
                                            drop_zone.response.rect.right()
                                        } else {
                                            drop_zone.response.rect.left()
                                        };
                                        ui.painter().line_segment(
                                            [
                                                egui::pos2(marker_x, drop_zone.response.rect.top()),
                                                egui::pos2(
                                                    marker_x,
                                                    drop_zone.response.rect.bottom(),
                                                ),
                                            ],
                                            egui::Stroke::new(4.0, Color32::LIGHT_BLUE),
                                        );
                                    }

                                    if let Some(source) = dropped
                                        && *source != *column
                                    {
                                        let insert_after = ui
                                            .ctx()
                                            .pointer_interact_pos()
                                            .is_some_and(|position| {
                                                position.x >= drop_zone.response.rect.center().x
                                            });
                                        requested_move = Some((*source, *column, insert_after));
                                    }
                                });
                            }
                        })
                        .body(|body| {
                            let message_width = column_order
                                .iter()
                                .position(|column| *column == TableColumn::Message)
                                .and_then(|index| body.widths().get(index).copied())
                                .unwrap_or(400.0);
                            let heights: Vec<f32> = table_rows
                                .iter()
                                .map(|index| {
                                    let Some(event) = store.get(*index) else {
                                        return row_height;
                                    };
                                    let group_detail = error_groups
                                        .get(index)
                                        .and_then(|group| error_group_summary(store, *group, timestamp_display, timestamp_format));
                                    let field_details = event_field_summaries(event, show_fields);
                                    let legacy_detail = (!show_fields)
                                        .then(|| console_argument_summary(event))
                                        .flatten();
                                    let detail_lines = field_details.len()
                                        + usize::from(legacy_detail.is_some())
                                        + usize::from(group_detail.is_some());
                                    if !wrapped {
                                        return row_height * (1 + detail_lines) as f32;
                                    }
                                    let characters_per_line = (message_width / 7.2).max(12.0);
                                    let lines = std::iter::once(event.message.as_str())
                                        .chain(legacy_detail.as_deref())
                                        .chain(field_details.iter().map(String::as_str))
                                        .chain(group_detail.as_deref())
                                        .map(|detail| {
                                            ((detail.chars().count() as f32
                                                / characters_per_line)
                                                .ceil() as usize)
                                                .max(1)
                                        })
                                        .sum::<usize>()
                                        .clamp(1, 12);
                                    row_height * lines as f32
                                })
                                .chain(std::iter::once(row_height * TAIL_HEADROOM_ROWS))
                                .collect();

                            body.heterogeneous_rows(heights.into_iter(), |mut row| {
                                if row.index() == table_rows.len() {
                                    for _ in &column_order {
                                        row.col(|_| {});
                                    }
                                    return;
                                }
                                let Some(store_index) = table_rows.get(row.index()).copied()
                                else {
                                    return;
                                };
                                let Some(event) = store.get(store_index) else {
                                    return;
                                };
                                let row_color = color_by
                                    .event_value(event)
                                    .as_deref()
                                    .map(stable_value_color);
                                let group_detail = error_groups
                                    .get(&store_index)
                                    .and_then(|group| error_group_summary(store, *group, timestamp_display, timestamp_format));
                                row.set_selected(selected == Some(store_index));
                                for column in &column_order {
                                    row.col(|ui| {
                                        if let Some(color) = row_color {
                                            ui.visuals_mut().override_text_color = Some(color);
                                        }
                                        show_event_cell(
                                            ui,
                                            *column,
                                            event,
                                            EventCellOptions {
                                                wrapped,
                                                semantic_highlighting,
                                                timestamp_display,
                                                timestamp_format,
                                                relative_start: session_starts.get(&event.app_session_id).copied(),
                                                group_detail: group_detail.as_deref(),
                                                show_fields,
                                            },
                                        );
                                    });
                                }
                                if row.response().clicked() {
                                    selected = Some(store_index);
                                }
                            });
                        })
                    })
                })
                .inner;

            let (middle_pressed, middle_down, primary_down, middle_pointer_position, pointer_delta) =
                ui.input(|input| {
                    (
                        input.pointer.button_pressed(PointerButton::Middle),
                        input.pointer.middle_down(),
                        input.pointer.primary_down(),
                        input.pointer.interact_pos(),
                        input.pointer.delta(),
                    )
                });
            if middle_pressed
                && middle_pointer_position
                    .is_some_and(|position| horizontal_output.inner_rect.contains(position))
            {
                self.middle_pan_active = true;
            }
            if !middle_down {
                self.middle_pan_active = false;
            }
            let middle_panned =
                self.middle_pan_active && middle_down && pointer_delta != egui::Vec2::ZERO;
            if middle_panned {
                horizontal_output.state.offset.x = panned_scroll_offset(
                    horizontal_output.state.offset.x,
                    pointer_delta.x,
                    horizontal_output.content_size.x,
                    horizontal_output.inner_rect.width(),
                );
                horizontal_output
                    .state
                    .store(ui.ctx(), horizontal_output.id);

                let vertical_output = &mut horizontal_output.inner;
                vertical_output.state.offset.y = panned_scroll_offset(
                    vertical_output.state.offset.y,
                    pointer_delta.y,
                    vertical_output.content_size.y,
                    vertical_output.inner_rect.height(),
                );
                vertical_output.state.store(ui.ctx(), vertical_output.id);
                ui.ctx().set_cursor_icon(egui::CursorIcon::Grabbing);
                ui.ctx().request_repaint();
            }

            let vertical_output = &mut horizontal_output.inner;
            let wheel_scrolled_away_from_latest = pointer_position
                .is_some_and(|position| vertical_output.inner_rect.contains(position))
                && scroll_delta_moves_away_from_latest(raw_vertical_scroll_delta, latest_at);
            // The table exposes its nested ScrollArea id even though it does not
            // expose the scrollbar Response. egui's vertical handle uses id.with(1),
            // which lets us distinguish a real scrollbar drag from text selection,
            // row clicks, and column resizing elsewhere in the table.
            let scrollbar_dragged = vertical_scrollbar_dragged(
                ui.ctx().dragged_id(),
                vertical_output.id,
                primary_down,
            );
            let manually_scrolled = wheel_scrolled_away_from_latest
                || (middle_panned && pointer_delta.y.abs() > f32::EPSILON)
                || scrollbar_dragged;
            if scroll_to_bottom_requested && !manually_scrolled && latest_at == LatestAt::Bottom {
                // `scroll_to_row` is evaluated while the virtual table is being laid out.
                // Use the completed output as the authority for the final position so
                // a changed filter, wrap width, or deferred row measurement cannot
                // leave records below the visible viewport.
                vertical_output.state.offset.y = latest_scroll_offset(
                    vertical_output.content_size.y,
                    vertical_output.inner_rect.height(),
                );
                vertical_output.state.store(ui.ctx(), vertical_output.id);
            }
            self.tail_was_at_bottom = !manually_scrolled
                && scroll_is_at_latest(
                    vertical_output.state.offset.y,
                    vertical_output.content_size.y,
                    vertical_output.inner_rect.height(),
                    row_height.max(2.0),
                    latest_at,
                );
            if !self.tail_was_at_bottom {
                self.store.set_pruning_paused(true);
            }
            // This is the only automatic-scroll mechanism. Keep an explicit latest
            // request alive until a subsequent frame reaches the actual edge; new
            // records, Wrap, filter changes, and Jump all use this same path.
            let previous_scroll_request = self.scroll_to_bottom_requested;
            (self.scroll_to_bottom_requested, self.scroll_settle_frames) = advance_scroll_request(
                scroll_to_bottom_requested,
                self.tail_was_at_bottom,
                manually_scrolled,
                latest_view_is_current,
                self.scroll_settle_frames,
            );
            if previous_scroll_request
                && !self.scroll_to_bottom_requested
                && let Some(action) = self.latest_navigation_diagnostic.take()
            {
                let maximum_offset = latest_scroll_offset(
                    vertical_output.content_size.y,
                    vertical_output.inner_rect.height(),
                );
                if manually_scrolled {
                    tracing::warn!(
                        target: "deebugee.diagnostics",
                        subsystem = "navigation",
                        event = "viewer.latest_navigation.canceled",
                        status = "canceled",
                        action = action.event_value(),
                        latest_at = latest_at.title(),
                        raw_scroll_delta_y = raw_vertical_scroll_delta,
                        middle_panned,
                        scrollbar_dragged,
                        current_offset = vertical_output.state.offset.y,
                        maximum_offset,
                        "[Navigation] Latest-event navigation canceled by manual input"
                    );
                } else {
                    tracing::info!(
                        target: "deebugee.diagnostics",
                        subsystem = "navigation",
                        event = "viewer.latest_navigation.completed",
                        status = "completed",
                        action = action.event_value(),
                        latest_at = latest_at.title(),
                        current_offset = vertical_output.state.offset.y,
                        maximum_offset,
                        content_height = vertical_output.content_size.y,
                        viewport_height = vertical_output.inner_rect.height(),
                        visible_row_count = table_rows.len(),
                        "[Navigation] Latest-event navigation completed"
                    );
                }
            }
            let resume_follow_after_deferred_refresh = should_resume_follow_at_latest_edge(
                self.stick_to_bottom,
                was_at_latest_before_table,
                self.tail_was_at_bottom,
                latest_view_is_current,
            );

            self.selected_row = selected;
            self.scroll_to_selected_requested = false;
            if let Some((source, target, insert_after)) = requested_move {
                move_column(&mut self.column_order, source, target, insert_after);
            }

            if !self.tail_was_at_bottom {
                let table_rect = horizontal_output.inner_rect;
                let (position, pivot, label) = match latest_at {
                    LatestAt::Top => (
                        egui::pos2(table_rect.center().x, table_rect.top() + 12.0),
                        egui::Align2::CENTER_TOP,
                        "↑  Jump to latest",
                    ),
                    LatestAt::Bottom => (
                        egui::pos2(table_rect.center().x, table_rect.bottom() - 12.0),
                        egui::Align2::CENTER_BOTTOM,
                        "↓  Jump to latest",
                    ),
                };
                let jump_clicked = egui::Area::new(egui::Id::new("jump_to_latest"))
                    .order(egui::Order::Foreground)
                    .fixed_pos(position)
                    .pivot(pivot)
                    .show(ui.ctx(), |ui| {
                        ui.add(
                            egui::Button::new(
                                RichText::new(label).strong().color(TEXT_PRIMARY),
                            )
                            .fill(SURFACE_2)
                            .stroke(Stroke::new(1.0, BORDER))
                            .corner_radius(8)
                            .min_size(egui::vec2(0.0, 36.0)),
                        )
                        .on_hover_text("Jump to the newest matching event")
                    })
                    .inner
                    .clicked();
                if jump_clicked {
                    self.tail_was_at_bottom = true;
                    self.latest_navigation_diagnostic =
                        Some(LatestNavigationAction::JumpButton);
                    tracing::info!(
                        target: "deebugee.diagnostics",
                        subsystem = "navigation",
                        event = "viewer.latest_navigation.requested",
                        status = "requested",
                        action = LatestNavigationAction::JumpButton.event_value(),
                        latest_at = latest_at.title(),
                        visible_row_count = table_rows.len(),
                        "[Navigation] Latest-event navigation requested"
                    );
                    self.request_scroll_to_latest();
                }
            }
            if resume_follow_after_deferred_refresh {
                // The user reached the edge of the previously rendered table while
                // newer rows or search results were pending. Carry that intent into
                // the next frame so the refreshed table remains anchored to Latest.
                self.request_scroll_to_latest();
            }
            if self.scroll_to_bottom_requested {
                ui.ctx().request_repaint();
            }
        });
    }
}

fn apply_primary_facet_click(selection: &mut FacetSelection, value: &str, additive: bool) {
    if additive {
        selection.toggle_include(value);
    } else if selection.included.len() == 1 && selection.included.contains(value) {
        selection.included.clear();
    } else {
        selection.excluded.remove(value);
        selection.include_only(value);
    }
}

fn highlighted_message(message: &str, default_color: Color32) -> LayoutJob {
    let mut job = LayoutJob::default();
    let mut cursor = 0;
    let mut plain_start = 0;

    while cursor < message.len() {
        let Some((end, tone)) = highlight_at(message, cursor) else {
            cursor += message[cursor..]
                .chars()
                .next()
                .expect("cursor is within the message")
                .len_utf8();
            continue;
        };

        append_message_text(
            &mut job,
            &message[plain_start..cursor],
            default_color,
            false,
        );
        append_message_text(&mut job, &message[cursor..end], tone.color(), true);
        cursor = end;
        plain_start = end;
    }
    append_message_text(&mut job, &message[plain_start..], default_color, false);
    job
}

fn append_message_text(job: &mut LayoutJob, text: &str, color: Color32, strong: bool) {
    if text.is_empty() {
        return;
    }
    job.append(
        text,
        0.0,
        TextFormat {
            color,
            font_id: FontId::proportional(14.0),
            italics: false,
            underline: if strong {
                Stroke::new(0.75, color)
            } else {
                Stroke::NONE
            },
            ..Default::default()
        },
    );
}

fn highlight_at(message: &str, start: usize) -> Option<(usize, MessageTone)> {
    if start > 0
        && message[..start]
            .chars()
            .next_back()
            .is_some_and(is_message_word_character)
    {
        return None;
    }

    MESSAGE_HIGHLIGHTS.iter().find_map(|(tone, phrase)| {
        let end = phrase_end_at(message, start, phrase)?;
        (!message[end..]
            .chars()
            .next()
            .is_some_and(is_message_word_character))
        .then_some((end, *tone))
    })
}

fn phrase_end_at(message: &str, start: usize, phrase: &str) -> Option<usize> {
    let mut position = start;
    let mut words = phrase.split(' ').peekable();
    while let Some(word) = words.next() {
        let candidate = message.get(position..position + word.len())?;
        if !candidate.eq_ignore_ascii_case(word) {
            return None;
        }
        position += word.len();

        if words.peek().is_some() {
            let separator_start = position;
            while message[position..]
                .chars()
                .next()
                .is_some_and(is_phrase_separator)
            {
                position += message[position..].chars().next()?.len_utf8();
            }
            if position == separator_start {
                return None;
            }
        }
    }
    Some(position)
}

fn is_message_word_character(character: char) -> bool {
    character.is_alphanumeric()
}

fn is_phrase_separator(character: char) -> bool {
    matches!(character, ' ' | '_' | '-' | '/' | '.')
}

struct EventCellOptions<'a> {
    wrapped: bool,
    semantic_highlighting: bool,
    timestamp_display: TimestampDisplay,
    timestamp_format: &'a str,
    relative_start: Option<DateTime<Utc>>,
    group_detail: Option<&'a str>,
    show_fields: bool,
}

fn show_event_cell(
    ui: &mut egui::Ui,
    column: TableColumn,
    event: &LogEvent,
    options: EventCellOptions<'_>,
) {
    match column {
        TableColumn::Timestamp => {
            ui.label(
                RichText::new(
                    options
                        .timestamp_display
                        .format(event, options.timestamp_format),
                )
                .monospace()
                .color(TEXT_MUTED),
            );
        }
        TableColumn::RelativeTime => {
            let value = options
                .relative_start
                .map(|start| format_relative_elapsed(event.timestamp - start))
                .unwrap_or_else(|| "—".to_owned());
            ui.label(RichText::new(value).monospace().color(TEXT_MUTED));
        }
        TableColumn::Level => {
            ui.label(
                RichText::new(event.level.to_string().to_uppercase())
                    .strong()
                    .small()
                    .color(level_color(event.level)),
            );
        }
        TableColumn::Source => {
            ui.label(&event.source);
        }
        TableColumn::Tag => {
            ui.label(event.tag());
        }
        TableColumn::Subsystem => {
            ui.label(&event.subsystem);
        }
        TableColumn::Event => {
            ui.label(&event.event);
        }
        TableColumn::Provider => {
            optional_cell(ui, event.provider.as_deref());
        }
        TableColumn::Correlation => {
            ui.label(event.correlation_id());
        }
        TableColumn::Duration => {
            if let Some(value) = event.duration_ms {
                ui.label(format!("{value:.1} ms"));
            } else {
                optional_cell(ui, None);
            }
        }
        TableColumn::Status => {
            let status = status_text(event.status.as_ref());
            optional_cell(ui, (status != "-").then_some(status.as_str()));
        }
        TableColumn::Message => {
            let label = if options.semantic_highlighting {
                Label::new(highlighted_message(
                    &event.message,
                    ui.visuals().text_color(),
                ))
            } else {
                Label::new(&event.message)
            };
            ui.vertical(|ui| {
                ui.add(if options.wrapped {
                    label.wrap()
                } else {
                    label.truncate()
                });
                if !options.show_fields
                    && let Some(detail) = console_argument_summary(event)
                {
                    let detail_label = Label::new(RichText::new(detail).color(TEXT_MUTED));
                    ui.add(if options.wrapped {
                        detail_label.wrap()
                    } else {
                        detail_label.truncate()
                    });
                }
                for detail in event_field_summaries(event, options.show_fields) {
                    let detail_label = Label::new(RichText::new(detail).color(TEXT_MUTED));
                    ui.add(if options.wrapped {
                        detail_label.wrap()
                    } else {
                        detail_label.truncate()
                    });
                }
                if let Some(detail) = options.group_detail {
                    let detail_label = Label::new(RichText::new(detail).small().color(TEXT_MUTED));
                    ui.add(if options.wrapped {
                        detail_label.wrap()
                    } else {
                        detail_label.truncate()
                    });
                }
            });
        }
    }
}

fn is_groupable_event(_event: &LogEvent) -> bool {
    true
}

fn is_error_event(event: &LogEvent) -> bool {
    matches!(event.level, Level::Error | Level::Fatal)
}

fn event_pin_key(event: &LogEvent) -> String {
    let json = serde_json::to_vec(event).unwrap_or_default();
    let hash = json.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    });
    format!("{hash:016x}")
}

fn event_index_by_key(store: &EventStore, key: &str) -> Option<usize> {
    (0..store.len()).find(|index| {
        store
            .get(*index)
            .is_some_and(|event| event_pin_key(event) == key)
    })
}

fn format_relative_elapsed(duration: chrono::Duration) -> String {
    let milliseconds = duration.num_milliseconds();
    let sign = if milliseconds < 0 { "-" } else { "+" };
    let absolute = milliseconds.unsigned_abs();
    if absolute < 60_000 {
        format!("{sign}{:.3}s", absolute as f64 / 1_000.0)
    } else if absolute < 3_600_000 {
        format!(
            "{sign}{}m {:.3}s",
            absolute / 60_000,
            (absolute % 60_000) as f64 / 1_000.0
        )
    } else {
        format!(
            "{sign}{}h {:02}m",
            absolute / 3_600_000,
            (absolute % 3_600_000) / 60_000
        )
    }
}

fn error_group_key(event: &LogEvent) -> String {
    let stack = ["stack", "stack_trace", "error_stack"]
        .iter()
        .find_map(|key| event.fields.get(*key).and_then(serde_json::Value::as_str))
        .unwrap_or_default();
    format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
        event.source,
        event.subsystem,
        event.event,
        event.error_kind.as_deref().unwrap_or_default(),
        normalize_error_shape(&event.message),
        normalize_error_shape(stack),
    )
}

/// Removes volatile numbers and normalizes spacing while retaining the useful
/// exception wording and call-site shape needed to avoid merging unrelated errors.
fn normalize_error_shape(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    let mut previous_was_space = true;
    let mut in_digits = false;
    for character in value.trim().chars() {
        if character.is_ascii_digit() {
            if !in_digits {
                normalized.push('#');
                in_digits = true;
            }
            previous_was_space = false;
        } else if character.is_whitespace() {
            if !previous_was_space {
                normalized.push(' ');
                previous_was_space = true;
            }
            in_digits = false;
        } else {
            normalized.extend(character.to_lowercase());
            previous_was_space = false;
            in_digits = false;
        }
    }
    normalized.trim_end().to_string()
}

fn error_group_summary(
    store: &EventStore,
    group: ErrorGroup,
    timestamp_display: TimestampDisplay,
    timestamp_format: &str,
) -> Option<String> {
    (group.count > 1).then(|| {
        let first = store
            .get(group.first_index)
            .map(|event| timestamp_display.format(event, timestamp_format))
            .unwrap_or_else(|| "unknown".to_string());
        let last = group
            .previous_index
            .and_then(|index| store.get(index))
            .map(|event| timestamp_display.format(event, timestamp_format))
            .unwrap_or_else(|| "unknown".to_string());
        format!(
            "Repeated {} times · first {first} · last {last}",
            group.count
        )
    })
}

/// Returns the useful error text from structured console arguments without changing the event.
/// Console strings are already included in `message`; this is only for Error-like objects.
fn console_argument_summary(event: &LogEvent) -> Option<String> {
    if event.event != "console.message" {
        return None;
    }

    let mut details = Vec::new();
    let arguments = event
        .fields
        .get("arguments")
        .or_else(|| event.fields.get("args"))?
        .as_array()?;
    for argument in arguments {
        let Some(object) = argument.as_object() else {
            continue;
        };
        let Some(message) = object
            .get("message")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|message| !message.is_empty() && *message != event.message)
        else {
            continue;
        };
        let detail = object
            .get("name")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map_or_else(|| message.to_string(), |name| format!("{name}: {message}"));
        if !details.contains(&detail) {
            details.push(detail);
        }
    }

    (!details.is_empty()).then(|| details.join(" · "))
}

fn event_field_summaries(event: &LogEvent, show_fields: bool) -> Vec<String> {
    if !show_fields || event.fields.is_empty() {
        return Vec::new();
    }

    let mut details = Vec::new();
    if let Some(arguments) = event
        .fields
        .get("arguments")
        .or_else(|| event.fields.get("args"))
    {
        details.push(format!(
            "args: {}",
            serde_json::to_string(arguments).unwrap_or_else(|_| "<unavailable>".to_owned())
        ));
    }

    let remaining = event
        .fields
        .iter()
        .filter(|(key, _)| !matches!(key.as_str(), "arguments" | "args"))
        .collect::<BTreeMap<_, _>>();
    if !remaining.is_empty() {
        details.push(format!(
            "fields: {}",
            serde_json::to_string(&remaining).unwrap_or_else(|_| "<unavailable>".to_owned())
        ));
    }
    details
}

fn optional_cell(ui: &mut egui::Ui, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        ui.label(value);
    } else {
        ui.label(RichText::new("—").color(Color32::from_rgb(78, 87, 102)));
    }
}

fn move_column(
    columns: &mut Vec<TableColumn>,
    source: TableColumn,
    target: TableColumn,
    insert_after: bool,
) {
    if source == target {
        return;
    }
    let Some(source_index) = columns.iter().position(|column| *column == source) else {
        return;
    };
    let moved = columns.remove(source_index);
    let Some(target_index) = columns.iter().position(|column| *column == target) else {
        columns.insert(source_index.min(columns.len()), moved);
        return;
    };
    let insertion_index = target_index + usize::from(insert_after);
    columns.insert(insertion_index.min(columns.len()), moved);
}

fn panned_scroll_offset(
    current_offset: f32,
    pointer_delta: f32,
    content_size: f32,
    viewport_size: f32,
) -> f32 {
    let maximum_offset = (content_size - viewport_size).max(0.0);
    (current_offset - pointer_delta).clamp(0.0, maximum_offset)
}

fn scroll_is_at_end(
    current_offset: f32,
    content_size: f32,
    viewport_size: f32,
    tolerance: f32,
) -> bool {
    let maximum_offset = (content_size - viewport_size).max(0.0);
    maximum_offset - current_offset <= tolerance.max(0.0)
}

fn latest_scroll_offset(content_size: f32, viewport_size: f32) -> f32 {
    (content_size - viewport_size).max(0.0)
}

fn scroll_is_at_latest(
    current_offset: f32,
    content_size: f32,
    viewport_size: f32,
    tolerance: f32,
    latest_at: LatestAt,
) -> bool {
    match latest_at {
        LatestAt::Bottom => {
            scroll_is_at_end(current_offset, content_size, viewport_size, tolerance)
        }
        LatestAt::Top => current_offset <= tolerance.max(0.0),
    }
}

fn scroll_delta_moves_away_from_latest(scroll_delta: f32, latest_at: LatestAt) -> bool {
    match latest_at {
        // In egui, a positive wheel delta moves the viewport toward earlier rows.
        LatestAt::Bottom => scroll_delta > f32::EPSILON,
        LatestAt::Top => scroll_delta < -f32::EPSILON,
    }
}

fn raw_mouse_wheel_delta_y(events: &[egui::Event]) -> f32 {
    events
        .iter()
        .filter_map(|event| match event {
            egui::Event::MouseWheel { delta, .. } => Some(delta.y),
            _ => None,
        })
        .sum()
}

fn order_visible_rows(rows: &mut [usize], latest_at: LatestAt) {
    if latest_at == LatestAt::Top {
        rows.reverse();
    }
}

fn latest_visible_row(rows: &[usize], latest_at: LatestAt) -> Option<usize> {
    match latest_at {
        LatestAt::Bottom => rows.last().copied(),
        LatestAt::Top => rows.first().copied(),
    }
}

fn latest_visible_id(store: &EventStore, rows: &[usize], latest_at: LatestAt) -> Option<u64> {
    latest_visible_row(rows, latest_at).and_then(|row| store.event_id(row))
}

fn visible_tail_changed(
    previous_latest: Option<u64>,
    store: &EventStore,
    current_rows: &[usize],
    latest_at: LatestAt,
) -> bool {
    previous_latest != latest_visible_id(store, current_rows, latest_at)
}

fn advance_scroll_request(
    requested: bool,
    reached_bottom: bool,
    manually_scrolled: bool,
    latest_view_is_current: bool,
    settle_frames: u8,
) -> (bool, u8) {
    if !requested || manually_scrolled {
        return (false, 0);
    }
    if !latest_view_is_current || !reached_bottom {
        return (true, LATEST_SETTLE_FRAMES);
    }
    let remaining = settle_frames.saturating_sub(1);
    (remaining > 0, remaining)
}

fn should_reanchor_after_wrap(
    wrap_changed: bool,
    follow_latest: bool,
    was_at_latest: bool,
    latest_request_pending: bool,
) -> bool {
    wrap_changed && follow_latest && (was_at_latest || latest_request_pending)
}

fn should_close_settings_popup(
    settings_button_clicked: bool,
    pointer_clicked: bool,
    clicked_elsewhere: bool,
    pointer_in_nested_popup: bool,
) -> bool {
    !settings_button_clicked && pointer_clicked && clicked_elsewhere && !pointer_in_nested_popup
}

fn can_refresh_filtered_rows_immediately(
    has_text_filter: bool,
    text_cache_is_current: bool,
) -> bool {
    !has_text_filter || text_cache_is_current
}

fn latest_view_is_current(
    filters_dirty: bool,
    has_text_filter: bool,
    text_cache_is_current: bool,
) -> bool {
    !filters_dirty && (!has_text_filter || text_cache_is_current)
}

fn should_resume_follow_at_latest_edge(
    follow_latest: bool,
    was_at_latest: bool,
    is_at_latest: bool,
    latest_view_is_current: bool,
) -> bool {
    follow_latest && !was_at_latest && is_at_latest && !latest_view_is_current
}

fn should_pause_pruning(paused: bool, tail_was_at_bottom: bool) -> bool {
    paused || !tail_was_at_bottom
}

fn vertical_scrollbar_dragged(
    dragged_id: Option<egui::Id>,
    scroll_area_id: egui::Id,
    primary_down: bool,
) -> bool {
    primary_down && dragged_id == Some(scroll_area_id.with(1))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IngestionRefreshDecision {
    mark_filters_dirty: bool,
    refresh_anchored_view: bool,
}

fn ingestion_refresh_decision(
    received_events: bool,
    paused: bool,
    tail_was_at_bottom: bool,
) -> IngestionRefreshDecision {
    let mark_filters_dirty = received_events && !paused;
    IngestionRefreshDecision {
        mark_filters_dirty,
        refresh_anchored_view: mark_filters_dirty && tail_was_at_bottom,
    }
}

impl eframe::App for ViewerApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        if self.project_setup.is_some() {
            self.project_setup_screen(ui);
            ui.ctx().request_repaint_after(Duration::from_millis(100));
            return;
        }

        self.drain_reader();
        self.drain_search_results();
        self.drain_update_check();

        let dropped_paths: Vec<PathBuf> = ui.input(|input| {
            input
                .raw
                .dropped_files
                .iter()
                .filter_map(|file| file.path.clone())
                .collect()
        });
        self.add_paths(dropped_paths);
        self.handle_keyboard_navigation(ui.ctx());

        if !self.paused && self.tail_was_at_bottom {
            if self.filter.text.trim().is_empty() {
                if self.search_due_at.take().is_some() || self.search_in_flight {
                    self.search_generation = self.search_worker.next_generation();
                    self.search_in_flight = false;
                }
                if self.filters_dirty {
                    self.refresh_filters();
                }
            } else if self.cached_search_is_current() {
                if self.filters_dirty {
                    self.refresh_filters();
                }
                if self.text_matches_event_count < self.store.len()
                    && self.search_due_at.is_none()
                    && !self.search_in_flight
                {
                    self.schedule_text_search(Duration::ZERO);
                }
            } else if self.search_due_at.is_none() && !self.search_in_flight {
                self.schedule_text_search(Duration::ZERO);
            }
            self.start_scheduled_search();
        }

        self.top_bar(ui);
        self.sidebar(ui);
        self.details_panel(ui);
        self.central_table(ui);
        self.session_explorer_window(ui.ctx());
        self.pin_note_window(ui.ctx());
        self.update_dialog(ui.ctx());

        ui.ctx().request_repaint_after(Duration::from_millis(100));
    }

    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        if let Err(error) = self.save_active_workspace() {
            self.last_error = Some(format!("Unable to save active workspace: {error}"));
        }
        let mut bookmarks_by_source = self.bookmarks_by_source.clone();
        if let Some(scope) = &self.bookmark_scope {
            bookmarks_by_source.insert(scope.clone(), self.bookmarks.clone());
        }
        let mut pins_by_source = self.pins_by_source.clone();
        if let Some(scope) = &self.bookmark_scope {
            pins_by_source.insert(scope.clone(), self.pins.clone());
        }
        let preferences = ViewerPreferences {
            version: 1,
            sources: self.sources.clone(),
            filter: self.filter.clone(),
            column_order: self.column_order.clone(),
            relative_time_column: true,
            wrapped_messages: self.wrapped_messages,
            semantic_highlighting: self.semantic_highlighting,
            stick_to_bottom: self.stick_to_bottom,
            color_by: self.color_by,
            bookmarks: self.bookmarks.clone(),
            bookmarks_by_source,
            pins: self.pins.clone(),
            pins_by_source,
            facet_order: self.facet_order.clone(),
            hidden_facets: self.hidden_facets.clone(),
            latest_at: self.latest_at,
            max_events: self.max_events,
            timestamp_display: self.timestamp_display,
            timestamp_format: self.timestamp_format.clone(),
            group_errors: self.group_errors,
            show_fields: self.show_fields,
            ui_scale: self.ui_scale,
        };
        eframe::set_value(storage, PREFERENCES_KEY, &preferences);
    }

    fn auto_save_interval(&self) -> Duration {
        Duration::from_secs(5)
    }
}

fn session_summary_card(
    ui: &mut egui::Ui,
    summary: &SessionSummary,
    timestamp_display: TimestampDisplay,
    timestamp_format: &str,
    compare_a: &mut Option<String>,
    compare_b: &mut Option<String>,
    filter_session: &mut Option<String>,
) {
    egui::Frame::new()
        .fill(SURFACE_2)
        .stroke(Stroke::new(1.0, BORDER))
        .corner_radius(6)
        .inner_margin(egui::Margin::symmetric(10, 8))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(
                    RichText::new(compact_identifier(&summary.id, 22))
                        .strong()
                        .monospace(),
                )
                .on_hover_text(&summary.id);
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.small_button("View").clicked() {
                        *filter_session = Some(summary.id.clone());
                    }
                    if ui
                        .selectable_label(compare_b.as_deref() == Some(&summary.id), "B")
                        .on_hover_text("Use this run as comparison B")
                        .clicked()
                    {
                        *compare_b = toggle_session_choice(
                            compare_b.take(),
                            &summary.id,
                            compare_a.as_deref(),
                        );
                    }
                    if ui
                        .selectable_label(compare_a.as_deref() == Some(&summary.id), "A")
                        .on_hover_text("Use this run as comparison A")
                        .clicked()
                    {
                        *compare_a = toggle_session_choice(
                            compare_a.take(),
                            &summary.id,
                            compare_b.as_deref(),
                        );
                    }
                });
            });
            ui.label(
                RichText::new(
                    timestamp_display.format_timestamp(summary.first_timestamp, timestamp_format),
                )
                .small()
                .color(TEXT_MUTED),
            );
            ui.horizontal_wrapped(|ui| {
                ui.label(format!("{} events", summary.event_count));
                ui.label(format_elapsed(summary.elapsed_ms()));
                if summary.warning_count > 0 {
                    ui.label(
                        RichText::new(format!("{} warnings", summary.warning_count)).color(WARNING),
                    );
                }
                if summary.error_count > 0 {
                    ui.label(
                        RichText::new(format!("{} errors", summary.error_count)).color(DANGER),
                    );
                }
            });
            ui.horizontal_wrapped(|ui| {
                if !summary.providers.is_empty() {
                    ui.label(
                        RichText::new(format!(
                            "Providers: {}",
                            summary
                                .providers
                                .iter()
                                .map(String::as_str)
                                .collect::<Vec<_>>()
                                .join(", ")
                        ))
                        .small()
                        .color(TEXT_MUTED),
                    );
                }
                if !summary.correlations.is_empty() {
                    ui.label(
                        RichText::new(format!("{} correlations", summary.correlations.len()))
                            .small()
                            .color(TEXT_MUTED),
                    );
                }
                if let Some(status) = summary.status() {
                    ui.label(
                        RichText::new(format!("Final: {status}"))
                            .small()
                            .color(ACCENT),
                    );
                }
            });
        });
}

fn session_comparison_ui(
    ui: &mut egui::Ui,
    summaries: &[SessionSummary],
    compare_a: Option<&str>,
    compare_b: Option<&str>,
) {
    ui.heading("Run Comparison");
    let Some(left) = compare_a.and_then(|id| summaries.iter().find(|summary| summary.id == id))
    else {
        ui.label(RichText::new("Choose run A from the session list.").color(TEXT_MUTED));
        return;
    };
    let Some(right) = compare_b.and_then(|id| summaries.iter().find(|summary| summary.id == id))
    else {
        ui.label(RichText::new("Choose run B from the session list.").color(TEXT_MUTED));
        return;
    };

    ui.horizontal_wrapped(|ui| {
        ui.label(RichText::new(format!("A  {}", compact_identifier(&left.id, 18))).strong());
        ui.label(RichText::new("versus").small().color(TEXT_MUTED));
        ui.label(RichText::new(format!("B  {}", compact_identifier(&right.id, 18))).strong());
    });
    ui.horizontal_wrapped(|ui| {
        comparison_metric(
            ui,
            "Events",
            left.event_count.to_string(),
            right.event_count.to_string(),
            left.event_count != right.event_count,
        );
        comparison_metric(
            ui,
            "Errors",
            left.error_count.to_string(),
            right.error_count.to_string(),
            left.error_count != right.error_count,
        );
        comparison_metric(
            ui,
            "Elapsed",
            format_elapsed(left.elapsed_ms()),
            format_elapsed(right.elapsed_ms()),
            left.elapsed_ms() != right.elapsed_ms(),
        );
    });
    ui.separator();

    let rows = compare_sessions(left, right);
    let changed = rows.iter().filter(|row| row.differs()).count();
    ui.label(
        RichText::new(format!(
            "{changed} differing event types · {} total",
            rows.len()
        ))
        .small()
        .color(if changed > 0 { WARNING } else { SUCCESS }),
    );
    ui.add_space(4.0);
    egui::ScrollArea::both()
        .id_salt("session_comparison")
        .show(ui, |ui| {
            egui::Grid::new("session_comparison_grid")
                .striped(true)
                .min_col_width(58.0)
                .show(ui, |ui| {
                    ui.label(RichText::new("Event").strong());
                    ui.label(RichText::new("Count A/B").strong());
                    ui.label(RichText::new("Warn A/B").strong());
                    ui.label(RichText::new("Error A/B").strong());
                    ui.label(RichText::new("Avg ms A/B").strong());
                    ui.label(RichText::new("Status A/B").strong());
                    ui.end_row();
                    for row in rows {
                        let color = if row.differs() { WARNING } else { TEXT_PRIMARY };
                        ui.label(RichText::new(&row.event).color(color))
                            .on_hover_text(format!(
                                "Maximum duration: {} / {}",
                                format_optional_duration(row.max_duration_a),
                                format_optional_duration(row.max_duration_b)
                            ));
                        ui.label(format!("{} / {}", row.count_a, row.count_b));
                        ui.label(format!("{} / {}", row.warnings_a, row.warnings_b));
                        ui.label(format!("{} / {}", row.errors_a, row.errors_b));
                        ui.label(format!(
                            "{} / {}",
                            format_optional_duration(row.average_duration_a),
                            format_optional_duration(row.average_duration_b)
                        ));
                        ui.label(format!(
                            "{} / {}",
                            row.status_a.as_deref().unwrap_or("-"),
                            row.status_b.as_deref().unwrap_or("-")
                        ));
                        ui.end_row();
                    }
                });
        });
}

fn comparison_metric(ui: &mut egui::Ui, name: &str, left: String, right: String, differs: bool) {
    egui::Frame::new()
        .fill(SURFACE_2)
        .corner_radius(5)
        .inner_margin(egui::Margin::symmetric(8, 5))
        .show(ui, |ui| {
            ui.vertical(|ui| {
                ui.label(RichText::new(name).small().color(TEXT_MUTED));
                ui.label(
                    RichText::new(format!("{left} / {right}"))
                        .strong()
                        .color(if differs { WARNING } else { SUCCESS }),
                );
            });
        });
}

fn toggle_session_choice(
    current: Option<String>,
    requested: &str,
    other: Option<&str>,
) -> Option<String> {
    if current.as_deref() == Some(requested) {
        None
    } else if other == Some(requested) {
        current
    } else {
        Some(requested.to_string())
    }
}

fn apply_session_filter(filter: &mut FilterState, session_id: String) {
    filter.clear();
    filter
        .facets
        .entry("app_session_id".to_owned())
        .or_default()
        .include_only(session_id);
}

fn compact_identifier(value: &str, max_characters: usize) -> String {
    if value.chars().count() <= max_characters {
        return value.to_string();
    }
    let visible = max_characters.saturating_sub(1);
    format!("{}…", value.chars().take(visible).collect::<String>())
}

fn format_elapsed(milliseconds: i64) -> String {
    if milliseconds < 1_000 {
        format!("{milliseconds} ms")
    } else if milliseconds < 60_000 {
        format!("{:.1} s", milliseconds as f64 / 1_000.0)
    } else {
        let minutes = milliseconds / 60_000;
        let seconds = (milliseconds % 60_000) / 1_000;
        format!("{minutes}m {seconds:02}s")
    }
}

fn format_optional_duration(duration_ms: Option<f64>) -> String {
    duration_ms.map_or_else(|| "-".to_string(), |duration| format!("{duration:.1}"))
}

fn remove_query_expression(query: &str, range: Range<usize>) -> String {
    if range.start > range.end || range.end > query.len() {
        return query.to_string();
    }
    let before = query[..range.start].trim_end();
    let after = query[range.end..].trim_start();
    match (before.is_empty(), after.is_empty()) {
        (true, true) => String::new(),
        (true, false) => after.to_string(),
        (false, true) => before.to_string(),
        (false, false) => format!("{before} {after}"),
    }
}

fn bookmark_name(filter: &FilterState) -> String {
    let mut parts = Vec::new();
    if !filter.text.trim().is_empty() {
        parts.push(format!("Search: {}", filter.text.trim()));
    }
    for (facet, selection) in &filter.facets {
        if !selection.included.is_empty() {
            parts.push(format!(
                "{}: {}",
                facet_title(facet),
                selection
                    .included
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(" + ")
            ));
        }
        if !selection.excluded.is_empty() {
            parts.push(format!(
                "Hide {}: {}",
                facet_title(facet),
                selection
                    .excluded
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(" + ")
            ));
        }
    }
    if let Some(level) = filter.minimum_level {
        parts.push(format!("Level ≥ {level}"));
    }
    if let Some(correlation) = filter.correlation.as_deref() {
        parts.push(format!("Correlation: {correlation}"));
    }

    let name = parts.join(" · ");
    if name.chars().count() <= 72 {
        name
    } else {
        format!("{}…", name.chars().take(71).collect::<String>())
    }
}

fn facet_title(facet: &str) -> String {
    facet
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|first| first.to_ascii_uppercase().to_string() + chars.as_str())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn facet_value_matches_search(value: &str, normalized_search: &str) -> bool {
    normalized_search.is_empty() || value.to_ascii_lowercase().contains(normalized_search)
}

fn facet_section_open_request(
    has_search_result: bool,
    expand_collapse_all: Option<bool>,
) -> Option<bool> {
    if has_search_result {
        Some(true)
    } else {
        expand_collapse_all
    }
}

fn normalize_source_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut normalized: Vec<PathBuf> = paths
        .into_iter()
        .map(|path| path.canonicalize().unwrap_or(path))
        .collect();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn bookmark_scope_key(paths: &[PathBuf]) -> Option<String> {
    let mut source_paths = Vec::new();
    for path in paths {
        if path.is_dir() {
            let Ok(entries) = std::fs::read_dir(path) else {
                continue;
            };
            source_paths.extend(
                entries
                    .flatten()
                    .map(|entry| entry.path())
                    .filter(|child| is_jsonl_path(child))
                    .map(|child| child.canonicalize().unwrap_or(child)),
            );
        } else {
            source_paths.push(path.canonicalize().unwrap_or_else(|_| path.clone()));
        }
    }
    source_paths.sort();
    source_paths.dedup();
    (!source_paths.is_empty()).then(|| {
        source_paths
            .iter()
            .map(|path| path.to_string_lossy())
            .collect::<Vec<_>>()
            .join("\u{1f}")
    })
}

fn load_workspace(path: &Path) -> Result<WorkspaceConfig, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("Unable to read workspace {}: {error}", path.display()))?;
    let workspace = toml::from_str::<WorkspaceConfig>(&text)
        .map_err(|error| format!("Unable to parse workspace {}: {error}", path.display()))?;
    if workspace.version != 1 {
        return Err(format!(
            "Unsupported workspace version {} in {}",
            workspace.version,
            path.display()
        ));
    }
    Ok(workspace)
}

fn workspace_display_name(path: &Path) -> Option<String> {
    let workspace_parent = path.parent()?;
    let project_root = (workspace_parent.file_name()? == ".deebugee")
        .then(|| workspace_parent.parent())
        .flatten()
        .unwrap_or(workspace_parent);
    project_root
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
}

fn is_jsonl_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            let name = name.to_ascii_lowercase();
            name.ends_with(".jsonl") || name.contains(".jsonl.")
        })
}

fn stable_value_color(value: &str) -> Color32 {
    // FNV-1a gives every value a stable, inexpensive pseudo-random hue without
    // storing an ever-growing color map for high-cardinality fields.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let hue = (hash % 3_600) as f32 / 3_600.0;
    Color32::from(egui::ecolor::Hsva::new(hue, 0.52, 0.92, 1.0))
}

fn level_color(level: Level) -> Color32 {
    match level {
        Level::Trace => Color32::from_rgb(113, 124, 143),
        Level::Debug => Color32::from_rgb(154, 165, 184),
        Level::Info => Color32::from_rgb(100, 179, 255),
        Level::Warn => WARNING,
        Level::Error => DANGER,
        Level::Fatal => Color32::from_rgb(255, 73, 103),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_opening_click_is_not_treated_as_an_outside_click() {
        assert!(!should_close_settings_popup(true, true, true, false));
        assert!(!should_close_settings_popup(false, true, true, true));
        assert!(should_close_settings_popup(false, true, true, false));
    }

    #[test]
    fn timestamp_display_keeps_utc_available_for_log_correlation() {
        let event = LogEvent::new(
            Level::Info,
            "app",
            "startup",
            "app.started",
            "Started",
            "session",
        );

        assert_eq!(
            TimestampDisplay::Utc.format(&event, "%H:%M:%S"),
            event.timestamp.format("%H:%M:%S").to_string()
        );
        assert_ne!(
            TimestampDisplay::Local.title(),
            TimestampDisplay::Utc.title()
        );
    }

    #[test]
    fn console_error_arguments_are_shown_as_a_secondary_message() {
        let mut event = LogEvent::new(
            Level::Error,
            "renderer",
            "smart_next_autoload",
            "console.message",
            "[Smart Next Autoload] Source preparation failed:",
            "session",
        );
        event.fields.insert(
            "args".to_string(),
            serde_json::json!([
                { "name": "Error", "message": "No later aired episode is available yet." }
            ]),
        );

        assert_eq!(
            console_argument_summary(&event).as_deref(),
            Some("Error: No later aired episode is available yet.")
        );

        event.event = "player.failed".to_string();
        assert_eq!(console_argument_summary(&event), None);
    }

    #[test]
    fn field_summaries_separate_args_from_remaining_fields() {
        let mut event = LogEvent::new(
            Level::Info,
            "renderer",
            "player",
            "console.message",
            "[Player] Started",
            "session",
        );
        event.fields.insert(
            "arguments".to_string(),
            serde_json::json!(["[Player] Started", { "attempt": 2 }]),
        );
        event
            .fields
            .insert("provider".to_string(), serde_json::json!("native"));

        assert!(event_field_summaries(&event, false).is_empty());
        assert_eq!(
            event_field_summaries(&event, true),
            vec![
                "args: [\"[Player] Started\",{\"attempt\":2}]".to_string(),
                "fields: {\"provider\":\"native\"}".to_string(),
            ]
        );
    }

    #[test]
    fn repeat_grouping_normalizes_volatile_numbers_without_merging_sources() {
        let first = LogEvent::new(
            Level::Error,
            "backend",
            "sync",
            "request.failed",
            "Request 1842 failed after 500 ms",
            "session",
        );
        let second = LogEvent::new(
            Level::Error,
            "backend",
            "sync",
            "request.failed",
            "Request 9917 failed after 20 ms",
            "session",
        );
        let other_source = LogEvent::new(
            Level::Error,
            "renderer",
            "sync",
            "request.failed",
            "Request 9917 failed after 20 ms",
            "session",
        );

        assert_eq!(error_group_key(&first), error_group_key(&second));
        assert_ne!(error_group_key(&first), error_group_key(&other_source));
        let information = LogEvent::new(
            Level::Info,
            "backend",
            "sync",
            "request.finished",
            "Request 9917 finished",
            "session",
        );
        assert!(is_groupable_event(&first));
        assert!(is_groupable_event(&information));
    }

    #[test]
    fn repeat_summary_shows_the_occurrence_before_the_current_event() {
        let mut store = EventStore::default();
        store.extend([
            session_event(
                "session",
                "request.failed",
                "2026-08-22T20:32:52Z",
                Level::Error,
                None,
                None,
            ),
            session_event(
                "session",
                "request.failed",
                "2026-08-22T21:14:08Z",
                Level::Error,
                None,
                None,
            ),
            session_event(
                "session",
                "request.failed",
                "2026-08-22T22:02:29Z",
                Level::Error,
                None,
                None,
            ),
        ]);

        let summary = error_group_summary(
            &store,
            ErrorGroup {
                count: 3,
                first_index: 0,
                previous_index: Some(1),
                latest_index: 2,
            },
            TimestampDisplay::Utc,
            "%d/%m/%Y %H:%M:%S",
        );

        assert_eq!(
            summary.as_deref(),
            Some("Repeated 3 times · first 22/08/2026 20:32:52 · last 22/08/2026 21:14:08")
        );
    }

    #[test]
    fn semantic_highlighting_prefers_negative_compound_phrases() {
        let message = "Not Found after a successful scan";
        let job = highlighted_message(message, TEXT_PRIMARY);
        let sections: Vec<(&str, Color32)> = job
            .sections
            .iter()
            .map(|section| {
                let range = section.byte_range.start.0..section.byte_range.end.0;
                (&message[range], section.format.color)
            })
            .collect();

        assert_eq!(
            sections,
            vec![
                ("Not Found", DANGER),
                (" after a ", TEXT_PRIMARY),
                ("successful", SUCCESS),
                (" scan", TEXT_PRIMARY),
            ]
        );
    }

    #[test]
    fn semantic_highlighting_supports_common_log_phrase_separators() {
        assert_eq!(
            highlight_at("not_found", 0),
            Some(("not_found".len(), MessageTone::Negative))
        );
        assert_eq!(
            highlight_at("connection-refused", 0),
            Some(("connection-refused".len(), MessageTone::Negative))
        );
        assert_eq!(highlight_at("not foundish", 0), None);
        assert_eq!(
            highlight_at("Complete", 0),
            Some(("Complete".len(), MessageTone::Positive))
        );
        assert_eq!(
            highlight_at("not configured", 0),
            Some(("not configured".len(), MessageTone::Negative))
        );
    }

    #[test]
    fn moving_columns_supports_before_and_after_drop_positions() {
        let mut columns = vec![
            TableColumn::Timestamp,
            TableColumn::Level,
            TableColumn::Source,
            TableColumn::Message,
        ];

        move_column(
            &mut columns,
            TableColumn::Message,
            TableColumn::Level,
            false,
        );
        assert_eq!(
            columns,
            vec![
                TableColumn::Timestamp,
                TableColumn::Message,
                TableColumn::Level,
                TableColumn::Source,
            ]
        );

        move_column(
            &mut columns,
            TableColumn::Timestamp,
            TableColumn::Source,
            true,
        );
        assert_eq!(
            columns,
            vec![
                TableColumn::Message,
                TableColumn::Level,
                TableColumn::Source,
                TableColumn::Timestamp,
            ]
        );
    }

    #[test]
    fn saved_column_order_is_deduplicated_and_forward_compatible() {
        let normalized = normalize_column_order(vec![
            TableColumn::Message,
            TableColumn::Source,
            TableColumn::Message,
        ]);

        assert_eq!(normalized[0], TableColumn::Message);
        assert_eq!(normalized[1], TableColumn::Source);
        assert_eq!(normalized.len(), TableColumn::ALL.len());
        let timestamp = normalized
            .iter()
            .position(|column| *column == TableColumn::Timestamp)
            .unwrap();
        assert_eq!(normalized[timestamp + 1], TableColumn::RelativeTime);
        for column in TableColumn::ALL {
            assert_eq!(
                normalized
                    .iter()
                    .filter(|candidate| **candidate == column)
                    .count(),
                1
            );
        }
    }

    #[test]
    fn middle_pan_offsets_are_clamped_on_both_ends() {
        assert_eq!(panned_scroll_offset(50.0, 20.0, 500.0, 200.0), 30.0);
        assert_eq!(panned_scroll_offset(10.0, 50.0, 500.0, 200.0), 0.0);
        assert_eq!(panned_scroll_offset(290.0, -50.0, 500.0, 200.0), 300.0);
        assert_eq!(panned_scroll_offset(20.0, -50.0, 100.0, 200.0), 0.0);
    }

    #[test]
    fn bottom_detection_uses_stable_row_height_tolerance() {
        assert!(scroll_is_at_end(300.0, 500.0, 200.0, 24.0));
        assert!(scroll_is_at_end(280.0, 500.0, 200.0, 24.0));
        assert!(!scroll_is_at_end(250.0, 500.0, 200.0, 24.0));
        assert!(scroll_is_at_end(0.0, 100.0, 200.0, 24.0));
    }

    #[test]
    fn latest_position_controls_order_and_scroll_edge() {
        let mut rows = vec![1, 2, 3];
        order_visible_rows(&mut rows, LatestAt::Top);
        assert_eq!(rows, vec![3, 2, 1]);
        assert!(scroll_is_at_latest(0.0, 500.0, 200.0, 1.0, LatestAt::Top));
        assert!(!scroll_is_at_latest(20.0, 500.0, 200.0, 1.0, LatestAt::Top));
    }

    #[test]
    fn visible_tail_detection_uses_stable_ingestion_identity() {
        let mut store = EventStore::new(8);
        let event =
            |message| LogEvent::new(Level::Info, "test", "viewer", "event", message, "session");
        store.extend([
            event("hidden"),
            event("visible older"),
            event("visible latest"),
        ]);
        let visible_rows = [1, 2];
        let previous_latest = latest_visible_id(&store, &visible_rows, LatestAt::Bottom).unwrap();

        // A hidden record does not change the visible tail.
        assert!(!visible_tail_changed(
            Some(previous_latest),
            &store,
            &visible_rows,
            LatestAt::Bottom,
        ));

        // A repeated message is still a new record and must advance Follow.
        store.push(event("visible latest"));
        assert!(visible_tail_changed(
            Some(previous_latest),
            &store,
            &[1, 3],
            LatestAt::Bottom
        ));

        // A prior event can move to a different index after pruning without
        // becoming a new visible tail.
        let mut pruned_store = EventStore::new(8);
        pruned_store.extend([event("visible older"), event("visible latest")]);
        let pruned_latest = latest_visible_id(&pruned_store, &[1], LatestAt::Bottom).unwrap();
        assert!(!visible_tail_changed(
            Some(pruned_latest),
            &pruned_store,
            &[1],
            LatestAt::Bottom,
        ));
    }

    #[test]
    fn bottom_latest_offset_uses_the_entire_rendered_table_height() {
        assert_eq!(latest_scroll_offset(900.0, 240.0), 660.0);
        assert_eq!(latest_scroll_offset(180.0, 240.0), 0.0);
    }

    #[test]
    fn only_scrolls_away_from_latest_cancel_a_pending_jump() {
        assert!(scroll_delta_moves_away_from_latest(1.0, LatestAt::Bottom));
        assert!(!scroll_delta_moves_away_from_latest(-1.0, LatestAt::Bottom));
        assert!(scroll_delta_moves_away_from_latest(-1.0, LatestAt::Top));
        assert!(!scroll_delta_moves_away_from_latest(1.0, LatestAt::Top));
    }

    #[test]
    fn pending_jump_cancellation_uses_only_current_raw_wheel_events() {
        let events = vec![
            egui::Event::PointerGone,
            egui::Event::MouseWheel {
                unit: egui::MouseWheelUnit::Point,
                delta: egui::vec2(0.0, 12.0),
                phase: egui::TouchPhase::Move,
                modifiers: egui::Modifiers::NONE,
            },
        ];
        assert_eq!(raw_mouse_wheel_delta_y(&events), 12.0);
        assert_eq!(raw_mouse_wheel_delta_y(&[]), 0.0);
    }

    #[test]
    fn tail_request_survives_until_the_new_offset_reaches_bottom() {
        assert_eq!(
            advance_scroll_request(true, false, false, true, LATEST_SETTLE_FRAMES),
            (true, LATEST_SETTLE_FRAMES)
        );
        assert_eq!(
            advance_scroll_request(true, true, false, true, LATEST_SETTLE_FRAMES),
            (true, 1)
        );
        assert_eq!(
            advance_scroll_request(true, true, false, true, 1),
            (false, 0)
        );
        assert_eq!(
            advance_scroll_request(true, false, true, true, LATEST_SETTLE_FRAMES),
            (false, 0)
        );
    }

    #[test]
    fn wrap_change_preserves_an_existing_latest_anchor() {
        assert!(should_reanchor_after_wrap(true, true, true, false));
        assert!(should_reanchor_after_wrap(true, true, false, true));
        assert!(!should_reanchor_after_wrap(true, true, false, false));
        assert!(!should_reanchor_after_wrap(true, false, true, false));
        assert!(!should_reanchor_after_wrap(false, true, true, false));
    }

    #[test]
    fn facet_changes_refresh_visible_rows_without_waiting_for_the_next_frame() {
        assert!(can_refresh_filtered_rows_immediately(false, false));
        assert!(can_refresh_filtered_rows_immediately(true, true));
        assert!(!can_refresh_filtered_rows_immediately(true, false));
    }

    #[test]
    fn ingestion_keeps_new_rows_pending_while_away_from_latest() {
        assert_eq!(
            ingestion_refresh_decision(true, false, false),
            IngestionRefreshDecision {
                mark_filters_dirty: true,
                refresh_anchored_view: false,
            }
        );
        assert_eq!(
            ingestion_refresh_decision(true, false, true),
            IngestionRefreshDecision {
                mark_filters_dirty: true,
                refresh_anchored_view: true,
            }
        );
        assert_eq!(
            ingestion_refresh_decision(true, true, false),
            IngestionRefreshDecision {
                mark_filters_dirty: false,
                refresh_anchored_view: false,
            }
        );
    }

    #[test]
    fn latest_request_waits_for_deferred_rows_and_search_results() {
        // Reaching the edge of the old table is not completion while its filtered
        // presentation is stale.
        assert_eq!(
            advance_scroll_request(true, true, false, false, 1),
            (true, LATEST_SETTLE_FRAMES)
        );
        // Once the refreshed table appears, the same request survives until that
        // larger table reaches its actual edge and settles.
        assert_eq!(
            advance_scroll_request(true, false, false, true, LATEST_SETTLE_FRAMES),
            (true, LATEST_SETTLE_FRAMES)
        );
        assert_eq!(
            advance_scroll_request(true, true, false, true, LATEST_SETTLE_FRAMES),
            (true, 1)
        );
        assert_eq!(
            advance_scroll_request(true, true, false, true, 1),
            (false, 0)
        );
    }

    #[test]
    fn returning_to_a_stale_edge_resumes_following() {
        assert!(should_resume_follow_at_latest_edge(
            true, false, true, false
        ));
        assert!(!should_resume_follow_at_latest_edge(
            false, false, true, false
        ));
        assert!(!should_resume_follow_at_latest_edge(
            true, true, true, false
        ));
        assert!(!should_resume_follow_at_latest_edge(
            true, false, true, true
        ));
    }

    #[test]
    fn pause_keeps_the_retained_window_and_selection_identity_stable() {
        let mut store = EventStore::new(2);
        let event =
            |message| LogEvent::new(Level::Info, "test", "viewer", "event", message, "session");
        store.extend([event("first"), event("selected")]);
        let selected_id = store.event_id(1).unwrap();

        store.set_pruning_paused(should_pause_pruning(true, true));
        store.push(event("new while paused"));

        assert_eq!(store.len(), 3);
        assert_eq!(store.event_id(1), Some(selected_id));

        store.set_pruning_paused(should_pause_pruning(false, true));
        assert_eq!(store.len(), 2);
        assert_eq!(store.event_id(0), Some(selected_id));
    }

    #[test]
    fn only_the_actual_vertical_scrollbar_drag_cancels_navigation() {
        let scroll_area_id = egui::Id::new("test_scroll_area");
        assert!(vertical_scrollbar_dragged(
            Some(scroll_area_id.with(1)),
            scroll_area_id,
            true
        ));
        assert!(!vertical_scrollbar_dragged(
            Some(egui::Id::new("selected_text")),
            scroll_area_id,
            true
        ));
        assert!(!vertical_scrollbar_dragged(
            Some(scroll_area_id.with(0)),
            scroll_area_id,
            true
        ));
        assert!(!vertical_scrollbar_dragged(
            Some(scroll_area_id.with(1)),
            scroll_area_id,
            false
        ));
    }

    #[test]
    fn value_colors_are_stable_and_value_specific() {
        assert_eq!(stable_value_color("backend"), stable_value_color("backend"));
        assert_ne!(
            stable_value_color("backend"),
            stable_value_color("renderer")
        );
    }

    #[test]
    fn clicking_an_included_facet_again_clears_it_even_with_hidden_values() {
        let mut selection = FacetSelection::default();
        selection.toggle_exclude("hidden");

        apply_primary_facet_click(&mut selection, "visible", false);
        assert!(selection.included.contains("visible"));
        assert!(selection.excluded.contains("hidden"));

        apply_primary_facet_click(&mut selection, "visible", false);
        assert!(selection.included.is_empty());
        assert!(selection.excluded.contains("hidden"));
    }

    #[test]
    fn bookmark_names_summarize_search_and_multi_facet_filters() {
        let mut filter = FilterState {
            text: "segment local".to_string(),
            ..FilterState::default()
        };
        filter
            .facets
            .entry("subsystem".to_string())
            .or_default()
            .toggle_include("player");
        filter
            .facets
            .get_mut("subsystem")
            .unwrap()
            .toggle_include("normalizer");

        let name = bookmark_name(&filter);
        assert!(name.contains("Search: segment local"));
        assert!(name.contains("Subsystem: normalizer + player"));
    }

    #[test]
    fn structured_query_chips_remove_only_their_expression() {
        let query = "sync duration_ms > 1000 provider=remote";
        let parsed = parse_structured_query(query, Utc::now());
        assert_eq!(parsed.predicates.len(), 2);

        let without_duration = remove_query_expression(query, parsed.predicates[0].range.clone());
        assert_eq!(without_duration, "sync provider=remote");

        let reparsed = parse_structured_query(&without_duration, Utc::now());
        let without_provider =
            remove_query_expression(&without_duration, reparsed.predicates[0].range.clone());
        assert_eq!(without_provider, "sync");
    }

    #[test]
    fn bookmark_scopes_are_stable_per_jsonl_source_set() {
        let alpha = PathBuf::from("C:/logs/alpha.jsonl");
        let beta = PathBuf::from("C:/logs/beta.jsonl");

        assert_eq!(
            bookmark_scope_key(&[alpha.clone(), beta.clone()]),
            bookmark_scope_key(&[beta.clone(), alpha.clone()])
        );
        assert_ne!(bookmark_scope_key(&[alpha]), bookmark_scope_key(&[beta]));
    }

    #[test]
    fn project_manifest_resolves_relative_sources_and_private_workspace() {
        let root =
            std::env::temp_dir().join(format!("dee-bugee-project-manifest-{}", std::process::id()));
        let manifest_directory = root.join(".deebugee");
        std::fs::create_dir_all(&manifest_directory).unwrap();
        std::fs::write(
            manifest_directory.join("project.toml"),
            r#"version = 1
id = "com.example.viewer-test"
name = "Viewer Test"
sources = ["logs"]
"#,
        )
        .unwrap();

        let project = load_project(&root).unwrap();
        assert_eq!(project.sources, vec![root.join("logs")]);
        assert_eq!(project_display_name(&root).as_deref(), Some("Viewer Test"));
        assert!(project.workspace_path.ends_with("workspace.toml"));
        assert!(
            project
                .workspace_path
                .to_string_lossy()
                .contains(&stable_project_key("com.example.viewer-test"))
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn project_sources_expand_environment_variables_without_guessing() {
        let expanded = expand_environment_variables("%DATA_ROOT%/logs", |name| {
            (name == "DATA_ROOT").then(|| std::ffi::OsString::from("C:/project-data"))
        })
        .unwrap();
        assert_eq!(expanded, "C:/project-data/logs");
        assert!(
            expand_environment_variables("%MISSING%/logs", |_| None)
                .unwrap_err()
                .contains("%MISSING%")
        );
    }

    #[test]
    fn project_ids_are_suggested_from_common_git_remote_shapes() {
        assert_eq!(
            project_id_from_remote("https://github.com/ExampleOrg/MyApp.git").as_deref(),
            Some("com.github.exampleorg.myapp")
        );
        assert_eq!(
            project_id_from_remote("git@github.com:ExampleOrg/MyApp.git").as_deref(),
            Some("com.github.exampleorg.myapp")
        );
        assert_eq!(
            project_id_from_remote("ssh://git@git.example.dev/Team/My App.git").as_deref(),
            Some("dev.example.git.team.my-app")
        );
    }

    #[test]
    fn selected_project_sources_are_saved_as_portable_relative_paths() {
        let root =
            std::env::temp_dir().join(format!("dee-bugee-project-source-{}", std::process::id()));
        let logs = root.join("logs").join("development");
        std::fs::create_dir_all(&logs).unwrap();

        assert_eq!(manifest_source_path(&root, &logs), "logs/development");
        assert_eq!(manifest_source_path(&root, &root), ".");

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn project_setup_hides_windows_verbatim_path_prefixes() {
        assert_eq!(
            display_path(Path::new(r"\\?\C:\work\MyApp")),
            r"C:\work\MyApp"
        );
        assert_eq!(
            display_path(Path::new(r"\\?\UNC\server\logs\MyApp")),
            r"\\server\logs\MyApp"
        );
    }

    #[test]
    fn picked_source_reuses_the_initial_empty_row() {
        let mut sources = vec![String::new()];
        add_project_source(&mut sources, "logs".to_string());
        assert_eq!(sources, vec!["logs"]);

        add_project_source(&mut sources, "other.jsonl".to_string());
        assert_eq!(sources, vec!["logs", "other.jsonl"]);
    }

    #[test]
    fn project_manifest_creation_refuses_an_unconfirmed_overwrite() {
        let root =
            std::env::temp_dir().join(format!("dee-bugee-project-write-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let mut config = ProjectConfig {
            version: 1,
            id: "com.example.write-test".to_string(),
            name: "Write Test".to_string(),
            sources: vec!["logs".to_string()],
        };

        let manifest = write_project_manifest(&root, &config, false).unwrap();
        assert!(write_project_manifest(&root, &config, false).is_err());

        config.name = "Updated Write Test".to_string();
        write_project_manifest(&root, &config, true).unwrap();
        let saved = load_project_config(&manifest).unwrap();
        assert_eq!(saved.name, "Updated Write Test");

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn non_interactive_project_configuration_writes_and_protects_the_manifest() {
        let root = std::env::temp_dir().join(format!(
            "dee-bugee-project-cli-write-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();

        let manifest = configure_project(ProjectConfiguration {
            root: root.clone(),
            id: "com.example.cli-test".to_string(),
            name: "CLI Test".to_string(),
            sources: vec!["logs/development".to_string()],
            overwrite: false,
        })
        .unwrap();
        let saved = load_project_config(&manifest).unwrap();
        assert_eq!(saved.id, "com.example.cli-test");
        assert_eq!(saved.name, "CLI Test");
        assert_eq!(saved.sources, ["logs/development"]);

        assert!(
            configure_project(ProjectConfiguration {
                root: root.clone(),
                id: "com.example.cli-test".to_string(),
                name: "CLI Test".to_string(),
                sources: vec!["logs/development".to_string()],
                overwrite: false,
            })
            .is_err()
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    fn session_event(
        session: &str,
        event_name: &str,
        timestamp: &str,
        level: Level,
        duration_ms: Option<f64>,
        status: Option<&str>,
    ) -> LogEvent {
        let mut event = LogEvent::new(
            level,
            "test",
            "session-test",
            event_name,
            "[Session Test] Event",
            session,
        );
        event.timestamp = timestamp.parse().unwrap();
        event.duration_ms = duration_ms;
        event.status = status.map(|value| serde_json::Value::String(value.to_string()));
        event
    }

    #[test]
    fn session_summaries_keep_outcomes_and_timing_correct_when_events_are_out_of_order() {
        let later = session_event(
            "run-a",
            "sync.completed",
            "2026-08-22T10:00:03Z",
            Level::Info,
            Some(120.0),
            Some("completed"),
        );
        let mut earlier = session_event(
            "run-a",
            "sync.started",
            "2026-08-22T10:00:00Z",
            Level::Warn,
            None,
            Some("started"),
        );
        earlier.provider = Some("local".to_string());
        earlier.request_id = Some("request-1".to_string());

        let mut explorer = SessionExplorerState::default();
        explorer.observe_many(&[later, earlier]);

        let summary = explorer.summaries.get("run-a").unwrap();
        assert_eq!(summary.event_count, 2);
        assert_eq!(summary.warning_count, 1);
        assert_eq!(summary.error_count, 0);
        assert_eq!(summary.elapsed_ms(), 3_000);
        assert_eq!(summary.status(), Some("completed"));
        assert!(summary.providers.contains("local"));
        assert!(summary.correlations.contains("request-1"));
    }

    #[test]
    fn session_comparison_prioritizes_missing_and_changed_event_types() {
        let mut left = SessionSummary::new(&session_event(
            "run-a",
            "sync.started",
            "2026-08-22T10:00:00Z",
            Level::Info,
            Some(100.0),
            Some("started"),
        ));
        left.observe(&session_event(
            "run-a",
            "sync.completed",
            "2026-08-22T10:00:01Z",
            Level::Info,
            Some(200.0),
            Some("completed"),
        ));
        let mut right = SessionSummary::new(&session_event(
            "run-b",
            "sync.started",
            "2026-08-22T11:00:00Z",
            Level::Info,
            Some(240.0),
            Some("started"),
        ));
        right.observe(&session_event(
            "run-b",
            "sync.failed",
            "2026-08-22T11:00:01Z",
            Level::Error,
            Some(500.0),
            Some("failed"),
        ));

        let rows = compare_sessions(&left, &right);
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(SessionComparisonRow::differs));
        assert!(rows[0].count_a == 0 || rows[0].count_b == 0);

        let started = rows.iter().find(|row| row.event == "sync.started").unwrap();
        assert_eq!(started.count_a, 1);
        assert_eq!(started.count_b, 1);
        assert_eq!(started.average_duration_a, Some(100.0));
        assert_eq!(started.average_duration_b, Some(240.0));
    }

    #[test]
    fn session_comparison_detects_maximum_duration_only_changes() {
        let mut left = SessionSummary::new(&session_event(
            "run-a",
            "sync.step",
            "2026-08-22T10:00:00Z",
            Level::Info,
            Some(0.0),
            None,
        ));
        left.observe(&session_event(
            "run-a",
            "sync.step",
            "2026-08-22T10:00:01Z",
            Level::Info,
            Some(100.0),
            None,
        ));
        let mut right = SessionSummary::new(&session_event(
            "run-b",
            "sync.step",
            "2026-08-22T11:00:00Z",
            Level::Info,
            Some(50.0),
            None,
        ));
        right.observe(&session_event(
            "run-b",
            "sync.step",
            "2026-08-22T11:00:01Z",
            Level::Info,
            Some(50.0),
            None,
        ));

        let rows = compare_sessions(&left, &right);
        assert_eq!(rows[0].average_duration_a, rows[0].average_duration_b);
        assert_ne!(rows[0].max_duration_a, rows[0].max_duration_b);
        assert!(rows[0].differs());
    }

    #[test]
    fn viewing_a_session_replaces_every_conflicting_filter() {
        let mut filter = FilterState {
            text: "failed provider=remote".to_owned(),
            minimum_level: Some(Level::Error),
            correlation: Some("request-other".to_owned()),
            ..FilterState::default()
        };
        filter
            .facets
            .entry("provider".to_owned())
            .or_default()
            .include_only("remote");
        filter
            .facets
            .entry("app_session_id".to_owned())
            .or_default()
            .toggle_exclude("run-a");

        apply_session_filter(&mut filter, "run-a".to_owned());

        assert!(filter.text.is_empty());
        assert_eq!(filter.minimum_level, None);
        assert_eq!(filter.correlation, None);
        assert_eq!(filter.facets.len(), 1);
        let session = &filter.facets["app_session_id"];
        assert_eq!(session.included.iter().collect::<Vec<_>>(), vec!["run-a"]);
        assert!(session.excluded.is_empty());
    }

    #[test]
    fn relative_time_anchor_uses_the_complete_session_summary() {
        let mut explorer = SessionExplorerState::default();
        explorer.observe_many(&[
            session_event(
                "run-a",
                "sync.started",
                "2026-08-22T10:00:00Z",
                Level::Info,
                None,
                None,
            ),
            session_event(
                "run-a",
                "sync.failed",
                "2026-08-22T10:00:05Z",
                Level::Error,
                None,
                None,
            ),
        ]);

        let starts = session_start_times(&explorer.summaries);
        assert_eq!(
            starts["run-a"],
            "2026-08-22T10:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn pin_lookup_finds_events_hidden_from_the_presented_table() {
        let hidden = session_event(
            "run-pin",
            "sync.started",
            "2026-08-22T10:00:00Z",
            Level::Info,
            None,
            None,
        );
        let visible = session_event(
            "run-pin",
            "sync.failed",
            "2026-08-22T10:00:01Z",
            Level::Error,
            None,
            None,
        );
        let hidden_key = event_pin_key(&hidden);
        let mut store = EventStore::default();
        store.extend([hidden, visible]);

        assert_eq!(event_index_by_key(&store, &hidden_key), Some(0));
    }

    #[test]
    fn comparison_slots_cannot_select_the_same_session() {
        assert_eq!(toggle_session_choice(None, "run-a", Some("run-a")), None);
        assert_eq!(
            toggle_session_choice(Some("run-a".to_string()), "run-a", None),
            None
        );
        assert_eq!(
            toggle_session_choice(None, "run-b", Some("run-a")),
            Some("run-b".to_string())
        );
    }

    #[test]
    fn pinned_events_and_notes_round_trip_through_workspace_toml() {
        let event = session_event(
            "run-pin",
            "sync.failed",
            "2026-08-22T10:00:01Z",
            Level::Error,
            Some(420.0),
            Some("failed"),
        );
        let mut workspace = WorkspaceConfig::new(vec![PathBuf::from("test.jsonl")]);
        workspace.pins.push(PinnedEvent {
            key: event_pin_key(&event),
            note: "Retry started after the provider timeout".to_owned(),
            event_json: serde_json::to_string(&event).unwrap(),
        });

        let encoded = toml::to_string(&workspace).unwrap();
        let decoded: WorkspaceConfig = toml::from_str(&encoded).unwrap();
        assert_eq!(decoded.pins, workspace.pins);
        assert_eq!(decoded.pins[0].event(), Some(event));
    }

    #[test]
    fn facet_layout_is_deduplicated_and_forward_compatible() {
        let order = normalize_facet_order(vec![
            "status".to_owned(),
            "fields.job".to_owned(),
            "status".to_owned(),
        ]);
        assert_eq!(&order[..2], ["status", "fields.job"]);
        assert!(
            DISPLAYED_FACETS
                .iter()
                .all(|facet| order.contains(&(*facet).to_owned()))
        );
    }

    #[test]
    fn facet_value_search_is_case_insensitive_and_empty_search_matches_all() {
        assert!(facet_value_matches_search("Remote Provider", "provider"));
        assert!(facet_value_matches_search("Remote Provider", "remote"));
        assert!(facet_value_matches_search("Remote Provider", ""));
        assert!(!facet_value_matches_search("Local Cache", "remote"));
    }

    #[test]
    fn facet_search_results_stay_open_during_expand_collapse_all_requests() {
        assert_eq!(facet_section_open_request(false, Some(true)), Some(true));
        assert_eq!(facet_section_open_request(false, Some(false)), Some(false));
        assert_eq!(facet_section_open_request(true, Some(false)), Some(true));
        assert_eq!(facet_section_open_request(false, None), None);
    }

    #[test]
    fn relative_time_is_signed_and_millisecond_precise() {
        assert_eq!(
            format_relative_elapsed(chrono::Duration::milliseconds(1_842)),
            "+1.842s"
        );
        assert_eq!(
            format_relative_elapsed(chrono::Duration::milliseconds(-25)),
            "-0.025s"
        );
    }
}
