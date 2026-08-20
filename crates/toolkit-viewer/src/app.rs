use std::{
    collections::BTreeMap,
    io::{BufWriter, Write},
    path::PathBuf,
    thread,
    time::Duration,
};

use crossbeam_channel::TryRecvError;
use dee_bugee_core::{EventStore, FilterState, PRIMARY_FACETS, status_text};
use dee_bugee_schema::{Level, LogEvent};
use eframe::egui::{self, Color32, Label, PointerButton, RichText, Sense, TextStyle};
use egui_extras::{Column, TableBuilder};
use serde::{Deserialize, Serialize};

use crate::follower::{ReaderCommand, ReaderHandle, ReaderMessage, spawn_reader};

const DISPLAYED_FACETS: [&str; 8] = [
    "level",
    "source",
    "subsystem",
    "target",
    "event",
    "provider",
    "status",
    "correlation",
];
const PREFERENCES_KEY: &str = "dee_bugee.viewer_preferences.v1";
const TAIL_HEADROOM_ROWS: f32 = 6.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TableColumn {
    Timestamp,
    Level,
    Source,
    Subsystem,
    Event,
    Provider,
    Correlation,
    Duration,
    Status,
    Message,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ColorBy {
    #[default]
    Off,
    Source,
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
    const ALL: [Self; 7] = [
        Self::Off,
        Self::Source,
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
            Self::Subsystem => "Subsystem",
            Self::Target => "Target",
            Self::Event => "Event",
            Self::Provider => "Provider",
            Self::Correlation => "Correlation",
        }
    }

    fn event_value(self, event: &LogEvent) -> Option<&str> {
        match self {
            Self::Off => None,
            Self::Source => Some(event.source.as_str()),
            Self::Subsystem => Some(event.subsystem.as_str()),
            Self::Target => event.target.as_deref(),
            Self::Event => Some(event.event.as_str()),
            Self::Provider => event.provider.as_deref(),
            Self::Correlation => Some(event.correlation_id()),
        }
        .filter(|value| !value.is_empty() && *value != "-")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FilterBookmark {
    name: String,
    filter: FilterState,
}

impl TableColumn {
    const ALL: [Self; 10] = [
        Self::Timestamp,
        Self::Level,
        Self::Source,
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
            Self::Level => "Level",
            Self::Source => "Source",
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
            Self::Timestamp => Column::initial(185.0).at_least(140.0),
            Self::Level => Column::initial(62.0).at_least(54.0),
            Self::Source => Column::initial(90.0).at_least(65.0),
            Self::Subsystem => Column::initial(110.0).at_least(75.0),
            Self::Event => Column::initial(150.0).at_least(90.0),
            Self::Provider => Column::initial(90.0).at_least(65.0),
            Self::Correlation => Column::initial(145.0).at_least(90.0),
            Self::Duration => Column::initial(82.0).at_least(65.0),
            Self::Status => Column::initial(70.0).at_least(55.0),
            Self::Message => Column::remainder().at_least(240.0),
        }
    }
}

fn default_column_order() -> Vec<TableColumn> {
    TableColumn::ALL.to_vec()
}

fn normalize_column_order(columns: Vec<TableColumn>) -> Vec<TableColumn> {
    let mut normalized = Vec::with_capacity(TableColumn::ALL.len());
    for column in columns.into_iter().chain(TableColumn::ALL) {
        if !normalized.contains(&column) {
            normalized.push(column);
        }
    }
    normalized
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
struct ViewerPreferences {
    version: u16,
    column_order: Vec<TableColumn>,
    wrapped_messages: bool,
    stick_to_bottom: bool,
    color_by: ColorBy,
    bookmarks: Vec<FilterBookmark>,
    latest_at: LatestAt,
}

impl Default for ViewerPreferences {
    fn default() -> Self {
        Self {
            version: 1,
            column_order: default_column_order(),
            wrapped_messages: true,
            stick_to_bottom: true,
            color_by: ColorBy::Off,
            bookmarks: Vec::new(),
            latest_at: LatestAt::Bottom,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkspaceConfig {
    version: u16,
    sources: Vec<PathBuf>,
    filter: FilterState,
    wrapped_messages: bool,
    stick_to_bottom: bool,
    #[serde(default)]
    color_by: ColorBy,
    #[serde(default)]
    bookmarks: Vec<FilterBookmark>,
    #[serde(default)]
    latest_at: LatestAt,
    #[serde(default = "default_column_order")]
    column_order: Vec<TableColumn>,
}

pub struct ViewerApp {
    store: EventStore,
    filter: FilterState,
    visible_rows: Vec<usize>,
    facet_counts: BTreeMap<String, Vec<(String, u64)>>,
    reader: ReaderHandle,
    sources: Vec<PathBuf>,
    selected_row: Option<usize>,
    invalid_records: u64,
    last_error: Option<String>,
    last_notice: Option<String>,
    paused: bool,
    filters_dirty: bool,
    wrapped_messages: bool,
    stick_to_bottom: bool,
    color_by: ColorBy,
    bookmarks: Vec<FilterBookmark>,
    latest_at: LatestAt,
    column_order: Vec<TableColumn>,
    middle_pan_active: bool,
    tail_was_at_bottom: bool,
    scroll_to_bottom_requested: bool,
    last_discarded_events: u64,
}

impl ViewerApp {
    pub fn new(
        creation_context: &eframe::CreationContext<'_>,
        initial_paths: Vec<PathBuf>,
    ) -> Self {
        creation_context.egui_ctx.set_visuals(egui::Visuals::dark());
        creation_context
            .egui_ctx
            .global_style_mut(|style| style.animation_time = 0.08);

        let mut preferences = creation_context
            .storage
            .and_then(|storage| eframe::get_value::<ViewerPreferences>(storage, PREFERENCES_KEY))
            .filter(|preferences| preferences.version == 1)
            .unwrap_or_default();
        preferences.column_order = normalize_column_order(preferences.column_order);

        let reader = spawn_reader(initial_paths);
        let mut app = Self {
            store: EventStore::default(),
            filter: FilterState::default(),
            visible_rows: Vec::new(),
            facet_counts: BTreeMap::new(),
            reader,
            sources: Vec::new(),
            selected_row: None,
            invalid_records: 0,
            last_error: None,
            last_notice: None,
            paused: false,
            filters_dirty: true,
            wrapped_messages: preferences.wrapped_messages,
            stick_to_bottom: preferences.stick_to_bottom,
            color_by: preferences.color_by,
            bookmarks: preferences.bookmarks,
            latest_at: preferences.latest_at,
            column_order: preferences.column_order,
            middle_pan_active: false,
            tail_was_at_bottom: true,
            scroll_to_bottom_requested: true,
            last_discarded_events: 0,
        };
        app.refresh_filters();
        app
    }

    fn drain_reader(&mut self) {
        let mut received_events = false;
        loop {
            match self.reader.messages.try_recv() {
                Ok(ReaderMessage::Batch(events)) => {
                    self.store.extend(events);
                    received_events = true;
                }
                Ok(ReaderMessage::InvalidRecord { path, line, error }) => {
                    self.invalid_records += 1;
                    self.last_error = Some(format!("{}:{line}: {error}", path.display()));
                }
                Ok(ReaderMessage::SourceOpened(path)) => {
                    if !self.sources.contains(&path) {
                        self.sources.push(path);
                        self.sources.sort();
                    }
                }
                Ok(ReaderMessage::SourceError { path, error }) => {
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
            self.selected_row = None;
            received_events = true;
        }
        if received_events && !self.paused {
            self.filters_dirty = true;
            if self.stick_to_bottom && self.tail_was_at_bottom {
                self.scroll_to_bottom_requested = true;
            }
        }
    }

    fn refresh_filters(&mut self) {
        self.visible_rows = self.store.query(&self.filter);
        order_visible_rows(&mut self.visible_rows, self.latest_at);
        self.facet_counts.clear();
        let mut facets: Vec<String> = DISPLAYED_FACETS
            .iter()
            .map(|facet| facet.to_string())
            .collect();
        facets.extend(
            self.store
                .facet_names()
                .filter(|facet| facet.starts_with("fields."))
                .take(20)
                .map(str::to_string),
        );
        for facet in facets {
            self.facet_counts
                .insert(facet.clone(), self.store.facet_counts(&facet, &self.filter));
        }
        self.filters_dirty = false;
    }

    fn add_paths(&mut self, paths: Vec<PathBuf>) {
        if paths.is_empty() {
            return;
        }
        if self
            .reader
            .commands
            .send(ReaderCommand::AddPaths(paths))
            .is_err()
        {
            self.last_error = Some("JSONL reader is unavailable".to_string());
        }
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
                self.stick_to_bottom = workspace.stick_to_bottom;
                self.color_by = workspace.color_by;
                self.bookmarks = workspace.bookmarks;
                self.latest_at = workspace.latest_at;
                self.column_order = normalize_column_order(workspace.column_order);
                self.add_paths(workspace.sources);
                self.filters_dirty = true;
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
        let workspace = WorkspaceConfig {
            version: 1,
            sources: self.sources.clone(),
            filter: self.filter.clone(),
            wrapped_messages: self.wrapped_messages,
            stick_to_bottom: self.stick_to_bottom,
            color_by: self.color_by,
            bookmarks: self.bookmarks.clone(),
            latest_at: self.latest_at,
            column_order: self.column_order.clone(),
        };
        match toml::to_string_pretty(&workspace)
            .map_err(|error| error.to_string())
            .and_then(|text| std::fs::write(&path, text).map_err(|error| error.to_string()))
        {
            Ok(()) => self.last_notice = Some(format!("Saved workspace {}", path.display())),
            Err(error) => self.last_error = Some(format!("Unable to save workspace: {error}")),
        }
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

    fn top_bar(&mut self, root: &mut egui::Ui) {
        egui::Panel::top("toolbar").show(root, |ui| {
            ui.horizontal_wrapped(|ui| {
                if ui.button("Open JSONL…").clicked()
                    && let Some(paths) = rfd::FileDialog::new()
                        .add_filter("JSON Lines", &["jsonl", "log"])
                        .pick_files()
                {
                    self.add_paths(paths);
                }
                if ui.button("Open log folder…").clicked()
                    && let Some(path) = rfd::FileDialog::new().pick_folder()
                {
                    self.add_paths(vec![path]);
                }
                if ui.button("Open workspace…").clicked() {
                    self.open_workspace();
                }
                if ui.button("Save workspace…").clicked() {
                    self.save_workspace();
                }
                if ui
                    .add_enabled(
                        !self.visible_rows.is_empty(),
                        egui::Button::new("Export filtered…"),
                    )
                    .clicked()
                {
                    self.export_filtered();
                }

                if ui
                    .selectable_label(self.paused, if self.paused { "Resume" } else { "Pause" })
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
                    .checkbox(&mut self.stick_to_bottom, "Stick to latest")
                    .changed()
                    && self.stick_to_bottom
                {
                    self.tail_was_at_bottom = true;
                    self.scroll_to_bottom_requested = true;
                }
                let previous_latest_at = self.latest_at;
                egui::ComboBox::from_id_salt("latest_at")
                    .selected_text(format!("Latest at: {}", self.latest_at.title()))
                    .show_ui(ui, |ui| {
                        for option in LatestAt::ALL {
                            ui.selectable_value(&mut self.latest_at, option, option.title());
                        }
                    });
                if self.latest_at != previous_latest_at {
                    self.filters_dirty = true;
                    self.tail_was_at_bottom = true;
                    self.scroll_to_bottom_requested = true;
                }
                ui.checkbox(&mut self.wrapped_messages, "Wrap messages");
                egui::ComboBox::from_id_salt("color_by")
                    .selected_text(format!("Color by: {}", self.color_by.title()))
                    .show_ui(ui, |ui| {
                        for option in ColorBy::ALL {
                            ui.selectable_value(&mut self.color_by, option, option.title());
                        }
                    });
                ui.label(RichText::new("↔ Drag headers · Middle-drag to pan table").weak());

                ui.separator();
                ui.label("Search");
                if ui
                    .add_sized(
                        [280.0, 24.0],
                        egui::TextEdit::singleline(&mut self.filter.text)
                            .hint_text("message, event, subsystem or field"),
                    )
                    .changed()
                {
                    self.filters_dirty = true;
                    self.tail_was_at_bottom = true;
                    self.scroll_to_bottom_requested = true;
                }

                egui::ComboBox::from_id_salt("minimum_level")
                    .selected_text(
                        self.filter
                            .minimum_level
                            .map(|level| format!("≥ {level}"))
                            .unwrap_or_else(|| "All levels".to_string()),
                    )
                    .show_ui(ui, |ui| {
                        if ui
                            .selectable_value(&mut self.filter.minimum_level, None, "All levels")
                            .changed()
                        {
                            self.filters_dirty = true;
                        }
                        for level in Level::ALL {
                            if ui
                                .selectable_value(
                                    &mut self.filter.minimum_level,
                                    Some(level),
                                    format!("≥ {level}"),
                                )
                                .changed()
                            {
                                self.filters_dirty = true;
                            }
                        }
                    });

                if ui
                    .add_enabled(self.filter.is_active(), egui::Button::new("Clear filters"))
                    .clicked()
                {
                    self.filter.clear();
                    self.filters_dirty = true;
                    self.tail_was_at_bottom = true;
                    self.scroll_to_bottom_requested = true;
                }

                ui.separator();
                ui.label(format!(
                    "{} shown / {} loaded",
                    self.visible_rows.len(),
                    self.store.len()
                ));
                if self.invalid_records > 0 {
                    ui.colored_label(Color32::YELLOW, format!("{} invalid", self.invalid_records));
                }
                if self.store.discarded_events() > 0 {
                    ui.colored_label(
                        Color32::YELLOW,
                        format!("{} outside memory window", self.store.discarded_events()),
                    );
                }
            });

            if let Some(error) = self.last_error.clone() {
                ui.horizontal(|ui| {
                    ui.colored_label(Color32::LIGHT_RED, &error);
                    if ui.small_button("Dismiss").clicked() {
                        self.last_error = None;
                    }
                });
            }
            if let Some(notice) = self.last_notice.clone() {
                ui.horizontal(|ui| {
                    ui.colored_label(Color32::LIGHT_GREEN, &notice);
                    if ui.small_button("Dismiss").clicked() {
                        self.last_notice = None;
                    }
                });
            }
        });
    }

    fn sidebar(&mut self, root: &mut egui::Ui) {
        egui::Panel::left("facets")
            .default_size(260.0)
            .min_size(180.0)
            .resizable(true)
            .show(root, |ui| {
                ui.heading("Filters");
                ui.small("Left-click: only · Ctrl+click: add · Right-click: hide");
                ui.separator();

                egui::ScrollArea::vertical().show(ui, |ui| {
                    for facet in DISPLAYED_FACETS {
                        let counts = self.facet_counts.get(facet).cloned().unwrap_or_default();
                        let active = self
                            .filter
                            .facets
                            .get(facet)
                            .is_some_and(|selection| !selection.is_empty());
                        let title = if active {
                            RichText::new(facet_title(facet))
                                .strong()
                                .color(Color32::LIGHT_BLUE)
                        } else {
                            RichText::new(facet_title(facet)).strong()
                        };

                        egui::CollapsingHeader::new(title)
                            .default_open(matches!(facet, "level" | "source" | "subsystem"))
                            .show(ui, |ui| {
                                for (value, count) in counts.into_iter().take(100) {
                                    self.facet_value_row(ui, facet, &value, count);
                                }
                            });
                    }

                    let extra_facets: Vec<_> = self
                        .store
                        .facet_names()
                        .filter(|facet| {
                            facet.starts_with("fields.")
                                && !PRIMARY_FACETS.contains(facet)
                                && !DISPLAYED_FACETS.contains(facet)
                        })
                        .map(str::to_string)
                        .collect();
                    if !extra_facets.is_empty() {
                        ui.separator();
                        ui.label(RichText::new("Discovered fields").strong());
                    }
                    for facet in extra_facets {
                        let counts = self.facet_counts.get(&facet).cloned().unwrap_or_default();
                        egui::CollapsingHeader::new(facet.trim_start_matches("fields.")).show(
                            ui,
                            |ui| {
                                for (value, count) in counts.into_iter().take(100) {
                                    self.facet_value_row(ui, &facet, &value, count);
                                }
                            },
                        );
                    }
                });
            });
    }

    fn facet_value_row(&mut self, ui: &mut egui::Ui, facet: &str, value: &str, count: u64) {
        let state = self.filter.facets.get(facet);
        let included = state.is_some_and(|selection| selection.included.contains(value));
        let excluded = state.is_some_and(|selection| selection.excluded.contains(value));
        let label = format!("{value}    {count}");
        let text = if excluded {
            RichText::new(label)
                .color(Color32::LIGHT_RED)
                .strikethrough()
        } else {
            RichText::new(label)
        };
        let response = ui
            .selectable_label(included, text)
            .on_hover_text("Left-click to show only; Ctrl+left-click to add; right-click to hide");

        if response.clicked_by(PointerButton::Primary) {
            let selection = self.filter.facets.entry(facet.to_string()).or_default();
            if ui.input(|input| input.modifiers.ctrl) {
                selection.toggle_include(value);
            } else if selection.included.len() == 1
                && selection.included.contains(value)
                && selection.excluded.is_empty()
            {
                selection.included.clear();
            } else {
                selection.excluded.remove(value);
                selection.include_only(value);
            }
            self.filters_dirty = true;
        }
        if response.clicked_by(PointerButton::Secondary) {
            self.filter
                .facets
                .entry(facet.to_string())
                .or_default()
                .toggle_exclude(value);
            self.filters_dirty = true;
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
        let mut remove_index = None;

        ui.horizontal_wrapped(|ui| {
            ui.label(RichText::new("Bookmarks").strong());
            if ui
                .add_enabled(
                    self.filter.is_active() && !already_saved,
                    egui::Button::new("＋ Add bookmark"),
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
                    .selectable_label(
                        bookmark.filter == self.filter,
                        format!("★ {}", bookmark.name),
                    )
                    .on_hover_text("Click to apply; right-click to remove");
                if response.clicked() {
                    apply_filter = Some(bookmark.filter.clone());
                }
                if response.clicked_by(PointerButton::Secondary) {
                    remove_index = Some(index);
                }
            }
        });
        ui.separator();

        if let Some(filter) = apply_filter {
            self.filter = filter;
            self.filters_dirty = true;
            self.tail_was_at_bottom = true;
            self.scroll_to_bottom_requested = true;
        }
        if let Some(index) = remove_index {
            self.bookmarks.remove(index);
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

        egui::Panel::bottom("details")
            .resizable(true)
            .default_size(190.0)
            .min_size(100.0)
            .show(root, |ui| {
                ui.horizontal(|ui| {
                    ui.heading("Selected event");
                    if ui.button("Filter correlation").clicked() {
                        self.filter.correlation = Some(event.correlation_id().to_string());
                        self.filters_dirty = true;
                    }
                    if self.filter.correlation.is_some() && ui.button("Clear correlation").clicked()
                    {
                        self.filter.correlation = None;
                        self.filters_dirty = true;
                    }
                    if ui.button("Close").clicked() {
                        self.selected_row = None;
                    }
                });
                egui::ScrollArea::vertical().show(ui, |ui| {
                    ui.label(RichText::new(&event.message).strong());
                    ui.separator();
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

    fn central_table(&mut self, root: &mut egui::Ui) {
        egui::CentralPanel::default().show(root, |ui| {
            if self.store.is_empty() {
                ui.centered_and_justified(|ui| {
                    ui.vertical_centered(|ui| {
                        ui.heading("Open or drop one or more JSONL files");
                        ui.label("The viewer follows appended records automatically.");
                    });
                });
                return;
            }

            self.bookmark_bar(ui);

            let row_height = ui.text_style_height(&TextStyle::Body) + 8.0;
            let manual_wheel_scroll = ui.rect_contains_pointer(ui.max_rect())
                && ui.input(|input| input.smooth_scroll_delta() != egui::Vec2::ZERO);
            let column_order = self.column_order.clone();
            let visible_rows = &self.visible_rows;
            let store = &self.store;
            let wrapped = self.wrapped_messages;
            let color_by = self.color_by;
            let latest_at = self.latest_at;
            let scroll_to_bottom_requested =
                self.scroll_to_bottom_requested && !manual_wheel_scroll;
            let mut selected = self.selected_row;
            let mut requested_move = None;
            let table_content_width = ui.available_width().max(1_400.0);
            let mut horizontal_output = egui::ScrollArea::horizontal()
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
                    if scroll_to_bottom_requested && !visible_rows.is_empty() {
                        let (row, alignment) = match latest_at {
                            LatestAt::Bottom => (visible_rows.len(), egui::Align::BOTTOM),
                            LatestAt::Top => (0, egui::Align::TOP),
                        };
                        table = table
                            .scroll_to_row(row, Some(alignment))
                            .animate_scrolling(false);
                    }

                    table
                        .header(row_height, |mut header| {
                            for column in &column_order {
                                header.col(|ui| {
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
                                                            RichText::new(format!(
                                                                "↔ {}",
                                                                column.title()
                                                            ))
                                                            .strong(),
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
                            let heights: Vec<f32> = visible_rows
                                .iter()
                                .map(|index| {
                                    if !wrapped {
                                        return row_height;
                                    }
                                    let chars = store
                                        .get(*index)
                                        .map_or(0, |event| event.message.chars().count());
                                    let characters_per_line = (message_width / 7.2).max(12.0);
                                    let lines = ((chars as f32 / characters_per_line).ceil()
                                        as usize)
                                        .clamp(1, 12);
                                    row_height * lines as f32
                                })
                                .chain(std::iter::once(row_height * TAIL_HEADROOM_ROWS))
                                .collect();

                            body.heterogeneous_rows(heights.into_iter(), |mut row| {
                                if row.index() == visible_rows.len() {
                                    for _ in &column_order {
                                        row.col(|_| {});
                                    }
                                    return;
                                }
                                let Some(store_index) = visible_rows.get(row.index()).copied()
                                else {
                                    return;
                                };
                                let Some(event) = store.get(store_index) else {
                                    return;
                                };
                                let row_color = color_by.event_value(event).map(stable_value_color);
                                row.set_selected(selected == Some(store_index));
                                for column in &column_order {
                                    row.col(|ui| {
                                        if let Some(color) = row_color {
                                            ui.visuals_mut().override_text_color = Some(color);
                                        }
                                        show_event_cell(ui, *column, event, wrapped);
                                    });
                                }
                                if row.response().clicked() {
                                    selected = Some(store_index);
                                }
                            });
                        })
                });

            let (middle_pressed, middle_down, pointer_position, pointer_delta) =
                ui.input(|input| {
                    (
                        input.pointer.button_pressed(PointerButton::Middle),
                        input.pointer.middle_down(),
                        input.pointer.interact_pos(),
                        input.pointer.delta(),
                    )
                });
            if middle_pressed
                && pointer_position
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

            let vertical_output = &horizontal_output.inner;
            let manually_scrolled = manual_wheel_scroll || middle_panned;
            self.tail_was_at_bottom = scroll_is_at_latest(
                vertical_output.state.offset.y,
                vertical_output.content_size.y,
                vertical_output.inner_rect.height(),
                if manually_scrolled {
                    1.0
                } else {
                    row_height.max(2.0)
                },
                latest_at,
            );
            // `scroll_to_row` updates the persisted offset after this frame. Keep the
            // request alive until the following frame observes that the viewport
            // really reached the end; otherwise a busy stream can disable tailing
            // between issuing the command and egui applying it.
            self.scroll_to_bottom_requested = retain_scroll_to_bottom_request(
                scroll_to_bottom_requested,
                self.tail_was_at_bottom,
                manually_scrolled,
            );
            if self.scroll_to_bottom_requested {
                ui.ctx().request_repaint();
            }

            self.selected_row = selected;
            if let Some((source, target, insert_after)) = requested_move {
                move_column(&mut self.column_order, source, target, insert_after);
            }
        });
    }
}

fn show_event_cell(ui: &mut egui::Ui, column: TableColumn, event: &LogEvent, wrapped: bool) {
    match column {
        TableColumn::Timestamp => {
            ui.label(event.timestamp_text());
        }
        TableColumn::Level => {
            ui.colored_label(level_color(event.level), event.level.to_string());
        }
        TableColumn::Source => {
            ui.label(&event.source);
        }
        TableColumn::Subsystem => {
            ui.label(&event.subsystem);
        }
        TableColumn::Event => {
            ui.label(&event.event);
        }
        TableColumn::Provider => {
            ui.label(event.provider.as_deref().unwrap_or("-"));
        }
        TableColumn::Correlation => {
            ui.label(event.correlation_id());
        }
        TableColumn::Duration => {
            ui.label(
                event
                    .duration_ms
                    .map(|value| format!("{value:.1} ms"))
                    .unwrap_or_else(|| "-".to_string()),
            );
        }
        TableColumn::Status => {
            ui.label(status_text(event.status.as_ref()));
        }
        TableColumn::Message => {
            let label = Label::new(&event.message);
            ui.add(if wrapped {
                label.wrap()
            } else {
                label.truncate()
            });
        }
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

fn order_visible_rows(rows: &mut [usize], latest_at: LatestAt) {
    if latest_at == LatestAt::Top {
        rows.reverse();
    }
}

fn retain_scroll_to_bottom_request(
    requested: bool,
    reached_bottom: bool,
    manually_scrolled: bool,
) -> bool {
    requested && !reached_bottom && !manually_scrolled
}

impl eframe::App for ViewerApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.drain_reader();

        let dropped_paths: Vec<PathBuf> = ui.input(|input| {
            input
                .raw
                .dropped_files
                .iter()
                .filter_map(|file| file.path.clone())
                .collect()
        });
        self.add_paths(dropped_paths);

        if self.filters_dirty && !self.paused {
            self.refresh_filters();
        }

        self.top_bar(ui);
        self.sidebar(ui);
        self.details_panel(ui);
        self.central_table(ui);

        ui.ctx().request_repaint_after(Duration::from_millis(100));
    }

    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        let preferences = ViewerPreferences {
            version: 1,
            column_order: self.column_order.clone(),
            wrapped_messages: self.wrapped_messages,
            stick_to_bottom: self.stick_to_bottom,
            color_by: self.color_by,
            bookmarks: self.bookmarks.clone(),
            latest_at: self.latest_at,
        };
        eframe::set_value(storage, PREFERENCES_KEY, &preferences);
    }

    fn auto_save_interval(&self) -> Duration {
        Duration::from_secs(5)
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
        Level::Trace => Color32::GRAY,
        Level::Debug => Color32::LIGHT_GRAY,
        Level::Info => Color32::LIGHT_BLUE,
        Level::Warn => Color32::YELLOW,
        Level::Error => Color32::LIGHT_RED,
        Level::Fatal => Color32::RED,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn tail_request_survives_until_the_new_offset_reaches_bottom() {
        assert!(retain_scroll_to_bottom_request(true, false, false));
        assert!(!retain_scroll_to_bottom_request(true, true, false));
        assert!(!retain_scroll_to_bottom_request(false, false, false));
        assert!(!retain_scroll_to_bottom_request(true, false, true));
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
}
