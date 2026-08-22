use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::Arc,
};

use chrono::Utc;
use dee_bugee_schema::{Level, LogEvent, scalar_text};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod query;

pub use query::{ParsedPredicate, ParsedQuery, QueryError, parse_structured_query};

pub const DEFAULT_MAX_EVENTS: usize = 250_000;

pub const PRIMARY_FACETS: [&str; 12] = [
    "level",
    "source",
    "tag",
    "subsystem",
    "target",
    "event",
    "provider",
    "status",
    "correlation",
    "app_session_id",
    "playback_session_id",
    "request_id",
];

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FacetSelection {
    pub included: BTreeSet<String>,
    pub excluded: BTreeSet<String>,
}

impl FacetSelection {
    pub fn include_only(&mut self, value: impl Into<String>) {
        self.included.clear();
        self.included.insert(value.into());
    }

    pub fn toggle_include(&mut self, value: impl Into<String>) {
        let value = value.into();
        self.excluded.remove(&value);
        if !self.included.remove(&value) {
            self.included.insert(value);
        }
    }

    pub fn toggle_exclude(&mut self, value: impl Into<String>) {
        let value = value.into();
        self.included.remove(&value);
        if !self.excluded.remove(&value) {
            self.excluded.insert(value);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.included.is_empty() && self.excluded.is_empty()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterState {
    pub facets: BTreeMap<String, FacetSelection>,
    pub text: String,
    pub minimum_level: Option<Level>,
    pub correlation: Option<String>,
}

impl FilterState {
    pub fn clear(&mut self) {
        *self = Self::default();
    }

    pub fn is_active(&self) -> bool {
        !self.facets.values().all(FacetSelection::is_empty)
            || !self.text.trim().is_empty()
            || self.minimum_level.is_some()
            || self.correlation.is_some()
    }
}

#[derive(Debug)]
pub struct EventStore {
    events: Vec<LogEvent>,
    event_ids: Vec<u64>,
    search_documents: Vec<Arc<str>>,
    indexes: BTreeMap<String, BTreeMap<String, RoaringBitmap>>,
    max_events: usize,
    pruning_paused: bool,
    discarded_events: u64,
    next_event_id: u64,
}

impl Default for EventStore {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_EVENTS)
    }
}

impl EventStore {
    pub fn new(max_events: usize) -> Self {
        Self {
            events: Vec::with_capacity(max_events.min(16_384)),
            event_ids: Vec::with_capacity(max_events.min(16_384)),
            search_documents: Vec::with_capacity(max_events.min(16_384)),
            indexes: BTreeMap::new(),
            max_events: max_events.clamp(1, u32::MAX as usize),
            pruning_paused: false,
            discarded_events: 0,
            next_event_id: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn discarded_events(&self) -> u64 {
        self.discarded_events
    }

    pub fn max_events(&self) -> usize {
        self.max_events
    }

    pub fn set_max_events(&mut self, max_events: usize) {
        self.max_events = max_events.clamp(1, u32::MAX as usize);
        self.prune_to_limit();
    }

    pub fn set_pruning_paused(&mut self, paused: bool) {
        self.pruning_paused = paused;
        if !paused {
            self.prune_to_limit();
        }
    }

    pub fn get(&self, index: usize) -> Option<&LogEvent> {
        self.events.get(index)
    }

    /// A monotonic ingestion identity that survives row-index shifts caused by pruning.
    pub fn event_id(&self, index: usize) -> Option<u64> {
        self.event_ids.get(index).copied()
    }

    pub fn push(&mut self, event: LogEvent) {
        let document = search_document(&event);
        let event_id = self.take_next_event_id();
        if self.pruning_paused || self.events.len() < self.max_events {
            let index = self.events.len() as u32;
            self.index_event(index, &event);
            self.search_documents.push(document);
            self.events.push(event);
            self.event_ids.push(event_id);
        } else {
            self.search_documents.push(document);
            self.events.push(event);
            self.event_ids.push(event_id);
            self.prune_to_limit();
        }
    }

    pub fn extend(&mut self, events: impl IntoIterator<Item = LogEvent>) {
        let events: Vec<_> = events.into_iter().collect();
        if !self.pruning_paused && self.events.len().saturating_add(events.len()) > self.max_events
        {
            for event in events {
                self.search_documents.push(search_document(&event));
                self.events.push(event);
                let event_id = self.take_next_event_id();
                self.event_ids.push(event_id);
            }
            self.prune_to_limit();
        } else {
            for event in events {
                let index = self.events.len() as u32;
                self.index_event(index, &event);
                self.search_documents.push(search_document(&event));
                self.events.push(event);
                let event_id = self.take_next_event_id();
                self.event_ids.push(event_id);
            }
        }
    }

    fn prune_to_limit(&mut self) -> usize {
        if self.pruning_paused {
            return 0;
        }
        let prune_count = self.events.len().saturating_sub(self.max_events);
        if prune_count > 0 {
            self.events.drain(..prune_count);
            self.event_ids.drain(..prune_count);
            self.search_documents.drain(..prune_count);
            self.discarded_events += prune_count as u64;
            self.rebuild_indexes();
        }
        prune_count
    }

    fn take_next_event_id(&mut self) -> u64 {
        let event_id = self.next_event_id;
        self.next_event_id = self.next_event_id.wrapping_add(1);
        event_id
    }

    pub fn facet_names(&self) -> impl Iterator<Item = &str> {
        self.indexes.keys().map(String::as_str)
    }

    pub fn query(&self, filter: &FilterState) -> Vec<usize> {
        let text_matches = self.search_matches(&filter.text);
        let parsed = parse_structured_query(&filter.text, Utc::now());
        let structured_matches = self.structured_matches(&parsed);
        self.query_bitmap(
            filter,
            None,
            text_matches.as_deref(),
            structured_matches.as_ref(),
        )
        .iter()
        .map(|index| index as usize)
        .collect()
    }

    pub fn facet_counts(&self, facet: &str, filter: &FilterState) -> Vec<(String, u64)> {
        let text_matches = self.search_matches(&filter.text);
        let parsed = parse_structured_query(&filter.text, Utc::now());
        let structured_matches = self.structured_matches(&parsed);
        self.facet_counts_with_matches(
            facet,
            filter,
            text_matches.as_deref(),
            structured_matches.as_ref(),
        )
    }

    pub fn query_with_facets(
        &self,
        filter: &FilterState,
        facets: &[String],
        text_matches: Option<&[usize]>,
    ) -> FilterResults {
        let parsed = parse_structured_query(&filter.text, Utc::now());
        let structured_matches = self.structured_matches(&parsed);
        let rows = self
            .query_bitmap(filter, None, text_matches, structured_matches.as_ref())
            .iter()
            .map(|index| index as usize)
            .collect();
        let facet_counts = facets
            .iter()
            .map(|facet| {
                (
                    facet.clone(),
                    self.facet_counts_with_matches(
                        facet,
                        filter,
                        text_matches,
                        structured_matches.as_ref(),
                    ),
                )
            })
            .collect();
        FilterResults { rows, facet_counts }
    }

    pub fn search_snapshot(&self, start_index: usize) -> SearchSnapshot {
        let start_index = start_index.min(self.search_documents.len());
        SearchSnapshot {
            start_index,
            documents: self.search_documents[start_index..].to_vec(),
        }
    }

    fn search_matches(&self, query: &str) -> Option<Vec<usize>> {
        let parsed = parse_structured_query(query, Utc::now());
        let terms = search_terms(&parsed.text);
        (!terms.is_empty()).then(|| {
            self.search_documents
                .iter()
                .enumerate()
                .filter_map(|(index, document)| {
                    document_matches_terms(document, &terms).then_some(index)
                })
                .collect()
        })
    }

    fn facet_counts_with_matches(
        &self,
        facet: &str,
        filter: &FilterState,
        text_matches: Option<&[usize]>,
        structured_matches: Option<&RoaringBitmap>,
    ) -> Vec<(String, u64)> {
        let Some(values) = self.indexes.get(facet) else {
            return Vec::new();
        };
        let has_other_filters = !filter.text.trim().is_empty()
            || filter.minimum_level.is_some()
            || filter.correlation.is_some()
            || filter
                .facets
                .iter()
                .any(|(name, selection)| name != facet && !selection.is_empty());
        let candidates = has_other_filters
            .then(|| self.query_bitmap(filter, Some(facet), text_matches, structured_matches));

        let mut counts: Vec<_> = values
            .iter()
            .filter_map(|(value, rows)| {
                let count = candidates.as_ref().map_or_else(
                    || rows.len(),
                    |candidates| rows.intersection_len(candidates),
                );
                (count > 0).then(|| (value.clone(), count))
            })
            .collect();
        counts.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        counts
    }

    fn query_bitmap(
        &self,
        filter: &FilterState,
        ignored_facet: Option<&str>,
        text_matches: Option<&[usize]>,
        structured_matches: Option<&RoaringBitmap>,
    ) -> RoaringBitmap {
        let mut result: RoaringBitmap = text_matches.map_or_else(
            || (0..self.events.len() as u32).collect(),
            |matches| {
                matches
                    .iter()
                    .copied()
                    .filter(|index| *index < self.events.len())
                    .map(|index| index as u32)
                    .collect()
            },
        );

        if let Some(structured_matches) = structured_matches {
            result &= structured_matches;
        }

        for (facet, selection) in &filter.facets {
            if ignored_facet == Some(facet.as_str()) || selection.is_empty() {
                continue;
            }

            let values = self.indexes.get(facet);
            if !selection.included.is_empty() {
                let mut included = RoaringBitmap::new();
                if let Some(values) = values {
                    for value in &selection.included {
                        if let Some(rows) = values.get(value) {
                            included |= rows;
                        }
                    }
                }
                result &= included;
            }

            if let Some(values) = values {
                for value in &selection.excluded {
                    if let Some(rows) = values.get(value) {
                        result -= rows;
                    }
                }
            }
        }

        if let Some(minimum) = filter.minimum_level {
            let mut allowed = RoaringBitmap::new();
            if let Some(levels) = self.indexes.get("level") {
                for level in Level::ALL {
                    if level.severity() >= minimum.severity()
                        && let Some(rows) = levels.get(&level.to_string())
                    {
                        allowed |= rows;
                    }
                }
            }
            result &= allowed;
        }

        if let Some(correlation) = filter.correlation.as_deref() {
            if let Some(rows) = self
                .indexes
                .get("correlation")
                .and_then(|values| values.get(correlation))
            {
                result &= rows;
            } else {
                result.clear();
            }
        }

        result
    }

    fn structured_matches(&self, parsed: &ParsedQuery) -> Option<RoaringBitmap> {
        if parsed.predicates.is_empty() && parsed.errors.is_empty() {
            return None;
        }
        if !parsed.errors.is_empty() {
            return Some(RoaringBitmap::new());
        }
        Some(
            self.events
                .iter()
                .enumerate()
                .filter_map(|(index, event)| {
                    parsed
                        .predicates
                        .iter()
                        .all(|predicate| predicate.matches(event))
                        .then_some(index as u32)
                })
                .collect(),
        )
    }

    fn rebuild_indexes(&mut self) {
        let events = std::mem::take(&mut self.events);
        self.indexes.clear();
        for (index, event) in events.iter().enumerate() {
            self.index_event(index as u32, event);
        }
        self.events = events;
    }

    fn index_event(&mut self, index: u32, event: &LogEvent) {
        self.add_index("level", event.level.to_string(), index);
        self.add_index("source", event.source.clone(), index);
        self.add_index("tag", event.tag(), index);
        self.add_index("subsystem", event.subsystem.clone(), index);
        self.add_optional_index("target", event.target.as_deref(), index);
        self.add_index("event", event.event.clone(), index);
        self.add_index("correlation", event.correlation_id().to_string(), index);
        self.add_index("app_session_id", event.app_session_id.clone(), index);
        self.add_optional_index(
            "playback_session_id",
            event.playback_session_id.as_deref(),
            index,
        );
        self.add_optional_index("request_id", event.request_id.as_deref(), index);
        self.add_optional_index("session_id", event.session_id.as_deref(), index);
        self.add_optional_index("provider", event.provider.as_deref(), index);
        if let Some(status) = event.status.as_ref().and_then(scalar_text) {
            self.add_index("status", status, index);
        }

        for (key, value) in &event.fields {
            if let Some(value) = scalar_text(value) {
                self.add_index(&format!("fields.{key}"), value, index);
            }
        }
    }

    fn add_optional_index(&mut self, facet: &str, value: Option<&str>, index: u32) {
        if let Some(value) = value.filter(|value| !value.is_empty()) {
            self.add_index(facet, value.to_string(), index);
        }
    }

    fn add_index(&mut self, facet: &str, value: String, index: u32) {
        self.indexes
            .entry(facet.to_string())
            .or_default()
            .entry(value)
            .or_default()
            .insert(index);
    }
}

#[derive(Debug)]
pub struct FilterResults {
    pub rows: Vec<usize>,
    pub facet_counts: BTreeMap<String, Vec<(String, u64)>>,
}

#[derive(Debug)]
pub struct SearchSnapshot {
    start_index: usize,
    documents: Vec<Arc<str>>,
}

impl SearchSnapshot {
    pub fn search(&self, query: &str, mut cancelled: impl FnMut() -> bool) -> Option<Vec<usize>> {
        let parsed = parse_structured_query(query, Utc::now());
        let terms = search_terms(&parsed.text);
        if terms.is_empty() {
            return Some((self.start_index..self.start_index + self.documents.len()).collect());
        }
        let mut matches = Vec::new();
        for (offset, document) in self.documents.iter().enumerate() {
            if offset % 256 == 0 && cancelled() {
                return None;
            }
            if document_matches_terms(document, &terms) {
                matches.push(self.start_index + offset);
            }
        }
        Some(matches)
    }
}

fn search_document(event: &LogEvent) -> Arc<str> {
    let mut text = String::with_capacity(event.message.len() + 192);
    for value in [
        event.message.as_str(),
        event.event.as_str(),
        event.subsystem.as_str(),
        event.source.as_str(),
        event.correlation_id(),
    ] {
        let _ = write!(text, " {value}");
    }
    for value in [
        event.target.as_deref(),
        event.provider.as_deref(),
        event.error_kind.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        let _ = write!(text, " {value}");
    }
    if let Some(status) = &event.status {
        let _ = write!(text, " {status}");
    }
    for (key, value) in &event.fields {
        let _ = write!(text, " {key} {value}");
    }
    Arc::from(text.to_ascii_lowercase())
}

fn document_matches_terms(document: &str, terms: &[String]) -> bool {
    terms.iter().all(|term| fuzzy_text_contains(document, term))
}

fn search_terms(query: &str) -> Vec<String> {
    query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn fuzzy_text_contains(text: &str, term: &str) -> bool {
    if text.contains(term) {
        return true;
    }
    if term.len() < 4 || term.len() > 64 {
        return false;
    }

    let allowed_distance = usize::from(term.len() >= 8) + 1;
    text.split(|character: char| !character.is_alphanumeric())
        .filter(|word| !word.is_empty() && word.len() <= 64)
        .any(|word| edit_distance_within(word.as_bytes(), term.as_bytes(), allowed_distance))
}

fn edit_distance_within(left: &[u8], right: &[u8], maximum: usize) -> bool {
    if left.len().abs_diff(right.len()) > maximum {
        return false;
    }

    let mut previous = [0; 65];
    let mut current = [0; 65];
    for (index, value) in previous.iter_mut().enumerate().take(right.len() + 1) {
        *value = index;
    }
    for (left_index, left_byte) in left.iter().enumerate() {
        current[0] = left_index + 1;
        let mut row_minimum = current[0];
        for (right_index, right_byte) in right.iter().enumerate() {
            current[right_index + 1] = (previous[right_index + 1] + 1)
                .min(current[right_index] + 1)
                .min(previous[right_index] + usize::from(left_byte != right_byte));
            row_minimum = row_minimum.min(current[right_index + 1]);
        }
        if row_minimum > maximum {
            return false;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[right.len()] <= maximum
}

pub fn status_text(value: Option<&Value>) -> String {
    value
        .and_then(scalar_text)
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(test)]
mod tests {
    use dee_bugee_schema::{Level, LogEvent};

    use super::*;

    fn event(level: Level, source: &str, subsystem: &str, message: &str) -> LogEvent {
        LogEvent::new(level, source, subsystem, "test.event", message, "app-1")
    }

    #[test]
    fn left_include_and_right_exclude_have_tri_state_semantics() {
        let mut store = EventStore::new(100);
        store.extend([
            event(Level::Info, "app", "sync", "one"),
            event(Level::Warn, "backend", "sync", "two"),
            event(Level::Error, "renderer", "player", "three"),
        ]);

        let mut filter = FilterState::default();
        filter
            .facets
            .entry("source".into())
            .or_default()
            .include_only("backend");
        assert_eq!(store.query(&filter), vec![1]);

        let source = filter.facets.get_mut("source").unwrap();
        source.included.clear();
        source.toggle_exclude("renderer");
        assert_eq!(store.query(&filter), vec![0, 1]);
    }

    #[test]
    fn tag_facet_groups_the_same_feature_across_sources() {
        let mut store = EventStore::new(100);
        store.extend([
            event(
                Level::Debug,
                "backend",
                "segment_detection.local",
                "[Segment Detection][Local] analysis started",
            ),
            event(
                Level::Debug,
                "renderer",
                "segment_detection.remote",
                "[Segment Detection][Remote] lookup started",
            ),
            event(
                Level::Info,
                "renderer",
                "whisperlive",
                "[WhisperLive] session started",
            ),
        ]);

        assert_eq!(
            store.facet_counts("tag", &FilterState::default()),
            vec![("Segment Detection".into(), 2), ("WhisperLive".into(), 1)]
        );
        let mut filter = FilterState::default();
        filter
            .facets
            .entry("tag".into())
            .or_default()
            .include_only("Segment Detection");
        assert_eq!(store.query(&filter), vec![0, 1]);
    }

    #[test]
    fn facet_counts_ignore_the_facets_own_selection() {
        let mut store = EventStore::new(100);
        store.extend([
            event(Level::Info, "backend", "sync", "one"),
            event(Level::Warn, "backend", "player", "two"),
            event(Level::Error, "renderer", "player", "three"),
        ]);
        let mut filter = FilterState::default();
        filter
            .facets
            .entry("source".into())
            .or_default()
            .include_only("backend");
        filter
            .facets
            .entry("subsystem".into())
            .or_default()
            .include_only("player");

        assert_eq!(
            store.facet_counts("source", &filter),
            vec![("backend".into(), 1), ("renderer".into(), 1)]
        );
    }

    #[test]
    fn bounded_store_discards_oldest_batch_and_rebuilds_indexes() {
        let mut store = EventStore::new(3);
        store.extend([
            event(Level::Info, "one", "test", "one"),
            event(Level::Info, "two", "test", "two"),
            event(Level::Info, "three", "test", "three"),
            event(Level::Info, "four", "test", "four"),
        ]);

        assert_eq!(store.len(), 3);
        assert_eq!(store.discarded_events(), 1);
        assert_eq!(store.get(0).unwrap().source, "two");
        assert_eq!(
            store.facet_counts("source", &FilterState::default()).len(),
            3
        );
    }

    #[test]
    fn changing_limit_prunes_immediately_and_keeps_the_newest_events() {
        let mut store = EventStore::new(5);
        store.extend([
            event(Level::Info, "one", "test", "one"),
            event(Level::Info, "two", "test", "two"),
            event(Level::Info, "three", "test", "three"),
            event(Level::Info, "four", "test", "four"),
            event(Level::Info, "five", "test", "five"),
        ]);

        store.set_max_events(2);

        assert_eq!(store.max_events(), 2);
        assert_eq!(store.len(), 2);
        assert_eq!(store.discarded_events(), 3);
        assert_eq!(store.get(0).unwrap().source, "four");
        assert_eq!(store.get(1).unwrap().source, "five");
        assert_eq!(
            store.facet_counts("source", &FilterState::default()),
            vec![("five".into(), 1), ("four".into(), 1)]
        );
    }

    #[test]
    fn extending_past_limit_keeps_exactly_the_latest_window() {
        let mut store = EventStore::new(3);
        store.extend([
            event(Level::Info, "one", "test", "one"),
            event(Level::Info, "two", "test", "two"),
            event(Level::Info, "three", "test", "three"),
            event(Level::Info, "four", "test", "four"),
            event(Level::Info, "five", "test", "five"),
        ]);

        assert_eq!(store.len(), 3);
        assert_eq!(store.discarded_events(), 2);
        assert_eq!(store.get(0).unwrap().source, "three");
        assert_eq!(store.get(2).unwrap().source, "five");
    }

    #[test]
    fn event_ids_remain_unique_after_pruning_shifts_indexes() {
        let mut store = EventStore::new(2);
        let duplicate = || event(Level::Info, "same", "same", "same");
        store.extend([duplicate(), duplicate()]);
        let retained_id = store.event_id(1).unwrap();
        store.push(duplicate());

        assert_eq!(store.len(), 2);
        assert_eq!(store.event_id(0), Some(retained_id));
        assert!(store.event_id(1).unwrap() > retained_id);
    }

    #[test]
    fn paused_pruning_keeps_older_rows_stable_until_resumed() {
        let mut store = EventStore::new(3);
        store.extend([
            event(Level::Info, "one", "test", "one"),
            event(Level::Info, "two", "test", "two"),
            event(Level::Info, "three", "test", "three"),
        ]);

        store.set_pruning_paused(true);
        store.extend([
            event(Level::Info, "four", "test", "four"),
            event(Level::Info, "five", "test", "five"),
        ]);

        assert_eq!(store.len(), 5);
        assert_eq!(store.discarded_events(), 0);
        assert_eq!(store.get(0).unwrap().source, "one");
        assert_eq!(
            store.facet_counts("source", &FilterState::default()).len(),
            5
        );

        store.set_pruning_paused(false);

        assert_eq!(store.len(), 3);
        assert_eq!(store.discarded_events(), 2);
        assert_eq!(store.get(0).unwrap().source, "three");
        assert_eq!(store.get(2).unwrap().source, "five");
    }

    #[test]
    fn indexed_facets_select_from_large_event_sets() {
        let mut store = EventStore::new(60_000);
        for index in 0..50_000 {
            store.push(event(
                Level::Info,
                if index % 2 == 0 {
                    "backend"
                } else {
                    "renderer"
                },
                if index % 5 == 0 { "player" } else { "sync" },
                "message",
            ));
        }
        let mut filter = FilterState::default();
        filter
            .facets
            .entry("source".into())
            .or_default()
            .include_only("backend");
        filter
            .facets
            .entry("subsystem".into())
            .or_default()
            .toggle_exclude("player");

        assert_eq!(store.query(&filter).len(), 20_000);
    }

    #[test]
    fn fuzzy_search_matches_separated_terms_and_small_typos() {
        let mut store = EventStore::new(100);
        store.push(event(
            Level::Debug,
            "backend",
            "intro_detection",
            "[Segment Detection][Local] analysis started",
        ));

        let mut filter = FilterState {
            text: "Segment Local".to_string(),
            ..FilterState::default()
        };
        assert_eq!(store.query(&filter), vec![0]);

        filter.text = "segmant locl".to_string();
        assert_eq!(store.query(&filter), vec![0]);

        filter.text = "segment remote".to_string();
        assert!(store.query(&filter).is_empty());
    }

    #[test]
    fn structured_queries_filter_exact_numeric_timestamp_and_field_values() {
        let mut matching = event(Level::Error, "backend", "sync", "sync failed");
        matching.provider = Some("remote".to_string());
        matching.duration_ms = Some(1_250.0);
        matching.status = Some(Value::String("failed".to_string()));
        matching.fields.insert("retry_count".into(), 3.into());
        matching.timestamp = "2026-08-22T10:00:00Z".parse().unwrap();

        let mut fast = event(Level::Info, "backend", "sync", "sync completed");
        fast.provider = Some("remote".to_string());
        fast.duration_ms = Some(80.0);
        fast.status = Some(Value::String("completed".to_string()));
        fast.fields.insert("retry_count".into(), 0.into());
        fast.timestamp = "2026-08-22T10:01:00Z".parse().unwrap();

        let mut missing_status = event(Level::Warn, "backend", "sync", "sync pending");
        missing_status.duration_ms = Some(2_000.0);
        missing_status.fields.insert("retry_count".into(), 4.into());
        missing_status.timestamp = "2026-08-22T10:02:00Z".parse().unwrap();

        let mut store = EventStore::new(100);
        store.extend([matching, fast, missing_status]);
        let filter = FilterState {
            text: "sync provider=remote duration_ms>=1000 fields.retry_count >= 2 status!=completed timestamp>=2026-08-22T09:59:00Z".to_string(),
            ..FilterState::default()
        };

        assert_eq!(store.query(&filter), vec![0]);
        assert_eq!(
            store.facet_counts("source", &filter),
            vec![("backend".to_string(), 1)]
        );
    }

    #[test]
    fn invalid_structured_queries_fail_closed() {
        let mut store = EventStore::new(10);
        store.push(event(Level::Info, "app", "sync", "sync completed"));
        let filter = FilterState {
            text: "duration_ms>slow".to_string(),
            ..FilterState::default()
        };

        assert!(store.query(&filter).is_empty());
    }

    #[test]
    fn one_search_result_is_reused_for_rows_and_facets() {
        let mut store = EventStore::new(100);
        store.extend([
            event(Level::Info, "backend", "sync", "cache connected"),
            event(Level::Warn, "renderer", "sync", "cache delayed"),
            event(Level::Error, "backend", "player", "decoder failed"),
        ]);
        let filter = FilterState {
            text: "cache".to_string(),
            ..FilterState::default()
        };
        let matches = store.search_snapshot(0).search("cache", || false).unwrap();
        let facets = vec!["source".to_string(), "subsystem".to_string()];
        let results = store.query_with_facets(&filter, &facets, Some(&matches));

        assert_eq!(results.rows, vec![0, 1]);
        assert_eq!(
            results.facet_counts["source"],
            vec![("backend".into(), 1), ("renderer".into(), 1)]
        );
        assert_eq!(results.facet_counts["subsystem"], vec![("sync".into(), 2)]);
    }

    #[test]
    fn snapshots_support_incremental_search_and_cancellation() {
        let mut store = EventStore::new(100);
        store.extend([
            event(Level::Info, "backend", "sync", "first match"),
            event(Level::Info, "backend", "sync", "unrelated"),
        ]);
        let initial = store.search_snapshot(0).search("match", || false).unwrap();
        assert_eq!(initial, vec![0]);

        store.push(event(Level::Info, "renderer", "sync", "second match"));
        let incremental = store.search_snapshot(2).search("match", || false).unwrap();
        assert_eq!(incremental, vec![2]);
        assert!(store.search_snapshot(0).search("match", || true).is_none());
    }

    #[test]
    fn structured_fields_are_normalized_once_and_remain_searchable() {
        let mut event = event(Level::Info, "backend", "sync", "request finished");
        event
            .fields
            .insert("CacheRegion".into(), Value::String("Asia-East".into()));
        let mut store = EventStore::new(100);
        store.push(event);

        let filter = FilterState {
            text: "cacheregion asia-east".to_string(),
            ..FilterState::default()
        };
        assert_eq!(store.query(&filter), vec![0]);
    }
}
