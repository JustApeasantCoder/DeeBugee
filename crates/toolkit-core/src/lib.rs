use std::collections::{BTreeMap, BTreeSet};

use dee_bugee_schema::{Level, LogEvent, scalar_text};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const DEFAULT_MAX_EVENTS: usize = 250_000;

pub const PRIMARY_FACETS: [&str; 11] = [
    "level",
    "source",
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
    indexes: BTreeMap<String, BTreeMap<String, RoaringBitmap>>,
    max_events: usize,
    discarded_events: u64,
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
            indexes: BTreeMap::new(),
            max_events: max_events.clamp(1, u32::MAX as usize),
            discarded_events: 0,
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

    pub fn get(&self, index: usize) -> Option<&LogEvent> {
        self.events.get(index)
    }

    pub fn push(&mut self, event: LogEvent) {
        if self.events.len() >= self.max_events {
            let prune_count = (self.max_events / 10).max(1);
            self.events.drain(..prune_count);
            self.discarded_events += prune_count as u64;
            self.rebuild_indexes();
        }

        let index = self.events.len() as u32;
        self.index_event(index, &event);
        self.events.push(event);
    }

    pub fn extend(&mut self, events: impl IntoIterator<Item = LogEvent>) {
        for event in events {
            self.push(event);
        }
    }

    pub fn facet_names(&self) -> impl Iterator<Item = &str> {
        self.indexes.keys().map(String::as_str)
    }

    pub fn query(&self, filter: &FilterState) -> Vec<usize> {
        self.query_bitmap(filter, None)
            .iter()
            .map(|index| index as usize)
            .collect()
    }

    pub fn facet_counts(&self, facet: &str, filter: &FilterState) -> Vec<(String, u64)> {
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
        let candidates = has_other_filters.then(|| self.query_bitmap(filter, Some(facet)));

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

    fn query_bitmap(&self, filter: &FilterState, ignored_facet: Option<&str>) -> RoaringBitmap {
        let mut result: RoaringBitmap = (0..self.events.len() as u32).collect();

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

        let terms = search_terms(&filter.text);
        if !terms.is_empty() {
            result = result
                .iter()
                .filter(|index| self.event_matches_terms(*index as usize, &terms))
                .collect();
        }

        result
    }

    fn event_matches_terms(&self, index: usize, terms: &[String]) -> bool {
        let Some(event) = self.events.get(index) else {
            return false;
        };

        let primary_text = [
            event.message.as_str(),
            event.event.as_str(),
            event.subsystem.as_str(),
            event.source.as_str(),
            event.correlation_id(),
        ];
        terms.iter().all(|term| {
            primary_text
                .iter()
                .any(|text| fuzzy_text_contains(text, term))
                || event
                    .target
                    .as_deref()
                    .is_some_and(|text| fuzzy_text_contains(text, term))
                || event
                    .provider
                    .as_deref()
                    .is_some_and(|text| fuzzy_text_contains(text, term))
                || event
                    .error_kind
                    .as_deref()
                    .is_some_and(|text| fuzzy_text_contains(text, term))
                || event
                    .status
                    .as_ref()
                    .is_some_and(|value| fuzzy_text_contains(&value.to_string(), term))
                || event.fields.iter().any(|(key, value)| {
                    fuzzy_text_contains(key, term) || fuzzy_text_contains(&value.to_string(), term)
                })
        })
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

fn search_terms(query: &str) -> Vec<String> {
    query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn fuzzy_text_contains(text: &str, term: &str) -> bool {
    let text = text.to_ascii_lowercase();
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

    let mut previous: Vec<usize> = (0..=right.len()).collect();
    let mut current = vec![0; right.len() + 1];
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
}
