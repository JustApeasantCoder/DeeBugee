use std::{fmt, str::FromStr};

use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use serde_json::{Map, Value};
use uuid::Uuid;

pub const SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
    Fatal,
}

impl Level {
    pub const ALL: [Self; 6] = [
        Self::Trace,
        Self::Debug,
        Self::Info,
        Self::Warn,
        Self::Error,
        Self::Fatal,
    ];

    pub const fn severity(self) -> u8 {
        match self {
            Self::Trace => 0,
            Self::Debug => 1,
            Self::Info => 2,
            Self::Warn => 3,
            Self::Error => 4,
            Self::Fatal => 5,
        }
    }
}

impl fmt::Display for Level {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::Fatal => "fatal",
        };
        formatter.write_str(value)
    }
}

impl FromStr for Level {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "trace" => Ok(Self::Trace),
            "debug" => Ok(Self::Debug),
            "info" | "information" => Ok(Self::Info),
            "warn" | "warning" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            "fatal" | "critical" => Ok(Self::Fatal),
            _ => Err(format!("unsupported log level: {value}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEvent {
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub schema_version: u16,
    #[serde(with = "timestamp_serde")]
    pub timestamp: DateTime<Utc>,
    pub level: Level,
    #[serde(deserialize_with = "deserialize_non_empty_string")]
    pub source: String,
    #[serde(deserialize_with = "deserialize_non_empty_string")]
    pub subsystem: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(deserialize_with = "deserialize_non_empty_string")]
    pub event: String,
    pub message: String,
    #[serde(deserialize_with = "deserialize_stringish")]
    pub app_session_id: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_stringish",
        skip_serializing_if = "Option::is_none"
    )]
    pub playback_session_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_stringish",
        skip_serializing_if = "Option::is_none"
    )]
    pub request_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_stringish",
        skip_serializing_if = "Option::is_none"
    )]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_negative_f64",
        skip_serializing_if = "Option::is_none"
    )]
    pub duration_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<Value>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub fields: Map<String, Value>,
}

impl LogEvent {
    pub fn new(
        level: Level,
        source: impl Into<String>,
        subsystem: impl Into<String>,
        event: impl Into<String>,
        message: impl Into<String>,
        app_session_id: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            timestamp: Utc
                .timestamp_millis_opt(Utc::now().timestamp_millis())
                .single()
                .expect("the current timestamp must be representable"),
            level,
            source: source.into(),
            subsystem: subsystem.into(),
            target: None,
            event: event.into(),
            message: message.into(),
            app_session_id: app_session_id.into(),
            playback_session_id: None,
            request_id: None,
            session_id: None,
            provider: None,
            duration_ms: None,
            error_kind: None,
            status: None,
            fields: Map::new(),
        }
    }

    pub fn new_app_session_id() -> String {
        Uuid::new_v4().to_string()
    }

    pub fn correlation_id(&self) -> &str {
        self.playback_session_id
            .as_deref()
            .or(self.request_id.as_deref())
            .or(self.session_id.as_deref())
            .unwrap_or(&self.app_session_id)
    }

    pub fn timestamp_text(&self) -> String {
        self.timestamp.to_rfc3339_opts(SecondsFormat::Millis, true)
    }

    pub fn tag(&self) -> String {
        const CONTEXT_SEGMENTS: [&str; 7] = [
            "backend", "frontend", "local", "remote", "renderer", "settings", "sidecar",
        ];

        let message_segments: Vec<&str> = self
            .message
            .trim_start()
            .split('[')
            .skip(1)
            .map_while(|segment| segment.split_once(']').map(|(value, _)| value.trim()))
            .filter(|value| !value.is_empty())
            .collect();
        let selected = message_segments
            .iter()
            .copied()
            .find(|value| {
                !CONTEXT_SEGMENTS
                    .iter()
                    .any(|context| value.eq_ignore_ascii_case(context))
            })
            .or_else(|| message_segments.first().copied());

        selected.map(str::to_string).unwrap_or_else(|| {
            self.subsystem
                .split('.')
                .find(|segment| {
                    !segment.is_empty()
                        && !CONTEXT_SEGMENTS
                            .iter()
                            .any(|context| segment.eq_ignore_ascii_case(context))
                })
                .or_else(|| {
                    self.subsystem
                        .split('.')
                        .find(|segment| !segment.is_empty())
                })
                .unwrap_or("Other")
                .split('_')
                .filter(|part| !part.is_empty())
                .map(|part| {
                    let mut chars = part.chars();
                    chars
                        .next()
                        .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
    }

    pub fn scalar_field_text(&self, key: &str) -> Option<String> {
        let value = self.fields.get(key)?;
        scalar_text(value)
    }
}

pub fn scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::Null | Value::Array(_) | Value::Object(_) => None,
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::String(value) => Some(value.clone()),
    }
}

fn deserialize_schema_version<'de, D>(deserializer: D) -> Result<u16, D::Error>
where
    D: Deserializer<'de>,
{
    let version = u16::deserialize(deserializer)?;
    if version == SCHEMA_VERSION {
        Ok(version)
    } else {
        Err(D::Error::custom(format!(
            "unsupported schema version {version}; expected {SCHEMA_VERSION}"
        )))
    }
}

fn deserialize_stringish<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    stringish(Value::deserialize(deserializer)?)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| D::Error::custom("expected a non-empty string or number identifier"))
}

fn deserialize_optional_stringish<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    value
        .map(|value| {
            stringish(value)
                .ok_or_else(|| D::Error::custom("expected a string, number, or null identifier"))
        })
        .transpose()
}

fn stringish(value: Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value),
        Value::Number(value) => Some(value.to_string()),
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
    }
}

fn deserialize_non_empty_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.is_empty() {
        Err(D::Error::custom("expected a non-empty string"))
    } else {
        Ok(value)
    }
}

fn deserialize_optional_non_negative_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<f64>::deserialize(deserializer)?;
    if value.is_some_and(|value| value < 0.0 || !value.is_finite()) {
        Err(D::Error::custom("expected a non-negative finite number"))
    } else {
        Ok(value)
    }
}

mod timestamp_serde {
    use super::*;

    pub fn serialize<S>(value: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(value.timestamp_millis())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let milliseconds = match value {
            Value::Number(number) => number
                .as_i64()
                .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok())),
            Value::String(text) => {
                return DateTime::parse_from_rfc3339(&text)
                    .map(|timestamp| timestamp.with_timezone(&Utc))
                    .map_err(D::Error::custom);
            }
            _ => None,
        }
        .ok_or_else(|| D::Error::custom("expected Unix milliseconds or an RFC 3339 timestamp"))?;

        Utc.timestamp_millis_opt(milliseconds)
            .single()
            .ok_or_else(|| D::Error::custom("timestamp is outside the supported range"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_preserves_v1_event() {
        let mut event = LogEvent::new(
            Level::Warn,
            "backend",
            "torrent",
            "torrent.slow",
            "Torrent is responding slowly",
            "app-1",
        );
        event.duration_ms = Some(1250.5);
        event.request_id = Some("request-1".to_string());
        event.fields.insert("status_code".into(), 503.into());

        let encoded = serde_json::to_string(&event).unwrap();
        let decoded: LogEvent = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, event);
        assert_eq!(decoded.schema_version, SCHEMA_VERSION);
        assert_eq!(decoded.correlation_id(), "request-1");
    }

    #[test]
    fn correlation_uses_most_specific_available_id() {
        let mut event = LogEvent::new(Level::Info, "app", "test", "test", "test", "app");
        event.session_id = Some("session".into());
        event.request_id = Some("request".into());
        event.playback_session_id = Some("playback".into());
        assert_eq!(event.correlation_id(), "playback");
    }

    #[test]
    fn reads_streamee_epoch_timestamp_and_numeric_session_id() {
        let raw = r#"{
            "schema_version": 1,
            "timestamp": 1786882449672,
            "level": "info",
            "source": "backend",
            "subsystem": "mpv.launch",
            "target": "streamee_lib",
            "event": "mpv.spawned",
            "message": "MPV process spawned",
            "app_session_id": "1786882386729-11152",
            "playback_session_id": 51872
        }"#;

        let event: LogEvent = serde_json::from_str(raw).unwrap();
        assert_eq!(event.timestamp.timestamp_millis(), 1_786_882_449_672);
        assert_eq!(event.playback_session_id.as_deref(), Some("51872"));
        assert_eq!(event.target.as_deref(), Some("streamee_lib"));

        let encoded = serde_json::to_value(event).unwrap();
        assert_eq!(encoded["timestamp"], 1_786_882_449_672_i64);
        assert_eq!(encoded["playback_session_id"], "51872");
    }

    #[test]
    fn also_reads_rfc3339_timestamp_for_adapter_compatibility() {
        let raw = r#"{
            "schema_version": 1,
            "timestamp": "2026-08-16T12:00:00.000Z",
            "level": "info",
            "source": "app",
            "subsystem": "startup",
            "event": "app.started",
            "message": "started",
            "app_session_id": "app-1"
        }"#;
        let event: LogEvent = serde_json::from_str(raw).unwrap();
        assert_eq!(event.timestamp_text(), "2026-08-16T12:00:00.000Z");
    }

    #[test]
    fn rejects_missing_or_unsupported_schema_contract_fields() {
        let missing_version = r#"{
            "timestamp": 1786882449672,
            "level": "info",
            "source": "app",
            "subsystem": "startup",
            "event": "app.started",
            "message": "started",
            "app_session_id": "app-1"
        }"#;
        assert!(serde_json::from_str::<LogEvent>(missing_version).is_err());

        let missing_level = missing_version.replace(
            "\"timestamp\": 1786882449672,",
            "\"schema_version\": 1, \"timestamp\": 1786882449672,",
        );
        let missing_level = missing_level.replace("\"level\": \"info\",", "");
        assert!(serde_json::from_str::<LogEvent>(&missing_level).is_err());

        let unsupported_version = missing_version.replace(
            "\"timestamp\": 1786882449672,",
            "\"schema_version\": 2, \"timestamp\": 1786882449672,",
        );
        assert!(serde_json::from_str::<LogEvent>(&unsupported_version).is_err());

        let valid_v1 = missing_version.replace(
            "\"timestamp\": 1786882449672,",
            "\"schema_version\": 1, \"timestamp\": 1786882449672,",
        );
        let boolean_identifier = valid_v1.replace("\"app-1\"", "true");
        assert!(serde_json::from_str::<LogEvent>(&boolean_identifier).is_err());

        let empty_source = valid_v1.replace("\"source\": \"app\"", "\"source\": \"\"");
        assert!(serde_json::from_str::<LogEvent>(&empty_source).is_err());

        let negative_duration = valid_v1.replace(
            "\"app_session_id\":",
            "\"duration_ms\": -1, \"app_session_id\":",
        );
        assert!(serde_json::from_str::<LogEvent>(&negative_duration).is_err());
    }

    #[test]
    fn derives_feature_tag_across_sources_and_context_prefixes() {
        let mut event = LogEvent::new(
            Level::Debug,
            "backend",
            "segment_detection.local",
            "segment_detection.message",
            "[Segment Detection][Local] Lookup started",
            "app-1",
        );
        assert_eq!(event.tag(), "Segment Detection");

        event.message = "[Settings][WhisperLive] Runtime test started".to_string();
        event.subsystem = "settings.whisperlive".to_string();
        assert_eq!(event.tag(), "WhisperLive");

        event.message = "Cache inventory refreshed".to_string();
        event.subsystem = "audio_normalizer.storage".to_string();
        assert_eq!(event.tag(), "Audio Normalizer");
    }
}
