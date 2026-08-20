use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use chrono::Utc;
use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
use dee_bugee_schema::{Level, LogEvent};
use serde_json::{Map, Number, Value};
use tracing::{Event, Subscriber, field::Visit};
use tracing_subscriber::{Layer, layer::Context};

pub const DEFAULT_QUEUE_CAPACITY: usize = 16_384;
pub const DEFAULT_ROTATION_BYTES: u64 = 50 * 1024 * 1024;
pub const DEFAULT_ARCHIVE_COUNT: usize = 4;

#[derive(Debug, Clone)]
pub struct LoggerConfig {
    pub path: PathBuf,
    pub source: String,
    pub app_session_id: String,
    pub queue_capacity: usize,
    pub rotation_bytes: u64,
    pub archive_count: usize,
}

impl LoggerConfig {
    pub fn new(path: impl Into<PathBuf>, source: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            source: source.into(),
            app_session_id: LogEvent::new_app_session_id(),
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            rotation_bytes: DEFAULT_ROTATION_BYTES,
            archive_count: DEFAULT_ARCHIVE_COUNT,
        }
    }
}

#[derive(Clone)]
pub struct ToolkitLayer {
    shared: Arc<SharedLogger>,
}

pub struct LoggerGuard {
    shared: Arc<SharedLogger>,
    worker: Option<JoinHandle<()>>,
}

enum WriterMessage {
    Event(Box<LogEvent>),
    Shutdown,
}

struct SharedLogger {
    sender: Sender<WriterMessage>,
    dropped: AtomicU64,
    source: String,
    app_session_id: String,
}

pub fn non_blocking_layer(config: LoggerConfig) -> std::io::Result<(ToolkitLayer, LoggerGuard)> {
    if let Some(parent) = config.path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let writer = RotatingWriter::open(config.path, config.rotation_bytes, config.archive_count)?;
    let (sender, receiver) = bounded(config.queue_capacity.max(1));
    let shared = Arc::new(SharedLogger {
        sender,
        dropped: AtomicU64::new(0),
        source: config.source,
        app_session_id: config.app_session_id,
    });
    let worker = thread::Builder::new()
        .name("debug-log-writer".to_string())
        .spawn(move || writer_loop(receiver, writer))?;

    Ok((
        ToolkitLayer {
            shared: Arc::clone(&shared),
        },
        LoggerGuard {
            shared,
            worker: Some(worker),
        },
    ))
}

impl ToolkitLayer {
    pub fn write(&self, event: LogEvent) {
        self.shared.submit(event);
    }
}

impl<S> Layer<S> for ToolkitLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let metadata = event.metadata();
        let mut visitor = JsonVisitor::default();
        event.record(&mut visitor);

        let message = visitor
            .fields
            .remove("message")
            .and_then(value_to_string)
            .unwrap_or_else(|| metadata.name().to_string());
        let subsystem = take_string(&mut visitor.fields, "subsystem")
            .unwrap_or_else(|| metadata.target().to_string());
        let event_name = take_string(&mut visitor.fields, "event")
            .unwrap_or_else(|| metadata.name().to_string());
        let mut log_event = LogEvent::new(
            tracing_level(metadata.level()),
            &self.shared.source,
            subsystem,
            event_name,
            message,
            &self.shared.app_session_id,
        );
        log_event.target = Some(metadata.target().to_string());
        log_event.playback_session_id = take_string(&mut visitor.fields, "playback_session_id");
        log_event.request_id = take_string(&mut visitor.fields, "request_id");
        log_event.session_id = take_string(&mut visitor.fields, "session_id");
        log_event.provider = take_string(&mut visitor.fields, "provider");
        log_event.error_kind = take_string(&mut visitor.fields, "error_kind");
        log_event.duration_ms = visitor.fields.remove("duration_ms").and_then(value_to_f64);
        log_event.status = visitor.fields.remove("status");
        log_event.fields = visitor.fields.into_iter().collect();
        self.shared.submit(log_event);
    }
}

impl SharedLogger {
    fn submit(&self, event: LogEvent) {
        self.report_dropped_if_possible();
        match self.sender.try_send(WriterMessage::Event(Box::new(event))) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => {}
        }
    }

    fn report_dropped_if_possible(&self) {
        let count = self.dropped.swap(0, Ordering::AcqRel);
        if count == 0 {
            return;
        }
        let mut event = LogEvent::new(
            Level::Warn,
            &self.source,
            "logging",
            "logger.events_dropped",
            format!("Dropped {count} log events because the writer queue was full"),
            &self.app_session_id,
        );
        event.fields.insert("count".to_string(), count.into());
        if let Err(TrySendError::Full(_)) =
            self.sender.try_send(WriterMessage::Event(Box::new(event)))
        {
            self.dropped.fetch_add(count, Ordering::Relaxed);
        }
    }
}

impl Drop for LoggerGuard {
    fn drop(&mut self) {
        self.shared.report_dropped_if_possible();
        let _ = self.shared.sender.send(WriterMessage::Shutdown);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn writer_loop(receiver: Receiver<WriterMessage>, mut writer: RotatingWriter) {
    let mut pending = Vec::with_capacity(256);
    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(WriterMessage::Event(event)) => pending.push(*event),
            Ok(WriterMessage::Shutdown) => {
                let _ = drain_available(&receiver, &mut pending);
                let _ = writer.write_batch(&pending);
                let _ = writer.flush();
                return;
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
        }
        let shutting_down = drain_available(&receiver, &mut pending);
        if !pending.is_empty() {
            let _ = writer.write_batch(&pending);
            pending.clear();
        }
        if shutting_down {
            let _ = writer.flush();
            return;
        }
    }
}

fn drain_available(receiver: &Receiver<WriterMessage>, pending: &mut Vec<LogEvent>) -> bool {
    while pending.len() < 1_024 {
        match receiver.try_recv() {
            Ok(WriterMessage::Event(event)) => pending.push(*event),
            Ok(WriterMessage::Shutdown) | Err(crossbeam_channel::TryRecvError::Disconnected) => {
                return true;
            }
            Err(crossbeam_channel::TryRecvError::Empty) => return false,
        }
    }
    false
}

struct RotatingWriter {
    path: PathBuf,
    file: BufWriter<File>,
    bytes_written: u64,
    rotation_bytes: u64,
    archive_count: usize,
}

impl RotatingWriter {
    fn open(path: PathBuf, rotation_bytes: u64, archive_count: usize) -> std::io::Result<Self> {
        let bytes_written = std::fs::metadata(&path).map_or(0, |metadata| metadata.len());
        let file = open_append(&path)?;
        Ok(Self {
            path,
            file: BufWriter::with_capacity(256 * 1024, file),
            bytes_written,
            rotation_bytes: rotation_bytes.max(1),
            archive_count,
        })
    }

    fn write_batch(&mut self, events: &[LogEvent]) -> std::io::Result<()> {
        for event in events {
            let mut record = serde_json::to_vec(event)?;
            record.push(b'\n');
            if self.bytes_written > 0
                && self.bytes_written + record.len() as u64 > self.rotation_bytes
            {
                self.rotate()?;
            }
            self.file.write_all(&record)?;
            self.bytes_written += record.len() as u64;
        }
        self.file.flush()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        self.file.flush()?;
        if self.archive_count > 0 {
            let oldest = archive_path(&self.path, self.archive_count);
            if oldest.exists() {
                std::fs::remove_file(oldest)?;
            }
            for generation in (1..self.archive_count).rev() {
                let current = archive_path(&self.path, generation);
                if current.exists() {
                    std::fs::rename(current, archive_path(&self.path, generation + 1))?;
                }
            }
            if self.path.exists() {
                std::fs::rename(&self.path, archive_path(&self.path, 1))?;
            }
        } else if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        self.file = BufWriter::with_capacity(256 * 1024, open_append(&self.path)?);
        self.bytes_written = 0;
        Ok(())
    }
}

fn open_append(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

fn archive_path(path: &Path, generation: usize) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(format!(".{generation}"));
    PathBuf::from(name)
}

#[derive(Default)]
struct JsonVisitor {
    fields: BTreeMap<String, Value>,
}

impl Visit for JsonVisitor {
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields.insert(field.name().to_string(), value.into());
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields.insert(field.name().to_string(), value.into());
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields.insert(field.name().to_string(), value.into());
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        let value = Number::from_f64(value).map_or(Value::Null, Value::Number);
        self.fields.insert(field.name().to_string(), value);
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), Value::String(value.to_string()));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields.insert(
            field.name().to_string(),
            Value::String(format!("{value:?}")),
        );
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.fields
            .insert(field.name().to_string(), Value::String(value.to_string()));
    }
}

fn tracing_level(level: &tracing::Level) -> Level {
    match *level {
        tracing::Level::TRACE => Level::Trace,
        tracing::Level::DEBUG => Level::Debug,
        tracing::Level::INFO => Level::Info,
        tracing::Level::WARN => Level::Warn,
        tracing::Level::ERROR => Level::Error,
    }
}

fn take_string(fields: &mut BTreeMap<String, Value>, key: &str) -> Option<String> {
    fields.remove(key).and_then(value_to_string)
}

fn value_to_string(value: Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
        value => Some(value.to_string()),
    }
}

fn value_to_f64(value: Value) -> Option<f64> {
    match value {
        Value::Number(value) => value.as_f64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

pub fn direct_event(
    level: Level,
    source: impl Into<String>,
    subsystem: impl Into<String>,
    event: impl Into<String>,
    message: impl Into<String>,
    app_session_id: impl Into<String>,
    fields: Map<String, Value>,
) -> LogEvent {
    let mut record = LogEvent::new(level, source, subsystem, event, message, app_session_id);
    record.timestamp = Utc::now();
    record.fields = fields;
    record
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use tracing_subscriber::prelude::*;

    use super::*;

    static NEXT_TEST: AtomicU64 = AtomicU64::new(1);

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "dee-bugee-{name}-{}-{}.jsonl",
            std::process::id(),
            NEXT_TEST.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn tracing_layer_promotes_filterable_fields() {
        let path = test_path("tracing");
        let mut config = LoggerConfig::new(&path, "backend");
        config.app_session_id = "app-1".into();
        let (layer, guard) = non_blocking_layer(config).unwrap();
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(
                subsystem = "torrent",
                event = "torrent.ready",
                request_id = "request-1",
                duration_ms = 42.5,
                peers = 12_u64,
                "Torrent ready"
            );
        });
        drop(guard);

        let line = std::fs::read_to_string(&path).unwrap();
        let event: LogEvent = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(event.source, "backend");
        assert_eq!(event.subsystem, "torrent");
        assert_eq!(event.event, "torrent.ready");
        assert_eq!(event.request_id.as_deref(), Some("request-1"));
        assert_eq!(event.duration_ms, Some(42.5));
        assert_eq!(event.fields["peers"], 12);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn rotation_happens_between_complete_json_records() {
        let path = test_path("rotation");
        let mut writer = RotatingWriter::open(path.clone(), 300, 2).unwrap();
        let events: Vec<_> = (0..8)
            .map(|index| {
                LogEvent::new(
                    Level::Info,
                    "backend",
                    "test",
                    "test.event",
                    format!("record-{index}-{}", "x".repeat(80)),
                    "app-1",
                )
            })
            .collect();
        writer.write_batch(&events).unwrap();
        writer.flush().unwrap();

        for candidate in [path.clone(), archive_path(&path, 1), archive_path(&path, 2)] {
            if candidate.exists() {
                let text = std::fs::read_to_string(&candidate).unwrap();
                for line in text.lines() {
                    serde_json::from_str::<LogEvent>(line).unwrap();
                }
                let _ = std::fs::remove_file(candidate);
            }
        }
    }
}
