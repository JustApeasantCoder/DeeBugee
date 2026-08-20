use std::{
    collections::BTreeMap,
    fs::{File, Metadata, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded};
use dee_bugee_schema::LogEvent;

const POLL_INTERVAL: Duration = Duration::from_millis(100);
const MAX_READ_PER_POLL: u64 = 4 * 1024 * 1024;
const MAX_BATCH_EVENTS: usize = 2_048;

#[derive(Debug)]
pub enum ReaderCommand {
    AddPaths(Vec<PathBuf>),
    Shutdown,
}

#[derive(Debug)]
pub enum ReaderMessage {
    Batch(Vec<LogEvent>),
    InvalidRecord {
        path: PathBuf,
        line: u64,
        error: String,
    },
    SourceOpened(PathBuf),
    SourceError {
        path: PathBuf,
        error: String,
    },
}

pub struct ReaderHandle {
    pub commands: Sender<ReaderCommand>,
    pub messages: Receiver<ReaderMessage>,
}

pub fn spawn_reader(initial_paths: Vec<PathBuf>) -> ReaderHandle {
    let (command_tx, command_rx) = bounded(32);
    let (message_tx, message_rx) = bounded(64);

    thread::Builder::new()
        .name("debug-log-reader".to_string())
        .spawn(move || reader_loop(command_rx, message_tx, initial_paths))
        .expect("failed to start JSONL reader thread");

    ReaderHandle {
        commands: command_tx,
        messages: message_rx,
    }
}

fn reader_loop(
    commands: Receiver<ReaderCommand>,
    messages: Sender<ReaderMessage>,
    initial_paths: Vec<PathBuf>,
) {
    let mut sources = BTreeMap::<PathBuf, FileCursor>::new();
    add_paths(&mut sources, initial_paths);

    loop {
        loop {
            match commands.try_recv() {
                Ok(ReaderCommand::AddPaths(paths)) => add_paths(&mut sources, paths),
                Ok(ReaderCommand::Shutdown) | Err(TryRecvError::Disconnected) => return,
                Err(TryRecvError::Empty) => break,
            }
        }

        for (path, cursor) in &mut sources {
            if let Err(error) = cursor.poll(path, &messages) {
                if cursor.last_error.as_deref() != Some(&error) {
                    let _ = messages.send(ReaderMessage::SourceError {
                        path: path.clone(),
                        error: error.clone(),
                    });
                    cursor.last_error = Some(error);
                }
            } else {
                cursor.last_error = None;
            }
        }

        thread::sleep(POLL_INTERVAL);
    }
}

fn add_paths(sources: &mut BTreeMap<PathBuf, FileCursor>, paths: Vec<PathBuf>) {
    for path in paths {
        if path.is_dir() {
            let Ok(entries) = std::fs::read_dir(&path) else {
                continue;
            };
            for entry in entries.flatten() {
                let child = entry.path();
                if is_jsonl_path(&child) {
                    sources.entry(normalize_path(child)).or_default();
                }
            }
        } else {
            sources.entry(normalize_path(path)).or_default();
        }
    }
}

fn normalize_path(path: PathBuf) -> PathBuf {
    path.canonicalize().unwrap_or(path)
}

fn is_jsonl_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            let name = name.to_ascii_lowercase();
            name.ends_with(".jsonl") || name.contains(".jsonl.")
        })
}

#[derive(Debug, Default)]
struct FileCursor {
    offset: u64,
    line: u64,
    partial: Vec<u8>,
    opened: bool,
    last_error: Option<String>,
    file_identity: Option<FileIdentity>,
}

impl FileCursor {
    fn poll(&mut self, path: &Path, messages: &Sender<ReaderMessage>) -> Result<(), String> {
        let metadata = std::fs::metadata(path).map_err(|error| error.to_string())?;
        if !metadata.is_file() {
            return Err("source is not a regular file".to_string());
        }

        let mut file = open_shared(path).map_err(|error| error.to_string())?;
        let file_identity = file_identity(&file, &metadata);
        let replaced = self
            .file_identity
            .is_some_and(|previous| Some(previous) != file_identity);
        if replaced || metadata.len() < self.offset {
            self.offset = 0;
            self.line = 0;
            self.partial.clear();
        }
        self.file_identity = file_identity;
        if metadata.len() == self.offset {
            self.announce_open(path, messages);
            return Ok(());
        }

        file.seek(SeekFrom::Start(self.offset))
            .map_err(|error| error.to_string())?;
        let available = metadata.len().saturating_sub(self.offset);
        let mut bytes = Vec::with_capacity(available.min(MAX_READ_PER_POLL) as usize);
        file.take(MAX_READ_PER_POLL)
            .read_to_end(&mut bytes)
            .map_err(|error| error.to_string())?;
        self.offset += bytes.len() as u64;
        self.partial.extend_from_slice(&bytes);
        self.announce_open(path, messages);
        self.parse_complete_lines(path, messages);
        Ok(())
    }

    fn announce_open(&mut self, path: &Path, messages: &Sender<ReaderMessage>) {
        if !self.opened {
            self.opened = true;
            let _ = messages.send(ReaderMessage::SourceOpened(path.to_path_buf()));
        }
    }

    fn parse_complete_lines(&mut self, path: &Path, messages: &Sender<ReaderMessage>) {
        let Some(last_newline) = self.partial.iter().rposition(|byte| *byte == b'\n') else {
            return;
        };
        let complete: Vec<u8> = self.partial.drain(..=last_newline).collect();
        let mut batch = Vec::with_capacity(MAX_BATCH_EVENTS.min(complete.len() / 128 + 1));

        for raw_line in complete.split(|byte| *byte == b'\n') {
            if raw_line.is_empty() {
                continue;
            }
            self.line += 1;
            let raw_line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
            match serde_json::from_slice::<LogEvent>(raw_line) {
                Ok(event) => {
                    batch.push(event);
                    if batch.len() >= MAX_BATCH_EVENTS
                        && messages
                            .send(ReaderMessage::Batch(std::mem::take(&mut batch)))
                            .is_err()
                    {
                        return;
                    }
                }
                Err(error) => {
                    let _ = messages.send(ReaderMessage::InvalidRecord {
                        path: path.to_path_buf(),
                        line: self.line,
                        error: error.to_string(),
                    });
                }
            }
        }

        if !batch.is_empty() {
            let _ = messages.send(ReaderMessage::Batch(batch));
        }
    }
}

#[cfg(windows)]
type FileIdentity = (u32, u64);

#[cfg(windows)]
fn file_identity(file: &File, _metadata: &Metadata) -> Option<FileIdentity> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };

    let mut information = unsafe { std::mem::zeroed::<BY_HANDLE_FILE_INFORMATION>() };
    let succeeded = unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut information) };
    (succeeded != 0).then(|| {
        let index =
            (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow);
        (information.dwVolumeSerialNumber, index)
    })
}

#[cfg(unix)]
type FileIdentity = (u64, u64);

#[cfg(unix)]
fn file_identity(_file: &File, metadata: &Metadata) -> Option<FileIdentity> {
    use std::os::unix::fs::MetadataExt;

    Some((metadata.dev(), metadata.ino()))
}

#[cfg(not(any(windows, unix)))]
type FileIdentity = ();

#[cfg(not(any(windows, unix)))]
fn file_identity(_file: &File, _metadata: &Metadata) -> Option<FileIdentity> {
    None
}

#[cfg(windows)]
fn open_shared(path: &Path) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;

    OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .open(path)
}

#[cfg(not(windows))]
fn open_shared(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().read(true).open(path)
}

impl Drop for ReaderHandle {
    fn drop(&mut self) {
        let _ = self.commands.try_send(ReaderCommand::Shutdown);
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use dee_bugee_schema::{Level, LogEvent};

    use super::*;

    #[test]
    fn cursor_waits_for_complete_jsonl_records_and_then_emits_them() {
        let path =
            std::env::temp_dir().join(format!("dee-bugee-follower-{}.jsonl", std::process::id()));
        let event = LogEvent::new(Level::Info, "backend", "test", "test", "hello", "app-1");
        let encoded = serde_json::to_string(&event).unwrap();
        let split = encoded.len() / 2;
        let (sender, receiver) = bounded(8);
        let mut cursor = FileCursor::default();

        {
            let mut file = File::create(&path).unwrap();
            file.write_all(&encoded.as_bytes()[..split]).unwrap();
        }
        cursor.poll(&path, &sender).unwrap();
        assert!(
            receiver
                .try_iter()
                .all(|message| !matches!(message, ReaderMessage::Batch(_)))
        );

        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            writeln!(file, "{}", &encoded[split..]).unwrap();
        }
        cursor.poll(&path, &sender).unwrap();
        let events: Vec<_> = receiver
            .try_iter()
            .filter_map(|message| match message {
                ReaderMessage::Batch(events) => Some(events),
                _ => None,
            })
            .flatten()
            .collect();

        assert_eq!(events, vec![event]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cursor_restarts_when_a_rotated_file_is_replaced() {
        let path = std::env::temp_dir().join(format!(
            "dee-bugee-follower-rotation-{}.jsonl",
            std::process::id()
        ));
        let archive = path.with_extension("jsonl.1");
        let first = LogEvent::new(Level::Info, "backend", "test", "first", "first", "app-1");
        let second = LogEvent::new(
            Level::Info,
            "backend",
            "test",
            "second",
            "a replacement record long enough to exceed the previous cursor offset",
            "app-1",
        );
        let (sender, receiver) = bounded(8);
        let mut cursor = FileCursor::default();

        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&first).unwrap()),
        )
        .unwrap();
        cursor.poll(&path, &sender).unwrap();
        let _ = receiver.try_iter().collect::<Vec<_>>();

        std::fs::rename(&path, &archive).unwrap();
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&second).unwrap()),
        )
        .unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() >= cursor.offset);
        cursor.poll(&path, &sender).unwrap();

        let events: Vec<_> = receiver
            .try_iter()
            .filter_map(|message| match message {
                ReaderMessage::Batch(events) => Some(events),
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(events, vec![second]);

        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(archive);
    }
}
