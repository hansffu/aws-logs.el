use chrono::{DateTime, NaiveDateTime};
use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct ViewerConfig {
    #[serde(rename = "timestamp-path")]
    timestamp_path: Option<String>,
    #[serde(rename = "level-path")]
    level_path: Option<String>,
    #[serde(rename = "message-path")]
    message_path: Option<String>,
    #[serde(rename = "extra-paths", default)]
    extra_paths: Vec<String>,
    #[serde(rename = "json-paths", default)]
    json_paths: Vec<String>,
}

#[derive(Clone, Debug, Default)]
struct RuntimeConfig {
    viewer: ViewerConfig,
    source_configs: HashMap<String, ViewerConfig>,
    max_entries: Option<usize>,
    chunk_size: usize,
    rebuild_chunk_size: usize,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "cmd", rename_all = "kebab-case")]
enum Command {
    Start {
        #[serde(rename = "socket-path")]
        socket_path: String,
        #[serde(rename = "max-entries")]
        max_entries: Option<usize>,
        #[serde(rename = "chunk-size")]
        chunk_size: Option<usize>,
        #[serde(rename = "rebuild-chunk-size")]
        rebuild_chunk_size: Option<usize>,
        #[serde(rename = "auto-delete-worker-files", default = "default_true")]
        auto_delete_worker_files: bool,
        config: ViewerConfig,
        #[serde(rename = "source-configs", default)]
        source_configs: HashMap<String, ViewerConfig>,
    },
    #[serde(rename = "configure-source")]
    ConfigureSource {
        source: String,
        config: ViewerConfig,
        #[serde(rename = "request-id")]
        request_id: Option<i64>,
    },
    Reset {
        #[serde(rename = "request-id")]
        request_id: Option<i64>,
    },
    Narrow {
        #[serde(default)]
        needle: Option<String>,
        #[serde(default)]
        needles: Vec<String>,
        #[serde(default)]
        operator: Option<String>,
        #[serde(default)]
        level: Option<String>,
        #[serde(rename = "request-id")]
        request_id: Option<i64>,
    },
    Rerender {
        #[serde(default)]
        needle: Option<String>,
        #[serde(default)]
        needles: Vec<String>,
        #[serde(default)]
        operator: Option<String>,
        #[serde(default)]
        level: Option<String>,
        #[serde(rename = "request-id")]
        request_id: Option<i64>,
    },
    #[serde(rename = "load-more")]
    LoadMore {
        limit: usize,
        direction: String,
        timestamp: Value,
        #[serde(rename = "entry-id")]
        entry_id: Option<i64>,
        prepend: Option<bool>,
        #[serde(rename = "request-id")]
        request_id: Option<i64>,
    },
    #[serde(rename = "entry-details")]
    EntryDetails {
        #[serde(rename = "entry-id")]
        entry_id: i64,
        #[serde(rename = "request-id")]
        request_id: Option<i64>,
    },
    Pull {
        #[serde(rename = "max-messages")]
        max_messages: Option<usize>,
    },
    Stop,
}

#[derive(Clone)]
struct Output {
    tx: Sender<String>,
}

impl Output {
    fn send(&self, value: Value) {
        let _ = self.tx.send(value_to_lisp(&value));
    }

    fn error(&self, message: impl Into<String>) {
        self.send(json!({"cmd": "error", "message": message.into()}));
    }

    fn complete(&self, request_id: Option<i64>) {
        if let Some(request_id) = request_id {
            self.send(json!({"cmd": "request-complete", "request-id": request_id}));
        }
    }
}

#[derive(Debug, Serialize)]
struct Entry {
    id: i64,
    #[serde(rename = "sort-key")]
    sort_key: f64,
    source: Option<String>,
    timestamp: String,
    level: String,
    message: String,
    #[serde(rename = "extra-fields")]
    extra_fields: Vec<String>,
}

#[derive(Debug, Serialize)]
struct FieldRow {
    k: String,
    v: String,
    #[serde(skip_serializing_if = "is_false")]
    b: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RenderMode {
    All,
    Narrow,
}

#[derive(Clone, Debug)]
struct NarrowFilter {
    terms: Vec<String>,
    operator: NarrowOperator,
    level: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NarrowOperator {
    And,
    Or,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PublishMode {
    Live,
    OnDemand,
}

enum ProcessMessage {
    Command(Command),
    IngestLine(String),
    Flush(Option<i64>),
    Shutdown,
}

struct Store {
    db: Connection,
    storage_path: PathBuf,
    auto_delete_worker_files: bool,
    config: RuntimeConfig,
    render_mode: RenderMode,
    render_narrow: Option<NarrowFilter>,
    publish_mode: PublishMode,
    pending_entries: VecDeque<Entry>,
    total_count: i64,
    level_counts: HashMap<String, i64>,
    output: Output,
}

impl Store {
    fn new(
        config: RuntimeConfig,
        output: Output,
        storage_path: PathBuf,
        auto_delete_worker_files: bool,
    ) -> rusqlite::Result<Self> {
        cleanup_storage_files(&storage_path);
        let db = Connection::open(&storage_path)?;
        setup_db(&db)?;
        Ok(Self {
            db,
            storage_path,
            auto_delete_worker_files,
            config,
            render_mode: RenderMode::All,
            render_narrow: None,
            publish_mode: PublishMode::Live,
            pending_entries: VecDeque::new(),
            total_count: 0,
            level_counts: HashMap::new(),
            output,
        })
    }

    fn reset(&mut self) {
        self.publish_mode = PublishMode::Live;
        self.render_mode = RenderMode::All;
        self.render_narrow = None;
        self.pending_entries.clear();
        self.total_count = 0;
        self.level_counts.clear();
        if let Err(err) = reset_log_entries(&self.db) {
            self.output.error(err.to_string());
        }
        self.output.send(json!({"cmd": "clear"}));
        self.output.send(self.status_payload(0));
    }

    fn ingest_lines(&mut self, lines: &[String]) -> rusqlite::Result<()> {
        let active_narrow = if self.render_mode == RenderMode::Narrow {
            self.render_narrow.clone()
        } else {
            None
        };
        let viewer_config = self.config.viewer.clone();
        let source_configs = self.config.source_configs.clone();
        let publish_live = self.publish_mode == PublishMode::Live;
        let tx = self.db.transaction()?;
        let mut entries = Vec::new();
        let mut inserted_count = 0;
        for line in lines {
            let (entry, level) = insert_log_entry(
                &tx,
                line,
                &viewer_config,
                &source_configs,
                active_narrow.as_ref(),
            )?;
            *self
                .level_counts
                .entry(normalize_count_level(&level))
                .or_insert(0) += 1;
            if let Some(entry) = entry {
                entries.push(entry);
            }
            inserted_count += 1;
        }
        tx.commit()?;
        self.total_count += inserted_count;
        if publish_live {
            self.queue_pending_entries(entries);
        }
        Ok(())
    }

    fn narrow(&mut self, filter: NarrowFilter) {
        self.publish_mode = PublishMode::Live;
        self.render_mode = RenderMode::Narrow;
        self.render_narrow = Some(filter);
        self.pending_entries.clear();
        self.publish_rerender_chunks();
    }

    fn rerender(&mut self, filter: Option<NarrowFilter>) {
        self.publish_mode = PublishMode::Live;
        self.pending_entries.clear();
        if let Some(filter) = filter {
            self.render_mode = RenderMode::Narrow;
            self.render_narrow = Some(filter);
        } else {
            self.render_mode = RenderMode::All;
            self.render_narrow = None;
        }
        self.publish_rerender_chunks();
    }

    fn queue_pending_entries(&mut self, entries: Vec<Entry>) {
        let max_entries = self.config.max_entries.map(|limit| limit.saturating_add(1));
        for entry in entries {
            self.pending_entries.push_back(entry);
            if let Some(max_entries) = max_entries {
                while self.pending_entries.len() > max_entries {
                    self.pending_entries.pop_front();
                }
            }
        }
    }

    fn pull(&mut self, max_messages: Option<usize>) {
        let max_messages = max_messages
            .or(self.config.max_entries)
            .map(|limit| limit.saturating_add(1));
        if let Some(max_messages) = max_messages {
            while self.pending_entries.len() > max_messages {
                self.pending_entries.pop_front();
            }
        }

        let entries: Vec<Entry> = self.pending_entries.drain(..).collect();
        self.output.send(self.status_payload(entries.len()));
        for batch in entries.chunks(self.config.chunk_size.max(1)) {
            self.output
                .send(json!({"cmd": "render-entries", "entries": batch}));
        }
        self.output.send(json!({"cmd": "pull-complete"}));
    }

    fn status_payload(&self, pending_pull_count: usize) -> Value {
        json!({
            "cmd": "status",
            "pending-pull-count": pending_pull_count,
            "total-count": self.total_count,
            "level-counts": level_counts_payload(&self.level_counts)
        })
    }

    fn publish_rerender_chunks(&self) {
        self.output.send(json!({"cmd": "clear"}));
        let narrow = if self.render_mode == RenderMode::Narrow {
            self.render_narrow.as_ref()
        } else {
            None
        };
        let output = self.output.clone();
        match stream_rerender_entry_batches(
            &self.db,
            self.config.max_entries,
            narrow,
            self.config.rebuild_chunk_size.max(1),
            move |batch| output.send(json!({"cmd": "render-entries", "entries": batch})),
        ) {
            Ok(()) => {}
            Err(err) => self.output.error(err.to_string()),
        }
    }

    fn load_more(
        &mut self,
        limit: usize,
        direction: &str,
        timestamp: &Value,
        entry_id: Option<i64>,
        prepend: bool,
        request_id: Option<i64>,
    ) -> rusqlite::Result<()> {
        let ts = normalize_boundary(timestamp);
        let Some(ts) = ts else {
            self.output.error("load-more requires a usable timestamp");
            self.output
                .send(json!({"cmd": "load-more-complete", "request-id": request_id}));
            self.output.complete(request_id);
            return Ok(());
        };
        let active_narrow = if self.render_mode == RenderMode::Narrow {
            self.render_narrow.as_ref()
        } else {
            None
        };
        self.publish_mode = PublishMode::OnDemand;
        let mut entries = match direction {
            "before" => select_entries_before(&self.db, ts, limit, active_narrow, entry_id)?,
            "after" => select_entries_after(&self.db, ts, limit, active_narrow, entry_id)?,
            _ => {
                self.output.error(format!(
                    "load-more direction must be before/after, got {direction}"
                ));
                Vec::new()
            }
        };
        let mut batches: Vec<Vec<Entry>> = Vec::new();
        while !entries.is_empty() {
            let take = self.config.chunk_size.max(1).min(entries.len());
            batches.push(entries.drain(..take).collect());
        }
        if prepend {
            batches.reverse();
        }
        for batch in batches {
            self.output
                .send(json!({"cmd": "render-entries", "entries": batch, "prepend": prepend}));
        }
        self.output
            .send(json!({"cmd": "load-more-complete", "request-id": request_id}));
        self.output.complete(request_id);
        Ok(())
    }

    fn entry_details(&self, entry_id: i64, request_id: Option<i64>) -> rusqlite::Result<()> {
        let row: Option<(String, Option<String>, Option<String>)> = self
            .db
            .query_row(
                "SELECT json, source, source_id FROM log_entry WHERE id = ?",
                params![entry_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let fields = row
            .as_ref()
            .map(|(text, source, source_id)| {
                let config = config_for_source(
                    &self.config.viewer,
                    &self.config.source_configs,
                    source_id.as_deref(),
                    source.as_deref(),
                );
                entry_fields(text, config)
            })
            .unwrap_or_default();
        self.output.send(json!({
            "cmd": "expand-details",
            "entry-id": entry_id,
            "raw": row.as_ref().map(|(text, _, _)| text),
            "fields": fields,
            "request-id": request_id
        }));
        self.output.complete(request_id);
        Ok(())
    }
}

fn setup_db(db: &Connection) -> rusqlite::Result<()> {
    db.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 5000;
         CREATE TABLE log_entry (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           timestamp_epoch REAL,
           timestamp TEXT,
           source TEXT,
           source_id TEXT,
           source_lc TEXT,
           level_path TEXT,
           level_lc TEXT,
           message_path TEXT,
           extra_paths TEXT,
           json TEXT NOT NULL,
           search_text_lc TEXT NOT NULL
         );
         CREATE INDEX log_entry_timestamp_idx ON log_entry(timestamp_epoch, id);
         CREATE INDEX log_entry_display_order_idx
           ON log_entry(
             CASE WHEN timestamp_epoch IS NULL THEN 1 ELSE 0 END,
             timestamp_epoch,
             id
           );
         CREATE INDEX log_entry_tail_order_idx
           ON log_entry(
             CASE WHEN timestamp_epoch IS NULL THEN 0 ELSE 1 END,
             timestamp_epoch DESC,
             id DESC
           );
         CREATE INDEX log_entry_level_timestamp_idx ON log_entry(level_lc, timestamp_epoch, id);
         CREATE INDEX log_entry_source_timestamp_idx ON log_entry(source_lc, timestamp_epoch, id);
         CREATE VIRTUAL TABLE log_entry_fts USING fts5(
           search_text_lc,
           content='log_entry',
           content_rowid='id',
           tokenize='trigram',
           detail='none'
         );",
    )
}

fn output_thread(rx: Receiver<String>) {
    let stdout = io::stdout();
    let mut out = stdout.lock();
    while let Ok(value) = rx.recv() {
        let _ = out.write_all(value.as_bytes());
        let _ = out.write_all(b"\n");
        let _ = out.flush();
    }
}

fn process_thread(mut store: Store, rx: Receiver<ProcessMessage>) {
    let storage_path = store.storage_path.clone();
    let auto_delete_worker_files = store.auto_delete_worker_files;
    let mut pending_message = None;
    let mut disconnected = false;

    loop {
        let message = match pending_message.take() {
            Some(message) => message,
            None => match rx.recv() {
                Ok(message) => message,
                Err(_) => break,
            },
        };

        match message {
            ProcessMessage::Command(command) => {
                if handle_process_command(&mut store, command) {
                    break;
                }
            }
            ProcessMessage::IngestLine(line) => {
                let mut lines = vec![line];
                while lines.len() < 1000 {
                    match rx.try_recv() {
                        Ok(ProcessMessage::IngestLine(line)) => lines.push(line),
                        Ok(message) => {
                            pending_message = Some(message);
                            break;
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            disconnected = true;
                            break;
                        }
                    }
                }
                if let Err(err) = store.ingest_lines(&lines) {
                    store.output.error(err.to_string());
                }
                if disconnected {
                    break;
                }
            }
            ProcessMessage::Flush(request_id) => {
                store.output.complete(request_id);
            }
            ProcessMessage::Shutdown => break,
        }
    }

    drop(store);
    if auto_delete_worker_files {
        cleanup_storage_files(&storage_path);
    }
}

fn handle_process_command(store: &mut Store, command: Command) -> bool {
    match command {
        Command::Start { .. } => {
            store
                .output
                .error("start command is only valid as the first worker command");
        }
        Command::Reset { request_id } => {
            store.reset();
            store.output.complete(request_id);
        }
        Command::ConfigureSource {
            source,
            config,
            request_id,
        } => {
            store.config.source_configs.insert(source, config);
            store.output.complete(request_id);
        }
        Command::Narrow {
            needle,
            needles,
            operator,
            level,
            request_id,
        } => {
            match normalize_filter(
                needle.as_deref(),
                &needles,
                operator.as_deref(),
                level.as_deref(),
            ) {
                Some(filter) => store.narrow(filter),
                None => store
                    .output
                    .error("narrow command requires at least one needle or level"),
            }
            store.output.complete(request_id);
        }
        Command::Rerender {
            needle,
            needles,
            operator,
            level,
            request_id,
        } => {
            store.rerender(normalize_filter(
                needle.as_deref(),
                &needles,
                operator.as_deref(),
                level.as_deref(),
            ));
            store.output.complete(request_id);
        }
        Command::LoadMore {
            limit,
            direction,
            timestamp,
            entry_id,
            prepend,
            request_id,
        } => {
            if let Err(err) = store.load_more(
                limit,
                &direction,
                &timestamp,
                entry_id,
                prepend.unwrap_or(false),
                request_id,
            ) {
                store.output.error(err.to_string());
            }
        }
        Command::EntryDetails {
            entry_id,
            request_id,
        } => {
            if let Err(err) = store.entry_details(entry_id, request_id) {
                store.output.error(err.to_string());
            }
        }
        Command::Pull { max_messages } => store.pull(max_messages),
        Command::Stop => return true,
    }
    false
}

fn storage_path_from_socket_path(socket_path: &str) -> PathBuf {
    let mut path = PathBuf::from(socket_path);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| format!("{name}.sqlite"))
        .unwrap_or_else(|| "json-log-viewer-worker.sqlite".to_string());
    path.set_file_name(file_name);
    path
}

fn cleanup_storage_files(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(sqlite_sidecar_path(path, "-wal"));
    let _ = fs::remove_file(sqlite_sidecar_path(path, "-shm"));
}

fn sqlite_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = path.as_os_str().to_os_string();
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

fn value_to_lisp(value: &Value) -> String {
    match value {
        Value::Null => "nil".to_string(),
        Value::Bool(true) => "t".to_string(),
        Value::Bool(false) => "nil".to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => lisp_string(text),
        Value::Array(items) => {
            let body = items
                .iter()
                .map(value_to_lisp)
                .collect::<Vec<_>>()
                .join(" ");
            format!("({body})")
        }
        Value::Object(map) => object_to_lisp_plist(map),
    }
}

fn object_to_lisp_plist(map: &Map<String, Value>) -> String {
    let mut parts = Vec::with_capacity(map.len() * 2);
    for (key, value) in map {
        parts.push(format!(":{}", key));
        parts.push(value_to_lisp(value));
    }
    format!("({})", parts.join(" "))
}

fn lisp_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn command_thread(process_tx: Sender<ProcessMessage>, output: Output, shutdown_tx: Sender<()>) {
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Command>(&line) {
            Ok(Command::Start { .. }) => {
                output.error("start command is only valid as the first worker command");
            }
            Ok(Command::Stop) => {
                let _ = shutdown_tx.send(());
                let _ = process_tx.send(ProcessMessage::Shutdown);
                break;
            }
            Ok(command) => {
                if process_tx.send(ProcessMessage::Command(command)).is_err() {
                    break;
                }
            }
            Err(err) => output.error(format!("failed to parse command: {err}")),
        }
    }
    let _ = shutdown_tx.send(());
    let _ = process_tx.send(ProcessMessage::Shutdown);
}

fn ingestion_thread(
    socket_path: String,
    process_tx: Sender<ProcessMessage>,
    output: Output,
    shutdown_rx: Receiver<()>,
    auto_delete_worker_files: bool,
) {
    let _ = fs::remove_file(&socket_path);
    let listener = match UnixListener::bind(&socket_path) {
        Ok(listener) => listener,
        Err(err) => {
            output.error(format!("failed to bind ingestion socket: {err}"));
            return;
        }
    };
    let _ = listener.set_nonblocking(true);
    output.send(json!({"cmd": "worker-ready", "socket-path": socket_path}));
    loop {
        if shutdown_rx.try_recv().is_ok() {
            break;
        }
        match listener.accept() {
            Ok((stream, _addr)) => {
                let process_tx = process_tx.clone();
                let output = output.clone();
                thread::spawn(move || handle_ingest_stream(stream, process_tx, output));
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(err) => {
                output.error(format!("ingestion socket accept failed: {err}"));
                break;
            }
        }
    }
    if auto_delete_worker_files {
        let _ = fs::remove_file(&socket_path);
    }
}

fn handle_ingest_stream(stream: UnixStream, process_tx: Sender<ProcessMessage>, output: Output) {
    let mut reader = BufReader::new(stream);
    let mut raw_line = Vec::new();
    loop {
        raw_line.clear();
        let Ok(nread) = reader.read_until(b'\n', &mut raw_line) else {
            break;
        };
        if nread == 0 {
            break;
        }
        if raw_line.ends_with(b"\n") {
            raw_line.pop();
        }
        if raw_line.ends_with(b"\r") {
            raw_line.pop();
        }
        if raw_line.is_empty() {
            continue;
        }
        if raw_line.starts_with(b"L ") {
            let line = String::from_utf8_lossy(&raw_line[2..]).into_owned();
            if process_tx.send(ProcessMessage::IngestLine(line)).is_err() {
                break;
            }
        } else if raw_line.starts_with(b"F ") {
            let request_id_text = String::from_utf8_lossy(&raw_line[2..]);
            match request_id_text.parse::<i64>() {
                Ok(request_id) => {
                    if process_tx
                        .send(ProcessMessage::Flush(Some(request_id)))
                        .is_err()
                    {
                        break;
                    }
                }
                Err(err) => output.error(format!("invalid ingest flush frame: {err}")),
            }
        } else {
            output.error("unknown ingest frame");
        }
    }
}

fn main() {
    let (out_tx, out_rx) = mpsc::channel::<String>();
    let output = Output { tx: out_tx.clone() };
    let output_for_store = output.clone();
    let output_handle = thread::spawn(move || output_thread(out_rx));

    let first_command = match read_start_command() {
        Ok(command) => command,
        Err(err) => {
            output.error(err);
            return;
        }
    };
    let Command::Start {
        socket_path,
        max_entries,
        chunk_size,
        rebuild_chunk_size,
        auto_delete_worker_files,
        config,
        source_configs,
    } = first_command
    else {
        output.error("first worker command must be start");
        return;
    };

    let runtime_config = RuntimeConfig {
        viewer: config.clone(),
        source_configs,
        max_entries,
        chunk_size: chunk_size.unwrap_or(100).max(1),
        rebuild_chunk_size: rebuild_chunk_size.or(chunk_size).unwrap_or(500).max(1),
    };
    let (process_tx, process_rx) = mpsc::channel::<ProcessMessage>();
    let storage_path = storage_path_from_socket_path(&socket_path);
    let store = match Store::new(
        runtime_config,
        output_for_store,
        storage_path,
        auto_delete_worker_files,
    ) {
        Ok(store) => store,
        Err(err) => {
            output.error(format!("failed to initialize store: {err}"));
            return;
        }
    };
    let process_handle = thread::spawn(move || process_thread(store, process_rx));

    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let ingest_process_tx = process_tx.clone();
    let ingest_output = output.clone();
    let socket_path_for_thread = socket_path.clone();
    let ingest_handle = thread::spawn(move || {
        ingestion_thread(
            socket_path_for_thread,
            ingest_process_tx,
            ingest_output,
            shutdown_rx,
            auto_delete_worker_files,
        );
    });
    let command_output = output.clone();
    let command_shutdown_tx = shutdown_tx.clone();
    let command_process_tx = process_tx.clone();
    let command_handle = thread::spawn(move || {
        command_thread(command_process_tx, command_output, command_shutdown_tx)
    });
    let _ = command_handle.join();
    let _ = shutdown_tx.send(());
    let _ = process_tx.send(ProcessMessage::Shutdown);
    let _ = ingest_handle.join();
    let _ = process_handle.join();
    drop(output);
    drop(out_tx);
    let _ = output_handle;
}

fn read_start_command() -> Result<Command, String> {
    let stdin = io::stdin();
    let mut line = String::new();
    stdin
        .lock()
        .read_line(&mut line)
        .map_err(|err| format!("failed to read start command: {err}"))?;
    serde_json::from_str::<Command>(&line)
        .map_err(|err| format!("failed to parse start command: {err}"))
}

fn normalize_needle(needle: Option<&str>) -> Option<String> {
    let trimmed = needle.unwrap_or("").trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_ascii_lowercase())
    }
}

fn normalize_operator(operator: Option<&str>) -> NarrowOperator {
    match operator.map(|value| value.trim().to_ascii_lowercase()) {
        Some(value) if value == "or" => NarrowOperator::Or,
        _ => NarrowOperator::And,
    }
}

fn normalize_filter(
    needle: Option<&str>,
    needles: &[String],
    operator: Option<&str>,
    level: Option<&str>,
) -> Option<NarrowFilter> {
    let mut terms = Vec::new();
    if let Some(term) = normalize_needle(needle) {
        terms.push(term);
    }
    for needle in needles {
        if let Some(term) = normalize_needle(Some(needle)) {
            terms.push(term);
        }
    }
    let level = normalize_needle(level);
    if terms.is_empty() && level.is_none() {
        None
    } else {
        Some(NarrowFilter {
            terms,
            operator: normalize_operator(operator),
            level,
        })
    }
}

fn insert_log_entry(
    db: &Connection,
    line: &str,
    default_config: &ViewerConfig,
    source_configs: &HashMap<String, ViewerConfig>,
    narrow: Option<&NarrowFilter>,
) -> rusqlite::Result<(Option<Entry>, String)> {
    let parsed = serde_json::from_str::<Value>(line).ok();
    let normalized = normalize_storage_json(line, parsed.as_ref());
    let json_text = serde_json::to_string(&normalized).unwrap_or_else(|_| line.to_string());
    let search_text_lc = search_text_for_storage(&normalized, line);
    let source = resolve_path(parsed.as_ref(), Some("source"));
    let source_id = resolve_path(parsed.as_ref(), Some("sourceId"));
    let source_lc = source.as_deref().map(normalize_index_value);
    let config = config_for_source(
        default_config,
        source_configs,
        source_id.as_deref(),
        source.as_deref(),
    );
    let timestamp = resolve_path(parsed.as_ref(), config.timestamp_path.as_deref())
        .unwrap_or_else(|| "-".to_string());
    let timestamp_epoch = parse_time(&timestamp);
    let level = resolve_path(parsed.as_ref(), config.level_path.as_deref())
        .unwrap_or_else(|| "-".to_string());
    let level_lc = normalize_index_value(&level);
    let message = resolve_path(parsed.as_ref(), config.message_path.as_deref())
        .unwrap_or_else(|| line.to_string());
    let extra_fields = config
        .extra_paths
        .iter()
        .filter_map(|path| resolve_path(parsed.as_ref(), Some(path)))
        .collect::<Vec<_>>();
    let extra_csv = extra_fields.join(",");
    db.execute(
        "INSERT INTO log_entry(timestamp_epoch, timestamp, source, source_id, source_lc, level_path, level_lc, message_path, extra_paths, json, search_text_lc)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            timestamp_epoch,
            timestamp,
            source,
            source_id,
            source_lc,
            level,
            level_lc,
            message,
            extra_csv,
            json_text,
            search_text_lc
        ],
    )?;
    let id = db.last_insert_rowid();
    db.execute(
        "INSERT INTO log_entry_fts(rowid, search_text_lc) VALUES (?, ?)",
        params![id, search_text_lc],
    )?;
    let entry = if narrow_matches(&normalized, &level, narrow) {
        Some(Entry {
            id,
            sort_key: timestamp_epoch.unwrap_or(1_000_000_000_000.0 + id as f64),
            source,
            timestamp: timestamp.clone(),
            level: level.clone(),
            message,
            extra_fields,
        })
    } else {
        None
    };
    Ok((entry, level))
}

fn search_text_for_storage(normalized: &Value, fallback: &str) -> String {
    serde_json::to_string(normalized)
        .unwrap_or_else(|_| fallback.to_string())
        .to_ascii_lowercase()
}

fn normalize_index_value(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn config_for_source<'a>(
    default_config: &'a ViewerConfig,
    source_configs: &'a HashMap<String, ViewerConfig>,
    source_id: Option<&str>,
    source: Option<&str>,
) -> &'a ViewerConfig {
    source_id
        .and_then(|source_id| source_configs.get(source_id))
        .or_else(|| source.and_then(|source| source_configs.get(source)))
        .unwrap_or(default_config)
}

fn normalize_storage_json(line: &str, parsed: Option<&Value>) -> Value {
    match parsed {
        Some(Value::Object(_)) => parsed.cloned().unwrap_or(Value::Null),
        Some(Value::Array(_)) => json!({ "value": parsed.cloned().unwrap_or(Value::Null) }),
        Some(value) => json!({ "value": value }),
        None => json!({ "raw": line }),
    }
}

fn narrow_matches(value: &Value, level: &str, narrow: Option<&NarrowFilter>) -> bool {
    match narrow {
        None => true,
        Some(filter) => {
            if let Some(filter_level) = &filter.level {
                if filter_level != &level.trim().to_ascii_lowercase() {
                    return false;
                }
            }
            if filter.terms.is_empty() {
                return true;
            }
            let Ok(text) = serde_json::to_string(value) else {
                return false;
            };
            let text = text.to_ascii_lowercase();
            match filter.operator {
                NarrowOperator::And => filter.terms.iter().all(|term| text.contains(term)),
                NarrowOperator::Or => filter.terms.iter().any(|term| text.contains(term)),
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum TextSearchPlan {
    None,
    Fts { query: String },
    Scan,
}

struct QueryPlan {
    from_sql: &'static str,
    where_sql: Option<String>,
    params: Vec<SqlValue>,
}

fn text_search_plan(filter: Option<&NarrowFilter>) -> TextSearchPlan {
    let Some(filter) = filter else {
        return TextSearchPlan::None;
    };
    if filter.terms.is_empty() {
        return TextSearchPlan::None;
    }
    if filter.terms.iter().all(|term| term.chars().count() >= 3) {
        TextSearchPlan::Fts {
            query: fts_query_for_terms(&filter.terms, filter.operator),
        }
    } else {
        TextSearchPlan::Scan
    }
}

fn fts_query_for_terms(terms: &[String], operator: NarrowOperator) -> String {
    let joiner = match operator {
        NarrowOperator::And => " AND ",
        NarrowOperator::Or => " OR ",
    };
    terms
        .iter()
        .map(|term| {
            let trigrams = term_trigrams(term);
            let trigram_query = trigrams
                .iter()
                .map(|trigram| format!("\"{}\"", escape_fts_phrase(trigram)))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("({trigram_query})")
        })
        .collect::<Vec<_>>()
        .join(joiner)
}

fn term_trigrams(term: &str) -> Vec<String> {
    let chars = term.chars().collect::<Vec<_>>();
    chars
        .windows(3)
        .map(|window| window.iter().collect::<String>())
        .collect()
}

fn escape_fts_phrase(value: &str) -> String {
    value.replace('"', "\"\"")
}

fn query_plan(narrow: Option<&NarrowFilter>) -> QueryPlan {
    let Some(filter) = narrow else {
        return QueryPlan {
            from_sql: "FROM log_entry e ",
            where_sql: None,
            params: Vec::new(),
        };
    };
    let mut conditions = Vec::new();
    let mut params = Vec::new();
    let from_sql = match text_search_plan(Some(filter)) {
        TextSearchPlan::None => "FROM log_entry e ",
        TextSearchPlan::Fts { query } => {
            conditions.push("log_entry_fts MATCH ?".to_string());
            params.push(SqlValue::Text(query));
            "FROM log_entry_fts JOIN log_entry e ON e.id = log_entry_fts.rowid "
        }
        TextSearchPlan::Scan => "FROM log_entry e ",
    };

    if !filter.terms.is_empty() {
        let joiner = match filter.operator {
            NarrowOperator::And => " AND ",
            NarrowOperator::Or => " OR ",
        };
        conditions.push(format!(
            "({})",
            filter
                .terms
                .iter()
                .map(|_| "instr(e.search_text_lc, ?) > 0")
                .collect::<Vec<_>>()
                .join(joiner)
        ));
        params.extend(filter.terms.iter().map(|term| SqlValue::Text(term.clone())));
    }

    if let Some(level) = &filter.level {
        conditions.push("e.level_lc = ?".to_string());
        params.push(SqlValue::Text(level.clone()));
    }

    QueryPlan {
        from_sql,
        where_sql: if conditions.is_empty() {
            None
        } else {
            Some(conditions.join(" AND "))
        },
        params,
    }
}

fn push_where(sql: &mut String, conditions: &[Option<String>]) {
    let conditions = conditions.iter().flatten().cloned().collect::<Vec<_>>();
    if !conditions.is_empty() {
        sql.push_str("WHERE ");
        sql.push_str(&conditions.join(" AND "));
        sql.push(' ');
    }
}

fn stream_rerender_entry_batches<F>(
    db: &Connection,
    max_entries: Option<usize>,
    narrow: Option<&NarrowFilter>,
    batch_size: usize,
    publish_batch: F,
) -> rusqlite::Result<()>
where
    F: FnMut(Vec<Entry>),
{
    let plan = query_plan(narrow);
    if let Some(limit) = max_entries {
        let mut sql = String::from(
            "SELECT id, timestamp_epoch, timestamp, source, level_path, message_path, extra_paths
             FROM (
               SELECT e.id, e.timestamp_epoch, e.timestamp, e.source, e.level_path, e.message_path, e.extra_paths ",
        );
        sql.push_str(plan.from_sql);
        push_where(&mut sql, &[plan.where_sql.clone()]);
        sql.push_str(
            "ORDER BY CASE WHEN timestamp_epoch IS NULL THEN 0 ELSE 1 END,
                     timestamp_epoch DESC, id DESC LIMIT ?
             )
             ORDER BY CASE WHEN timestamp_epoch IS NULL THEN 1 ELSE 0 END, timestamp_epoch, id",
        );
        let mut values = plan.params;
        values.push(SqlValue::Integer(limit as i64));
        stream_entries(
            db.prepare(&sql)?
                .query_map(params_from_iter(values), row_to_entry)?,
            batch_size,
            publish_batch,
        )
    } else {
        let mut sql = String::from(
            "SELECT e.id, e.timestamp_epoch, e.timestamp, e.source, e.level_path, e.message_path, e.extra_paths ",
        );
        sql.push_str(plan.from_sql);
        push_where(&mut sql, &[plan.where_sql.clone()]);
        sql.push_str(
            "ORDER BY CASE WHEN timestamp_epoch IS NULL THEN 1 ELSE 0 END, timestamp_epoch, id",
        );
        stream_entries(
            db.prepare(&sql)?
                .query_map(params_from_iter(plan.params), row_to_entry)?,
            batch_size,
            publish_batch,
        )
    }
}

fn select_entries_before(
    db: &Connection,
    timestamp: f64,
    limit: usize,
    narrow: Option<&NarrowFilter>,
    boundary_id: Option<i64>,
) -> rusqlite::Result<Vec<Entry>> {
    let plan = query_plan(narrow);
    let mut sql = String::from(
        "SELECT e.id, e.timestamp_epoch, e.timestamp, e.source, e.level_path, e.message_path, e.extra_paths ",
    );
    sql.push_str(plan.from_sql);
    let mut values = vec![
        SqlValue::Real(timestamp),
        SqlValue::Real(timestamp),
        SqlValue::Integer(boundary_id.unwrap_or(i64::MAX)),
    ];
    values.extend(plan.params);
    push_where(
        &mut sql,
        &[
            Some("(e.timestamp_epoch < ? OR (e.timestamp_epoch = ? AND e.id < ?))".to_string()),
            plan.where_sql,
        ],
    );
    sql.push_str("ORDER BY timestamp_epoch DESC, id DESC LIMIT ?");
    values.push(SqlValue::Integer(limit as i64));
    let mut entries = rows_to_entries(
        db.prepare(&sql)?
            .query_map(params_from_iter(values), row_to_entry)?,
    )?;
    entries.reverse();
    Ok(entries)
}

fn select_entries_after(
    db: &Connection,
    timestamp: f64,
    limit: usize,
    narrow: Option<&NarrowFilter>,
    boundary_id: Option<i64>,
) -> rusqlite::Result<Vec<Entry>> {
    let plan = query_plan(narrow);
    let mut sql = String::from(
        "SELECT e.id, e.timestamp_epoch, e.timestamp, e.source, e.level_path, e.message_path, e.extra_paths ",
    );
    sql.push_str(plan.from_sql);
    let mut values = vec![
        SqlValue::Real(timestamp),
        SqlValue::Real(timestamp),
        SqlValue::Integer(boundary_id.unwrap_or(0)),
    ];
    values.extend(plan.params);
    push_where(
        &mut sql,
        &[
            Some("(e.timestamp_epoch > ? OR (e.timestamp_epoch = ? AND e.id > ?))".to_string()),
            plan.where_sql,
        ],
    );
    sql.push_str("ORDER BY timestamp_epoch ASC, id ASC LIMIT ?");
    values.push(SqlValue::Integer(limit as i64));
    rows_to_entries(
        db.prepare(&sql)?
            .query_map(params_from_iter(values), row_to_entry)?,
    )
}

fn normalize_count_level(level: &str) -> String {
    let level = level.trim().to_ascii_lowercase();
    if level.is_empty() {
        "-".to_string()
    } else {
        level
    }
}

fn level_counts_payload(level_counts: &HashMap<String, i64>) -> Vec<Value> {
    let mut counts = level_counts
        .iter()
        .map(|(level, count)| (level.clone(), *count))
        .collect::<Vec<_>>();
    counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    counts
        .into_iter()
        .map(|(level, count)| {
            json!({
                "level": level,
                "count": count
            })
        })
        .collect()
}

fn row_to_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<Entry> {
    let id: i64 = row.get(0)?;
    let sort_key: Option<f64> = row.get(1)?;
    let timestamp: Option<String> = row.get(2)?;
    let source: Option<String> = row.get(3)?;
    let level: Option<String> = row.get(4)?;
    let message: Option<String> = row.get(5)?;
    let extras: Option<String> = row.get(6)?;
    Ok(Entry {
        id,
        sort_key: sort_key.unwrap_or(1_000_000_000_000.0 + id as f64),
        source,
        timestamp: timestamp.unwrap_or_else(|| "-".to_string()),
        level: level.unwrap_or_else(|| "-".to_string()),
        message: message.unwrap_or_else(|| "-".to_string()),
        extra_fields: extras
            .unwrap_or_default()
            .split(',')
            .filter(|part| !part.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
    })
}

fn rows_to_entries<T>(rows: T) -> rusqlite::Result<Vec<Entry>>
where
    T: Iterator<Item = rusqlite::Result<Entry>>,
{
    rows.collect()
}

fn stream_entries<T, F>(rows: T, batch_size: usize, mut publish_batch: F) -> rusqlite::Result<()>
where
    T: Iterator<Item = rusqlite::Result<Entry>>,
    F: FnMut(Vec<Entry>),
{
    let mut batch = Vec::with_capacity(batch_size);
    for row in rows {
        batch.push(row?);
        if batch.len() >= batch_size {
            publish_batch(std::mem::take(&mut batch));
        }
    }
    if !batch.is_empty() {
        publish_batch(batch);
    }
    Ok(())
}

fn reset_log_entries(db: &Connection) -> rusqlite::Result<()> {
    db.execute("DELETE FROM log_entry_fts", [])?;
    db.execute("DELETE FROM log_entry", [])?;
    Ok(())
}

fn entry_fields(json_text: &str, config: &ViewerConfig) -> Vec<FieldRow> {
    let parsed = serde_json::from_str::<Value>(json_text).ok();
    json_object_rows(parsed.as_ref(), json_text, &config.json_paths)
}

fn json_object_rows(
    parsed: Option<&Value>,
    raw_line: &str,
    json_paths: &[String],
) -> Vec<FieldRow> {
    let json_path_set = json_paths.iter().cloned().collect::<HashSet<_>>();
    let rows = parsed
        .map(|value| flatten_node(value, &json_path_set, None))
        .unwrap_or_default();
    if rows.is_empty() {
        vec![FieldRow {
            k: "raw".to_string(),
            v: raw_line.to_string(),
            b: false,
        }]
    } else {
        rows
    }
}

fn flatten_node(
    value: &Value,
    json_paths: &HashSet<String>,
    prefix: Option<String>,
) -> Vec<FieldRow> {
    if let Some(prefix_text) = prefix.as_ref() {
        if json_paths.contains(prefix_text) {
            return vec![FieldRow {
                k: prefix_text.clone(),
                v: pretty_json_value(value),
                b: true,
            }];
        }
    }
    match value {
        Value::Object(map) => {
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            if keys.is_empty() {
                return prefix
                    .map(|k| {
                        vec![FieldRow {
                            k,
                            v: String::new(),
                            b: false,
                        }]
                    })
                    .unwrap_or_default();
            }
            keys.into_iter()
                .flat_map(|key| {
                    let child_prefix = join_path(prefix.as_deref(), &key);
                    flatten_node(&map[&key], json_paths, Some(child_prefix))
                })
                .collect()
        }
        Value::Array(items) => {
            let base = prefix.unwrap_or_else(|| "value".to_string());
            if items.is_empty() {
                return vec![FieldRow {
                    k: base,
                    v: "[]".to_string(),
                    b: false,
                }];
            }
            items
                .iter()
                .enumerate()
                .flat_map(|(idx, item)| {
                    flatten_node(item, json_paths, Some(format!("{base}[{idx}]")))
                })
                .collect()
        }
        _ => vec![FieldRow {
            k: prefix.unwrap_or_else(|| "value".to_string()),
            v: value_to_string(value).unwrap_or_default(),
            b: false,
        }],
    }
}

fn pretty_json_value(value: &Value) -> String {
    let parsed_string_value = match value {
        Value::String(text) if looks_like_json(text) => serde_json::from_str::<Value>(text).ok(),
        _ => None,
    };
    let value = parsed_string_value.as_ref().unwrap_or(value);
    serde_json::to_string_pretty(value)
        .unwrap_or_else(|_| value_to_string(value).unwrap_or_default())
}

fn resolve_path(parsed: Option<&Value>, path: Option<&str>) -> Option<String> {
    let parsed = parsed?;
    let path = path?;
    if path.trim().is_empty() {
        return None;
    }
    let flattened = flatten_path_values(parsed);
    if let Some(value) = flattened.iter().find_map(|(key, value)| {
        if key == path {
            Some(value.clone())
        } else {
            None
        }
    }) {
        return Some(value);
    }
    json_get_path_value(parsed, path)
        .as_ref()
        .and_then(value_to_summary_string)
}

fn flatten_path_values(value: &Value) -> Vec<(String, String)> {
    fn flatten(value: &Value, prefix: Option<String>, out: &mut Vec<(String, String)>) {
        match value {
            Value::Object(map) => {
                let mut keys = map.keys().cloned().collect::<Vec<_>>();
                keys.sort();
                if keys.is_empty() {
                    if let Some(prefix) = prefix {
                        out.push((prefix, String::new()));
                    }
                } else {
                    for key in keys {
                        flatten(&map[&key], Some(join_path(prefix.as_deref(), &key)), out);
                    }
                }
            }
            Value::Array(items) => {
                let base = prefix.unwrap_or_else(|| "value".to_string());
                if items.is_empty() {
                    out.push((base, "[]".to_string()));
                } else {
                    for (idx, item) in items.iter().enumerate() {
                        flatten(item, Some(format!("{base}[{idx}]")), out);
                    }
                }
            }
            _ => {
                out.push((
                    prefix.unwrap_or_else(|| "value".to_string()),
                    value_to_summary_string(value).unwrap_or_default(),
                ));
            }
        }
    }
    let mut out = Vec::new();
    flatten(value, None, &mut out);
    out
}

fn join_path(prefix: Option<&str>, part: &str) -> String {
    match prefix {
        Some(prefix) if !prefix.is_empty() => format!("{prefix}.{part}"),
        _ => part.to_string(),
    }
}

fn json_get_path_value(value: &Value, path: &str) -> Option<Value> {
    let mut current = value.clone();
    for part in split_path(path) {
        if let Value::String(text) = &current {
            if looks_like_json(text) {
                current = serde_json::from_str::<Value>(text).ok()?;
            }
        }
        current = match current {
            Value::Object(mut map) => map.remove(&part)?,
            Value::Array(items) => items.get(part.parse::<usize>().ok()?)?.clone(),
            _ => return None,
        };
    }
    Some(current)
}

fn looks_like_json(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with('{') || trimmed.starts_with('[')
}

fn split_path(path: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut chars = path.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                if let Some(next) = chars.next() {
                    current.push(next);
                } else {
                    current.push('\\');
                }
            }
            '.' => {
                parts.push(current);
                current = String::new();
            }
            _ => current.push(ch),
        }
    }
    parts.push(current);
    parts
}

fn value_to_summary_string(value: &Value) -> Option<String> {
    match value {
        Value::Object(_) | Value::Array(_) => serde_json::to_string(value).ok(),
        _ => value_to_string(value),
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        Value::Bool(true) => Some("true".to_string()),
        Value::Bool(false) => Some("false".to_string()),
        Value::Number(number) => Some(number.to_string()),
        Value::Object(_) | Value::Array(_) => serde_json::to_string(value).ok(),
    }
}

fn parse_time(value: &str) -> Option<f64> {
    if value.is_empty() || value == "-" {
        return None;
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Some(dt.timestamp() as f64 + dt.timestamp_subsec_nanos() as f64 / 1_000_000_000.0);
    }
    for format in [
        "%Y-%m-%dT%H:%M:%S%.fZ",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(value, format) {
            return Some(
                dt.and_utc().timestamp() as f64
                    + dt.and_utc().timestamp_subsec_nanos() as f64 / 1_000_000_000.0,
            );
        }
    }
    None
}

fn normalize_boundary(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => parse_time(text),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ViewerConfig {
        ViewerConfig {
            timestamp_path: Some("timestamp".to_string()),
            level_path: Some("level".to_string()),
            message_path: Some("msg".to_string()),
            extra_paths: Vec::new(),
            json_paths: Vec::new(),
        }
    }

    fn test_db() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        setup_db(&db).unwrap();
        db
    }

    fn insert_test_line(db: &Connection, line: &str) {
        insert_log_entry(db, line, &test_config(), &HashMap::new(), None).unwrap();
    }

    fn collect_rerender(
        db: &Connection,
        max_entries: Option<usize>,
        narrow: Option<&NarrowFilter>,
    ) -> Vec<Entry> {
        let mut entries = Vec::new();
        stream_rerender_entry_batches(db, max_entries, narrow, 2, |batch| entries.extend(batch))
            .unwrap();
        entries
    }

    fn filter(terms: &[&str], operator: NarrowOperator, level: Option<&str>) -> NarrowFilter {
        NarrowFilter {
            terms: terms.iter().map(|term| term.to_string()).collect(),
            operator,
            level: level.map(ToOwned::to_owned),
        }
    }

    #[test]
    fn fts_search_returns_hidden_json_matches() {
        let db = test_db();
        insert_test_line(
            &db,
            r#"{"timestamp":"2026-01-01T00:00:00Z","level":"info","msg":"visible","payload":{"request":"needle-123"}}"#,
        );
        insert_test_line(
            &db,
            r#"{"timestamp":"2026-01-01T00:00:01Z","level":"info","msg":"other","payload":{"request":"ignore"}}"#,
        );
        let narrow = filter(&["needle"], NarrowOperator::And, None);

        assert!(matches!(
            text_search_plan(Some(&narrow)),
            TextSearchPlan::Fts { .. }
        ));
        let entries = collect_rerender(&db, None, Some(&narrow));

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].message, "visible");
    }

    #[test]
    fn short_terms_use_scan_fallback() {
        let db = test_db();
        insert_test_line(
            &db,
            r#"{"timestamp":"2026-01-01T00:00:00Z","level":"info","msg":"xy visible"}"#,
        );
        insert_test_line(
            &db,
            r#"{"timestamp":"2026-01-01T00:00:01Z","level":"info","msg":"other"}"#,
        );
        let narrow = filter(&["xy"], NarrowOperator::And, None);

        assert_eq!(text_search_plan(Some(&narrow)), TextSearchPlan::Scan);
        let entries = collect_rerender(&db, None, Some(&narrow));

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].message, "xy visible");
    }

    #[test]
    fn and_or_and_level_filters_preserve_behavior() {
        let db = test_db();
        insert_test_line(
            &db,
            r#"{"timestamp":"2026-01-01T00:00:00Z","level":"INFO","msg":"alpha beta"}"#,
        );
        insert_test_line(
            &db,
            r#"{"timestamp":"2026-01-01T00:00:01Z","level":"error","msg":"alpha only"}"#,
        );
        insert_test_line(
            &db,
            r#"{"timestamp":"2026-01-01T00:00:02Z","level":"error","msg":"gamma"}"#,
        );

        let and_filter = filter(&["alpha", "beta"], NarrowOperator::And, None);
        let or_level_filter = filter(&["alpha", "gamma"], NarrowOperator::Or, Some("error"));

        assert_eq!(
            collect_rerender(&db, None, Some(&and_filter))
                .into_iter()
                .map(|entry| entry.message)
                .collect::<Vec<_>>(),
            vec!["alpha beta"]
        );
        assert_eq!(
            collect_rerender(&db, None, Some(&or_level_filter))
                .into_iter()
                .map(|entry| entry.message)
                .collect::<Vec<_>>(),
            vec!["alpha only", "gamma"]
        );
    }

    #[test]
    fn capped_rerender_limits_newest_then_orders_for_display() {
        let db = test_db();
        for idx in 0..5 {
            insert_test_line(
                &db,
                &format!(
                    r#"{{"timestamp":"2026-01-01T00:00:0{idx}Z","level":"info","msg":"hit-{idx}"}}"#
                ),
            );
        }
        let narrow = filter(&["hit"], NarrowOperator::And, None);

        let messages = collect_rerender(&db, Some(3), Some(&narrow))
            .into_iter()
            .map(|entry| entry.message)
            .collect::<Vec<_>>();

        assert_eq!(messages, vec!["hit-2", "hit-3", "hit-4"]);
    }

    #[test]
    fn reset_removes_base_and_fts_rows() {
        let db = test_db();
        insert_test_line(
            &db,
            r#"{"timestamp":"2026-01-01T00:00:00Z","level":"info","msg":"needle"}"#,
        );

        reset_log_entries(&db).unwrap();

        let base_count: i64 = db
            .query_row("SELECT count(*) FROM log_entry", [], |row| row.get(0))
            .unwrap();
        let fts_matches: i64 = db
            .query_row(
                "SELECT count(*) FROM log_entry_fts WHERE log_entry_fts MATCH 'nee'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(base_count, 0);
        assert_eq!(fts_matches, 0);
    }

    #[test]
    fn widen_ordering_queries_use_ordering_indexes() {
        let db = test_db();
        let display_plan: String = db
            .query_row(
                "EXPLAIN QUERY PLAN
                 SELECT e.id FROM log_entry e
                 ORDER BY CASE WHEN timestamp_epoch IS NULL THEN 1 ELSE 0 END,
                          timestamp_epoch, id",
                [],
                |row| row.get(3),
            )
            .unwrap();
        let tail_plan: String = db
            .query_row(
                "EXPLAIN QUERY PLAN
                 SELECT e.id FROM log_entry e
                 ORDER BY CASE WHEN timestamp_epoch IS NULL THEN 0 ELSE 1 END,
                          timestamp_epoch DESC, id DESC
                 LIMIT 10",
                [],
                |row| row.get(3),
            )
            .unwrap();

        assert!(display_plan.contains("log_entry_display_order_idx"));
        assert!(tail_plan.contains("log_entry_tail_order_idx"));
    }

    #[test]
    fn source_config_can_resolve_nested_kafka_payload_fields() {
        let db = test_db();
        let line = r#"{"connection":"cluster-a","key":"record-key","payload":{"key":"record-key","requestId":"request-2","type":"ExampleEvent"},"source":"kafka","sourceId":"kafka:cluster-a/example.events.v1","timestamp":"2026-06-14T12:00:00.000Z","topic":"example.events.v1"}"#;
        let default_config = ViewerConfig {
            timestamp_path: Some("timestamp".to_string()),
            level_path: Some("level".to_string()),
            message_path: Some("message".to_string()),
            extra_paths: Vec::new(),
            json_paths: Vec::new(),
        };
        let mut source_configs = HashMap::new();
        source_configs.insert(
            "kafka:cluster-a/example.events.v1".to_string(),
            ViewerConfig {
                timestamp_path: Some("timestamp".to_string()),
                level_path: Some("level".to_string()),
                message_path: Some("payload.type".to_string()),
                extra_paths: vec![
                    "topic".to_string(),
                    "key".to_string(),
                    "payload.type".to_string(),
                ],
                json_paths: vec!["payload".to_string()],
            },
        );

        let (entry, _) =
            insert_log_entry(&db, line, &default_config, &source_configs, None).unwrap();
        let entry = entry.unwrap();

        assert_eq!(entry.source.as_deref(), Some("kafka"));
        assert_eq!(entry.message, "ExampleEvent");
        assert_eq!(
            entry.extra_fields,
            vec!["example.events.v1", "record-key", "ExampleEvent"]
        );
    }
}
