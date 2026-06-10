use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use serde_json::{Map, Value};
use std::env;
use std::io::{self, BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::{self, Command, Stdio};
use std::sync::atomic::{AtomicI32, Ordering};

static CHILD_PGID: AtomicI32 = AtomicI32::new(0);

extern "C" fn forward_signal(signal: i32) {
    let pgid = CHILD_PGID.load(Ordering::SeqCst);
    if pgid > 0 {
        unsafe {
            libc::kill(-pgid, signal);
        }
    }
    unsafe {
        libc::_exit(128 + signal);
    }
}

#[derive(Debug)]
enum Mode {
    Kube {
        namespace: String,
        target: String,
        kind: String,
    },
    Kafka {
        connection: String,
        topic: String,
        payload_json: bool,
    },
}

#[derive(Debug)]
struct Config {
    socket_path: String,
    mode: Mode,
    command: Vec<String>,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("json-log-viewer-ingest-wrapper: {err}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    install_signal_handlers();
    let config = parse_args(env::args().skip(1).collect())?;
    if config.command.is_empty() {
        return Err("missing source command after --".to_string());
    }

    let mut socket = UnixStream::connect(&config.socket_path)
        .map_err(|err| format!("failed to connect ingestion socket: {err}"))?;

    let mut command = Command::new(&config.command[0]);
    command
        .args(&config.command[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command
        .spawn()
        .map_err(|err| format!("failed to start source command: {err}"))?;
    CHILD_PGID.store(child.id() as i32, Ordering::SeqCst);

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "source command stdout was not captured".to_string())?;
    let reader = BufReader::new(stdout);
    for line in reader.lines() {
        let line = line.map_err(|err| format!("failed to read source output: {err}"))?;
        let Some(json_line) = normalize_line(&config.mode, &line)? else {
            continue;
        };
        write_ingest_line(&mut socket, &json_line)
            .map_err(|err| format!("failed to write ingestion socket: {err}"))?;
    }
    let status = child
        .wait()
        .map_err(|err| format!("failed to wait for source command: {err}"))?;
    CHILD_PGID.store(0, Ordering::SeqCst);
    if let Some(code) = status.code() {
        process::exit(code);
    }
    process::exit(1);
}

fn install_signal_handlers() {
    unsafe {
        let handler = forward_signal as *const () as libc::sighandler_t;
        libc::signal(libc::SIGTERM, handler);
        libc::signal(libc::SIGINT, handler);
        libc::signal(libc::SIGHUP, handler);
    }
}

fn parse_args(args: Vec<String>) -> Result<Config, String> {
    let mut idx = 0;
    let mut socket_path = None;
    while idx < args.len() {
        match args[idx].as_str() {
            "--socket" => {
                idx += 1;
                socket_path = args.get(idx).cloned();
                idx += 1;
            }
            "kube" | "kafka" => break,
            other => return Err(format!("unexpected argument before mode: {other}")),
        }
    }

    let socket_path = socket_path.ok_or_else(|| "missing --socket PATH".to_string())?;
    let mode_name = args
        .get(idx)
        .ok_or_else(|| "missing mode: kube or kafka".to_string())?;
    idx += 1;

    let mode = match mode_name.as_str() {
        "kube" => {
            let mut namespace = String::new();
            let mut target = String::new();
            let mut kind = String::new();
            while idx < args.len() && args[idx] != "--" {
                match args[idx].as_str() {
                    "--namespace" => {
                        idx += 1;
                        namespace = args.get(idx).cloned().unwrap_or_default();
                        idx += 1;
                    }
                    "--target" => {
                        idx += 1;
                        target = args.get(idx).cloned().unwrap_or_default();
                        idx += 1;
                    }
                    "--kind" => {
                        idx += 1;
                        kind = args.get(idx).cloned().unwrap_or_default();
                        idx += 1;
                    }
                    other => return Err(format!("unexpected kube option: {other}")),
                }
            }
            Mode::Kube {
                namespace,
                target,
                kind,
            }
        }
        "kafka" => {
            let mut connection = String::new();
            let mut topic = String::new();
            let mut payload_json = false;
            while idx < args.len() && args[idx] != "--" {
                match args[idx].as_str() {
                    "--connection" => {
                        idx += 1;
                        connection = args.get(idx).cloned().unwrap_or_default();
                        idx += 1;
                    }
                    "--topic" => {
                        idx += 1;
                        topic = args.get(idx).cloned().unwrap_or_default();
                        idx += 1;
                    }
                    "--payload-format" => {
                        idx += 1;
                        payload_json = args.get(idx).map(|v| v == "json").unwrap_or(false);
                        idx += 1;
                    }
                    other => return Err(format!("unexpected kafka option: {other}")),
                }
            }
            Mode::Kafka {
                connection,
                topic,
                payload_json,
            }
        }
        other => return Err(format!("unsupported mode: {other}")),
    };

    if args.get(idx).map(String::as_str) != Some("--") {
        return Err("missing -- before source command".to_string());
    }
    idx += 1;
    Ok(Config {
        socket_path,
        mode,
        command: args[idx..].to_vec(),
    })
}

fn normalize_line(mode: &Mode, line: &str) -> Result<Option<String>, String> {
    match mode {
        Mode::Kube {
            namespace,
            target,
            kind,
        } => Ok(normalize_kube_line(line, namespace, target, kind)),
        Mode::Kafka {
            connection,
            topic,
            payload_json,
        } => normalize_kafka_line(line, connection, topic, *payload_json),
    }
}

fn write_ingest_line(socket: &mut UnixStream, json_line: &str) -> io::Result<()> {
    let mut frame = Vec::with_capacity(json_line.len() + 3);
    frame.extend_from_slice(b"L ");
    frame.extend_from_slice(json_line.as_bytes());
    frame.push(b'\n');
    socket.write_all(&frame)
}

fn normalize_kube_line(line: &str, namespace: &str, target: &str, kind: &str) -> Option<String> {
    let clean = line.trim_end_matches('\r');
    if clean.is_empty() {
        return None;
    }
    let without_prefix = strip_kubectl_prefix(clean);
    let (timestamp, message) = split_timestamp_prefix(&without_prefix);
    let payload = parse_json_maybe(message).unwrap_or_else(|| Value::String(message.to_string()));
    let mut obj = Map::new();
    if let Some(timestamp) = timestamp {
        obj.insert(
            "timestamp".to_string(),
            Value::String(timestamp.to_string()),
        );
    }
    obj.insert("raw".to_string(), Value::String(without_prefix));
    obj.insert(
        "namespace".to_string(),
        Value::String(namespace.to_string()),
    );
    obj.insert("target".to_string(), Value::String(target.to_string()));
    obj.insert("kind".to_string(), Value::String(kind.to_string()));
    obj.insert("payload".to_string(), payload);
    Some(Value::Object(obj).to_string())
}

fn strip_kubectl_prefix(line: &str) -> String {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    if let Some(index) = tokens.iter().position(|token| is_timestamp(token)) {
        if index > 0 {
            return tokens[index..].join(" ");
        }
    }
    line.to_string()
}

fn split_timestamp_prefix(line: &str) -> (Option<&str>, &str) {
    let Some((head, tail)) = line.split_once(char::is_whitespace) else {
        return (None, line);
    };
    if is_timestamp(head) {
        (Some(head), tail.trim_start())
    } else {
        (None, line)
    }
}

fn is_timestamp(value: &str) -> bool {
    DateTime::parse_from_rfc3339(value).is_ok()
}

fn normalize_kafka_line(
    line: &str,
    connection: &str,
    fallback_topic: &str,
    payload_json: bool,
) -> Result<Option<String>, String> {
    let clean = line.trim_end_matches('\r');
    if clean.is_empty() {
        return Ok(None);
    }
    let envelope = parse_json_maybe(clean);
    let envelope_obj = envelope.as_ref().and_then(Value::as_object);
    let topic = get_string(envelope_obj, "topic").unwrap_or_else(|| fallback_topic.to_string());
    let partition = get_value(envelope_obj, "partition");
    let offset = get_value(envelope_obj, "offset");
    let timestamp = get_value(envelope_obj, "ts")
        .and_then(Value::as_i64)
        .and_then(epoch_ms_to_iso8601);
    let key_size = get_value(envelope_obj, "key_size").and_then(Value::as_i64);
    let key = if key_size.is_some_and(|size| size < 0) {
        None
    } else {
        get_value(envelope_obj, "key").cloned()
    };
    let headers = get_value(envelope_obj, "headers").map(normalize_headers);
    let payload = get_value(envelope_obj, "payload").cloned();
    let payload_node = payload.as_ref().and_then(|payload| match payload {
        Value::Object(_) | Value::Array(_) => Some(payload.clone()),
        Value::String(text) => parse_json_maybe(text),
        _ => None,
    });
    let display_payload = if payload_json {
        payload_node.clone().or(payload)
    } else {
        payload
    };
    let level = payload_node
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|payload| {
            ["level", "severity", "logLevel", "lvl"]
                .iter()
                .find_map(|key| payload.get(*key).and_then(value_to_string))
        });

    let mut obj = Map::new();
    if let Some(timestamp) = timestamp {
        obj.insert("timestamp".to_string(), Value::String(timestamp));
    }
    if let Some(level) = level {
        obj.insert("level".to_string(), Value::String(level));
    }
    obj.insert("raw".to_string(), Value::String(clean.to_string()));
    obj.insert(
        "connection".to_string(),
        Value::String(connection.to_string()),
    );
    if !topic.is_empty() {
        obj.insert("topic".to_string(), Value::String(topic));
    }
    if let Some(partition) = partition {
        obj.insert("partition".to_string(), partition.clone());
    }
    if let Some(offset) = offset {
        obj.insert("offset".to_string(), offset.clone());
    }
    if let Some(key) = key {
        obj.insert("key".to_string(), key);
    }
    if let Some(headers) = headers {
        obj.insert("headers".to_string(), headers);
    }
    if let Some(payload) = display_payload {
        obj.insert("payload".to_string(), payload);
    }
    Ok(Some(Value::Object(obj).to_string()))
}

fn get_value<'a>(obj: Option<&'a Map<String, Value>>, key: &str) -> Option<&'a Value> {
    obj.and_then(|obj| obj.get(key))
}

fn get_string(obj: Option<&Map<String, Value>>, key: &str) -> Option<String> {
    get_value(obj, key).and_then(value_to_string)
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(true) => Some("true".to_string()),
        Value::Bool(false) => Some("false".to_string()),
        Value::Null => None,
        _ => Some(value.to_string()),
    }
}

fn normalize_headers(headers: &Value) -> Value {
    match headers {
        Value::Array(items) => {
            let mut obj = Map::new();
            let mut iter = items.iter();
            while let Some(name) = iter.next() {
                let Some(name) = value_to_string(name) else {
                    let _ = iter.next();
                    continue;
                };
                let value = iter.next().cloned().unwrap_or(Value::Null);
                match obj.remove(&name) {
                    None => {
                        obj.insert(name, value);
                    }
                    Some(Value::Array(mut existing)) => {
                        existing.push(value);
                        obj.insert(name, Value::Array(existing));
                    }
                    Some(existing) => {
                        obj.insert(name, Value::Array(vec![existing, value]));
                    }
                }
            }
            Value::Object(obj)
        }
        _ => headers.clone(),
    }
}

fn epoch_ms_to_iso8601(ms: i64) -> Option<String> {
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|ts| ts.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn parse_json_maybe(value: &str) -> Option<Value> {
    let trimmed = value.trim_start();
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return None;
    }
    serde_json::from_str(value).ok()
}
