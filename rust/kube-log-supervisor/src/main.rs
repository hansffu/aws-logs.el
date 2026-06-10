use chrono::DateTime;
use futures::{pin_mut, AsyncBufReadExt, StreamExt, TryStreamExt};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::api::{Api, LogParams};
use kube::config::KubeConfigOptions;
use kube::runtime::watcher::{watcher, Config as WatcherConfig, Event};
use kube::{Client, Config, ResourceExt};
use regex::Regex;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::io::{self, Write};
use std::os::unix::net::UnixStream;
use std::process;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone)]
struct Args {
    socket_path: String,
    context: Option<String>,
    namespace: Option<String>,
    target_kind: String,
    target: String,
    tail_lines: Option<i64>,
    since_seconds: Option<i64>,
    filter: Option<String>,
}

#[derive(Clone)]
struct Supervisor {
    args: Arc<Args>,
    namespace: String,
    pods: Api<Pod>,
    socket: Arc<Mutex<UnixStream>>,
    active: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
    live_pods: Arc<RwLock<HashSet<String>>>,
    filter: Option<Regex>,
}

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    if let Err(err) = run().await {
        eprintln!("kube-log-supervisor: {err}");
        process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let args = Arc::new(parse_args(env::args().skip(1).collect())?);
    let namespace = resolve_namespace(&args).await?;
    let socket = UnixStream::connect(&args.socket_path)
        .map_err(|err| format!("failed to connect ingestion socket: {err}"))?;
    let filter = match &args.filter {
        Some(pattern) if !pattern.is_empty() => {
            Some(Regex::new(pattern).map_err(|err| format!("invalid filter regex: {err}"))?)
        }
        _ => None,
    };
    let client = kube_client(&args).await?;
    let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let supervisor = Supervisor {
        args: Arc::clone(&args),
        namespace: namespace.clone(),
        pods,
        socket: Arc::new(Mutex::new(socket)),
        active: Arc::new(Mutex::new(HashMap::new())),
        live_pods: Arc::new(RwLock::new(HashSet::new())),
        filter,
    };

    eprintln!(
        "kube-log-supervisor: starting {} logs for namespace={} target={}",
        args.target_kind, namespace, args.target
    );

    match args.target_kind.as_str() {
        "pod" => supervisor.run_single_pod(args.target.clone()).await,
        "deployment" => {
            let deployments: Api<Deployment> = Api::namespaced(client, &namespace);
            let deployment = deployments
                .get(&args.target)
                .await
                .map_err(|err| format!("failed to get deployment {}: {err}", args.target))?;
            let selector = deployment_selector(&deployment)?;
            eprintln!("kube-log-supervisor: watching pods with selector {selector}");
            supervisor.run_deployment(selector).await
        }
        other => Err(format!("unsupported target kind: {other}")),
    }
}

async fn kube_client(args: &Args) -> Result<Client, String> {
    let config = if args.context.is_some() {
        Config::from_kubeconfig(&KubeConfigOptions {
            context: args.context.clone(),
            cluster: None,
            user: None,
        })
        .await
        .map_err(|err| format!("failed to load kubeconfig: {err}"))?
    } else {
        Config::infer()
            .await
            .map_err(|err| format!("failed to infer kube config: {err}"))?
    };
    Client::try_from(config).map_err(|err| format!("failed to create kube client: {err}"))
}

async fn resolve_namespace(args: &Args) -> Result<String, String> {
    if let Some(namespace) = &args.namespace {
        if !namespace.is_empty() {
            return Ok(namespace.clone());
        }
    }
    let config = if args.context.is_some() {
        Config::from_kubeconfig(&KubeConfigOptions {
            context: args.context.clone(),
            cluster: None,
            user: None,
        })
        .await
        .map_err(|err| format!("failed to load kubeconfig: {err}"))?
    } else {
        Config::infer()
            .await
            .map_err(|err| format!("failed to infer kube config: {err}"))?
    };
    Ok(config.default_namespace)
}

impl Supervisor {
    async fn run_single_pod(self, pod_name: String) -> Result<(), String> {
        self.live_pods.write().await.insert(pod_name.clone());
        self.start_pod_stream(pod_name).await;
        wait_for_shutdown().await;
        Ok(())
    }

    async fn run_deployment(self, selector: String) -> Result<(), String> {
        let stream = watcher(
            self.pods.clone(),
            WatcherConfig::default().labels(&selector),
        );
        pin_mut!(stream);
        while let Some(event) = stream.next().await {
            match event {
                Ok(Event::Apply(pod)) | Ok(Event::InitApply(pod)) => {
                    self.apply_pod(pod).await;
                }
                Ok(Event::Delete(pod)) => {
                    self.delete_pod(&pod.name_any()).await;
                }
                Ok(Event::Init) => {
                    eprintln!("kube-log-supervisor: pod watch restarted");
                }
                Ok(Event::InitDone) => {
                    eprintln!("kube-log-supervisor: pod watch initialized");
                }
                Err(err) => {
                    eprintln!("kube-log-supervisor: pod watch error: {err}");
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
        Ok(())
    }

    async fn apply_pod(&self, pod: Pod) {
        let name = pod.name_any();
        if !pod_is_loggable(&pod) {
            self.delete_pod(&name).await;
            return;
        }
        let mut live = self.live_pods.write().await;
        let is_new = live.insert(name.clone());
        drop(live);
        if is_new {
            eprintln!("kube-log-supervisor: starting pod stream {name}");
            self.start_pod_stream(name).await;
        }
    }

    async fn delete_pod(&self, pod_name: &str) {
        self.live_pods.write().await.remove(pod_name);
        if let Some(handle) = self.active.lock().await.remove(pod_name) {
            eprintln!("kube-log-supervisor: stopping pod stream {pod_name}");
            handle.abort();
        }
    }

    async fn start_pod_stream(&self, pod_name: String) {
        let mut active = self.active.lock().await;
        if active.contains_key(&pod_name) {
            return;
        }
        let supervisor = self.clone();
        let key = pod_name.clone();
        let handle = tokio::spawn(async move {
            supervisor.stream_pod_with_retry(pod_name).await;
        });
        active.insert(key, handle);
    }

    async fn stream_pod_with_retry(self, pod_name: String) {
        let mut attempt = 0u32;
        loop {
            if !self.live_pods.read().await.contains(&pod_name) {
                break;
            }
            match self.stream_pod_once(&pod_name).await {
                Ok(()) => {
                    eprintln!("kube-log-supervisor: pod stream ended: {pod_name}");
                    attempt = attempt.saturating_add(1);
                }
                Err(err) => {
                    eprintln!("kube-log-supervisor: pod stream error for {pod_name}: {err}");
                    attempt = attempt.saturating_add(1);
                }
            }
            let delay = retry_delay(attempt);
            sleep(delay).await;
        }
    }

    async fn stream_pod_once(&self, pod_name: &str) -> Result<(), String> {
        let params = LogParams {
            follow: true,
            tail_lines: self.args.tail_lines,
            since_seconds: self.args.since_seconds,
            timestamps: true,
            ..LogParams::default()
        };
        let stream = self
            .pods
            .log_stream(pod_name, &params)
            .await
            .map_err(|err| format!("failed to open log stream: {err}"))?;
        let mut lines = stream.lines();
        while let Some(line) = lines
            .try_next()
            .await
            .map_err(|err| format!("failed to read log stream: {err}"))?
        {
            if let Some(json_line) = normalize_kube_line(
                &line,
                &self.namespace,
                &self.args.target,
                &self.args.target_kind,
                pod_name,
                self.filter.as_ref(),
            )? {
                write_ingest_line(&self.socket, &json_line)
                    .await
                    .map_err(|err| format!("failed to write ingestion socket: {err}"))?;
            }
        }
        Ok(())
    }
}

async fn write_ingest_line(socket: &Arc<Mutex<UnixStream>>, json_line: &str) -> io::Result<()> {
    let mut socket = socket.lock().await;
    socket.write_all(b"L ")?;
    socket.write_all(json_line.as_bytes())?;
    socket.write_all(b"\n")?;
    socket.flush()
}

async fn wait_for_shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}

fn retry_delay(attempt: u32) -> Duration {
    Duration::from_secs(2u64.saturating_pow(attempt.min(4)))
}

fn pod_is_loggable(pod: &Pod) -> bool {
    if pod.metadata.deletion_timestamp.is_some() {
        return false;
    }
    let phase = pod
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref());
    !matches!(phase, Some("Succeeded" | "Failed"))
}

fn deployment_selector(deployment: &Deployment) -> Result<String, String> {
    let selector = deployment
        .spec
        .as_ref()
        .map(|spec| &spec.selector)
        .ok_or_else(|| "deployment has no spec.selector".to_string())?;
    label_selector_to_string(selector)
}

fn label_selector_to_string(selector: &LabelSelector) -> Result<String, String> {
    let mut parts = Vec::new();
    if let Some(labels) = &selector.match_labels {
        let sorted: BTreeMap<_, _> = labels.iter().collect();
        for (key, value) in sorted {
            parts.push(format!("{key}={value}"));
        }
    }
    if let Some(expressions) = &selector.match_expressions {
        for expr in expressions {
            let key = &expr.key;
            match expr.operator.as_str() {
                "In" => {
                    let values = expr.values.clone().unwrap_or_default().join(",");
                    parts.push(format!("{key} in ({values})"));
                }
                "NotIn" => {
                    let values = expr.values.clone().unwrap_or_default().join(",");
                    parts.push(format!("{key} notin ({values})"));
                }
                "Exists" => parts.push(key.clone()),
                "DoesNotExist" => parts.push(format!("!{key}")),
                other => return Err(format!("unsupported selector operator: {other}")),
            }
        }
    }
    if parts.is_empty() {
        return Err("deployment selector is empty".to_string());
    }
    Ok(parts.join(","))
}

fn normalize_kube_line(
    line: &str,
    namespace: &str,
    target: &str,
    kind: &str,
    pod: &str,
    filter: Option<&Regex>,
) -> Result<Option<String>, String> {
    let clean = line.trim_end_matches('\r');
    if clean.is_empty() {
        return Ok(None);
    }
    let (timestamp, message) = split_timestamp_prefix(clean);
    if let Some(filter) = filter {
        if !filter.is_match(clean) && !filter.is_match(message) {
            return Ok(None);
        }
    }
    let payload = parse_json_maybe(message).unwrap_or_else(|| Value::String(message.to_string()));
    let mut obj = Map::new();
    if let Some(timestamp) = timestamp {
        obj.insert(
            "timestamp".to_string(),
            Value::String(timestamp.to_string()),
        );
    }
    obj.insert("raw".to_string(), Value::String(clean.to_string()));
    obj.insert(
        "namespace".to_string(),
        Value::String(namespace.to_string()),
    );
    obj.insert("target".to_string(), Value::String(target.to_string()));
    obj.insert("kind".to_string(), Value::String(kind.to_string()));
    obj.insert("pod".to_string(), Value::String(pod.to_string()));
    obj.insert("payload".to_string(), payload);
    serde_json::to_string(&Value::Object(obj))
        .map(Some)
        .map_err(|err| format!("failed to serialize log line: {err}"))
}

fn split_timestamp_prefix(line: &str) -> (Option<&str>, &str) {
    let Some((head, tail)) = line.split_once(char::is_whitespace) else {
        return (None, line);
    };
    if DateTime::parse_from_rfc3339(head).is_ok() {
        (Some(head), tail.trim_start())
    } else {
        (None, line)
    }
}

fn parse_json_maybe(value: &str) -> Option<Value> {
    serde_json::from_str(value.trim()).ok()
}

fn parse_args(args: Vec<String>) -> Result<Args, String> {
    let mut socket_path = None;
    let mut context = None;
    let mut namespace = None;
    let mut target_kind = None;
    let mut target = None;
    let mut tail_lines = None;
    let mut since_seconds = None;
    let mut filter = None;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--socket" => {
                idx += 1;
                socket_path = args.get(idx).cloned();
            }
            "--context" => {
                idx += 1;
                context = args.get(idx).cloned().filter(|s| !s.is_empty());
            }
            "--namespace" => {
                idx += 1;
                namespace = args.get(idx).cloned().filter(|s| !s.is_empty());
            }
            "--target-kind" => {
                idx += 1;
                target_kind = args.get(idx).cloned();
            }
            "--target" => {
                idx += 1;
                target = args.get(idx).cloned();
            }
            "--tail" => {
                idx += 1;
                tail_lines = parse_i64_arg(args.get(idx), "--tail")?;
            }
            "--since" => {
                idx += 1;
                since_seconds = parse_since_arg(args.get(idx))?;
            }
            "--filter" => {
                idx += 1;
                filter = args.get(idx).cloned().filter(|s| !s.is_empty());
            }
            other => return Err(format!("unexpected argument: {other}")),
        }
        idx += 1;
    }
    Ok(Args {
        socket_path: socket_path.ok_or_else(|| "missing --socket PATH".to_string())?,
        context,
        namespace,
        target_kind: target_kind.ok_or_else(|| "missing --target-kind KIND".to_string())?,
        target: target.ok_or_else(|| "missing --target NAME".to_string())?,
        tail_lines,
        since_seconds,
        filter,
    })
}

fn parse_i64_arg(value: Option<&String>, name: &str) -> Result<Option<i64>, String> {
    let value = value.ok_or_else(|| format!("missing value for {name}"))?;
    value
        .parse::<i64>()
        .map(Some)
        .map_err(|err| format!("invalid {name}: {err}"))
}

fn parse_since_arg(value: Option<&String>) -> Result<Option<i64>, String> {
    let value = value.ok_or_else(|| "missing value for --since".to_string())?;
    if value.is_empty() {
        return Ok(None);
    }
    if let Ok(seconds) = value.parse::<i64>() {
        return Ok(Some(seconds));
    }
    let (number, unit) = value.split_at(value.len().saturating_sub(1));
    let amount = number
        .parse::<i64>()
        .map_err(|_| format!("unsupported --since value: {value}"))?;
    let multiplier = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86400,
        "w" => 604800,
        _ => return Err(format!("unsupported --since value: {value}")),
    };
    Ok(Some(amount * multiplier))
}
