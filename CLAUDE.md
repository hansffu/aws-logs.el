# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Byte-compile** (catches syntax errors and warnings):
```
emacs -Q --batch -L . -f batch-byte-compile aws-logs.el aws-logs-query.el aws-logs-insights.el aws-logs-tail.el json-log-viewer.el json-log-viewer-shared.el json-log-viewer-repository.el json-log-viewer-async-worker.el async-job-queue.el composite-log-viewer.el kube-logs.el kafka-logs.el
```

**Run all ERT tests**:
```
emacs -Q --batch -L . -L test -l test/aws-logs-core-test.el -l test/kafka-logs-test.el -l test/kube-logs-test.el -l test/composite-log-viewer-test.el -f ert-run-tests-batch-and-exit
```

**Build Rust worker/runtime binaries**:
```
cargo build
```

or through Nix:
```
just worker-build
just worker-build-nix
```

**Run a single test file**:
```
emacs -Q --batch -L . -L test -l test/aws-logs-core-test.el -f ert-run-tests-batch-and-exit
```

**Run a single test by name**:
```
emacs -Q --batch -L . -L test -l test/aws-logs-core-test.el --eval '(ert-run-tests-batch-and-exit "test-name-here")'
```

**Load interactively**:
```
emacs -Q --eval "(add-to-list 'load-path \".\")" -l aws-logs.el
```

## Architecture

The repo is a family of Emacs Lisp packages built around a shared log viewer core and Rust runtime.

The major architecture goal is to reduce load on Emacs and reduce blocking:

- Keep process filters lightweight.
- Move high-volume parsing, storage, filtering, lazy detail lookup, and stream supervision into Rust.
- Use Unix socket ingestion for log volume and stdin/stdout JSON control commands for low-volume worker coordination.
- Let Emacs own UI state, overlays, keymaps, transients, and process lifecycle, but avoid storing or rendering full log details in Emacs.

### Layered Structure

1. **UI/session layer** — `aws-logs.el`, `kube-logs.el`, `kafka-logs.el`: transient menus, session variables, presets.
2. **Source execution layer** — `aws-logs-tail.el`, `aws-logs-insights.el`, `kube-logs.el`, `kafka-logs.el`: builds CLI commands, runs subprocesses, and normalizes output into viewer JSON.
3. **Shared viewer layer** — `json-log-viewer.el`: creates buffers, tracks render state, starts the Rust worker, sends control commands, pulls rendered batches, handles overlays/folds/narrowing/sliding windows.
4. **Rust runtime** — `json-log-viewer-worker`, `json-log-viewer-ingest-wrapper`, `kube-log-supervisor`: handles high-volume ingestion, SQLite storage, filtering/rerender, lazy details, and Kubernetes stream supervision outside Emacs.

### Key Design Points

- **Data contract**: every source adapter normalizes its output into JSON line strings before handing them to `json-log-viewer`. The viewer never parses source-specific CLI formats.
- **Worker-owned SQLite storage**: raw JSON and summary metadata are written to a per-buffer SQLite file owned by the Rust worker. Summary lines are rendered in Emacs; full details are loaded lazily on expand.
- **Pull-based streaming display**: wrappers/supervisors can ingest continuously while Emacs periodically pulls small visible batches, skipping hidden buffers unless background refresh is enabled.
- **Buffer-local isolation**: each viewer buffer has its own Rust worker, socket path, pull timer, path config, source config, filter state, and overlays. There is no global data registry.
- **Narrowing replays from stored JSON**: filter/narrow operations run from the worker store, not from visible summary text, so hidden fields in raw JSON are searchable.
- **Composite source config**: `composite-log-viewer` combines kube/kafka sources in one viewer. Kube and Kafka register per-source render paths through `json-log-viewer-register-source-config`.

### File Roles

| File | Role |
|---|---|
| `json-log-viewer.el` | Core viewer and Rust worker client: buffer API, rendering, overlays, narrow/widen, sliding windows, pull loop |
| `json-log-viewer-shared.el` | JSON parsing + path resolution helpers (dotted-key, escaped-dot, flattening) |
| `json-log-viewer-repository.el` | Legacy Emacs-side SQLite schema + reusable query operations |
| `json-log-viewer-async-worker.el` | Legacy Emacs-side ingest/summarize/truncation worker logic |
| `async-job-queue.el` | Legacy ordered async job queue over a subordinate Emacs process |
| `json-log-viewer-evil.el` | Optional Evil keybindings (pure integration, no data flow) |
| `aws-logs.el` | CloudWatch transient UI, session state, presets, saved-query workflow |
| `aws-logs-tail.el` | `aws logs tail` subprocess, ECS JSON streaming pipeline with backpressure |
| `aws-logs-insights.el` | Async Logs Insights query/poll/render; freezes source context for refresh |
| `aws-logs-query.el` | Logs Insights query major mode + popup editor |
| `kube-logs.el` | Kubernetes transient UI, one-shot `kubectl` path, Rust/kubectl stream backends, composite integration |
| `kafka-logs.el` | `kcat` transient UI, JSON normalization, auth-source credential lookup, composite integration |
| `composite-log-viewer.el` | Shared multi-source kube/kafka viewer buffers |
| `rust/json-log-viewer-worker/src/main.rs` | Rust worker for SQLite storage, JSON parsing, filtering, lazy details, pull batches |
| `rust/json-log-viewer-ingest-wrapper/src/main.rs` | Rust wrapper that runs kube/kafka CLI commands and writes normalized frames to the worker socket |
| `rust/kube-log-supervisor/src/main.rs` | kube-rs supervisor for Kubernetes follow mode |

### Sync Points

Transient options and `composite-log-viewer-create` source plists are two entry points into the same source adapters. Keep option names, defaults, validation, rendering behavior, README examples, and tests synchronized.

Kubernetes options that should stay aligned:

- `kube-logs-context` / `:context`
- `kube-logs-namespace` / `:namespace`
- `kube-logs-namespace-enabled` / `:namespace-enabled`
- `kube-logs-target-kind` / `:target-kind`
- `kube-logs-target` / `:target`
- `kube-logs-filter` / `:filter`
- `kube-logs-stream-backend` / `:stream-backend`
- `kube-logs-debug-process-buffer` / `:debug-process-buffer`
- `kube-logs-timestamp-path` / `:timestamp-path`
- `kube-logs-level-path` / `:level-path`
- `kube-logs-message-path` / `:message-path`
- `kube-logs-extra-paths` / `:extra-paths`

Kube composite exceptions: composite sources always follow from now, force tail to `0`, clear `since`, and append into a shared viewer.

Kafka options that should stay aligned:

- `kafka-logs-connection` / `:connection`
- `kafka-logs-topic` / `:topic`
- `kafka-logs-filter` / `:filter`
- `kafka-logs-value-format` / `:value-format`
- `kafka-logs-payload-format` / `:payload-format`
- `kafka-logs-message-path` / `:message-path`
- `kafka-logs-json-paths` / `:json-paths`
- `kafka-logs-extra-paths` / `:extra-paths`

Kafka composite exceptions: composite sources always stream from topic end, ignore time range/since/max message options, and append into a shared viewer.

## Coding Conventions

- All files use `;;; -*- lexical-binding: t; -*-`.
- Public symbols: `package-name-symbol`. Internal helpers: `package-name--symbol` (double dash).
- User-configurable settings use `defcustom` with explicit docstrings distinguishing session vs default scope.
- Commit style: short imperative subjects (e.g., `fix narrowing in composite buffer`), one logical change per commit.

## Path Resolution

`json-log-viewer-shared--resolve-path` handles two ambiguous cases:

- Unescaped `a.b.c` matches either a nested key `{a: {b: {c: ...}}}` or a dotted key `{a: {"b.c": ...}}` (tries nested first, falls back to dotted).
- Escaped `a.b\\.c` matches only the dotted key `{a: {"b.c": ...}}`.

This matters when configuring `:level-path`, `:message-path`, `:extra-paths`, and `:json-paths` for sources with dotted field names (common in ECS/OpenTelemetry JSON logs).
