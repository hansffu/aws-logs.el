# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Byte-compile** (catches syntax errors and warnings):
```
emacs -Q --batch -L . -f batch-byte-compile aws-logs.el aws-logs-query.el aws-logs-insights.el aws-logs-tail.el json-log-viewer.el json-log-viewer-shared.el json-log-viewer-repository.el json-log-viewer-async-worker.el async-job-queue.el kube-logs.el kafka-logs.el
```

**Run all ERT tests**:
```
emacs -Q --batch -L . -L test -l test/aws-logs-core-test.el -l test/kafka-logs-test.el -l test/kube-logs-test.el -f ert-run-tests-batch-and-exit
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

The repo is a family of Emacs Lisp packages built around a shared log viewer core.

### Layered Structure

1. **UI/session layer** — `aws-logs.el`, `kube-logs.el`, `kafka-logs.el`: transient menus, session variables, presets.
2. **Source execution layer** — `aws-logs-tail.el`, `aws-logs-insights.el`: builds CLI commands, runs subprocesses, normalizes output into JSON lines.
3. **Shared viewer layer** — `json-log-viewer.el`: stores JSON in per-buffer SQLite, async ingestion, foldable overlay rendering, narrowing, refresh, auto-follow, subscriber notifications.
4. **Async infrastructure** — `async-job-queue.el`: serializes jobs into subordinate Emacs worker processes to keep SQLite/normalization work off the UI thread.

### Key Design Points

- **Data contract**: every source adapter normalizes its output into JSON line strings before handing them to `json-log-viewer`. The viewer never parses source-specific CLI formats.
- **SQLite-backed storage**: raw JSON and summary metadata are written to a per-buffer SQLite file. Summary lines are rendered eagerly; full details are loaded lazily on expand. This keeps large streaming buffers usable without materializing all detail content.
- **Buffer-local isolation**: each viewer buffer has its own SQLite handle, async queue, path config, filter state, and overlays — no global data registry.
- **Narrowing replays from stored JSON**: filter/narrow operations run from the SQLite store, not from visible summary text, so hidden fields in the raw JSON are searchable.

### File Roles

| File | Role |
|---|---|
| `json-log-viewer.el` | Core viewer: buffer API, rendering, overlays, narrow/widen, refresh |
| `json-log-viewer-shared.el` | JSON parsing + path resolution helpers (dotted-key, escaped-dot, flattening) |
| `json-log-viewer-repository.el` | SQLite schema + reusable query operations |
| `json-log-viewer-async-worker.el` | Worker-side ingest, summarize, truncation logic (runs in subordinate Emacs) |
| `async-job-queue.el` | Ordered async job queue over a subordinate Emacs process |
| `json-log-viewer-evil.el` | Optional Evil keybindings (pure integration, no data flow) |
| `aws-logs.el` | CloudWatch transient UI, session state, presets, saved-query workflow |
| `aws-logs-tail.el` | `aws logs tail` subprocess, ECS JSON streaming pipeline with backpressure |
| `aws-logs-insights.el` | Async Logs Insights query/poll/render; freezes source context for refresh |
| `aws-logs-query.el` | Logs Insights query major mode + popup editor |
| `kube-logs.el` | `kubectl logs` transient UI + JSON normalization |
| `kafka-logs.el` | `kcat` transient UI + JSON normalization + auth-source credential lookup |

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
