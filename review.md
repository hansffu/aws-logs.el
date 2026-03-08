# Codebase Review: aws-logs.el

## Overview

A well-architected family of Emacs Lisp packages (8,077 lines of source, 1,298 lines of tests) providing a unified log viewing experience across CloudWatch, Kubernetes, and Kafka. The layered architecture cleanly separates UI/session, source execution, shared viewer, and async infrastructure concerns.

## Architecture & Design (Excellent)

The layered design is the standout strength of this codebase:

- **Data contract**: All source adapters normalize to JSON lines before handing to `json-log-viewer` -- this keeps the viewer generic and makes adding new sources straightforward.
- **SQLite-backed storage**: Smart choice for large streaming buffers. Lazy detail loading and async ingestion via subordinate Emacs processes keeps the UI responsive.
- **Buffer-local isolation**: No global state registries; each buffer owns its SQLite handle, async queue, and subscriber list. This prevents cross-buffer interference.
- **Subscriber/notification system**: Clean pub/sub enabling composite buffers without the viewer needing to know about multi-source merging.

## Code Quality & Style (Very Good)

- Consistent `lexical-binding: t` across all files.
- Public/private naming convention (`package-name-` vs `package-name--`) consistently applied.
- `defcustom` usage is thorough with good docstrings distinguishing session vs default scope.
- Commit history shows clean, imperative-style messages.

## Issues Found

### 1. Two Failing Tests

```
FAILED  kafka-logs-line->json-line-json-payload-test
FAILED  kube-logs-make-viewer-buffer-uses-fixed-summary-paths-test
```

**kafka-logs test** (`test/kafka-logs-test.el:86`): The test expects `(alist-get 'message parsed)` to be `"boom"`, but gets `nil`. The test passes a JSON string payload and expects message extraction, but `kafka-logs--line->json-line` puts the parsed JSON into a `"payload"` key -- the `"message"` field is nested inside the payload, not at the top level. The test assertion doesn't match the actual data model.

**kube-logs test** (`test/kube-logs-test.el:66`): The test expects `kube-logs--make-viewer-buffer` to pass `:level-path "payload.log\\.level"` to `json-log-viewer-make-buffer`, but the actual call passes `kube-logs-level-path` which is `nil` by default. The test expects hardcoded paths, but the code uses the `defcustom` variables `kube-logs-level-path` / `kube-logs-message-path`. The test needs to either set those variables or the expectations need updating.

### 2. Byte-Compilation Warning

```
kafka-logs.el:1073:21: Warning: the function 'org-read-date' is not known to be defined.
```

`kafka-logs.el` uses `org-read-date` for the time picker but only has `(require 'org)` at runtime inside `kafka-logs--read-org-time`. A `declare-function` for `org-read-date` would silence this, similar to how `aws-logs.el` handles it (`aws-logs.el:225`).

### 3. Duplicated Logic

Several utility functions are duplicated across files:

| Function | Defined in |
|---|---|
| `*--parse-json-maybe` | `json-log-viewer-shared.el`, `aws-logs-tail.el`, `aws-logs-insights.el`, `kube-logs.el`, `kafka-logs.el` |
| `*--value->string` | `json-log-viewer-shared.el`, `aws-logs-insights.el`, `kafka-logs.el` |
| `*--level-face` | `json-log-viewer.el`, `aws-logs-insights.el` |
| `*--parse-time` | `json-log-viewer.el`, `json-log-viewer-async-worker.el` |
| `*--alist-like-p` | `json-log-viewer-shared.el`, `kafka-logs.el` |
| `*--normalize-json-value` | `json-log-viewer-shared.el`, `kafka-logs.el` |
| `*--truncate` | `json-log-viewer.el`, `aws-logs-insights.el` |

The shared helpers in `json-log-viewer-shared.el` exist precisely for this purpose. The source adapters could require and use them instead of maintaining private copies. This isn't a bug, but it's a maintenance risk -- if you fix a parsing edge case in one copy, the others remain stale.

### 4. `aws-logs-insights.el` Bypasses Shared Viewer Abstractions

`aws-logs-insights.el` implements its own summary rendering pipeline (`aws-logs--insights-summary`, field resolution, level faces) rather than using the viewer's built-in path-based rendering. This made sense historically but now that `json-log-viewer-make-buffer` supports `:timestamp-path`, `:level-path`, etc., the Insights adapter converts rows to synthetic `__summary_*` JSON fields and feeds them through the viewer. This works but creates a parallel code path that must be kept in sync.

### 5. Unused Parameters and Dead Code

- `json-log-viewer--storage-put-entry-details` (`json-log-viewer.el:258`): Is a no-op "compatibility" stub. The parameters are prefixed with `_` but the function is still called from `json-log-viewer--insert-entry`. Could be removed along with the call site.
- `json-log-viewer--storage-remove-entry-details-batch` (`json-log-viewer.el:261`): Another no-op stub with no callers.
- `json-log-viewer--make-async-job` (`json-log-viewer.el:421`): Compatibility wrapper with an unused `_preserve-filter` parameter. Only referenced internally.
- `async-job-queue--worker-main` (`async-job-queue.el:284`): Standalone worker entrypoint that duplicates the logic already inlined in `async-job-queue--worker-eval-form`. Appears unused.

### 6. Potential Resource Leak in Composite Buffer

`composite-json-log-viewer.el` requires the `async` package (`(require 'async)`) and uses `async-start` for backfill chunks. If the composite buffer is killed mid-backfill, `composite-json-log-viewer--cleanup` unsubscribes from sources and resets counts, but doesn't cancel in-flight `async-start` processes. The async callback checks `(buffer-live-p composite-buffer)`, so it won't crash, but the subordinate Emacs processes may linger until completion.

### 7. `json-log-viewer.el` Size

At 1,873 lines, this file handles buffer management, overlay rendering, filtering, streaming, SQLite coordination, async queue management, UI rendering, and subscriber notifications. Some of this (particularly the async queue coordination and streaming eviction logic) could be extracted to keep the file more focused.

## Test Coverage

- **58/60 tests pass** (2 failures described above).
- **Core viewer**: Well-tested -- path resolution, SQLite storage, narrowing, streaming, eviction, auto-follow.
- **Source adapters**: Arg-building and line normalization tested; no subprocess integration tests.
- **Composite buffer**: Good async integration tests with real queue processing.
- **Gap**: No error-path testing (malformed JSON, subprocess failures, auth errors). No stress/concurrency tests.

## Security Considerations

- **Shell injection via grep filters**: `aws-logs-filter`, `kube-logs-filter`, and `kafka-logs-filter` are passed through `shell-quote-argument` before being interpolated into shell commands. This is correct.
- **SQLite temp files**: Created with `make-temp-file` and cleaned up on buffer kill. WAL mode with busy timeout is appropriate for concurrent worker access.
- **Credential handling**: `kafka-logs.el` resolves credentials via `auth-source` correctly, and passwords flow through `auth-source-search` rather than being stored in plain text variables (though `:password` in the connection plist is also supported as a direct string, which users should be cautioned about).

## Summary

| Area | Rating |
|---|---|
| Architecture | Excellent |
| Code conventions | Very good |
| Test coverage | Good (core), sparse (adapters) |
| Code duplication | Moderate concern |
| Dead code | Minor |
| Security | Good |

**Priority fixes:**
1. Fix the 2 failing tests
2. Add `declare-function` for `org-read-date` in `kafka-logs.el`
3. Remove dead no-op stubs

**Recommended improvements:**
1. Consolidate duplicated utility functions to use `json-log-viewer-shared.el`
2. Cancel in-flight async backfill processes on composite buffer kill
3. Consider splitting `json-log-viewer.el` (extract streaming/eviction or async coordination)
