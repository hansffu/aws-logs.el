# Startup Freeze: Emacs Hard-Blocks Until First Log Entry Arrives

**Priority:** High
**Files:** `kube-logs.el` (~537), `aws-logs-tail.el` (~604, ~629, ~673), `kafka-logs.el` (~928, ~972)
**Root cause in:** `json-log-viewer.el` (`json-log-viewer-push`, line ~2039), `async-job-queue.el` (`async-job-queue-push`, line ~350)

---

## Description

When a new log viewer buffer is opened, Emacs freezes completely (no cursor
movement, no minibuffer, no other buffers) until the first log entry arrives.
Once it unfreezes, all buffered entries appear simultaneously.

---

## Reproduction

1. Open a kube-logs buffer with a pod that has many recent log lines
   (e.g. `--tail=500` or a high-traffic pod).
2. Observe that Emacs is completely unresponsive from the moment the buffer
   opens (showing "No results.") until the first entries render.

The freeze is more severe when:
- The pod has many recent log lines (longer pipe saturation).
- The machine is slow to spawn new processes (longer worker startup time).
- Network latency to the cluster is low (kubectl delivers lines faster than
  the worker is ready).

---

## Root Cause

Opening a viewer buffer starts two processes concurrently:

1. **Async worker** (`emacs -Q --batch ...`) — spawned by
   `json-log-viewer--start-async-queue` via `async-job-queue-create`. Takes
   ~1–2 seconds to evaluate its init form (load worker file, open SQLite DB).
   Only begins reading stdin after init completes.

2. **Source subprocess** (`kubectl logs`, `aws logs tail`, `kcat`) — started
   immediately after the buffer is created, before the worker is ready.

The source subprocess connects and sends a burst of recent log lines. Each
line is processed by the source's process filter, which calls
`json-log-viewer-push` → `async-job-queue-push` → `process-send-string` to
the worker's stdin pipe. The worker is not yet reading stdin, so the OS pipe
buffer (~64 KB, roughly 100–300 JSON lines) fills up.

Once the pipe buffer is full, `process-send-string` blocks the main Emacs
thread. Emacs is frozen until the worker finishes loading and drains the pipe.
At that point all queued entries are processed and rendered at once.

The "No results." state persists throughout the freeze because no entries have
been rendered yet — they are queued in the pipe, not yet processed by the
worker.

---

## Impact

- **UX**: Emacs appears completely hung on every new log buffer open. Users
  cannot interact with any buffer during the freeze.
- **All streaming sources affected**: `kube-logs`, `aws-logs-tail`, and
  `kafka-logs` all start their subprocess immediately and are all susceptible.
- **Severity scales with log volume**: busier pods / larger `--tail` values
  cause longer freezes.

---

## Suggested Fix

Defer starting the source subprocess until the async worker signals that it
is ready to receive jobs. This eliminates pipe saturation entirely.

### Implementation outline

**Step 1 — Worker emits a ready signal from the init-func**

`json-log-viewer--make-async-queue-init-func` (`json-log-viewer.el`, line ~453)
constructs the init lambda that runs in the worker. `async-job-queue--worker-send-func`
is already set before the init-func is called (see `async-job-queue.el`
line 113). The existing `(:event id payload)` message type calls the
callback with the payload directly (id is ignored). So the init-func can
emit a ready signal with no changes to `async-job-queue.el`:

```elisp
;; At the end of the init lambda, after json-log-viewer-async-worker-init:
(funcall async-job-queue--worker-send-func '(:event nil (:worker-ready t)))
```

**Step 2 — Main thread intercepts the ready signal**

The callback lambda in `json-log-viewer--start-async-queue` (`json-log-viewer.el`,
line ~510) handles all worker output. Add a guard for `:worker-ready` before
the normal result path, and invoke a buffer-local ready hook:

```elisp
(cond
 ((and (listp result) (plist-member result :worker-ready))
  (when json-log-viewer--on-worker-ready
    (funcall json-log-viewer--on-worker-ready)
    (setq json-log-viewer--on-worker-ready nil)))
 (t
  ;; existing result handling ...
  ))
```

Add a buffer-local variable:
```elisp
(defvar-local json-log-viewer--on-worker-ready nil
  "Function called once when the async worker signals readiness.")
```

**Step 3 — `json-log-viewer-make-buffer` accepts `:on-ready`**

```elisp
(cl-defun json-log-viewer-make-buffer (buffer-name &key ... on-ready)
  ...
  (setq-local json-log-viewer--on-worker-ready on-ready)
  ...)
```

**Step 4 — Sources defer subprocess start to the `:on-ready` callback**

Example for `kube-logs--run-stream`:

```elisp
(defun kube-logs--run-stream ()
  (let* ((args (kube-logs--logs-args))
         (command (kube-logs--command-with-filter args t))
         (buffer nil)
         (on-ready
          (lambda ()
            (let ((process (make-process
                            :name (kube-logs--process-name)
                            :buffer buffer
                            :command command
                            :noquery t
                            :connection-type 'pipe)))
              (with-current-buffer buffer
                (setq-local kube-logs--process process))
              (set-process-filter process #'kube-logs--stream-process-filter)
              (set-process-sentinel process #'kube-logs--stream-process-sentinel)
              (set-process-query-on-exit-flag process nil)
              (message "Started kube logs stream for %s"
                       (kube-logs--target-description))))))
    (setq buffer (kube-logs--make-viewer-buffer on-ready))
    (with-current-buffer buffer
      (setq-local kube-logs--pending-fragment "")
      (display-buffer buffer))))
```

The same pattern applies to `aws-logs-tail.el` and `kafka-logs.el`.
