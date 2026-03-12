# Worker Init Error Kills Process and Loses All Pending Jobs

**Priority:** High
**File:** `async-job-queue.el`, lines 114–121
**Also affects:** `json-log-viewer-async-worker.el` (init-func caller)

---

## Description

When the worker's `init-func` raises an error, the worker process sends a
`:lifecycle-error` message and immediately calls `(kill-emacs 0)`. Any jobs
already pushed to the queue before the init error is detected are permanently
lost — their callbacks are never called and no error is surfaced to the user.

Additionally, a single job-processing error (line 130–131) is currently handled
gracefully (`:error` is sent, worker continues), but an error raised *outside*
`funcall process-func` — for example inside the `pcase` dispatch (line 141–142)
— also fires `:lifecycle-error` without `kill-emacs`. That inconsistency means
some lifecycle errors crash the worker while others do not.

---

## Reproduction

1. Create a queue with an `init-func` that conditionally raises an error:
   ```elisp
   (async-job-queue-create
     (lambda (x) x)
     (lambda (result) (message "result: %S" result))
     :init-func (lambda () (error "DB not available")))
   ```
2. Push several jobs before the main process detects the `:lifecycle-error`
   callback.
3. Observe: jobs are silently dropped; no error is reported per job.

---

## Root Cause

`async-job-queue.el`, lines 114–121:

```elisp
(condition-case init-err
    (when init-func
      (funcall init-func))
  (error
   (funcall send (list :lifecycle-error
                       (format "INIT-FUNC failed: %s"
                               (error-message-string init-err))))
   (kill-emacs 0)))   ; <-- terminates immediately, all queued jobs lost
```

The main process receives `:lifecycle-error` via the process filter, but by
then the process is already dead. The callback dispatch for pending jobs
(`async-job-queue--pending` list) is never flushed. Pending callbacks are
leaked and the user sees no indication that their log ingestion silently stopped.

---

## Impact

- **Correctness:** Log entries pushed immediately after queue creation (common
  during buffer setup) are silently dropped if the SQLite `open` or WAL
  `PRAGMA` fails.
- **Debuggability:** The only error surface is a `:lifecycle-error` payload
  received asynchronously. If no handler inspects it, the failure is invisible.
- **UX:** The viewer buffer appears to work (no error thrown in the UI) but
  never populates.

---

## Suggested Fix

1. **In the main process**: when a `:lifecycle-error` is received, iterate over
   all pending callbacks and call them with an error sentinel so callers can
   surface the failure.

2. **Worker init failure**: instead of `kill-emacs` after a non-fatal init
   error, consider whether the queue can be restarted. If not, at least flush
   `:error` for every pending job ID before exiting.

3. **Document the invariant**: make explicit in the docstring of
   `async-job-queue-create` that an init-func failure is fatal to the queue
   and all pending work is discarded.

```elisp
;; In callback dispatch (main process), on :lifecycle-error:
(dolist (pending async-job-queue--pending)
  (funcall callback-func (list :error (car pending) "Worker lifecycle error")))
(setq async-job-queue--pending nil)
```
