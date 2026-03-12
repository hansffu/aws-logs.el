# Async Await Hard-Blocks the UI Thread for Up to 15 Seconds

**Priority:** High
**File:** `json-log-viewer.el`, lines 342–346, 566
**Also:** `json-log-viewer.el`, line 1494

---

## Description

`json-log-viewer--async-await-pending-count` spins in a busy-wait loop until
the worker's pending job count drops to a target value or a 15-second deadline
is reached. This is called synchronously on the main thread during buffer
operations such as `narrow` and `rerender`. For large datasets or a slow worker,
this can freeze Emacs entirely for up to 15 seconds with no progress indicator.

---

## Reproduction

1. Open a CloudWatch log group with a large number of entries (e.g. 10,000+).
2. Trigger a narrowing operation (`zn` or `json-log-viewer-narrow`) with a
   broad filter string that matches many rows.
3. Observe that Emacs is completely unresponsive (no cursor movement, no
   minibuffer, no other buffers) until the wait completes.

The problem is more severe when:
- The worker is performing a CPU-heavy operation (JSON flattening + SQLite
  write for many entries).
- The machine is under load.
- The `json-log-viewer-stream-max-entries` cap hasn't been reached yet
  (more entries = longer worker processing time).

---

## Root Cause

`json-log-viewer.el`, lines 342–346:

```elisp
(defun json-log-viewer--async-await-pending-count (target-count)
  "Block until pending queue count reaches TARGET-COUNT or timeout."
  (let ((deadline (+ (float-time) 15.0)))
    (while (and (> (json-log-viewer--async-pending-count) target-count)
                (< (float-time) deadline))
      (accept-process-output nil 0.05))))
```

`accept-process-output` with a 0.05-second timeout does yield to the process
filter (so output is received), but Emacs does not redisplay or process keyboard
input between iterations. From the user's perspective, this is equivalent to a
blocking call.

This function is called at `json-log-viewer.el` line 566 during the `narrow`
operation:

```elisp
(let ((before (json-log-viewer--async-pending-count)))
  ;; ... push narrow job ...
  (json-log-viewer--async-await-pending-count before))
```

---

## Impact

- **UX**: Emacs appears completely hung. Users may force-quit or assume a crash.
- **Composability**: No other buffers, timers, or async operations can run
  during the wait. Any overlapping `aws logs tail` streams also stall.
- **Reliability**: The 15-second timeout is arbitrary. Legitimate slow operations
  are silently abandoned after the timeout, leaving the viewer in a partially
  rendered state.

---

## Suggested Fix

### Option A — Remove the synchronous wait entirely

The narrow job is already async. Instead of waiting for it to finish before
returning, display a "Narrowing..." header line or spinner immediately, and let
the render callback update the buffer when done. This is the architecturally
correct approach since the rest of the rendering pipeline is already
callback-driven.

### Option B — Shorten timeout and add a visible indicator

If the synchronous flush is retained (e.g. for simplicity), reduce the timeout
to 2–3 seconds and display a message in the minibuffer:

```elisp
(message "Narrowing log entries...")
(json-log-viewer--async-await-pending-count before)
(message nil)
```

Also make the timeout configurable via a `defcustom`:

```elisp
(defcustom json-log-viewer-async-timeout-seconds 15.0
  "Max seconds to wait for async worker during synchronous operations."
  :type 'number
  :group 'json-log-viewer)
```

### Option C — Use `with-timeout` instead of manual deadline loop

```elisp
(with-timeout (json-log-viewer-async-timeout-seconds
               (message "Warning: async worker timed out"))
  (while (> (json-log-viewer--async-pending-count) target-count)
    (accept-process-output nil 0.05)))
```

`with-timeout` at least allows the timeout to be visible and does not silently
abandon work.
