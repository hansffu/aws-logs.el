# Out-of-Order Entries Corrupt Display When Source Ordering Is Not Guaranteed

**Priority:** Medium
**File:** `json-log-viewer.el`, lines 147–154, 174–178, 802–875

---

## Description

The viewer maintains a sorted display by inserting each new entry at the correct
position relative to existing overlays, using a `sort-key` (derived from
`timestamp_epoch`). This insertion logic assumes that entries arrive
approximately in order. When `json-log-viewer--stream-assume-ordered` is `t`,
the viewer optimizes by always appending/prepending without a position search.

When this flag is set but the source delivers entries out of order (e.g. Kafka
multi-partition fanout, backfill + live merge, or a log source with clock skew),
entries are placed in the wrong visual position. The buffer then displays logs
in an incorrect order that does not match the SQLite store. Subsequent
narrow/rerender operations restore the correct order from SQLite, but the live
streaming view remains corrupted between refreshes.

---

## When This Occurs

The flag `json-log-viewer--stream-assume-ordered` is set to `t` in sources that
stream live (append/prepend mode). Any source with these characteristics can
trigger the issue:

- **Kafka**: messages from different partitions arrive interleaved with no
  global ordering guarantee.
- **kubectl logs**: pod restarts can cause timestamp regression (new container
  starts from epoch 0 until NTP syncs).
- **aws logs tail**: multiple log streams merged by the CLI can have slight
  clock drift between hosts.
- **Backfill + live merge** (composite viewer pattern): historical entries
  prepended while live entries are appended — a common race condition during
  startup.

---

## Root Cause

`json-log-viewer.el`, lines 174–178:

```elisp
(defvar-local json-log-viewer--stream-assume-ordered nil
  "When non-nil, assume entries arrive in sort-key order.
Used by streaming sources to avoid per-entry position search.")
```

When this is set, the render path skips the binary-search insertion and always
places the new entry at the head or tail of the overlay list. If an entry with
an earlier timestamp arrives after entries with later timestamps, it is placed
at the wrong end.

The `sort-key` comparison function at lines 865–875 is correct, but it is only
used when `stream-assume-ordered` is `nil`.

---

## Impact

- **Correctness**: The on-screen order of log entries does not match the actual
  event timeline. Users may draw incorrect conclusions about causality from
  the display.
- **Silent**: No warning is emitted when an out-of-order entry is detected.
- **Inconsistency between view and store**: The SQLite store always orders by
  `(timestamp_epoch, id)`, so after a narrow/rerender the display corrects
  itself. This creates a confusing "it was wrong, now it's right" experience.

---

## Suggested Fix

### Option A — Detect and correct out-of-order entries in streaming mode

When `stream-assume-ordered` is set and a new entry's sort-key is less than
the most recently inserted entry's sort-key, fall back to the sorted-insertion
path for that entry:

```elisp
(if (and json-log-viewer--stream-assume-ordered
         (json-log-viewer--sort-key<
          (json-log-viewer--json-entry-sort-key entry)
          json-log-viewer--last-stream-sort-key))
    ;; out-of-order: use full sorted insertion
    (json-log-viewer--insert-entry-sorted entry)
  ;; in-order fast path
  (json-log-viewer--append-entry entry))
```

Track `json-log-viewer--last-stream-sort-key` as a buffer-local variable
updated after each append.

### Option B — Remove `stream-assume-ordered` entirely

The performance win from skipping position search is only significant for very
high-throughput streams. The sorted-insertion path uses a linear scan which is
acceptable for typical batch sizes. Removing the flag simplifies the code and
eliminates the correctness risk.

### Option C — Only set `stream-assume-ordered` for sources with guaranteed ordering

Audit each source adapter and only enable the flag for `aws-logs-tail.el`
(single log stream, CloudWatch guarantees per-stream ordering). Do not set it
for Kafka or composite sources.
