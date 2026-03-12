# Stale Overlay References Accumulate in `json-log-viewer--entry-overlays`

**Priority:** Medium
**File:** `json-log-viewer.el`, lines 117, 946–953, 1082, 1697, 1733–1757

---

## Description

`json-log-viewer--entry-overlays` is a buffer-local list that holds every entry
overlay created since the last `clear`. Individual overlays are deleted via
`delete-overlay` in some code paths (e.g. when trimming old entries at the
stream cap, line 1755), but the deleted overlays are not always pruned from
the list immediately. The list can therefore accumulate references to overlays
that no longer exist in the buffer, causing unnecessary iteration over dead
objects and intermittent correctness issues in functions that traverse the list.

---

## Affected Code Paths

**Overlay deletion without list cleanup** (`json-log-viewer.el`, lines 1733–1757):

```elisp
(let ((kept nil))
  (dolist (entry-overlay json-log-viewer--entry-overlays)
    (if (member entry-overlay to-delete)
        (progn
          (delete-overlay entry-overlay)  ; deleted from buffer...
          (when-let ((fold-ov ...))
            (delete-overlay fold-ov)))
      (push entry-overlay kept)))         ; ...but the kept/discarded split
  (setq json-log-viewer--entry-overlays kept))  ; this is actually correct here
```

The trim loop above is actually correct — it rebuilds `kept`. However,
`json-log-viewer--entry-overlays-in-buffer-order` at line 1099 does **not**
filter dead overlays:

```elisp
(dolist (entry-ov json-log-viewer--entry-overlays)
  ...)  ; no (overlay-buffer entry-ov) check
```

If an overlay is deleted outside the trim path (e.g. buffer killed partially,
fold overlay cleanup, or external package interaction), dead references remain
in the list until the next full `clear`.

Additionally, `json-log-viewer--clear-overlays` (line 946) calls
`(mapc #'delete-overlay ...)` and then sets the list to `nil` — this is the
only reliable cleanup point. Any code that deletes individual overlays between
clears leaves the list transiently inconsistent.

---

## Impact

- **Correctness**: Functions like `json-log-viewer--entry-overlays-in-buffer-order`
  and the current-line tracker iterate over all entries including dead ones.
  A dead overlay returns `nil` for `overlay-start`/`overlay-end`, which can
  cause `sort` to misbehave or movement commands to position point incorrectly.
- **Performance**: Long-running streaming buffers that hit the
  `json-log-viewer-stream-max-entries` cap repeatedly trim overlays. Over time,
  if any trim path leaks references, the list grows unbounded and iteration
  becomes O(n) where n includes discarded entries.
- **GC pressure**: Dead overlay objects pinned by the list are not eligible for
  garbage collection.

---

## Suggested Fix

### Option A — Filter dead overlays lazily in the traversal helper

In `json-log-viewer--entry-overlays-in-buffer-order`, skip overlays whose
buffer has been deleted:

```elisp
(dolist (entry-ov json-log-viewer--entry-overlays)
  (when (overlay-buffer entry-ov)   ; nil if overlay was deleted
    ...))
```

This is low-cost (one predicate call per overlay) and prevents all downstream
misuse without requiring changes to every deletion site.

### Option B — Centralize all overlay deletion through a helper

Replace direct `delete-overlay` calls with a wrapper that also removes the
overlay from `json-log-viewer--entry-overlays`:

```elisp
(defun json-log-viewer--delete-entry-overlay (ov)
  (delete-overlay ov)
  (setq json-log-viewer--entry-overlays
        (delq ov json-log-viewer--entry-overlays)))
```

This is more explicit but requires updating all call sites.

Option A is recommended as a minimal, safe fix. Option B is preferable long-term.
