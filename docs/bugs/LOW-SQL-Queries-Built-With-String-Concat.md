# SQL Queries Built with String Concatenation Are Hard to Read and Maintain

**Priority:** Low
**File:** `json-log-viewer-repository.el`, lines 120–198

---

## Description

SQL query strings in `json-log-viewer-repository.el` are constructed by
concatenating string literals with `concat`. While all user-supplied values are
correctly passed as parameterized vector arguments (so there is no SQL injection
risk), the formatting makes the queries difficult to read and compare.

Example from `json-log-viewer-repository-select-logs-before` (lines 121–128):

```elisp
(sqlite-select
 db
 (concat
  "SELECT id, timestamp_epoch, json FROM log_entry "
  "WHERE timestamp_epoch < ? "
  "ORDER BY timestamp_epoch DESC, id DESC LIMIT ?")
 (vector timestamp limit))
```

The same logical query appears in 4 variants across the function (with/without
`timestamp`, with/without `limit`), each written as a separate `concat` block.
This makes it hard to spot differences between the variants and to verify that
all four handle `NULL` timestamp rows consistently.

---

## Impact

- **Readability**: Multi-line `concat` SQL is harder to scan than a single
  multi-line string literal.
- **Maintainability**: Adding a new column to the SELECT list requires editing
  every variant independently. A missed update causes a column mismatch.
- **Consistency risk**: The four `SELECT` branches in
  `json-log-viewer-repository-select-logs-before` have slightly different
  `ORDER BY` clauses. The `CASE WHEN timestamp_epoch IS NULL` guard is present
  in some branches but not others, which may be intentional but is not obvious
  from the current formatting.

---

## Example of the Inconsistency

Branch with `timestamp + limit` (lines 121–128):
```sql
ORDER BY timestamp_epoch DESC, id DESC LIMIT ?
```
— no NULL guard.

Branch with `limit` only (lines 137–146):
```sql
ORDER BY
  CASE WHEN timestamp_epoch IS NULL THEN 1 ELSE 0 END,
  timestamp_epoch DESC, id DESC LIMIT ?
```
— has NULL guard.

If this difference is intentional (NULL rows are never present when a timestamp
is provided), a comment explaining this would prevent future regressions.

---

## Suggested Fix

### Use multi-line string literals instead of `concat`

Emacs Lisp supports multi-line strings natively. Prefer:

```elisp
(sqlite-select
 db
 "SELECT id, timestamp_epoch, json
  FROM log_entry
  WHERE timestamp_epoch < ?
  ORDER BY timestamp_epoch DESC, id DESC
  LIMIT ?"
 (vector timestamp limit))
```

### Extract shared SELECT clause into a constant

```elisp
(defconst json-log-viewer-repository--entry-columns
  "id, timestamp_epoch, timestamp, level_path, message_path, extra_paths")
```

Use it in all queries to ensure column lists stay in sync.

### Add comments for intentional NULL-guard omissions

Where the `CASE WHEN timestamp_epoch IS NULL` guard is intentionally absent,
add a brief comment:

```elisp
;; timestamp is always non-nil here (caller provides explicit value),
;; so no NULL guard is needed in ORDER BY
```
