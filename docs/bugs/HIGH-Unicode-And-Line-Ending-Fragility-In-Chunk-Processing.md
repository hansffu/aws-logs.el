# Unicode and Line-Ending Fragility in Chunk Processing

**Priority:** High
**File:** `aws-logs-tail.el`, lines 259, 299, 359–363
**Also affects:** `kube-logs.el` (uses same `split-string "\n"` pattern)

---

## Description

The ECS tail pipeline assembles process filter output chunks into log lines
using `split-string` on `\n`. Two related issues can silently corrupt or drop
log entries:

1. **`\r\n` line endings**: Sources that emit CRLF (Windows-style) line endings
   produce lines with a trailing `\r`. The `string-trim-right` at line 259
   handles this for one code path, but `aws-logs--tail-consume-chunk-lines`
   at line 363 uses a raw `split-string` split with no CRLF normalization. Any
   `\r` that survives becomes part of the JSON string, causing `json-parse-string`
   to fail silently.

2. **Multi-byte character split across chunk boundaries**: Emacs process filters
   receive output in arbitrary byte chunks. A multi-byte UTF-8 sequence (e.g. a
   3-byte emoji or Japanese character) can be split across two consecutive
   filter invocations. When the incomplete byte sequence is passed to
   `json-parse-string`, it raises a decoding error and the entire line is
   dropped.

---

## Reproduction

### `\r\n` issue

Route a CloudWatch log group that emits CRLF JSON through `aws logs tail`
(common with .NET/Windows workloads or when using a log forwarder that adds
`\r\n`). Observe that narrowing or field extraction fails because the stored
JSON contains literal `\r` characters embedded in string values or at the end
of the JSON object.

### Multi-byte split

Enable a log stream with high-throughput messages containing non-ASCII
characters (e.g. Japanese text). Set `aws-logs-tail-chunk-size` to a small
value to increase split probability. Observe occasional dropped lines in the
viewer that can be reproduced consistently.

---

## Root Cause

**`aws-logs-tail.el`, line 363:**

```elisp
(defun aws-logs--tail-consume-chunk-lines (chunk)
  ;; ...
  (let* ((combined (concat aws-logs--tail-partial-line chunk))
         (parts (split-string combined "\n")))   ; <-- no \r handling
```

**`aws-logs-tail.el`, line 299 (comint path):**

```elisp
(split-string output "\n" t)   ; <-- no \r handling, no multi-byte guard
```

Emacs process filters receive raw bytes decoded by the process's coding system.
If the process is started without an explicit `:coding` argument, Emacs uses the
system default, which may not be `utf-8-unix`. Even with UTF-8 coding, the
filter can be called with a partial multi-byte sequence if the OS pipe flushes
mid-character.

---

## Impact

- **Data loss**: lines containing non-ASCII characters may be silently dropped
  in high-throughput streams.
- **Corrupted storage**: `\r` characters stored in SQLite JSON break
  `json-parse-string` in subsequent narrowing/rerender operations, causing
  those entries to be silently excluded from query results.
- **Hard to diagnose**: errors in the async worker are swallowed
  (`:error` messages are not surfaced to the user by default).

---

## Suggested Fix

1. **Explicit UTF-8 coding**: pass `:coding 'utf-8-unix` to `make-process` in
   `aws-logs-tail.el` so Emacs handles CRLF normalization and multi-byte
   assembly before the filter receives data.

2. **Explicit CRLF strip in chunk consumer**: in
   `aws-logs--tail-consume-chunk-lines`, replace `\r\n` before splitting:
   ```elisp
   (let* ((combined (concat aws-logs--tail-partial-line
                            (replace-regexp-in-string "\r" "" chunk)))
          (parts (split-string combined "\n")))
   ```

3. **Guard json-parse-string**: wrap the JSON parse call in a
   `condition-case` and log a warning (rather than silently dropping) when
   decoding fails, so corrupted lines are visible in a `*Messages*`-style
   buffer.
