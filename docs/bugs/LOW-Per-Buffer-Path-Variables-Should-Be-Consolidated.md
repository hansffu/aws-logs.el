# Per-Buffer Path Variables Should Be Consolidated into a Single Config Plist

**Priority:** Low
**File:** `json-log-viewer.el`, lines 183–195, 537–541

---

## Description

The viewer stores per-buffer path configuration in five separate buffer-local
variables:

```elisp
(defvar-local json-log-viewer--timestamp-path nil ...)
(defvar-local json-log-viewer--level-path nil ...)
(defvar-local json-log-viewer--message-path nil ...)
(defvar-local json-log-viewer--extra-paths nil ...)
(defvar-local json-log-viewer--json-paths nil ...)
```

These five variables are always set together at buffer-creation time and always
read together when building the worker job payload (lines 537–541). They are
never set or read independently. This creates unnecessary API surface and
fragility: adding a new path-type field requires touching variable declaration,
initialization, worker-job construction, and any code that inspects paths.

---

## Impact

- **Maintenance burden**: Adding a new path type (e.g. a `trace-id-path`) requires
  changes in at least 4 places: `defvar-local`, `json-log-viewer-make-buffer`
  call sites, the worker job plist construction, and any code that reads the
  variable.
- **API verbosity**: `json-log-viewer-make-buffer` already accepts these as
  keyword arguments — mapping them to 5 separate variables instead of one plist
  is redundant.
- **Documentation**: Each variable has its own docstring that largely duplicates
  the others. A single config variable with a structured docstring is cleaner.

---

## Affected Code

Worker job construction at lines 537–541 copies all five into a plist anyway:

```elisp
(list :timestamp-path json-log-viewer--timestamp-path
      :level-path     json-log-viewer--level-path
      :message-path   json-log-viewer--message-path
      :extra-paths    json-log-viewer--extra-paths
      :json-paths     json-log-viewer--json-paths)
```

This duplication is the tell that the five variables are conceptually one unit.

---

## Suggested Fix

Replace the five variables with a single buffer-local config plist:

```elisp
(defvar-local json-log-viewer--path-config nil
  "Plist of path configuration for this viewer buffer.
Keys: :timestamp-path, :level-path, :message-path, :extra-paths, :json-paths.")
```

Set it once at buffer creation:

```elisp
(setq json-log-viewer--path-config
      (list :timestamp-path timestamp-path
            :level-path     level-path
            :message-path   message-path
            :extra-paths    extra-paths
            :json-paths     json-paths))
```

Pass it directly to the worker job:

```elisp
json-log-viewer--path-config   ; instead of 5 individual key-value pairs
```

Individual path values can be accessed via `plist-get` where needed.

This is a purely internal refactor with no user-visible behavior change.
It should be done in a single commit to avoid intermediate breakage.
