# Transient State Can Diverge from Session Variables

**Priority:** Medium
**File:** `aws-logs.el`, lines 800–835

---

## Description

Session state in `aws-logs.el` lives in two places simultaneously:

1. **Session variables** (`aws-logs-log-group`, `aws-logs-profile`,
   `aws-logs-filter`, etc.) — the runtime state used by `aws-logs-tail-run`
   and `aws-logs-insights-run`.
2. **Transient infix values** — what the user sees in the transient menu UI.

These two representations are only synchronized at the moment a suffix action
fires (`aws-logs-tail` or `aws-logs-insights`), via
`aws-logs--sync-session-from-transient`. Outside of a live transient invocation,
the two can silently diverge.

---

## Reproduction Scenarios

### Scenario 1 — Programmatic update not reflected in transient

A user calls `(setq aws-logs-log-group "/aws/lambda/my-fn")` from
`*scratch*` or an init file. They then open `aws-logs-transient`. The transient
reads its initial values from the session variables at popup time (via
`:init-value` callbacks), but if those callbacks read the `defvar` default
rather than the current `defvar` binding, the displayed value will not match
the updated session variable.

### Scenario 2 — Preset load bypasses transient sync

`aws-logs-load-preset` updates session variables directly. If the transient is
already open (or was previously cached by transient), it does not re-read the
session variables. The user sees stale infix values until they close and reopen
the menu.

### Scenario 3 — `aws-logs--sync-session-from-transient` is a no-op outside transient

`aws-logs--sync-session-from-transient` checks
`(eq transient-current-command 'aws-logs-transient)`. If called from
`M-x aws-logs-tail` directly (bypassing the transient), the sync is skipped
entirely and stale session variables are used:

```elisp
(defun aws-logs--sync-session-from-transient ()
  (when (and (boundp 'transient-current-command)   ; guard
             (eq transient-current-command 'aws-logs-transient))
    ...))  ; no-op if not in transient
```

---

## Impact

- **Silent wrong execution**: A user who edits a session variable directly,
  then invokes tail/insights, may be querying the wrong log group or profile
  with no indication that the transient display is stale.
- **Preset confusion**: Loading a preset while the transient is open leaves the
  menu showing pre-preset values until the next full menu open.
- **No single source of truth**: It is not clear to users (or maintainers)
  whether the session variables or the transient infix values are authoritative.

---

## Suggested Fix

### Establish session variables as the single source of truth

Make the transient infix objects read from session variables every time the
transient is opened, rather than relying on the sync-on-action approach:

- Ensure all `:init-value` callbacks in `transient-define-infix` forms read the
  current session variable, not the `defcustom` default.
- When a preset is loaded, close and reopen the transient (or provide a
  `transient-reset` call) so infix values reflect the new state.

### Document the data flow explicitly

Add a comment at the top of the session variable block in `aws-logs.el`
explaining:

> Session variables are the authoritative runtime state. Transient infix fields
> are a UI projection of session variables. They are read into session variables
> at action time via `aws-logs--sync-session-from-transient`.

This prevents future contributors from adding session-variable mutations that
bypass the transient and expecting them to be reflected in the UI.
