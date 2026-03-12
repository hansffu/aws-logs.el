# Worker Bootstrap Embeds Entire Program as a `--eval` CLI Argument

**Priority:** Low
**File:** `async-job-queue.el`, lines 72–188

---

## Description

The async worker process is bootstrapped by serializing the entire worker
program (process function, init function, teardown function) as a Lisp
expression, base64-encoding the function bodies, embedding them in a `let*`
form, and passing the whole thing as a single `--eval` argument to a child
Emacs process:

```elisp
(list (async-job-queue--emacs-program)
      "-Q" "--batch"
      "--eval" eval-form)   ; eval-form is a >1KB string
```

This approach works and is safe (the round-trip serialization is validated before
launch), but it has several practical drawbacks.

---

## Problems

### 1. Command-line argument length limits

On some systems (particularly those using older kernels or non-Linux platforms),
the maximum length of a single command-line argument is limited (commonly 128KB
on Linux via `MAX_ARG_STRLEN`, but can be as low as 4KB in some environments).
For queues with large `init-func` closures (e.g. one that captures a large
lookup table or configuration plist), the base64-encoded `--eval` argument can
exceed these limits, causing a silent `make-process` failure.

### 2. Debuggability

When the worker crashes, the error is reported against a synthetic `--eval`
form with no source file or line number. Stack traces reference anonymous lambdas
reconstructed from `read`. This makes debugging worker-side errors significantly
harder than if the worker were a named `.el` file.

### 3. Readability / Maintainability

`async-job-queue--worker-eval-form` generates code dynamically (a `let*` with
`defvar`, `defun`, and a `while` loop). This is the most complex part of the
codebase and is entirely untestable in isolation — you cannot load the worker
bootstrap code into a REPL and inspect it. Any change to the worker protocol
requires regenerating and re-reading the generated form.

### 4. `prin1-to-string` / `read` roundtrip fragility

Function serialization relies on `prin1-to-string` producing a form that `read`
can reconstruct into an equivalent function. This works for simple closures but
fails for:
- Closures that capture byte-compiled functions (not always readable).
- Functions defined with `cl-defun` that use `&key` arguments (some internal
  forms are not `read`-safe).
- Functions that reference symbols not available in the worker's minimal
  environment (`-Q` suppresses all init files and packages).

---

## Suggested Fix

### Extract the worker event loop into a standalone file

Move the worker REPL (the `while t` loop, `defvar` declarations, and
`defun async-job-queue-worker-publish`) into a new file:
`async-job-queue-worker.el`.

Launch the worker with:

```elisp
(list (async-job-queue--emacs-program)
      "-Q" "--batch"
      "-L" load-path-entry
      "-l" "async-job-queue-worker"
      "--eval" startup-form)   ; much smaller: just sets the 3 function names
```

The `startup-form` would only need to pass the 3 encoded function bodies, not
embed the entire event loop. This:
- Eliminates the `MAX_ARG_STRLEN` risk.
- Gives debuggable stack traces with file/line information.
- Makes the worker code independently testable and readable.
- Allows byte-compilation of the worker for better performance.

This is a non-trivial refactor but has no user-visible behavior change.
It is most valuable to do before adding more complex worker-side logic.
