;;; kube-logs.el --- Kubernetes logs transient UI -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Transient-driven Kubernetes logs viewer built on top of json-log-viewer.
;; Supports context/namespace/target selection, follow/tail/since controls,
;; and reusable presets.
;;
;;; Code:

(require 'cl-lib)
(require 'json)
(require 'subr-x)
(require 'transient)

(require 'composite-log-viewer)
(require 'json-log-viewer)

(declare-function json-log-viewer-ingest-wrapper-executable "json-log-viewer" ())
(declare-function json-log-viewer-kube-log-supervisor-executable "json-log-viewer" ())
(declare-function json-log-viewer-worker-socket-path "json-log-viewer"
                  (&optional buffer-or-name))
(declare-function json-log-viewer-run-when-ready "json-log-viewer"
                  (buffer-or-name function))
(declare-function json-log-viewer-composite-buffer-p "composite-log-viewer"
                  (&optional buffer-or-name))
(declare-function json-log-viewer-replace-log-lines "json-log-viewer"
                  (buffer-or-name log-lines &optional preserve-filter))
(declare-function json-log-viewer-register-source-config "json-log-viewer"
                  (buffer-or-name source &rest args))
(declare-function json-log-viewer-transient-bind-terminal-return "json-log-viewer"
                  (prefix command))
(declare-function json-log-viewer-unique-source-id "json-log-viewer"
                  (buffer-or-name source &rest args))

(define-derived-mode kube-logs-viewer-mode json-log-viewer-mode "Kube-Logs"
  "Major mode for Kubernetes log buffers rendered with `json-log-viewer`."
  :group 'kube-logs)

(defcustom kube-logs-kubectl "kubectl"
  "Kubectl executable used by kube-logs."
  :type 'string
  :group 'kube-logs)

(defcustom kube-logs-default-context nil
  "Default Kubernetes context for new Emacs sessions."
  :type '(choice (const :tag "Current kubectl default" nil) string)
  :group 'kube-logs)

(defcustom kube-logs-default-namespace "default"
  "Default Kubernetes namespace for new Emacs sessions."
  :type 'string
  :group 'kube-logs)

(defcustom kube-logs-default-namespace-enabled t
  "When non-nil, pass explicit --namespace/-n in kubectl commands.

When nil, kube-logs relies on kubectl's current-context namespace."
  :type 'boolean
  :group 'kube-logs)

(defcustom kube-logs-default-target-kind "pod"
  "Default workload kind for kube-logs.

Supported values are \"pod\" and \"deployment\"."
  :type '(choice (const "pod") (const "deployment"))
  :group 'kube-logs)

(defcustom kube-logs-default-target nil
  "Default pod/deployment name for new Emacs sessions."
  :type '(choice (const :tag "None" nil) string)
  :group 'kube-logs)

(defcustom kube-logs-default-follow nil
  "Default follow mode for new Emacs sessions."
  :type 'boolean
  :group 'kube-logs)

(defcustom kube-logs-default-tail-lines 200
  "Default value for --tail in kube-logs.

When nil, kube-logs does not pass --tail."
  :type '(choice (const :tag "No --tail" nil) integer)
  :group 'kube-logs)

(defcustom kube-logs-default-since nil
  "Default value for --since in kube-logs.

When nil, kube-logs does not pass --since."
  :type '(choice (const :tag "No --since" nil) string)
  :group 'kube-logs)

(defcustom kube-logs-default-filter nil
  "Default regex filter for kube logs output.

When non-nil, output is piped through grep with this regex."
  :type '(choice (const :tag "No filter" nil) string)
  :group 'kube-logs)

(defcustom kube-logs-stream-drain-interval 0.05
  "Interval in seconds between stream drain timer ticks for kube-logs."
  :type 'number
  :group 'kube-logs)

(defcustom kube-logs-stream-max-lines-per-batch 250
  "Maximum lines rendered per stream drain tick for kube-logs."
  :type 'integer
  :group 'kube-logs)

(defcustom kube-logs-stream-backend 'rust
  "Backend used for streaming kube logs.

`rust' uses the kube-rs supervisor for pod/deployment follow mode.
`kubectl' keeps the legacy kubectl logs stream."
  :type '(choice (const :tag "Rust kube-rs supervisor" rust)
                 (const :tag "kubectl logs" kubectl))
  :group 'kube-logs)

(defcustom kube-logs-stream-retry-enabled t
  "When non-nil, restart kube log follow processes after disconnects."
  :type 'boolean
  :group 'kube-logs)

(defcustom kube-logs-stream-retry-max-delay 30
  "Maximum reconnect delay in seconds for kube log follow processes."
  :type 'integer
  :group 'kube-logs)

(defcustom kube-logs-stream-retry-reset-after 60
  "Seconds after which a live kube log stream resets its retry backoff."
  :type 'integer
  :group 'kube-logs)

(defcustom kube-logs-debug-process-buffer nil
  "When non-nil, keep a process buffer for Rust supervisor diagnostics.

Supervisor stderr is always forwarded to `message'."
  :type 'boolean
  :group 'kube-logs)

(defcustom kube-logs-level-path nil
  "Path to the level field in the log JSON.
Set this to match your log format, e.g. \"payload.log.level\"."
  :type 'string
  :group 'kube-logs)

(defcustom kube-logs-message-path nil
  "Path to the message field in the log JSON.
Set this to match your log format, e.g. \"payload.message\"."
  :type 'string
  :group 'kube-logs)

(defcustom kube-logs-extra-paths '()
  "Additional paths to display in log summaries.
Set this to match your log format, e.g. \\='(\"payload.service.name\")."
  :type '(repeat string)
  :group 'kube-logs)

(defcustom kube-logs-timestamp-path "timestamp"
  "Path to the timestamp field in the log JSON.
Set this to match your log format, e.g. \"@timestamp\"."
  :type 'string
  :group 'kube-logs)

(defgroup kube-logs nil
  "Kubernetes logs transient UI and rendering."
  :group 'tools)

(defvar kube-logs-context kube-logs-default-context
  "Selected Kubernetes context for this Emacs session.")

(defvar kube-logs-namespace kube-logs-default-namespace
  "Selected Kubernetes namespace for this Emacs session.")

(defvar kube-logs-namespace-enabled kube-logs-default-namespace-enabled
  "Non-nil means kube-logs passes explicit namespace to kubectl.")

(defvar kube-logs-target-kind kube-logs-default-target-kind
  "Selected target kind for this Emacs session (\"pod\" or \"deployment\").")

(defvar kube-logs-target kube-logs-default-target
  "Selected pod/deployment name for this Emacs session.")

(defvar kube-logs-follow kube-logs-default-follow
  "Non-nil means stream logs with --follow.")

(defvar kube-logs-tail-lines kube-logs-default-tail-lines
  "Selected --tail line limit for this Emacs session, or nil.")

(defvar kube-logs-since kube-logs-default-since
  "Selected --since duration for this Emacs session, or nil.")

(defvar kube-logs-filter kube-logs-default-filter
  "Regex filter for kubectl logs output in this Emacs session, or nil.")

(defvar kube-logs-viewer-buffer nil
  "Selected json-log-viewer buffer for Kubernetes ingestion, or nil.

When nil, kube-logs creates its normal dedicated viewer buffer.")

(defvar kube-logs-presets nil
  "Alist of named kube-logs presets.

Each element has the form (NAME . PLIST).")

(defvar-local kube-logs--process nil
  "Process associated with the current kube-logs viewer buffer.")

(defvar-local kube-logs--processes nil
  "Processes associated with the current kube-logs viewer buffer.")

(defvar-local kube-logs--initialized-p nil
  "Non-nil when kube-logs local lifecycle state is installed.")

(defvar-local kube-logs--pending-fragment ""
  "Trailing incomplete process output fragment for streaming buffers.")

(defvar-local kube-logs--stream-chunks-in nil
  "LIFO queue of pending stream output chunks waiting for reversal.")

(defvar-local kube-logs--stream-chunks-out nil
  "FIFO queue of pending stream output chunks ready for draining.")

(defvar-local kube-logs--stream-pending-lines nil
  "Parsed full lines waiting to be converted and rendered.")

(defvar-local kube-logs--stream-drain-timer nil
  "Per-buffer timer used to drain queued stream data incrementally.")

(defvar-local kube-logs--stream-retry-timers nil
  "Retry timers waiting to restart kube log streams in this buffer.")

(defvar-local kube-logs--once-output-buffer nil
  "Temporary process output buffer for one-shot asynchronous fetches.")

(defvar-local kube-logs--process-log-buffer nil
  "Diagnostics buffer for the current kube-logs process.")

(defvar-local kube-logs--process-log-pending-fragment ""
  "Pending incomplete diagnostics line for the current kube-logs process.")

(defvar-local kube-logs--viewer-context nil
  "Context displayed in the current viewer buffer header.")

(defvar-local kube-logs--viewer-namespace nil
  "Namespace displayed in the current viewer buffer header.")

(defvar-local kube-logs--viewer-namespace-enabled nil
  "Namespace override state displayed in current viewer buffer header.")

(defvar-local kube-logs--viewer-target-kind nil
  "Target kind displayed in the current viewer buffer header.")

(defvar-local kube-logs--viewer-target nil
  "Target name displayed in the current viewer buffer header.")

(defvar-local kube-logs--viewer-source-id nil
  "Source ID used for source-specific rendering in composite buffers.")

(defvar-local kube-logs--viewer-follow nil
  "Follow state displayed in the current viewer buffer header.")

(defvar-local kube-logs--viewer-tail nil
  "Tail limit displayed in the current viewer buffer header.")

(defvar-local kube-logs--viewer-since nil
  "Since value displayed in the current viewer buffer header.")

(defconst kube-logs--target-kinds '("pod" "deployment")
  "Supported Kubernetes target kinds.")

(defconst kube-logs--preset-keys
  '(:context :namespace :namespace-enabled :target-kind :target :follow :tail-lines :since :filter)
  "Allowed keys for kube-logs presets.")

(defun kube-logs--setup-main-transient ()
  "Set up the main kube-logs transient."
  (transient-setup 'kube-logs-transient)
  (json-log-viewer-transient-bind-terminal-return
   'kube-logs-transient 'kube-logs-action-run))

(defun kube-logs--setup-formatting-transient ()
  "Set up the kube-logs formatting transient."
  (transient-setup 'kube-logs-formatting-transient)
  (json-log-viewer-transient-bind-terminal-return
   'kube-logs-formatting-transient 'kube-logs-formatting-done))

(defun kube-logs--transient-reprompt ()
  "Refresh transient so UI reflects current backing fields."
  (transient-quit-one)
  (kube-logs--setup-main-transient))

(defun kube-logs--context-args (&optional context)
  "Return kubectl context args for CONTEXT or current session context."
  (let ((ctx (or context kube-logs-context)))
    (if (and ctx (not (string-empty-p ctx)))
        (list (format "--context=%s" ctx))
      nil)))

(defun kube-logs--namespace-display (namespace-enabled namespace)
  "Return human-readable namespace label.

NAMESPACE-ENABLED and NAMESPACE are explicit values from state."
  (if namespace-enabled
      (or namespace "-")
    "(context default)"))

(defun kube-logs--run-kubectl-lines (args)
  "Run kubectl with ARGS and return output lines.

Signals `user-error' on failure."
  (with-temp-buffer
    (let* ((exit-code (apply #'call-process kube-logs-kubectl nil t nil args))
           (output (string-trim-right (buffer-string))))
      (unless (zerop exit-code)
        (user-error "kubectl failed (%s): %s"
                    exit-code
                    (if (string-empty-p output) "no output" output)))
      (split-string output "\n" t))))

(defun kube-logs--list-contexts ()
  "Return available kube contexts."
  (kube-logs--run-kubectl-lines '("config" "get-contexts" "-o" "name")))

(defun kube-logs--list-namespaces ()
  "Return available namespaces for current context."
  (kube-logs--run-kubectl-lines
   (append (kube-logs--context-args)
           '("get" "namespaces"
             "-o"
             "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}"))))

(defun kube-logs--resource-plural ()
  "Return kubectl resource plural for `kube-logs-target-kind'."
  (pcase kube-logs-target-kind
    ("pod" "pods")
    ("deployment" "deployments")
    (_ (user-error "Unsupported target kind: %s" kube-logs-target-kind))))

(defun kube-logs--target-ref ()
  "Return kubectl logs target reference (e.g. pod/name)."
  (unless (and kube-logs-target-kind (member kube-logs-target-kind kube-logs--target-kinds))
    (user-error "Unsupported target kind: %s" kube-logs-target-kind))
  (unless (and kube-logs-target (not (string-empty-p kube-logs-target)))
    (user-error "Select a target first"))
  (format "%s/%s" kube-logs-target-kind kube-logs-target))

(defun kube-logs--list-targets ()
  "Return targets for current kind and namespace."
  (when (and kube-logs-namespace-enabled
             (or (null kube-logs-namespace)
                 (string-empty-p kube-logs-namespace)))
    (user-error "Set a namespace first or disable namespace override with -n"))
  (let* ((resource (kube-logs--resource-plural))
         (lines (kube-logs--run-kubectl-lines
                 (append
                  (kube-logs--context-args)
                  (list "get" resource)
                  (when kube-logs-namespace-enabled
                    (list "-n" kube-logs-namespace))
                  (list "-o" "name")))))
    (sort
     (mapcar (lambda (line)
               (replace-regexp-in-string "\\`[^/]+/" "" line))
             lines)
     #'string-lessp)))

(defun kube-logs--logs-args ()
  "Build kubectl logs args from current session backing fields."
  (append
   (kube-logs--context-args)
   (list "logs" (kube-logs--target-ref))
   (when (equal kube-logs-target-kind "deployment")
     (list "--all-pods"))
   (when kube-logs-namespace-enabled
     (progn
       (when (or (null kube-logs-namespace) (string-empty-p kube-logs-namespace))
         (user-error "Set a namespace first or disable namespace override with -n"))
       (list "--namespace" kube-logs-namespace)))
   (list "--prefix")
   (list "--timestamps")
   (when kube-logs-follow
     (list "--follow"))
   (when kube-logs-tail-lines
     (list (format "--tail=%s" kube-logs-tail-lines)))
   (when (and kube-logs-since (not (string-empty-p kube-logs-since)))
     (list (format "--since=%s" kube-logs-since)))))

(defun kube-logs--strip-kubectl-prefix (line)
  "Drop kubectl --prefix fields from LINE by finding the first timestamp token."
  (let* ((tokens (split-string line "[[:space:]]+" t))
         (rest tokens))
    (while (and rest
                (not (ignore-errors (date-to-time (car rest)))))
      (setq rest (cdr rest)))
    (if (and rest (not (eq rest tokens)))
        (string-join rest " ")
      line)))

(defun kube-logs--target-description ()
  "Return one-line target description for transient."
  (format "%s/%s"
          (or kube-logs-target-kind "-")
          (or kube-logs-target "-")))

(defun kube-logs--viewer-buffer-name ()
  "Return kube-logs viewer buffer name for current selection."
  (format "*Kube logs - %s/%s*"
          (kube-logs--namespace-display kube-logs-namespace-enabled kube-logs-namespace)
          (or kube-logs-target "-")))

(defun kube-logs--process-name ()
  "Return process name for current selection."
  (format "kube-logs:%s:%s"
          (kube-logs--namespace-display kube-logs-namespace-enabled kube-logs-namespace)
          (or kube-logs-target "-")))

(defun kube-logs--viewer-header-lines (_state)
  "Return header lines for the current kube-logs viewer buffer."
  (list
   (cons "Context" (or kube-logs--viewer-context "(kubectl default)"))
   (cons "Namespace"
         (kube-logs--namespace-display
          kube-logs--viewer-namespace-enabled
          kube-logs--viewer-namespace))
   (cons "Target" (format "%s/%s"
                          (or kube-logs--viewer-target-kind "-")
                          (or kube-logs--viewer-target "-")))
   (cons "Follow" (if kube-logs--viewer-follow "yes" "no"))
   (cons "Tail" (if kube-logs--viewer-tail
                    (number-to-string kube-logs--viewer-tail)
                  "none"))
   (cons "Since" (or kube-logs--viewer-since "none"))
   (cons "Filter" (or kube-logs-filter "none"))))

(defun kube-logs--command-with-filter (args &optional line-buffered)
  "Return process command list for kubectl ARGS with optional grep filter.

When LINE-BUFFERED is non-nil and a filter is set, use grep --line-buffered."
  (let ((regex (and kube-logs-filter (not (string-empty-p kube-logs-filter)) kube-logs-filter)))
    (if (not regex)
        (cons kube-logs-kubectl args)
      (let* ((kubectl-cmd (string-join (mapcar #'shell-quote-argument
                                               (cons kube-logs-kubectl args))
                                       " "))
             (grep-cmd (string-join
                        (append
                         (list "grep")
                         (when line-buffered (list "--line-buffered"))
                         (list "-E" (shell-quote-argument regex)))
                        " "))
             (full (format "%s | %s" kubectl-cmd grep-cmd)))
        (list shell-file-name shell-command-switch full)))))

(defun kube-logs--wrapper-command (socket-path command)
  "Return Rust ingestion wrapper command for SOCKET-PATH and source COMMAND."
  (append
   (list (json-log-viewer-ingest-wrapper-executable)
         "--socket" socket-path
         "kube"
         "--namespace" (or kube-logs--viewer-namespace kube-logs-namespace "")
         "--target" (or kube-logs--viewer-target kube-logs-target "")
         "--kind" (or kube-logs--viewer-target-kind kube-logs-target-kind "")
         "--source-id" (or kube-logs--viewer-source-id "kube")
         "--")
   command))

(defun kube-logs--supervisor-command (socket-path)
  "Return kube-rs supervisor command for SOCKET-PATH."
  (append
   (list (json-log-viewer-kube-log-supervisor-executable)
         "--socket" socket-path)
   (when (and kube-logs-context (not (string-empty-p kube-logs-context)))
     (list "--context" kube-logs-context))
   (when kube-logs-namespace-enabled
     (when (or (null kube-logs-namespace) (string-empty-p kube-logs-namespace))
       (user-error "Set a namespace first or disable namespace override with -n"))
     (list "--namespace" kube-logs-namespace))
   (list "--target-kind" kube-logs-target-kind
         "--target" kube-logs-target
         "--source-id" (or kube-logs--viewer-source-id "kube"))
   (when kube-logs-tail-lines
     (list "--tail" (number-to-string kube-logs-tail-lines)))
   (when (and kube-logs-since (not (string-empty-p kube-logs-since)))
     (list "--since" kube-logs-since))
   (when (and kube-logs-filter (not (string-empty-p kube-logs-filter)))
     (list "--filter" kube-logs-filter))))

(defun kube-logs--process-log-buffer-name ()
  "Return process diagnostics buffer name for the current kube log stream."
  (format "*Kube logs process - %s/%s*"
          (kube-logs--namespace-display kube-logs-namespace-enabled kube-logs-namespace)
          (or kube-logs-target "-")))

(defun kube-logs--make-process-log-buffer ()
  "Create and initialize the current kube log diagnostics buffer."
  (when kube-logs-debug-process-buffer
    (let ((buffer (get-buffer-create (kube-logs--process-log-buffer-name))))
      (with-current-buffer buffer
        (let ((inhibit-read-only t))
          (erase-buffer))
        (fundamental-mode))
      buffer)))

(defun kube-logs--supervisor-process-filter (viewer-buffer proc output)
  "Forward supervisor PROC stderr OUTPUT to messages and optional debug buffer."
  (when (buffer-live-p viewer-buffer)
    (with-current-buffer viewer-buffer
      (when-let ((process-buffer (and proc (process-buffer proc))))
        (with-current-buffer process-buffer
          (let ((inhibit-read-only t))
            (goto-char (point-max))
            (insert output))))
      (let* ((combined (concat kube-logs--process-log-pending-fragment output))
             (parts (split-string combined "\n"))
             (complete-lines (if (string-suffix-p "\n" combined) parts (butlast parts)))
             (rest (if (string-suffix-p "\n" combined) "" (car (last parts)))))
        (setq kube-logs--process-log-pending-fragment (or rest ""))
        (dolist (line complete-lines)
          (let ((text (string-trim line)))
            (unless (string-empty-p text)
              (message "%s" text))))))))

(defun kube-logs--install-viewer-keymap ()
  "Install buffer-local keymap tweaks for kube logs viewer buffers."
  (let ((map (copy-keymap (current-local-map))))
    (define-key map (kbd "q") #'kube-logs-quit-process-and-window)
    (use-local-map map)))

(defun kube-logs--kill-buffer-process (buffer)
  "Stop process and cleanup state associated with BUFFER, if any."
  (when (buffer-live-p buffer)
    (with-current-buffer buffer
      (kube-logs--cancel-stream-retry-timers)
      (let ((processes (delete-dups
                        (delq nil
                              (append kube-logs--processes
                                      (list kube-logs--process
                                            (and (derived-mode-p 'kube-logs-viewer-mode)
                                                 (get-buffer-process buffer))))))))
        (dolist (proc processes)
          (when (process-live-p proc)
            (process-put proc 'kube-logs-stop-requested t)
            (delete-process proc)))
        (setq kube-logs--process nil)
        (setq kube-logs--processes nil))
      (setq kube-logs--pending-fragment "")
      (when (timerp kube-logs--stream-drain-timer)
        (cancel-timer kube-logs--stream-drain-timer))
      (setq kube-logs--stream-drain-timer nil)
      (setq kube-logs--stream-chunks-in nil)
      (setq kube-logs--stream-chunks-out nil)
      (setq kube-logs--stream-pending-lines nil)
      (setq kube-logs--stream-retry-timers nil)
      (when (buffer-live-p kube-logs--once-output-buffer)
        (kill-buffer kube-logs--once-output-buffer))
      (setq kube-logs--once-output-buffer nil)
      (setq kube-logs--process-log-buffer nil)
      (setq kube-logs--process-log-pending-fragment ""))))

(defun kube-logs--add-buffer-process (buffer process)
  "Track PROCESS as an active kube source for BUFFER."
  (when (buffer-live-p buffer)
    (with-current-buffer buffer
      (setq-local kube-logs--process process)
      (cl-pushnew process kube-logs--processes :test #'eq))))

(defun kube-logs--remove-buffer-process (buffer process)
  "Stop tracking PROCESS for BUFFER."
  (when (buffer-live-p buffer)
    (with-current-buffer buffer
      (setq kube-logs--processes (delq process kube-logs--processes))
      (when (eq kube-logs--process process)
        (setq kube-logs--process (car kube-logs--processes))))))

(defun kube-logs--stream-retry-delay (attempt)
  "Return reconnect delay in seconds for retry ATTEMPT."
  (min (max 1 (or kube-logs-stream-retry-max-delay 30))
       (expt 2 (min attempt 4))))

(defun kube-logs--cancel-stream-retry-timers ()
  "Cancel pending kube stream retry timers in the current buffer."
  (dolist (timer kube-logs--stream-retry-timers)
    (when (timerp timer)
      (cancel-timer timer)))
  (setq kube-logs--stream-retry-timers nil))

(defun kube-logs--remove-stream-retry-timer (timer)
  "Stop tracking retry TIMER in the current buffer."
  (setq kube-logs--stream-retry-timers
        (delq timer kube-logs--stream-retry-timers)))

(defun kube-logs--process-retry-attempt (process)
  "Return the next retry attempt for PROCESS."
  (let* ((started-at (and (processp process)
                          (process-get process 'kube-logs-started-at)))
         (elapsed (and (numberp started-at)
                       (- (float-time) started-at)))
         (reset-after (or kube-logs-stream-retry-reset-after 60)))
    (if (and (numberp elapsed)
             (numberp reset-after)
             (> reset-after 0)
             (>= elapsed reset-after))
        1
      (1+ (or (and (processp process)
                   (process-get process 'kube-logs-retry-attempt))
              0)))))

(defun kube-logs--mark-process-retryable
    (process viewer-buffer restart-fn attempt description)
  "Store retry metadata on PROCESS for VIEWER-BUFFER.
RESTART-FN is called with the next retry attempt after disconnects."
  (when (processp process)
    (process-put process 'kube-logs-viewer-buffer viewer-buffer)
    (process-put process 'kube-logs-restart-fn restart-fn)
    (process-put process 'kube-logs-retry-attempt (or attempt 0))
    (process-put process 'kube-logs-description description)
    (process-put process 'kube-logs-started-at (float-time))))

(defun kube-logs--schedule-process-retry (viewer-buffer process event)
  "Schedule a reconnect for PROCESS in VIEWER-BUFFER after EVENT."
  (when (and kube-logs-stream-retry-enabled
             (processp process)
             (not (process-get process 'kube-logs-stop-requested))
             (buffer-live-p viewer-buffer))
    (let ((restart-fn (process-get process 'kube-logs-restart-fn)))
      (when restart-fn
        (let* ((attempt (kube-logs--process-retry-attempt process))
               (delay (kube-logs--stream-retry-delay attempt))
               (description (or (process-get process 'kube-logs-description)
                                "kube logs"))
               timer)
          (message "kube logs stream disconnected for %s (%s); retrying in %ss"
                   description
                   (string-trim event)
                   delay)
          (setq timer
                (run-at-time
                 delay nil
                 (lambda ()
                   (when (buffer-live-p viewer-buffer)
                     (with-current-buffer viewer-buffer
                       (kube-logs--remove-stream-retry-timer timer)
                       (funcall restart-fn attempt))))))
          (with-current-buffer viewer-buffer
            (push timer kube-logs--stream-retry-timers)))))))

(defun kube-logs--reset-buffer-state (buffer &optional install-keymap)
  "Reset kube-logs local state in BUFFER.

When INSTALL-KEYMAP is non-nil, install kube-logs key bindings."
  (with-current-buffer buffer
    (setq-local kube-logs--process nil)
    (setq-local kube-logs--processes nil)
    (setq-local kube-logs--initialized-p t)
    (setq-local kube-logs--pending-fragment "")
    (setq-local kube-logs--stream-chunks-in nil)
    (setq-local kube-logs--stream-chunks-out nil)
    (setq-local kube-logs--stream-pending-lines nil)
    (setq-local kube-logs--stream-drain-timer nil)
    (setq-local kube-logs--stream-retry-timers nil)
    (setq-local kube-logs--once-output-buffer nil)
    (setq-local kube-logs--process-log-buffer nil)
    (setq-local kube-logs--process-log-pending-fragment "")
    (add-hook 'kill-buffer-hook
              (lambda ()
                (kube-logs--kill-buffer-process (current-buffer)))
              nil t)
    (when install-keymap
      (kube-logs--install-viewer-keymap))))

(defun kube-logs--set-viewer-state (buffer)
  "Set current session metadata on BUFFER."
  (with-current-buffer buffer
    (setq-local kube-logs--viewer-context kube-logs-context)
    (setq-local kube-logs--viewer-namespace kube-logs-namespace)
    (setq-local kube-logs--viewer-namespace-enabled kube-logs-namespace-enabled)
    (setq-local kube-logs--viewer-target-kind kube-logs-target-kind)
    (setq-local kube-logs--viewer-target kube-logs-target)
    (setq-local kube-logs--viewer-source-id
                (or kube-logs--viewer-source-id "kube"))
    (setq-local kube-logs--viewer-follow kube-logs-follow)
    (setq-local kube-logs--viewer-tail kube-logs-tail-lines)
    (setq-local kube-logs--viewer-since kube-logs-since)))

(defun kube-logs-quit-process-and-window ()
  "Stop log process for current buffer and close the window."
  (interactive)
  (kube-logs--kill-buffer-process (current-buffer))
  (quit-window t))

(defun kube-logs--selected-viewer-buffer-p ()
  "Return non-nil when kube-logs should use a selected viewer buffer."
  (and kube-logs-viewer-buffer
       (not (string-empty-p kube-logs-viewer-buffer))))

(defun kube-logs--selected-viewer-buffer ()
  "Return selected kube-logs viewer buffer, or nil when unset."
  (when (kube-logs--selected-viewer-buffer-p)
    (json-log-viewer-get-buffer kube-logs-viewer-buffer)))

(defun kube-logs--selected-composite-viewer-buffer-p ()
  "Return non-nil when the selected viewer is a composite log viewer."
  (and (kube-logs--selected-viewer-buffer-p)
       (json-log-viewer-composite-buffer-p kube-logs-viewer-buffer)))

(defun kube-logs--register-composite-source-config
    (buffer &optional source-id timestamp-path level-path message-path extra-paths)
  "Register current kube formatting for composite BUFFER."
  (let ((target (json-log-viewer-get-buffer buffer)))
    (when (json-log-viewer-composite-buffer-p target)
      (json-log-viewer-register-source-config
       target
       (or source-id
           (with-current-buffer target kube-logs--viewer-source-id)
           "kube")
       :timestamp-path (or timestamp-path kube-logs-timestamp-path)
       :level-path (or level-path kube-logs-level-path)
       :message-path (or message-path kube-logs-message-path)
       :extra-paths (or extra-paths kube-logs-extra-paths)))))

(defun kube-logs--composite-source-id
    (namespace-enabled namespace target-kind target &optional source-id)
  "Return source ID for a Kube composite source."
  (or source-id
      (format "kube:%s/%s/%s"
              (kube-logs--namespace-display namespace-enabled namespace)
              (or target-kind "-")
              (or target "-"))))

(defun kube-logs--plist-value (plist key default)
  "Return PLIST KEY value, or DEFAULT when KEY is absent."
  (if (plist-member plist key)
      (plist-get plist key)
    default))

(defun kube-logs--normalize-source-target-kind (value)
  "Normalize source target kind VALUE to a kube-logs target kind string."
  (cond
   ((symbolp value) (symbol-name value))
   ((stringp value) value)
   (t value)))

(defun kube-logs--initialize-viewer-buffer (buffer &optional install-keymap)
  "Initialize kube-logs state in BUFFER.

When INSTALL-KEYMAP is non-nil, install kube-logs key bindings."
  (kube-logs--reset-buffer-state buffer install-keymap)
  (kube-logs--set-viewer-state buffer))

(defun kube-logs--append-to-viewer-buffer (buffer &optional source-id)
  "Prepare BUFFER for an additional kube source without stopping existing ones."
  (with-current-buffer buffer
    (unless kube-logs--initialized-p
      (setq-local kube-logs--initialized-p t)
      (add-hook 'kill-buffer-hook
                (lambda ()
                  (kube-logs--kill-buffer-process (current-buffer)))
                nil t))
    (setq-local kube-logs--viewer-source-id (or source-id "kube")))
  (kube-logs--set-viewer-state buffer))

(defun kube-logs--make-viewer-buffer (&optional on-ready)
  "Create kube logs viewer buffer.
ON-READY is called once the async worker is ready to receive jobs."
  (if-let ((selected (kube-logs--selected-viewer-buffer)))
      (if (json-log-viewer-composite-buffer-p selected)
          (progn
            (kube-logs--append-to-viewer-buffer selected)
            (when on-ready
              (json-log-viewer-run-when-ready selected on-ready))
            selected)
        (kube-logs--kill-buffer-process selected)
        (kube-logs--initialize-viewer-buffer selected)
        (when on-ready
          (json-log-viewer-run-when-ready
           selected
           (lambda ()
             (json-log-viewer-replace-log-lines selected nil nil)
             (funcall on-ready))))
        selected)
    (let* ((buffer-name (kube-logs--viewer-buffer-name))
           (existing (get-buffer buffer-name))
           buffer)
      (when existing
        (kube-logs--kill-buffer-process existing))
      (setq buffer
            (json-log-viewer-make-buffer
             buffer-name
             :timestamp-path kube-logs-timestamp-path
             :level-path kube-logs-level-path
             :message-path kube-logs-message-path
             :extra-paths kube-logs-extra-paths
             :mode #'kube-logs-viewer-mode
             :header-lines-function #'kube-logs--viewer-header-lines))
      (kube-logs--initialize-viewer-buffer buffer t)
      (when on-ready
        (json-log-viewer-run-when-ready buffer on-ready))
      buffer)))

(defun kube-logs-stream-to-buffer (buffer source)
  "Start a Kubernetes log stream SOURCE into composite log viewer BUFFER.

SOURCE is a plist.  Supported keys are:

- `:context': kubectl context, or nil for the kubectl default.
- `:source-id': optional composite source ID. Defaults to
  `kube:NAMESPACE/KIND/TARGET'.
- `:namespace': namespace string.
- `:namespace-enabled': non-nil to pass `--namespace'.
- `:target-kind': `pod', `deployment', \"pod\", or \"deployment\".
- `:target': pod or deployment name.
- `:filter': optional grep regex.
- `:stream-backend': `rust' or `kubectl'.
- `:debug-process-buffer': non-nil to keep Rust supervisor diagnostics.
- `:timestamp-path', `:level-path', `:message-path', `:extra-paths':
  summary formatting paths.

The stream always follows from now with `--tail=0' and no `--since'."
  (let* ((viewer (json-log-viewer-get-buffer buffer))
         (context (kube-logs--plist-value
                   source :context kube-logs-default-context))
         (source-id (kube-logs--plist-value source :source-id nil))
         (namespace (kube-logs--plist-value
                     source :namespace kube-logs-default-namespace))
         (namespace-enabled (kube-logs--plist-value
                             source :namespace-enabled
                             kube-logs-default-namespace-enabled))
         (target-kind (kube-logs--normalize-source-target-kind
                       (kube-logs--plist-value
                        source :target-kind kube-logs-default-target-kind)))
         (target (kube-logs--plist-value
                  source :target kube-logs-default-target))
         (filter (kube-logs--plist-value
                  source :filter kube-logs-default-filter))
         (stream-backend (kube-logs--plist-value
                          source :stream-backend kube-logs-stream-backend))
         (debug-process-buffer
          (kube-logs--plist-value
           source :debug-process-buffer kube-logs-debug-process-buffer))
         (timestamp-path (kube-logs--plist-value
                          source :timestamp-path kube-logs-timestamp-path))
         (level-path (kube-logs--plist-value
                      source :level-path kube-logs-level-path))
         (message-path (kube-logs--plist-value
                        source :message-path kube-logs-message-path))
         (extra-paths (kube-logs--plist-value
                       source :extra-paths kube-logs-extra-paths))
         (kube-logs-viewer-buffer (buffer-name viewer))
         (kube-logs-context context)
         (kube-logs-namespace namespace)
         (kube-logs-namespace-enabled namespace-enabled)
         (kube-logs-target-kind target-kind)
         (kube-logs-target target)
         (kube-logs-follow t)
         (kube-logs-tail-lines 0)
         (kube-logs-since nil)
         (kube-logs-filter filter)
         (kube-logs-stream-backend stream-backend)
         (kube-logs-debug-process-buffer debug-process-buffer)
         (kube-logs-timestamp-path timestamp-path)
         (kube-logs-level-path level-path)
         (kube-logs-message-path message-path)
         (kube-logs-extra-paths extra-paths))
    (unless (json-log-viewer-composite-buffer-p viewer)
      (user-error "Kube composite source requires a composite log viewer buffer"))
    (when (and kube-logs-namespace-enabled
               (or (null kube-logs-namespace)
                   (string-empty-p kube-logs-namespace)))
      (user-error "Set a namespace first or disable namespace override with :namespace-enabled nil"))
    (unless (and kube-logs-target-kind (member kube-logs-target-kind kube-logs--target-kinds))
      (user-error "Select a target kind first"))
    (unless (and kube-logs-target (not (string-empty-p kube-logs-target)))
      (user-error "Select a target first"))
    (unless (memq kube-logs-stream-backend '(rust kubectl))
      (user-error ":stream-backend must be rust or kubectl, got: %S"
                  kube-logs-stream-backend))
    (let* ((normalized-source-id
            (kube-logs--composite-source-id
             kube-logs-namespace-enabled
             kube-logs-namespace
             kube-logs-target-kind
             kube-logs-target
             source-id))
           (allocated-source-id
            (json-log-viewer-unique-source-id
             viewer normalized-source-id
             :timestamp-path kube-logs-timestamp-path
             :level-path kube-logs-level-path
             :message-path kube-logs-message-path
             :extra-paths kube-logs-extra-paths))
           (captured-context kube-logs-context)
          (captured-namespace kube-logs-namespace)
          (captured-namespace-enabled kube-logs-namespace-enabled)
          (captured-source-id allocated-source-id)
          (captured-target-kind kube-logs-target-kind)
          (captured-target kube-logs-target)
          (captured-filter kube-logs-filter)
          (captured-stream-backend kube-logs-stream-backend)
          (captured-debug-process-buffer kube-logs-debug-process-buffer)
          (captured-timestamp-path kube-logs-timestamp-path)
          (captured-level-path kube-logs-level-path)
          (captured-message-path kube-logs-message-path)
          (captured-extra-paths kube-logs-extra-paths)
          (description (kube-logs--target-description)))
      (kube-logs--append-to-viewer-buffer viewer allocated-source-id)
      (kube-logs--register-composite-source-config
       viewer allocated-source-id
       kube-logs-timestamp-path kube-logs-level-path
       kube-logs-message-path kube-logs-extra-paths)
      (json-log-viewer-run-when-ready
       viewer
       (lambda ()
         (let ((kube-logs-context captured-context)
               (kube-logs-namespace captured-namespace)
               (kube-logs-namespace-enabled captured-namespace-enabled)
               (kube-logs-target-kind captured-target-kind)
               (kube-logs-target captured-target)
               (kube-logs-follow t)
               (kube-logs-tail-lines 0)
               (kube-logs-since nil)
               (kube-logs-filter captured-filter)
               (kube-logs-stream-backend captured-stream-backend)
               (kube-logs-debug-process-buffer captured-debug-process-buffer)
               (kube-logs-timestamp-path captured-timestamp-path)
               (kube-logs-level-path captured-level-path)
               (kube-logs-message-path captured-message-path)
               (kube-logs-extra-paths captured-extra-paths)
               (kube-logs--viewer-source-id captured-source-id))
           (let ((viewer-buffer (current-buffer)))
             (pcase kube-logs-stream-backend
               ('rust
                (let* ((socket-path (json-log-viewer-worker-socket-path viewer-buffer))
                       (command (kube-logs--supervisor-command socket-path))
                       (log-buffer (kube-logs--make-process-log-buffer)))
                  (kube-logs--start-supervisor-process
                   viewer-buffer command description log-buffer)))
               ('kubectl
                (let* ((args (kube-logs--logs-args))
                       (command (kube-logs--command-with-filter args t))
                       (socket-path (json-log-viewer-worker-socket-path viewer-buffer))
                       (wrapper-command
                        (let ((kube-logs--viewer-namespace captured-namespace)
                              (kube-logs--viewer-target captured-target)
                              (kube-logs--viewer-target-kind captured-target-kind)
                              (kube-logs--viewer-source-id captured-source-id))
                          (kube-logs--wrapper-command socket-path command))))
                  (kube-logs--start-kubectl-stream-process
                   viewer-buffer wrapper-command description))))))))
      viewer)))

(defun kube-logs--parse-json-maybe (value)
  "Parse VALUE as JSON object/list when possible."
  (when (and (stringp value)
             (string-match-p "\\`[[:space:]\n\r\t]*[{\\[]" value))
    (condition-case nil
        (json-parse-string value :object-type 'alist :array-type 'list
                           :null-object nil :false-object :false)
      (error nil))))

(defun kube-logs--split-timestamp-prefix (line)
  "Split LINE into (TIMESTAMP . MESSAGE) when timestamp prefix exists."
  (if (and (stringp line)
           (string-match "\\`\\([^[:space:]]+\\)\\s-+\\(.*\\)\\'" line))
      (let ((timestamp (match-string 1 line))
            (message (match-string 2 line)))
        (if (ignore-errors (date-to-time timestamp))
            (cons timestamp message)
          (cons nil line)))
    (cons nil line)))

(defun kube-logs--line->json-line (line)
  "Convert one kubectl log LINE into one JSON line for json-log-viewer."
  (let* ((clean (string-trim-right (or line "") "\r")))
    (unless (string-empty-p clean)
      (let* ((without-prefix (kube-logs--strip-kubectl-prefix clean))
             (split (kube-logs--split-timestamp-prefix without-prefix))
             (timestamp (car split))
             (message (or (cdr split) ""))
             (parsed (kube-logs--parse-json-maybe message))
             (obj (make-hash-table :test 'equal)))
        (when timestamp
          (puthash "timestamp" timestamp obj))
        (puthash "source" "kube" obj)
        (when kube-logs--viewer-source-id
          (puthash "sourceId" kube-logs--viewer-source-id obj))
        (puthash "raw" without-prefix obj)
        (puthash "namespace" (or kube-logs--viewer-namespace kube-logs-namespace "") obj)
        (puthash "target" (or kube-logs--viewer-target kube-logs-target "") obj)
        (puthash "kind" (or kube-logs--viewer-target-kind kube-logs-target-kind "") obj)
        (puthash "payload" (or parsed message) obj)
        (json-serialize obj)))))

(defun kube-logs--lines->json-lines (lines)
  "Convert kubectl output LINES to json-log-viewer JSON lines."
  (delq nil (mapcar #'kube-logs--line->json-line lines)))

(defun kube-logs--consume-chunk-lines (chunk)
  "Consume process CHUNK and return complete lines in current buffer."
  (let* ((combined (concat kube-logs--pending-fragment chunk))
         (has-newline (string-suffix-p "\n" combined))
         (parts (split-string combined "\n"))
         (complete-lines (if has-newline parts (butlast parts)))
         (rest (if has-newline "" (car (last parts)))))
    (setq kube-logs--pending-fragment (or rest ""))
    complete-lines))

(defun kube-logs--flush-pending-fragment ()
  "Flush pending trailing fragment in current buffer."
  (when (and kube-logs--pending-fragment
             (not (string-empty-p kube-logs--pending-fragment)))
    (let ((line kube-logs--pending-fragment))
      (setq kube-logs--pending-fragment "")
      (when-let ((json-line (kube-logs--line->json-line line)))
        (json-log-viewer-push (current-buffer) (list json-line))))))


(defun kube-logs--stream-queue-empty-p ()
  "Return non-nil when no stream output is waiting to be rendered."
  (and (null kube-logs--stream-chunks-in)
       (null kube-logs--stream-chunks-out)
       (null kube-logs--stream-pending-lines)))

(defun kube-logs--stream-cancel-drain-timer ()
  "Cancel and clear stream drain timer for current buffer."
  (when (timerp kube-logs--stream-drain-timer)
    (cancel-timer kube-logs--stream-drain-timer))
  (setq kube-logs--stream-drain-timer nil))

(defun kube-logs--stream-drain-on-timer (buffer)
  "Drain queued stream output for BUFFER from timer callbacks."
  (if (not (buffer-live-p buffer))
      nil
    (with-current-buffer buffer
      (condition-case err
          (kube-logs--stream-drain nil)
        (error
         (kube-logs--stream-cancel-drain-timer)
         (message "kube-logs drain failed: %s" (error-message-string err)))))))

(defun kube-logs--stream-schedule-drain ()
  "Ensure periodic draining is scheduled for current buffer."
  (unless (timerp kube-logs--stream-drain-timer)
    (let ((interval (max 0.01 (or kube-logs-stream-drain-interval 0.05))))
      (setq kube-logs--stream-drain-timer
            (run-at-time interval interval
                         #'kube-logs--stream-drain-on-timer
                         (current-buffer))))))

(defun kube-logs--stream-enqueue-chunk (chunk)
  "Queue one process output CHUNK for later incremental rendering."
  (when (and (stringp chunk) (> (length chunk) 0))
    (push chunk kube-logs--stream-chunks-in)
    (kube-logs--stream-schedule-drain)))

(defun kube-logs--stream-pop-chunk ()
  "Pop the next queued process output chunk, or nil."
  (unless kube-logs--stream-chunks-out
    (when kube-logs--stream-chunks-in
      (setq kube-logs--stream-chunks-out (nreverse kube-logs--stream-chunks-in))
      (setq kube-logs--stream-chunks-in nil)))
  (prog1 (car kube-logs--stream-chunks-out)
    (setq kube-logs--stream-chunks-out (cdr kube-logs--stream-chunks-out))))

(defun kube-logs--stream-pop-lines (max-lines)
  "Pop up to MAX-LINES complete streamed lines in order."
  (let ((lines nil)
        (count 0))
    (while (< count max-lines)
      (unless kube-logs--stream-pending-lines
        (if-let ((chunk (kube-logs--stream-pop-chunk)))
            (setq kube-logs--stream-pending-lines
                  (kube-logs--consume-chunk-lines chunk))
          (setq count max-lines)))
      (while (and kube-logs--stream-pending-lines
                  (< count max-lines))
        (push (pop kube-logs--stream-pending-lines) lines)
        (setq count (1+ count))))
    (nreverse lines)))

(defun kube-logs--stream-drain (&optional drain-all)
  "Render queued streamed output in batches.

When DRAIN-ALL is non-nil, consume the full queue in one call."
  (let ((batch-size (max 1 (or kube-logs-stream-max-lines-per-batch 250)))
        (more t))
    (while more
      (let* ((limit (if drain-all most-positive-fixnum batch-size))
             (lines (kube-logs--stream-pop-lines limit))
             (json-lines (kube-logs--lines->json-lines lines)))
        (when json-lines
          (json-log-viewer-push (current-buffer) json-lines))
        (setq more (and drain-all
                        (not (kube-logs--stream-queue-empty-p))))))
    (when (kube-logs--stream-queue-empty-p)
      (kube-logs--stream-cancel-drain-timer))))

(defun kube-logs--stream-process-filter (process output)
  "Process filter for streaming kube logs PROCESS OUTPUT."
  (let ((buffer (process-buffer process)))
    (when (buffer-live-p buffer)
      (with-current-buffer buffer
        ;; Keep process filter lightweight: queue output and render on timer ticks.
        (kube-logs--stream-enqueue-chunk output)))))

(defun kube-logs--wrapper-process-filter (_process output)
  "Report low-volume wrapper diagnostics from OUTPUT."
  (let ((text (string-trim output)))
    (unless (string-empty-p text)
      (message "kube-logs wrapper: %s" text))))

(defun kube-logs--stream-process-sentinel (process event)
  "Process sentinel for streaming kube logs PROCESS EVENT."
  (let ((buffer (process-buffer process)))
    (when (buffer-live-p buffer)
      (with-current-buffer buffer
        (kube-logs--stream-drain t)
        (kube-logs--stream-cancel-drain-timer)
        (kube-logs--flush-pending-fragment)
        (kube-logs--remove-buffer-process buffer process)))
    (when (and (memq (process-status process) '(exit signal))
               (buffer-live-p buffer))
      (kube-logs--schedule-process-retry buffer process event))
    (when (and (memq (process-status process) '(exit signal))
               (not (zerop (process-exit-status process)))
               (not (and kube-logs-filter
                         (= (process-exit-status process) 1))))
      (message "kubectl logs exited (%s): %s"
               (process-exit-status process)
               (string-trim event)))))

(defun kube-logs--start-kubectl-stream-process
    (viewer-buffer wrapper-command description &optional attempt)
  "Start kubectl log wrapper for VIEWER-BUFFER.
WRAPPER-COMMAND is reused for reconnects.  DESCRIPTION is used in messages.
ATTEMPT is the current retry attempt, or nil for the initial start."
  (let* ((retry-attempt (or attempt 0))
         (process
          (make-process
           :name (kube-logs--process-name)
           :buffer viewer-buffer
           :command wrapper-command
           :noquery t
           :connection-type 'pipe
           :filter #'kube-logs--wrapper-process-filter))
         (restart-fn
          (lambda (next-attempt)
            (kube-logs--start-kubectl-stream-process
             viewer-buffer wrapper-command description next-attempt))))
    (set-process-sentinel process #'kube-logs--stream-process-sentinel)
    (set-process-query-on-exit-flag process nil)
    (kube-logs--mark-process-retryable
     process viewer-buffer restart-fn retry-attempt description)
    (kube-logs--add-buffer-process viewer-buffer process)
    (message "%s kube logs stream for %s"
             (if (> retry-attempt 0) "Restarted" "Started")
             description)
    process))

(defun kube-logs--run-once ()
  "Fetch logs once asynchronously and render in json-log-viewer."
  (let* ((append-to-existing (kube-logs--selected-composite-viewer-buffer-p))
         (buffer (kube-logs--make-viewer-buffer))
         (_ (kube-logs--register-composite-source-config buffer))
         (args (kube-logs--logs-args))
         (command (kube-logs--command-with-filter args nil))
         (output-buffer (generate-new-buffer " *kube-logs-once*"))
         (label (kube-logs--target-description))
         (process
          (make-process
           :name (kube-logs--process-name)
           :buffer output-buffer
           :command command
           :noquery t
           :connection-type 'pipe
           :sentinel
           (lambda (proc event)
             (when (memq (process-status proc) '(exit signal))
               (let ((exit-code (process-exit-status proc))
                     (output (with-current-buffer output-buffer
                               (buffer-string))))
                 (unwind-protect
                     (when (buffer-live-p buffer)
                       (with-current-buffer buffer
                         (kube-logs--remove-buffer-process buffer proc)
                         (when (eq kube-logs--once-output-buffer output-buffer)
                           (setq kube-logs--once-output-buffer nil))
                         (if (or (zerop exit-code)
                                 (and kube-logs-filter (= exit-code 1)))
                             (let* ((raw-lines (split-string output "\n" t))
                                    (json-lines (kube-logs--lines->json-lines raw-lines)))
                               (if append-to-existing
                                   (json-log-viewer-push buffer json-lines)
                                 (json-log-viewer-replace-log-lines buffer json-lines nil))
                               (message "Fetched kube logs for %s" label))
                           (message "kubectl logs failed (%s): %s"
                                    exit-code
                                    (if (string-empty-p (string-trim output))
                                        (string-trim event)
                                      (string-trim output))))))
                   (kill-buffer output-buffer))))))))
    (with-current-buffer buffer
      (setq-local kube-logs--once-output-buffer output-buffer))
    (display-buffer buffer)
    (message "Fetching kube logs for %s..." label)
    (set-process-query-on-exit-flag process nil)
    (kube-logs--add-buffer-process buffer process)))

(defun kube-logs--run-stream-kubectl ()
  "Start streaming logs through kubectl and render them in json-log-viewer."
  (let* ((args (kube-logs--logs-args))
         (command (kube-logs--command-with-filter args t))
         (description (kube-logs--target-description))
         buffer)
    (setq buffer
          (kube-logs--make-viewer-buffer
           (lambda ()
             (let* ((viewer-buffer (current-buffer))
                    (_ (kube-logs--register-composite-source-config viewer-buffer))
                    (socket-path (json-log-viewer-worker-socket-path viewer-buffer))
                    (wrapper-command
                     (kube-logs--wrapper-command socket-path command)))
               (kube-logs--start-kubectl-stream-process
                viewer-buffer wrapper-command description)))))
    (display-buffer buffer)))

(defun kube-logs--supervisor-sentinel (viewer-buffer proc event)
  "Handle kube supervisor PROC lifecycle EVENT for VIEWER-BUFFER."
  (when (memq (process-status proc) '(exit signal))
    (when (buffer-live-p viewer-buffer)
      (kube-logs--remove-buffer-process viewer-buffer proc))
    (kube-logs--schedule-process-retry viewer-buffer proc event)
    (when (not (zerop (process-exit-status proc)))
      (message "kube log supervisor exited (%s): %s"
               (process-exit-status proc)
               (string-trim event)))))

(defun kube-logs--start-supervisor-process
    (viewer-buffer command description log-buffer &optional attempt)
  "Start Rust kube supervisor for VIEWER-BUFFER.
COMMAND is reused for reconnects.  DESCRIPTION is used in messages.
LOG-BUFFER receives diagnostics when non-nil.  ATTEMPT is nil for the initial
start, or the current retry attempt."
  (let* ((retry-attempt (or attempt 0))
         (process
          (make-process
           :name (kube-logs--process-name)
           :buffer log-buffer
           :command command
           :noquery t
           :connection-type 'pipe
           :filter
           (lambda (proc output)
             (kube-logs--supervisor-process-filter viewer-buffer proc output))
           :sentinel
           (lambda (proc event)
             (kube-logs--supervisor-sentinel viewer-buffer proc event))))
         (restart-fn
          (lambda (next-attempt)
            (kube-logs--start-supervisor-process
             viewer-buffer command description log-buffer next-attempt))))
    (set-process-query-on-exit-flag process nil)
    (kube-logs--mark-process-retryable
     process viewer-buffer restart-fn retry-attempt description)
    (kube-logs--add-buffer-process viewer-buffer process)
    (with-current-buffer viewer-buffer
      (setq-local kube-logs--process-log-buffer log-buffer))
    (if log-buffer
        (message "%s kube log supervisor for %s; debug buffer %s"
                 (if (> retry-attempt 0) "Restarted" "Started")
                 description
                 (buffer-name log-buffer))
      (message "%s kube log supervisor for %s"
               (if (> retry-attempt 0) "Restarted" "Started")
               description))
    process))

(defun kube-logs--run-stream-rust ()
  "Start streaming logs through the Rust kube supervisor."
  (let ((description (kube-logs--target-description))
        buffer)
    (setq
     buffer
     (kube-logs--make-viewer-buffer
      (lambda ()
        (let* ((viewer-buffer (current-buffer))
               (_ (kube-logs--register-composite-source-config viewer-buffer))
               (socket-path (json-log-viewer-worker-socket-path viewer-buffer))
               (command (kube-logs--supervisor-command socket-path))
               (log-buffer (kube-logs--make-process-log-buffer)))
          (kube-logs--start-supervisor-process
           viewer-buffer command description log-buffer)))))
    (display-buffer buffer)))

(defun kube-logs--run-stream ()
  "Start streaming logs and render them in json-log-viewer."
  (if (eq kube-logs-stream-backend 'rust)
      (kube-logs--run-stream-rust)
    (kube-logs--run-stream-kubectl)))

(defun kube-logs-run ()
  "Run kubectl logs using current session selections."
  (interactive)
  (when (and kube-logs-namespace-enabled
             (or (null kube-logs-namespace)
                 (string-empty-p kube-logs-namespace)))
    (user-error "Set a namespace first or disable namespace override with -n"))
  (unless (and kube-logs-target-kind (member kube-logs-target-kind kube-logs--target-kinds))
    (user-error "Select a target kind first"))
  (unless (and kube-logs-target (not (string-empty-p kube-logs-target)))
    (user-error "Select a target first"))
  (if kube-logs-follow
      (kube-logs--run-stream)
    (kube-logs--run-once)))

(defun kube-logs--preset-plist-valid-p (plist)
  "Return non-nil if PLIST is valid for `kube-logs-make-preset'."
  (let ((cursor plist))
    (while cursor
      (let ((key (car cursor)))
        (unless (keywordp key)
          (user-error "Preset key must be a keyword, got: %S" key))
        (unless (memq key kube-logs--preset-keys)
          (user-error "Unsupported preset key: %S" key)))
      (setq cursor (cddr cursor))))
  t)

(defun kube-logs-make-preset (name &rest options)
  "Create or replace named kube-logs preset NAME with OPTIONS plist."
  (let ((preset-name (if (symbolp name) (symbol-name name) name)))
    (unless (stringp preset-name)
      (user-error "Preset name must be a string or symbol, got: %S" name))
    (unless (zerop (% (length options) 2))
      (user-error "Preset options must be key/value pairs"))
    (kube-logs--preset-plist-valid-p options)
    (setq kube-logs-presets (assoc-delete-all preset-name kube-logs-presets))
    (push (cons preset-name options) kube-logs-presets)
    (car kube-logs-presets)))

(defun kube-logs--apply-preset-plist (plist)
  "Apply preset PLIST to current kube-logs session backing fields."
  (dolist (entry '((:context . kube-logs-context)
                   (:namespace . kube-logs-namespace)
                   (:namespace-enabled . kube-logs-namespace-enabled)
                   (:target-kind . kube-logs-target-kind)
                   (:target . kube-logs-target)
                   (:follow . kube-logs-follow)
                   (:tail-lines . kube-logs-tail-lines)
                   (:since . kube-logs-since)
                   (:filter . kube-logs-filter)))
    (let ((key (car entry))
          (var (cdr entry)))
      (when (plist-member plist key)
        (set var (plist-get plist key))))))

(transient-define-suffix kube-logs-apply-preset ()
  "Select and apply a preset from `kube-logs-presets'."
  :transient t
  (interactive)
  (unless kube-logs-presets
    (user-error "No presets configured; use `kube-logs-make-preset'"))
  (let* ((name (completing-read "Preset: " (mapcar #'car kube-logs-presets) nil t))
         (preset (assoc name kube-logs-presets)))
    (unless preset
      (user-error "Preset not found: %s" name))
    (kube-logs--apply-preset-plist (cdr preset))
    (kube-logs--transient-reprompt)))

(transient-define-suffix kube-logs-toggle-follow ()
  "Toggle follow mode for kubectl logs."
  :description (lambda ()
                 (format "Follow: %s" (if kube-logs-follow "on" "off")))
  :transient t
  (interactive)
  (setq kube-logs-follow (not kube-logs-follow))
  (kube-logs--transient-reprompt))

(transient-define-suffix kube-logs-toggle-stream-backend ()
  "Toggle stream backend for follow mode."
  :description (lambda ()
                 (format "Stream backend: %s" kube-logs-stream-backend))
  :transient t
  (interactive)
  (setq kube-logs-stream-backend
        (if (eq kube-logs-stream-backend 'rust) 'kubectl 'rust))
  (kube-logs--transient-reprompt))

(transient-define-suffix kube-logs-toggle-debug-process-buffer ()
  "Toggle Rust supervisor debug process buffer."
  :description (lambda ()
                 (format "Debug process buffer: %s"
                         (if kube-logs-debug-process-buffer "on" "off")))
  :transient t
  (interactive)
  (setq kube-logs-debug-process-buffer (not kube-logs-debug-process-buffer))
  (kube-logs--transient-reprompt))

(transient-define-suffix kube-logs-select-context ()
  "Set Kubernetes context."
  :description (lambda ()
                 (format "Context: %s" (or kube-logs-context "(kubectl default)")))
  :transient t
  (interactive)
  (let* ((choices (ignore-errors (kube-logs--list-contexts)))
         (value
          (if (and choices (listp choices) (> (length choices) 0))
              (completing-read "Context (empty=kubectl default): " choices nil nil)
            (string-trim (read-string "Context (empty=kubectl default): ")))))
    (setq kube-logs-context (unless (string-empty-p value) value))
    ;; Context changed; selected target may no longer exist.
    (setq kube-logs-target nil)
    (kube-logs--transient-reprompt)))

(transient-define-suffix kube-logs-set-namespace ()
  "Set explicit namespace and enable namespace override."
  :description (lambda ()
                 (format "Namespace: %s"
                         (kube-logs--namespace-display
                          kube-logs-namespace-enabled
                          kube-logs-namespace)))
  :transient t
  (interactive)
  (let* ((choices (ignore-errors (kube-logs--list-namespaces)))
         (value
          (if (and choices (listp choices) (> (length choices) 0))
              (completing-read "Namespace: " choices nil t)
            (string-trim (read-string "Namespace: ")))))
    (when (string-empty-p value)
      (user-error "Namespace cannot be empty"))
    (setq kube-logs-namespace value)
    (setq kube-logs-namespace-enabled t)
    ;; Namespace changed; selected target may no longer exist.
    (setq kube-logs-target nil)
    (kube-logs--transient-reprompt)))

(transient-define-suffix kube-logs-toggle-namespace-override ()
  "Toggle explicit namespace override for kubectl commands."
  :description (lambda ()
                 (format "Namespace override: %s"
                         (if kube-logs-namespace-enabled "on" "off")))
  :transient t
  (interactive)
  (setq kube-logs-namespace-enabled (not kube-logs-namespace-enabled))
  ;; Namespace scope changed; selected target may no longer exist.
  (setq kube-logs-target nil)
  (kube-logs--transient-reprompt))

(defun kube-logs--select-target-by-kind (kind)
  "Select target of KIND and set current selection.

This stores one active target; choosing pod/deployment replaces the other."
  (let* ((choices (let ((kube-logs-target-kind kind))
                    (ignore-errors (kube-logs--list-targets))))
         (prompt (format "%s: " (capitalize kind)))
         (value
          (if (and choices (listp choices) (> (length choices) 0))
              (completing-read prompt choices nil t)
            (string-trim (read-string prompt)))))
    (when (string-empty-p value)
      (user-error "%s cannot be empty" (capitalize kind)))
    (setq kube-logs-target-kind kind)
    (setq kube-logs-target value)))

(transient-define-suffix kube-logs-select-pod ()
  "Select pod target."
  :description (lambda ()
                 (format "Pod: %s"
                         (if (equal kube-logs-target-kind "pod")
                             (or kube-logs-target "-")
                           "-")))
  :transient t
  (interactive)
  (kube-logs--select-target-by-kind "pod")
  (kube-logs--transient-reprompt))

(transient-define-suffix kube-logs-select-deployment ()
  "Select deployment target."
  :description (lambda ()
                 (format "Deployment: %s"
                         (if (equal kube-logs-target-kind "deployment")
                             (or kube-logs-target "-")
                           "-")))
  :transient t
  (interactive)
  (kube-logs--select-target-by-kind "deployment")
  (kube-logs--transient-reprompt))

(transient-define-infix kube-logs-infix-tail-lines ()
  :description "Tail lines"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj)
                (transient-infix-set obj
                                     (when kube-logs-tail-lines
                                       (number-to-string kube-logs-tail-lines))))
  :reader (lambda (_prompt initial _hist)
            (let* ((input (string-trim (read-string "Tail lines (empty=none): " (or initial "")))))
              (setq kube-logs-tail-lines
                    (unless (string-empty-p input)
                      (let ((n (string-to-number input)))
                        (when (<= n 0)
                          (user-error "Tail lines must be a positive integer"))
                        n)))
              input))
  :argument "--tail=")

(transient-define-infix kube-logs-infix-since ()
  :description "Since"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj) (transient-infix-set obj kube-logs-since))
  :reader (lambda (_prompt initial _hist)
            (let ((input (string-trim (read-string "Since (e.g. 5m, empty=none): " (or initial "")))))
              (setq kube-logs-since (unless (string-empty-p input) input))
              input))
  :argument "--since=")

(transient-define-infix kube-logs-infix-filter ()
  :description "Filter regex"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj) (transient-infix-set obj kube-logs-filter))
  :reader (lambda (_prompt initial _hist)
            (let ((input (string-trim (read-string "Filter regex (empty=none): "
                                                   (or initial "")))))
              (setq kube-logs-filter (unless (string-empty-p input) input))
              input))
  :argument "--filter=")

(transient-define-suffix kube-logs-action-run ()
  "Run kube logs with current selections."
  :transient nil
  (interactive)
  (kube-logs--sync-session-from-transient)
  (kube-logs-run))

(transient-define-infix kube-logs-infix-timestamp-path ()
  :description "Timestamp path"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj) (transient-infix-set obj kube-logs-timestamp-path))
  :reader (lambda (_prompt initial _hist)
            (let ((input (string-trim (read-string "Timestamp path (empty=unset): "
                                                   (or initial "")))))
              (setq kube-logs-timestamp-path
                    (unless (string-empty-p input) input))
              input))
  :argument "--timestamp-path=")

(transient-define-infix kube-logs-infix-level-path ()
  :description "Level path"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj) (transient-infix-set obj kube-logs-level-path))
  :reader (lambda (_prompt initial _hist)
            (let ((input (string-trim (read-string "Level path (empty=unset): "
                                                   (or initial "")))))
              (setq kube-logs-level-path
                    (unless (string-empty-p input) input))
              input))
  :argument "--level-path=")

(transient-define-infix kube-logs-infix-message-path ()
  :description "Message path"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj) (transient-infix-set obj kube-logs-message-path))
  :reader (lambda (_prompt initial _hist)
            (let ((input (string-trim (read-string "Message path (empty=unset): "
                                                   (or initial "")))))
              (setq kube-logs-message-path
                    (unless (string-empty-p input) input))
              input))
  :argument "--message-path=")

(defun kube-logs--formatting-reprompt ()
  "Refresh the formatting transient."
  (transient-quit-one)
  (kube-logs--setup-formatting-transient))

(defun kube-logs--formatting-extra-summary ()
  "Return one-line summary of currently configured extra paths."
  (if kube-logs-extra-paths
      (string-join kube-logs-extra-paths ", ")
    "none"))

(transient-define-suffix kube-logs-formatting-extra-add ()
  "Add one extra summary path."
  :transient t
  (interactive)
  (let ((input (string-trim (read-string "Add extra path: "))))
    (when (string-empty-p input)
      (user-error "Path cannot be empty"))
    (unless (member input kube-logs-extra-paths)
      (setq kube-logs-extra-paths
            (append kube-logs-extra-paths (list input))))
    (kube-logs--formatting-reprompt)))

(transient-define-suffix kube-logs-formatting-extra-delete ()
  "Delete one extra summary path."
  :transient t
  (interactive)
  (unless kube-logs-extra-paths
    (user-error "No extra paths to delete"))
  (let ((selection (completing-read "Delete extra path: "
                                    kube-logs-extra-paths nil t)))
    (setq kube-logs-extra-paths
          (delete selection (copy-sequence kube-logs-extra-paths)))
    (kube-logs--formatting-reprompt)))

(transient-define-suffix kube-logs-formatting-done ()
  "Return from formatting transient to the main transient."
  :transient nil
  (interactive)
  (transient-quit-one)
  (kube-logs--setup-main-transient))

(transient-define-prefix kube-logs-formatting-transient ()
  "Formatting options for kube-logs JSON-path rendering."
  :remember-value 'exit
  [["Fields"
    ("t" "Timestamp path" kube-logs-infix-timestamp-path)
    ("l" "Level path" kube-logs-infix-level-path)
    ("m" "Message path" kube-logs-infix-message-path)]

   [4 :description (lambda () (format "Extras: %s" (kube-logs--formatting-extra-summary)))
      ("a" "Add extra path" kube-logs-formatting-extra-add)
      ("d" "Delete extra path" kube-logs-formatting-extra-delete)]]

  [["Done"
    ("<return>" "Back to main" kube-logs-formatting-done)]]
  (interactive)
  (kube-logs--setup-formatting-transient))

(transient-define-suffix kube-logs-open-formatting ()
  "Open formatting transient."
  :transient nil
  (interactive)
  (kube-logs--setup-formatting-transient))

(defun kube-logs--sync-session-from-transient ()
  "Sync backing session vars from active `kube-logs-transient` infix args."
  (when (and (boundp 'transient-current-command)
             (eq transient-current-command 'kube-logs-transient))
    (let* ((args (transient-args 'kube-logs-transient))
           (tail (transient-arg-value "--tail=" args))
           (since (transient-arg-value "--since=" args))
           (filter (transient-arg-value "--filter=" args)))
      (setq kube-logs-tail-lines
            (unless (or (null tail) (string-empty-p tail))
              (let ((n (string-to-number tail)))
                (when (<= n 0)
                  (user-error "Tail lines must be a positive integer"))
                n)))
      (setq kube-logs-since
            (unless (or (null since) (string-empty-p since)) since))
      (setq kube-logs-filter
            (unless (or (null filter) (string-empty-p filter)) filter)))))

(defun kube-logs--viewer-buffer-description ()
  "Return transient description for the selected kube viewer buffer."
  (format "Buffer: %s" (or kube-logs-viewer-buffer "new")))

(transient-define-suffix kube-logs-select-viewer-buffer ()
  "Select an existing json-log-viewer buffer for Kubernetes ingestion."
  :description #'kube-logs--viewer-buffer-description
  :transient t
  (interactive)
  (let* ((prompt (if kube-logs-viewer-buffer
                     (format "Send kube logs to buffer (empty=new, current %s): "
                             kube-logs-viewer-buffer)
                   "Send kube logs to buffer (empty=new): "))
         (input (string-trim
                 (completing-read
                  prompt (mapcar #'buffer-name (buffer-list)) nil nil
                  nil 'buffer-name-history))))
    (setq kube-logs-viewer-buffer
          (unless (string-empty-p input)
            (buffer-name (json-log-viewer-get-buffer input))))
    (kube-logs--transient-reprompt)))

(transient-define-prefix kube-logs-transient ()
  "Transient menu for selecting and running Kubernetes logs."
  :remember-value 'exit
  [[("@" "Apply preset…" kube-logs-apply-preset)]]
  [["Config"
    ("-m" "Tail lines" kube-logs-infix-tail-lines)
    ("-s" "Since" kube-logs-infix-since)
    ("-F" "Filter" kube-logs-infix-filter)
    ("-f" kube-logs-toggle-follow)
    ("-B" kube-logs-select-viewer-buffer)
    ("-b" kube-logs-toggle-stream-backend)
    ("-D" kube-logs-toggle-debug-process-buffer)]
   ["Target"
    ("c" kube-logs-select-context)
    ("n" kube-logs-set-namespace)
    ("p" kube-logs-select-pod)
    ("d" kube-logs-select-deployment)]]
  [[4 :description (lambda () (format "Active target: %s" (kube-logs--target-description)))]]
  [["Actions"
    ("<return>" "Run logs" kube-logs-action-run)
    ("f" "Formatting…" kube-logs-open-formatting)]]
  (interactive)
  (kube-logs--setup-main-transient))

(defun kube-logs ()
  "Open kube-logs transient UI."
  (interactive)
  (call-interactively #'kube-logs-transient))

(provide 'kube-logs)
;;; kube-logs.el ends here
