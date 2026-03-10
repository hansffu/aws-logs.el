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

(require 'json-log-viewer)

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

(defvar kube-logs-presets nil
  "Alist of named kube-logs presets.

Each element has the form (NAME . PLIST).")

(defvar-local kube-logs--process nil
  "Process associated with the current kube-logs viewer buffer.")

(defvar-local kube-logs--pending-fragment ""
  "Trailing incomplete process output fragment for streaming buffers.")

(defvar-local kube-logs--once-output-buffer nil
  "Temporary process output buffer for one-shot asynchronous fetches.")

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

(defun kube-logs--transient-reprompt ()
  "Refresh transient so UI reflects current backing fields."
  (transient-quit-one)
  (transient-setup 'kube-logs-transient))

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

(defun kube-logs--install-viewer-keymap ()
  "Install buffer-local keymap tweaks for kube logs viewer buffers."
  (let ((map (copy-keymap (current-local-map))))
    (define-key map (kbd "q") #'kube-logs-quit-process-and-window)
    (use-local-map map)))

(defun kube-logs--kill-buffer-process (buffer)
  "Stop process and cleanup state associated with BUFFER, if any."
  (when (buffer-live-p buffer)
    (with-current-buffer buffer
      (let ((proc (or kube-logs--process
                      (get-buffer-process buffer))))
        (when (process-live-p proc)
          (delete-process proc))
        (setq kube-logs--process nil))
      (setq kube-logs--pending-fragment "")
      (when (buffer-live-p kube-logs--once-output-buffer)
        (kill-buffer kube-logs--once-output-buffer))
      (setq kube-logs--once-output-buffer nil))))

(defun kube-logs-quit-process-and-window ()
  "Stop log process for current buffer and close the window."
  (interactive)
  (kube-logs--kill-buffer-process (current-buffer))
  (quit-window t))

(defun kube-logs--make-viewer-buffer ()
  "Create kube logs viewer buffer."
  (let* ((buffer-name (kube-logs--viewer-buffer-name))
         (existing (get-buffer buffer-name))
         buffer)
    (when existing
      (kube-logs--kill-buffer-process existing))
    (setq buffer
          (json-log-viewer-make-buffer
           buffer-name
           :timestamp-path "timestamp"
           :level-path kube-logs-level-path
           :message-path kube-logs-message-path
           :extra-paths kube-logs-extra-paths
           :mode #'kube-logs-viewer-mode
           :header-lines-function #'kube-logs--viewer-header-lines))
    (with-current-buffer buffer
      (setq-local kube-logs--process nil)
      (setq-local kube-logs--pending-fragment "")
      (setq-local kube-logs--once-output-buffer nil)
      (setq-local kube-logs--viewer-context kube-logs-context)
      (setq-local kube-logs--viewer-namespace kube-logs-namespace)
      (setq-local kube-logs--viewer-namespace-enabled kube-logs-namespace-enabled)
      (setq-local kube-logs--viewer-target-kind kube-logs-target-kind)
      (setq-local kube-logs--viewer-target kube-logs-target)
      (setq-local kube-logs--viewer-follow kube-logs-follow)
      (setq-local kube-logs--viewer-tail kube-logs-tail-lines)
      (setq-local kube-logs--viewer-since kube-logs-since)
      (add-hook 'kill-buffer-hook (lambda () (kube-logs--kill-buffer-process (current-buffer))) nil t)
      (kube-logs--install-viewer-keymap))
    buffer))

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

(defun kube-logs--stream-process-filter (process output)
  "Process filter for streaming kube logs PROCESS OUTPUT."
  (let ((buffer (process-buffer process)))
    (when (buffer-live-p buffer)
      (with-current-buffer buffer
        (let* ((lines (kube-logs--consume-chunk-lines output))
               (json-lines (kube-logs--lines->json-lines lines)))
          (when json-lines
            (json-log-viewer-push buffer json-lines)))))))

(defun kube-logs--stream-process-sentinel (process event)
  "Process sentinel for streaming kube logs PROCESS EVENT."
  (let ((buffer (process-buffer process)))
    (when (buffer-live-p buffer)
      (with-current-buffer buffer
        (kube-logs--flush-pending-fragment)
        (setq kube-logs--process nil)))
    (when (and (memq (process-status process) '(exit signal))
               (not (zerop (process-exit-status process)))
               (not (and kube-logs-filter
                         (= (process-exit-status process) 1))))
      (message "kubectl logs exited (%s): %s"
               (process-exit-status process)
               (string-trim event)))))

(defun kube-logs--run-once ()
  "Fetch logs once asynchronously and render in json-log-viewer."
  (let* ((buffer (kube-logs--make-viewer-buffer))
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
                         (when (eq kube-logs--process proc)
                           (setq kube-logs--process nil))
                         (when (eq kube-logs--once-output-buffer output-buffer)
                           (setq kube-logs--once-output-buffer nil))
                         (if (or (zerop exit-code)
                                 (and kube-logs-filter (= exit-code 1)))
                             (let* ((raw-lines (split-string output "\n" t))
                                    (json-lines (kube-logs--lines->json-lines raw-lines)))
                               (json-log-viewer-replace-log-lines buffer json-lines nil)
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
    (with-current-buffer buffer
      (setq-local kube-logs--process process))))

(defun kube-logs--run-stream ()
  "Start streaming logs and render them in json-log-viewer."
  (let* ((buffer (kube-logs--make-viewer-buffer))
         (args (kube-logs--logs-args))
         (command (kube-logs--command-with-filter args t))
         (process (make-process
                   :name (kube-logs--process-name)
                   :buffer buffer
                   :command command
                   :noquery t
                   :connection-type 'pipe)))
    (with-current-buffer buffer
      (setq-local kube-logs--process process)
      (setq-local kube-logs--pending-fragment "")
      (display-buffer buffer))
    (set-process-filter process #'kube-logs--stream-process-filter)
    (set-process-sentinel process #'kube-logs--stream-process-sentinel)
    (set-process-query-on-exit-flag process nil)
    (message "Started kube logs stream for %s" (kube-logs--target-description))))

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

(transient-define-prefix kube-logs-transient ()
  "Transient menu for selecting and running kubectl logs."
  :remember-value 'exit
  [[("@" "Apply preset…" kube-logs-apply-preset)]]
  [["Config"
    ("-m" "Tail lines" kube-logs-infix-tail-lines)
    ("-s" "Since" kube-logs-infix-since)
    ("-F" "Filter" kube-logs-infix-filter)
    ("-f" kube-logs-toggle-follow)]
   ["Target"
    ("c" kube-logs-select-context)
    ("n" kube-logs-set-namespace)
    ("-n" kube-logs-set-namespace)
    ("p" kube-logs-select-pod)
    ("d" kube-logs-select-deployment)]]
  [[4 :description (lambda () (format "Active target: %s" (kube-logs--target-description)))]]
  [["Actions"
    ("RET" "Run logs" kube-logs-action-run)]]
  (interactive)
  (transient-setup 'kube-logs-transient))

(defun kube-logs ()
  "Open kube-logs transient UI."
  (interactive)
  (call-interactively #'kube-logs-transient))

(provide 'kube-logs)
;;; kube-logs.el ends here
