;;; aws-logs.el --- Description -*- lexical-binding: t; -*-
;;
;; Copyright (C) 2021 Hans Fredrik Furholt
;;
;; Author: Hans Fredrik Furholt <https://github.com/hansffu>
;; Created: March 19, 2021
;; Modified: March 19, 2021
;; Version: 0.0.1
;; Keywords: Symbol’s value as variable is void: finder-known-keywords
;; Homepage: https://github.com/hansffu/aws-logs
;; Package-Requires: ((emacs "30.2"))
;;
;; This file is not part of GNU Emacs.
;;
;;; Commentary:
;;
;;  Description
;;
;;; Code:

(require 'subr-x)
(require 'comint)
(require 'transient)

(require 'aws-logs-query)

(defcustom aws-logs-cli "aws"
  "The cli command for aws cli."
  :type 'string
  :group 'aws-logs)
(defcustom aws-logs-endpoint nil
  "Customize endpoint."
  :type 'string
  :group 'aws-logs)
(defcustom aws-logs-region "eu-north-1"
  "Customize region."
  :type 'string
  :group 'aws-logs)
(defcustom aws-logs-format "detailed"
  "Sets the --format option on aws command."
  :type 'string
  :options '("detailed" "short" "json")
  :group 'aws-logs)
(defcustom aws-logs-default-profile "dev"
  "Default AWS CLI profile name to use.

The transient UI updates the session variable `aws-logs-profile` using `setq`.
If you want to change the default for new Emacs sessions, customize this option."
  :type '(choice (const :tag "Default" nil) string)
  :group 'aws-logs)

(defcustom aws-logs-since "10m"
  "Default time range to use as --since for `aws logs tail`."
  :type 'string
  :group 'aws-logs)

(defcustom aws-logs-default-log-group nil
  "Default CloudWatch log group used to initialize `aws-logs-log-group`."
  :type '(choice (const :tag "None" nil) string)
  :group 'aws-logs)

(defcustom aws-logs-default-query nil
  "Default Logs Insights query used to initialize `aws-logs-query`."
  :type '(choice (const :tag "None" nil) string)
  :group 'aws-logs)

(defcustom aws-logs-default-follow nil
  "Default value for `aws-logs-follow`."
  :type 'boolean
  :group 'aws-logs)

(defcustom aws-logs-default-since aws-logs-since
  "Default time range used to initialize `aws-logs-time-range`."
  :type 'string
  :group 'aws-logs)
(defcustom aws-logs-mode-hook '()
  "Hook for customizing aws-logs-mode."
  :type 'hook
  :group 'aws-logs)

(defcustom aws-logs-default-group nil
  "Default log group to use (legacy).

Prefer `aws-logs-default-log-group`."
  :type '(choice (const :tag "None" nil) string)
  :group 'aws-logs)

;; Backing fields for transient options.
;; These are the source of truth for aws-logs option state.

(defvar aws-logs-profile aws-logs-default-profile
  "AWS CLI profile name to use for this Emacs session.")

(defvar aws-logs-log-group aws-logs-default-log-group
  "Selected CloudWatch log group for this Emacs session.")

(defvar aws-logs-query aws-logs-default-query
  "Selected CloudWatch Logs Insights query string for this Emacs session, or nil when disabled.")

(defvar aws-logs-follow aws-logs-default-follow
  "Non-nil means follow (tail) logs for this Emacs session.")

(defvar aws-logs-time-range aws-logs-default-since
  "Selected time range (e.g. 10m) for this Emacs session. Used as --since for `aws logs tail`.")

(defun aws-logs--transient-reprompt ()
  "Refresh transient so UI reflects current backing fields." 
  (transient-quit-one)
  (transient-setup 'aws-logs-transient))


(declare-function aws-logs--insights-query-edit "aws-logs-query" (initial))
(declare-function aws-logs-insights-query-mode "aws-logs-query")
(defvar aws-logs-mode-map
  (let ((map (make-keymap)))
    (define-key map "q" #'aws-logs-quit-process-and-window)
    map)
  "Keymap for aws-logs-mode.")

(defun aws-logs--query ()
  "Return a one-line summary of `aws-logs-query` for display in transient."
  (if (and aws-logs-query (not (string-empty-p aws-logs-query)))
      (let ((s (replace-regexp-in-string "[\n\t ]+" " " aws-logs-query)))
        (if (> (length s) 80)
            (concat (substring s 0 77) "…")
          s))
    "— (none)"))

(defun aws-logs-quit-process-and-window ()
  "Quit current process and window."
  (interactive)
  (let ((proc (get-buffer-process (current-buffer))))
    (when (process-live-p proc)
      (delete-process proc))
    (quit-window t)))


(defun aws-logs--command (&rest args)
  "Build cli command with endpoint, region and ARGS."
  (let ((endpoint (if aws-logs-endpoint (format "--endpoint-url=%s" aws-logs-endpoint) ""))
        (region (format "--region=%s" aws-logs-region))
        (profile (when aws-logs-profile (format "--profile=%s" aws-logs-profile))))
    (string-join (delq nil (append (list aws-logs-cli endpoint region profile) args)) " ")))

(defun aws-logs--list-log-groups ()
  "Return log group name for all log groups."
  (let* ((result (with-temp-buffer
                   (list :exit-status
                         (call-process-shell-command (aws-logs--command "logs" "describe-log-groups") nil t)
                         :output
                         (buffer-string))))
         (output (plist-get result :output))
         (log-groups (alist-get 'logGroups (json-parse-string output :object-type 'alist) ))
         )
    (mapcar (lambda (log-group) (alist-get 'logGroupName log-group)) log-groups)
    )
  )

(define-derived-mode aws-logs-mode comint-mode "AWS-LOGS"
  "Displays logs fetched by aws-logs command."
  :interactive nil
  :group 'aws-logs
  (read-only-mode 1)
  )

(defun aws-logs-simple ()
  "Select a stream to tail."
  (interactive)
  (let* ((log-group (completing-read "Log group: " (aws-logs--list-log-groups)))
         (process (start-process-shell-command "aws-cli"
                                               (format "*AWS logs - %s*" log-group)
                                               (aws-logs--command "logs tail" log-group
                                                                  "--follow"
                                                                  "--format" aws-logs-format
                                                                  "--since" aws-logs-since))))
    (with-current-buffer (process-buffer process)
      (display-buffer (current-buffer))
      (aws-logs-mode)
      (set-process-filter process 'comint-output-filter)
      )
    ))

(transient-define-infix aws-logs-infix-log-group ()
  :description "Log group"
  :class 'transient-option
  :allow-empty nil
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-log-group))
  :reader (lambda (prompt initial hist)
            (let ((choices (aws-logs--list-log-groups)))
              (if choices
                  (let ((val (completing-read (concat prompt " ") choices nil t initial hist)))
                    (setq aws-logs-log-group val)
                    val)
                (read-string (concat prompt " (no groups found) ") ""))))
  :argument "--log-group=")

;; Infix for showing current Logs Insights query status. Editing is done via
;; suffix commands (see `aws-logs-query-edit`, `aws-logs-query-clear`, etc.).
(transient-define-infix aws-logs-infix-query ()
  :description "Query (Logs Insights)"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj)
                ;; Ensure the visible value always reflects the backing field
                ;; when the transient is entered/re-entered.
                (transient-infix-set obj aws-logs-query))
  :reader (lambda (_prompt initial _hist)
            ;; Keep this lightweight: just accept raw input if someone insists.
            ;; Most users should use the Edit/Preset/Clear suffixes.
            (let ((val (read-string "Query: " (or initial ""))))
              (setq aws-logs-query (if (string-empty-p val) nil val))
              val))
  :argument "--query=")

(transient-define-suffix aws-logs-query-edit ()
  "Edit the current Logs Insights query in a popup, and store it in the backing field."
  :transient t
  (interactive)
  (let ((edited (aws-logs--insights-query-edit aws-logs-query)))
    (when edited
      (setq aws-logs-query (if (string-empty-p edited) nil edited))
      (aws-logs--transient-reprompt))))

(transient-define-suffix aws-logs-query-clear ()
  "Disable the Logs Insights query (unset `aws-logs-query`)."
  :transient t
  (interactive)
  (setq aws-logs-query nil)
  (aws-logs--transient-reprompt))

(defcustom aws-logs-insights-query-presets nil
  "Alist of named Logs Insights query presets.

Each entry is (NAME . QUERY)."
  :type '(alist :key-type string :value-type string)
  :group 'aws-logs)

(transient-define-suffix aws-logs-query-preset ()
  "Select a preset Logs Insights query and store it in the backing field."
  :transient t
  (interactive)
  (unless aws-logs-insights-query-presets
    (user-error "No presets configured in `aws-logs-insights-query-presets`"))
  (let* ((name (completing-read "Preset: " (mapcar #'car aws-logs-insights-query-presets) nil t))
         (query (cdr (assoc name aws-logs-insights-query-presets))))
    (setq aws-logs-query query)
    (aws-logs--transient-reprompt)))

(transient-define-infix aws-logs-infix-follow ()
  :description "Follow (tail)"
  :class 'transient-switch
  :argument "--follow"
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-follow))
  :reader (lambda (_prompt initial _hist)
            (setq aws-logs-follow (not initial))
            aws-logs-follow))

(transient-define-infix aws-logs-infix-profile ()
  :description "Profile"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-profile))
  :reader (lambda (prompt initial hist)
            (let ((val (read-string prompt initial hist)))
              (setq aws-logs-profile (if (string-empty-p val) nil val))
              aws-logs-profile))
  :argument "--profile=")

;; Infix for specifying --since / time-range
(transient-define-infix aws-logs-infix-since ()
  :description "Since / Time range"
  :class 'transient-option
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-time-range))
  :reader (lambda (prompt initial hist)
            (let ((val (read-string prompt initial hist)))
              (setq aws-logs-time-range val)
              val))
  :argument "--since=")


(defun aws-logs--getopt (name args)
  "Get value for option prefix NAME (e.g. \"--since=\") from ARGS."
  (when-let ((s (seq-find (lambda (a) (string-prefix-p name a)) args)))
    (substring s (length name))))

(defun aws-logs--tail-args ()
  "Build argument list for `aws logs tail` using current backing fields."
  (append
   (list "logs" "tail" aws-logs-log-group
         "--format" aws-logs-format)
   (when aws-logs-follow
     (list "--follow"))
   (when (and aws-logs-time-range (not (string-empty-p aws-logs-time-range)))
     (list "--since" aws-logs-time-range))))

(defun aws-logs--global-args ()
  "Build common AWS CLI arguments from current backing fields."
  (delq nil
        (list
         (when aws-logs-endpoint (format "--endpoint-url=%s" aws-logs-endpoint))
         (format "--region=%s" aws-logs-region)
         (when aws-logs-profile (format "--profile=%s" aws-logs-profile)))))

(defun aws-logs--parse-time-range-to-seconds (time-range)
  "Convert TIME-RANGE like 10m/2h/1d/30s to seconds.

Returns nil when TIME-RANGE cannot be parsed."
  (when (and time-range
             (string-match "\\`\\([0-9]+\\)\\([smhdw]\\)\\'" time-range))
    (let ((value (string-to-number (match-string 1 time-range)))
          (unit (match-string 2 time-range)))
      (* value
         (pcase unit
           ("s" 1)
           ("m" 60)
           ("h" 3600)
           ("d" 86400)
           ("w" 604800))))))

(defun aws-logs--insights-time-window ()
  "Return (START END) epoch seconds for an Insights query."
  (let* ((seconds (or (aws-logs--parse-time-range-to-seconds aws-logs-time-range)
                      (user-error "Unsupported --since format for Insights: %s" aws-logs-time-range)))
         (end (floor (float-time)))
         (start (- end seconds)))
    (list start end)))

(defun aws-logs--call-cli-json (&rest args)
  "Run AWS CLI with ARGS and parse JSON result."
  (with-temp-buffer
    (let ((exit-code (apply #'call-process aws-logs-cli nil t nil args))
          (output (string-trim (buffer-string))))
      (unless (zerop exit-code)
        (user-error "AWS CLI failed (%s): %s" exit-code output))
      (condition-case _err
          (json-parse-string output :object-type 'alist)
        (error
         (user-error "Failed to parse AWS CLI JSON output: %s" output))))))

(defun aws-logs--wait-for-insights-results (query-id)
  "Poll get-query-results for QUERY-ID until complete."
  (let ((max-polls 120)
        (poll-delay 1)
        (poll-count 0)
        payload
        status)
    (while (< poll-count max-polls)
      (setq payload (apply #'aws-logs--call-cli-json
                           (append (aws-logs--global-args)
                                   (list "logs" "get-query-results"
                                         "--query-id" query-id))))
      (setq status (alist-get 'status payload))
      (cond
       ((string= status "Complete")
        (setq poll-count max-polls))
       ((member status '("Scheduled" "Running" "Unknown"))
        (setq poll-count (1+ poll-count))
        (sleep-for poll-delay))
       (t
        (user-error "Insights query did not complete: %s" status))))
    (if (string= status "Complete")
        payload
      (user-error "Timed out waiting for Insights query results"))))

(defface aws-logs-insights-timestamp-face
  '((t :inherit shadow))
  "Face for Insights timestamp summaries."
  :group 'aws-logs)

(defface aws-logs-insights-level-face
  '((t :inherit font-lock-constant-face))
  "Face for Insights level summaries."
  :group 'aws-logs)

(defface aws-logs-insights-message-face
  '((t :inherit default))
  "Face for Insights message summaries."
  :group 'aws-logs)

(defvar-local aws-logs--insights-overlays nil
  "Fold overlays in the current Insights results buffer.")

(defun aws-logs--insights-clear-overlays ()
  "Remove all fold overlays in the current buffer."
  (mapc #'delete-overlay aws-logs--insights-overlays)
  (setq aws-logs--insights-overlays nil))

(defun aws-logs--insights-overlay-at-point ()
  "Return fold overlay at point, or nil."
  (or (get-text-property (point) 'aws-logs-details-overlay)
      (get-text-property (line-beginning-position) 'aws-logs-details-overlay)
      (catch 'found
        (dolist (ov (overlays-at (point)))
          (when (overlay-get ov 'aws-logs-fold)
            (throw 'found ov)))
        nil)))

(defun aws-logs-insights-toggle-entry ()
  "Toggle fold state for current Insights result entry."
  (interactive)
  (when-let ((ov (aws-logs--insights-overlay-at-point)))
    (overlay-put ov 'invisible (not (overlay-get ov 'invisible)))))

(defun aws-logs-insights-toggle-all ()
  "Toggle fold state for all Insights result entries."
  (interactive)
  (let ((show-any nil))
    (dolist (ov aws-logs--insights-overlays)
      (when (overlay-get ov 'invisible)
        (setq show-any t)))
    (dolist (ov aws-logs--insights-overlays)
      (overlay-put ov 'invisible (not show-any)))))

(defvar-keymap aws-logs-insights-results-mode-map
  :doc "Keymap for Insights results buffers."
  "TAB" #'aws-logs-insights-toggle-entry
  "<tab>" #'aws-logs-insights-toggle-entry
  "<backtab>" #'aws-logs-insights-toggle-all)

(define-derived-mode aws-logs-insights-results-mode special-mode "AWS-Insights"
  "Major mode for formatted AWS Logs Insights query results."
  :group 'aws-logs
  (setq-local truncate-lines t)
  (setq-local buffer-invisibility-spec '(t)))

(defconst aws-logs--insights-timestamp-candidates
  '("@timestamp" "timestamp" "time" "ts"
    "message.@timestamp" "message.timestamp" "message.time" "message.ts")
  "Field/path candidates for summary timestamp.")

(defconst aws-logs--insights-level-candidates
  '("level" "severity" "logLevel" "lvl"
    "message.level" "message.log.level" "message.severity" "log.level")
  "Field/path candidates for summary level.")

(defconst aws-logs--insights-message-candidates
  '("@message" "message" "msg"
    "message.message" "log.message")
  "Field/path candidates for summary message.")

(defun aws-logs--insights-value->string (value)
  "Convert VALUE into a display string."
  (cond
   ((stringp value) value)
   ((numberp value) (number-to-string value))
   ((eq value t) "true")
   ((eq value :false) "false")
   ((null value) nil)
   (t (format "%s" value))))

(defun aws-logs--insights-parse-json-maybe (value)
  "Parse VALUE as JSON object/list when possible."
  (when (and (stringp value)
             (string-match-p "\\`[[:space:]\n\r\t]*[{\\[]" value))
    (condition-case nil
        (json-parse-string value :object-type 'alist :array-type 'list
                           :null-object nil :false-object :false)
      (error nil))))

(defun aws-logs--insights-get-child (node key)
  "Return child KEY from NODE."
  (cond
   ((hash-table-p node)
    (or (gethash key node)
        (gethash (intern-soft key) node)))
   ((listp node)
    (or (alist-get key node nil nil #'equal)
        (when-let ((sym (intern-soft key)))
          (alist-get sym node))))
   (t nil)))

(defun aws-logs--insights-get-path (node path)
  "Return value at PATH in NODE, where PATH is dot-separated."
  (let ((parts (split-string path "\\."))
        (current node)
        (failed nil))
    (while (and parts (not failed))
      (setq current (aws-logs--insights-get-child current (car parts)))
      (unless current
        (setq failed t))
      (setq parts (cdr parts)))
    (unless failed
      current)))

(defun aws-logs--insights-row-fields (row)
  "Return alist of string field/value pairs from ROW."
  (let (fields)
    (mapc (lambda (cell)
            (let ((name (aws-logs--insights-value->string (alist-get 'field cell)))
                  (value (aws-logs--insights-value->string (alist-get 'value cell))))
              (when name
                (push (cons name (or value "")) fields))))
          row)
    (nreverse fields)))

(defun aws-logs--insights-json-sources (fields)
  "Build an alist of parsed JSON roots from FIELDS."
  (let (sources)
    (dolist (pair fields)
      (when-let ((json (aws-logs--insights-parse-json-maybe (cdr pair))))
        (push (cons (car pair) json) sources)))
    sources))

(defun aws-logs--insights-resolve (fields sources candidate)
  "Resolve CANDIDATE from FIELDS and parsed JSON SOURCES."
  (or (cdr (assoc candidate fields))
      (let* ((parts (split-string candidate "\\."))
             (root (car parts))
             (rest (string-join (cdr parts) ".")))
        (or
         (when-let ((root-json (cdr (assoc root sources))))
           (if (string-empty-p rest)
               (aws-logs--insights-value->string root-json)
             (aws-logs--insights-value->string
              (aws-logs--insights-get-path root-json rest))))
         (when (= (length parts) 1)
           (catch 'found
             (dolist (source sources)
               (when-let ((val (aws-logs--insights-get-path (cdr source) candidate)))
                 (throw 'found (aws-logs--insights-value->string val))))
             nil))))))

(defun aws-logs--insights-first-match (fields sources candidates)
  "Return first non-empty match from CANDIDATES."
  (catch 'found
    (dolist (candidate candidates)
      (when-let ((value (aws-logs--insights-resolve fields sources candidate)))
        (unless (string-empty-p value)
          (throw 'found value))))
    nil))

(defun aws-logs--insights-level-face (level)
  "Return face symbol suitable for LEVEL."
  (let ((normalized (downcase (or level ""))))
    (cond
     ((string-match-p "\\`\\(error\\|fatal\\|crit\\|panic\\)" normalized) 'error)
     ((string-match-p "\\`\\(warn\\|warning\\)" normalized) 'warning)
     ((string-match-p "\\`\\(debug\\|trace\\)" normalized) 'font-lock-doc-face)
     (t 'aws-logs-insights-level-face))))

(defun aws-logs--insights-truncate (value limit)
  "Truncate VALUE to LIMIT characters."
  (if (> (length value) limit)
      (concat (substring value 0 (- limit 1)) "…")
    value))

(defun aws-logs--insights-summary (fields sources)
  "Build formatted summary line from FIELDS and JSON SOURCES."
  (let* ((timestamp (or (aws-logs--insights-first-match
                         fields sources aws-logs--insights-timestamp-candidates)
                        "-"))
         (level (or (aws-logs--insights-first-match
                     fields sources aws-logs--insights-level-candidates)
                    "-"))
         (message (or (aws-logs--insights-first-match
                       fields sources aws-logs--insights-message-candidates)
                      (cdr (car fields))
                      "-")))
    (concat
     (propertize timestamp 'face 'aws-logs-insights-timestamp-face)
     " "
     (propertize (upcase level) 'face (aws-logs--insights-level-face level))
     " "
     (propertize (aws-logs--insights-truncate message 240) 'face 'aws-logs-insights-message-face))))

(defun aws-logs--insights-insert-entry (row)
  "Insert one foldable results entry for ROW."
  (let* ((fields (aws-logs--insights-row-fields row))
         (sources (aws-logs--insights-json-sources fields))
         (summary-start (point))
         details-start details-end ov)
    (insert (aws-logs--insights-summary fields sources) "\n")
    (setq details-start (point))
    (dolist (pair fields)
      (insert "  " (car pair) ": " (cdr pair) "\n"))
    (insert "\n")
    (setq details-end (point))
    (setq ov (make-overlay details-start details-end))
    (overlay-put ov 'aws-logs-fold t)
    (overlay-put ov 'invisible t)
    (push ov aws-logs--insights-overlays)
    (put-text-property summary-start (1- details-start) 'aws-logs-details-overlay ov)))

(defun aws-logs--render-insights-results (buffer query-id start-time end-time payload)
  "Render Insights query PAYLOAD into BUFFER."
  (with-current-buffer buffer
    (let ((inhibit-read-only t)
          (rows (alist-get 'results payload)))
      (aws-logs-insights-results-mode)
      (aws-logs--insights-clear-overlays)
      (erase-buffer)
      (insert (format "Insights query id: %s\n" query-id))
      (insert (format "Log group: %s\n" aws-logs-log-group))
      (insert (format "Time window: %s -> %s\n" start-time end-time))
      (insert "TAB: toggle entry  S-TAB: toggle all  q: quit\n\n")
      (if (or (null rows) (= (length rows) 0))
          (insert "No results.\n")
        (mapc #'aws-logs--insights-insert-entry rows)))
    (goto-char (point-min))
    (display-buffer buffer)))

(transient-define-suffix aws-logs-tail ()
  "Start streaming logs in a dedicated buffer using current transient selections."
  :transient nil
  (interactive)
  (unless (and aws-logs-log-group (not (string-empty-p aws-logs-log-group)))
    (user-error "Select a log group first"))
  (let* ((buffer (get-buffer-create (format "*AWS logs - %s*" aws-logs-log-group)))
         (process-name (format "aws-logs-tail:%s" aws-logs-log-group))
         (args (append (aws-logs--global-args) (aws-logs--tail-args)))
         (process (apply #'start-process process-name buffer aws-logs-cli args)))
    (with-current-buffer buffer
      (let ((inhibit-read-only t))
        (erase-buffer))
      (display-buffer buffer)
      (aws-logs-mode)
      (set-process-filter process #'comint-output-filter))
    (set-process-query-on-exit-flag process nil)
    (message "Started logs tail for %s" aws-logs-log-group)))

(transient-define-suffix aws-logs-insights ()
  "Run Logs Insights query and show results in a dedicated buffer."
  :transient nil
  (interactive)
  (unless (and aws-logs-log-group (not (string-empty-p aws-logs-log-group)))
    (user-error "Select a log group first"))
  (unless (and aws-logs-query (not (string-empty-p aws-logs-query)))
    (user-error "Set a Logs Insights query first"))
  (let* ((time-window (aws-logs--insights-time-window))
         (start-time (nth 0 time-window))
         (end-time (nth 1 time-window))
         (start-payload (apply #'aws-logs--call-cli-json
                               (append
                                (aws-logs--global-args)
                                (list "logs" "start-query"
                                      "--log-group-name" aws-logs-log-group
                                      "--start-time" (number-to-string start-time)
                                      "--end-time" (number-to-string end-time)
                                      "--query-string" aws-logs-query))))
         (query-id (alist-get 'queryId start-payload))
         (results (aws-logs--wait-for-insights-results query-id))
         (buffer (get-buffer-create (format "*AWS insights - %s*" aws-logs-log-group))))
    (aws-logs--render-insights-results buffer query-id start-time end-time results)
    (message "Insights query completed for %s" aws-logs-log-group)))

(defun aws-logs--dwim-description ()
  "Return dynamic description for the DWIM action."
  (let ((target (if (and aws-logs-query (not (string-empty-p aws-logs-query)))
                    "Insights"
                  "Tail")))
    (concat "Run " (propertize (format "(%s)" target) 'face 'transient-active-infix))))

(transient-define-suffix aws-logs-dwim ()
  "Run tail or Logs Insights depending on whether a query is set."
  :description #'aws-logs--dwim-description
  :transient nil
  (interactive)
  (if (and aws-logs-query (not (string-empty-p aws-logs-query)))
      (call-interactively #'aws-logs-insights)
    (call-interactively #'aws-logs-tail)))

(defvar aws-logs--transient-history nil)


;; Main transient prefix
(transient-define-prefix aws-logs-transient ()
  "AWS Logs transient menu.

Use the Tail action to stream logs with current selections."
  :remember-value 'exit
  [["Config"
    ("-g" aws-logs-infix-log-group)
    ("-s" "Since" aws-logs-infix-since)
    ("-f" "Follow (tail)" aws-logs-infix-follow)
    ("-p" "Profile" aws-logs-infix-profile)]

   [4 :description (lambda () (format "Query: %s" (aws-logs--query)))
      ("-q" "Edit query…" aws-logs-query-edit)
      ("Q" "Preset…" aws-logs-query-preset)
      ("X" "Clear query" aws-logs-query-clear)]]

  [["Actions"
    ("RET" aws-logs-dwim)
    ("t" "Tail" aws-logs-tail)
    ("l" "Logs Insights" aws-logs-insights)]]
  (interactive)
  (transient-setup 'aws-logs-transient))

(defun aws-logs ()
  "Open aws-logs transient menu for selecting and running log-tail options."
  (interactive)
  (call-interactively #'aws-logs-transient))

(provide 'aws-logs)
;;; aws-logs.el ends here
