;;; aws-logs-insights.el --- Insights execution and rendering -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Runs CloudWatch Logs Insights queries and renders foldable results.
;;
;;; Code:

(require 'json)
(require 'subr-x)

(defvar aws-logs-cli)
(defvar aws-logs-endpoint)
(defvar aws-logs-region)
(defvar aws-logs-profile)
(defvar aws-logs-log-group)
(defvar aws-logs-query)
(defvar aws-logs-time-range)
(defvar aws-logs-custom-time-range)
(defvar aws-logs-summary-timestamp-field)
(defvar aws-logs-summary-level-field)
(defvar aws-logs-summary-message-field)
(defvar aws-logs-summary-extra-fields)

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

(defface aws-logs-insights-extra-face
  '((t :inherit font-lock-variable-name-face))
  "Face for bracketed extra segments in Insights summaries."
  :group 'aws-logs)

(defface aws-logs-insights-key-face
  '((t :inherit font-lock-keyword-face))
  "Face for keys in expanded Insights result details."
  :group 'aws-logs)

(defvar-local aws-logs--insights-overlays nil
  "Fold overlays in the current Insights results buffer.")

(defun aws-logs--insights-global-args ()
  "Build common AWS CLI arguments from current backing fields."
  (delq nil
        (list
         (when aws-logs-endpoint (format "--endpoint-url=%s" aws-logs-endpoint))
         (format "--region=%s" aws-logs-region)
         (when aws-logs-profile (format "--profile=%s" aws-logs-profile)))))

(defun aws-logs--insights-parse-time-range-to-seconds (time-range)
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
  (if aws-logs-custom-time-range
      (let* ((from-time (or (ignore-errors (date-to-time (car aws-logs-custom-time-range)))
                            (user-error "Invalid custom from-time: %s"
                                        (car aws-logs-custom-time-range))))
             (to-time (or (ignore-errors (date-to-time (cdr aws-logs-custom-time-range)))
                          (user-error "Invalid custom to-time: %s"
                                      (cdr aws-logs-custom-time-range))))
             (start (floor (float-time from-time)))
             (end (floor (float-time to-time))))
        (when (>= start end)
          (user-error "Custom from/to range must have From before To"))
        (list start end))
    (let* ((seconds (or (aws-logs--insights-parse-time-range-to-seconds aws-logs-time-range)
                        (user-error "Unsupported --since format for Insights: %s" aws-logs-time-range)))
           (end (floor (float-time)))
           (start (- end seconds)))
      (list start end))))

(defun aws-logs--insights-call-cli-json (&rest args)
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
      (setq payload (apply #'aws-logs--insights-call-cli-json
                           (append (aws-logs--insights-global-args)
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

(defun aws-logs--insights-summary-value (fields sources explicit-field candidates)
  "Return summary value from EXPLICIT-FIELD or CANDIDATES."
  (if (and explicit-field (not (string-empty-p explicit-field)))
      (or (aws-logs--insights-resolve fields sources explicit-field) "-")
    (or (aws-logs--insights-first-match fields sources candidates) "-")))

(defun aws-logs--insights-summary (fields sources)
  "Build formatted summary line from FIELDS and JSON SOURCES."
  (let* ((timestamp (aws-logs--insights-summary-value
                     fields sources
                     aws-logs-summary-timestamp-field
                     aws-logs--insights-timestamp-candidates))
         (level (aws-logs--insights-summary-value
                 fields sources
                 aws-logs-summary-level-field
                 aws-logs--insights-level-candidates))
         (message (if (and aws-logs-summary-message-field
                           (not (string-empty-p aws-logs-summary-message-field)))
                      (or (aws-logs--insights-resolve
                           fields sources aws-logs-summary-message-field)
                          "-")
                    (or (aws-logs--insights-first-match
                         fields sources aws-logs--insights-message-candidates)
                        (cdr (car fields))
                        "-")))
         (extras (delq nil
                       (mapcar (lambda (field)
                                 (when-let ((value (aws-logs--insights-resolve fields sources field)))
                                   (unless (string-empty-p value)
                                     value)))
                               aws-logs-summary-extra-fields))))
    (concat
     (propertize timestamp 'face 'aws-logs-insights-timestamp-face)
     " "
     (propertize (upcase level) 'face (aws-logs--insights-level-face level))
     (if extras
         (concat " "
                 (mapconcat (lambda (value)
                              (propertize
                               (format "[%s]" (aws-logs--insights-truncate value 80))
                               'face 'aws-logs-insights-extra-face))
                            extras
                            " "))
       "")
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
      (insert "  ")
      (insert (propertize (car pair) 'face 'aws-logs-insights-key-face))
      (insert ": " (cdr pair) "\n"))
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

(defun aws-logs-insights-run ()
  "Run Logs Insights query and display formatted results."
  (unless (and aws-logs-log-group (not (string-empty-p aws-logs-log-group)))
    (user-error "Select a log group first"))
  (unless (and aws-logs-query (not (string-empty-p aws-logs-query)))
    (user-error "Set a Logs Insights query first"))
  (let* ((time-window (aws-logs--insights-time-window))
         (start-time (nth 0 time-window))
         (end-time (nth 1 time-window))
         (start-payload (apply #'aws-logs--insights-call-cli-json
                               (append
                                (aws-logs--insights-global-args)
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

(provide 'aws-logs-insights)
;;; aws-logs-insights.el ends here
