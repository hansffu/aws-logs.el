;;; aws-logs-insights.el --- Insights execution and rendering -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Runs CloudWatch Logs Insights queries and renders foldable results.
;;
;;; Code:

(require 'json)
(require 'subr-x)
(require 'cl-lib)
(require 'json-log-viewer)

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
(defvar aws-logs-insights-live-narrow-max-rows)
(defvar aws-logs-insights-live-narrow-debounce)

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

(defvar-local aws-logs--insights-refresh-context nil
  "Refresh context plist for the current Insights viewer buffer.")

(defvar-local aws-logs--insights-query-id nil
  "Current query id displayed in the Insights header.")

(defvar-local aws-logs--insights-start-time nil
  "Current start time displayed in the Insights header.")

(defvar-local aws-logs--insights-end-time nil
  "Current end time displayed in the Insights header.")

(defvar-local aws-logs--insights-log-group nil
  "Current log group displayed in the Insights header.")

(defvar-local aws-logs--insights-seen-signatures nil
  "Hash table of row signatures already rendered in this buffer.")

(defvar-local aws-logs--insights-extra-fields nil
  "Extra summary fields used when converting rows to viewer log lines.")

(defvar-local aws-logs--insights-extra-paths nil
  "Synthetic extra summary paths used by the viewer for this buffer.")

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

(defun aws-logs--insights-window-context ()
  "Return refresh context plist for current run."
  (if aws-logs-custom-time-range
      (let* ((time-window (aws-logs--insights-time-window))
             (start (nth 0 time-window))
             (end (nth 1 time-window)))
        (list :mode 'custom-range
              :start start
              :end end))
    (let* ((time-window (aws-logs--insights-time-window))
           (start (nth 0 time-window))
           (end (nth 1 time-window)))
      (list :mode 'since
            :start start
            :end end
            :last-end end))))

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

(defun aws-logs--insights-run-query-window (start-time end-time)
  "Run Insights query for START-TIME..END-TIME and return (QUERY-ID . PAYLOAD)."
  (let* ((start-payload (apply #'aws-logs--insights-call-cli-json
                               (append
                                (aws-logs--insights-global-args)
                                (list "logs" "start-query"
                                      "--log-group-name" aws-logs-log-group
                                      "--start-time" (number-to-string start-time)
                                      "--end-time" (number-to-string end-time)
                                      "--query-string" aws-logs-query))))
         (query-id (alist-get 'queryId start-payload))
         (results (aws-logs--wait-for-insights-results query-id)))
    (cons query-id results)))

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

(defun aws-logs--insights-summary (fields)
  "Build formatted summary line from FIELDS."
  (let* ((sources (aws-logs--insights-json-sources fields))
         (timestamp (aws-logs--insights-summary-value
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

(defun aws-logs--insights-row-signature (row)
  "Return a stable signature string for ROW."
  (prin1-to-string row))

(defun aws-logs--insights-summary-components (fields sources extra-fields)
  "Return summary plist for FIELDS and SOURCES using EXTRA-FIELDS."
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
         (extras
          (mapcar (lambda (field)
                    (when-let ((value (aws-logs--insights-resolve fields sources field)))
                      (unless (string-empty-p value)
                        value)))
                  extra-fields)))
    (list :timestamp timestamp :level level :message message :extras extras)))

(defun aws-logs--insights-extra-summary-paths (extra-fields)
  "Return synthetic summary extra paths for EXTRA-FIELDS."
  (cl-loop for _field in extra-fields
           for index from 0
           collect (format "__summary_extra_%d" index)))

(defun aws-logs--insights-row->json-line (row extra-fields extra-paths)
  "Convert Insights ROW to one JSON line using EXTRA-FIELDS and EXTRA-PATHS."
  (let* ((fields (aws-logs--insights-row-fields row))
         (sources (aws-logs--insights-json-sources fields))
         (summary (aws-logs--insights-summary-components fields sources extra-fields))
         (json-object (make-hash-table :test 'equal))
         (summary-alist (list (cons "__summary_timestamp" (or (plist-get summary :timestamp) "-"))
                              (cons "__summary_level" (or (plist-get summary :level) "-"))
                              (cons "__summary_message" (or (plist-get summary :message) "-")))))
    (cl-mapc (lambda (path value)
               (when value
                 (push (cons path value) summary-alist)))
             extra-paths
             (plist-get summary :extras))
    (dolist (pair (append fields (nreverse summary-alist)))
      (puthash (car pair) (cdr pair) json-object))
    (json-serialize json-object)))

(defun aws-logs--insights-row->viewer-line (row)
  "Convert ROW to one viewer JSON line using current buffer-local settings."
  (aws-logs--insights-row->json-line row aws-logs--insights-extra-fields aws-logs--insights-extra-paths))

(defun aws-logs--insights-header-lines (_state)
  "Return header lines for the current Insights viewer buffer."
  (list (cons "Query ID" (format "%s" (or aws-logs--insights-query-id "-")))
        (cons "Log group" (or aws-logs--insights-log-group "-"))
        (cons "Time window"
              (format "%s -> %s"
                      (or aws-logs--insights-start-time "-")
                      (or aws-logs--insights-end-time "-")))))

(defun aws-logs--insights-refresh-log-lines (old-log-lines)
  "Refresh callback for the Insights viewer.

OLD-LOG-LINES is the current list of raw viewer lines."
  (unless aws-logs--insights-refresh-context
    (user-error "Missing Insights refresh context in buffer"))
  (unless aws-logs--insights-seen-signatures
    (setq aws-logs--insights-seen-signatures (make-hash-table :test 'equal)))
  (pcase (plist-get aws-logs--insights-refresh-context :mode)
    ('since
     (let* ((refresh-start (plist-get aws-logs--insights-refresh-context :last-end))
            (refresh-end (floor (float-time)))
            (refresh-result (aws-logs--insights-run-query-window refresh-start refresh-end))
            (refresh-query-id (car refresh-result))
            (refresh-payload (cdr refresh-result))
            (refresh-rows (append (alist-get 'results refresh-payload) nil))
            unseen-rows)
       (dolist (row refresh-rows)
         (let ((sig (aws-logs--insights-row-signature row)))
           (unless (gethash sig aws-logs--insights-seen-signatures)
             (puthash sig t aws-logs--insights-seen-signatures)
             (push row unseen-rows))))
       (setq unseen-rows (nreverse unseen-rows))
       (setq aws-logs--insights-query-id refresh-query-id)
       (setq aws-logs--insights-end-time refresh-end)
       (setq aws-logs--insights-refresh-context
             (plist-put (copy-sequence aws-logs--insights-refresh-context)
                        :last-end refresh-end))
       (append old-log-lines (mapcar #'aws-logs--insights-row->viewer-line unseen-rows))))
    ('custom-range
     (let* ((refresh-start (plist-get aws-logs--insights-refresh-context :start))
            (refresh-end (plist-get aws-logs--insights-refresh-context :end))
            (refresh-result (aws-logs--insights-run-query-window refresh-start refresh-end))
            (refresh-query-id (car refresh-result))
            (refresh-payload (cdr refresh-result))
            (refresh-rows (append (alist-get 'results refresh-payload) nil)))
       (setq aws-logs--insights-query-id refresh-query-id)
       (setq aws-logs--insights-start-time refresh-start)
       (setq aws-logs--insights-end-time refresh-end)
       (clrhash aws-logs--insights-seen-signatures)
       (dolist (row refresh-rows)
         (puthash (aws-logs--insights-row-signature row) t aws-logs--insights-seen-signatures))
       (mapcar #'aws-logs--insights-row->viewer-line refresh-rows)))
    (_
     (user-error "Unknown refresh mode: %S"
                 (plist-get aws-logs--insights-refresh-context :mode)))))

(defun aws-logs-insights-refresh ()
  "Refresh current Insights viewer buffer."
  (interactive)
  (json-log-viewer-refresh))

(defun aws-logs-insights-run ()
  "Run Logs Insights query and display formatted results."
  (unless (and aws-logs-log-group (not (string-empty-p aws-logs-log-group)))
    (user-error "Select a log group first"))
  (unless (and aws-logs-query (not (string-empty-p aws-logs-query)))
    (user-error "Set a Logs Insights query first"))
  (let* ((refresh-context (aws-logs--insights-window-context))
         (start-time (plist-get refresh-context :start))
         (end-time (plist-get refresh-context :end))
         (result (aws-logs--insights-run-query-window start-time end-time))
         (query-id (car result))
         (payload (cdr result))
         (rows (append (alist-get 'results payload) nil))
         (extra-fields (append aws-logs-summary-extra-fields nil))
         (extra-paths (aws-logs--insights-extra-summary-paths extra-fields))
         (initial-lines (mapcar (lambda (row)
                                  (aws-logs--insights-row->json-line row extra-fields extra-paths))
                                rows))
         (buffer-name (format "*AWS insights - %s*" aws-logs-log-group))
         (buffer
          (json-log-viewer-make-buffer
           buffer-name
           :log-lines initial-lines
           :timestamp-path "__summary_timestamp"
           :level-path "__summary_level"
           :message-path "__summary_message"
           :extra-paths extra-paths
           :refresh-function #'aws-logs--insights-refresh-log-lines
           :streaming nil
           :direction 'newest-first
           :header-lines-function #'aws-logs--insights-header-lines
           :live-narrow-max-rows aws-logs-insights-live-narrow-max-rows
           :live-narrow-debounce aws-logs-insights-live-narrow-debounce)))
    (with-current-buffer buffer
      (setq-local aws-logs--insights-refresh-context (copy-sequence refresh-context))
      (setq-local aws-logs--insights-query-id query-id)
      (setq-local aws-logs--insights-start-time start-time)
      (setq-local aws-logs--insights-end-time end-time)
      (setq-local aws-logs--insights-log-group aws-logs-log-group)
      (setq-local aws-logs--insights-extra-fields extra-fields)
      (setq-local aws-logs--insights-extra-paths extra-paths)
      (setq-local aws-logs--insights-seen-signatures (make-hash-table :test 'equal))
      (dolist (row rows)
        (puthash (aws-logs--insights-row-signature row) t aws-logs--insights-seen-signatures))
      ;; Refresh header now that buffer-local header state is initialized.
      (json-log-viewer--refresh-header))
    (display-buffer buffer)
    (message "Insights query completed for %s" aws-logs-log-group)))

(provide 'aws-logs-insights)
;;; aws-logs-insights.el ends here
