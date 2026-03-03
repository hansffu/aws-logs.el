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
(defvar aws-logs-insights-refresh-overlap-seconds)

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

(defvar-local aws-logs--insights-source-global-args nil
  "Frozen AWS CLI global args used for this buffer's Insights requests.")

(defvar-local aws-logs--insights-source-log-group nil
  "Frozen log group used for this buffer's Insights requests.")

(defvar-local aws-logs--insights-source-query nil
  "Frozen query string used for this buffer's Insights requests.")

(defvar-local aws-logs--insights-active-request-id 0
  "Monotonic request id for current buffer's async Insights lifecycle.")

(defvar-local aws-logs--insights-request-in-flight nil
  "Non-nil when an async Insights request is running for this buffer.")

(defvar-local aws-logs--insights-active-process nil
  "Current active AWS CLI process for this buffer's async Insights request.")

(defvar-local aws-logs--insights-active-timer nil
  "Current poll timer for this buffer's async Insights request.")

(defconst aws-logs--insights-max-polls 120
  "Maximum number of async poll attempts for one Insights query.")

(defconst aws-logs--insights-poll-delay 1
  "Seconds between async poll attempts for one Insights query.")

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

(defun aws-logs--insights-cancel-active-request ()
  "Cancel active async request state in the current buffer."
  (when (timerp aws-logs--insights-active-timer)
    (cancel-timer aws-logs--insights-active-timer))
  (setq aws-logs--insights-active-timer nil)
  (when (process-live-p aws-logs--insights-active-process)
    (delete-process aws-logs--insights-active-process))
  (setq aws-logs--insights-active-process nil)
  (setq aws-logs--insights-request-in-flight nil))

(defun aws-logs--insights-begin-request ()
  "Start a new async Insights request in the current buffer.

Returns the new request id."
  (aws-logs--insights-cancel-active-request)
  (setq aws-logs--insights-active-request-id (1+ aws-logs--insights-active-request-id))
  (setq aws-logs--insights-request-in-flight t)
  aws-logs--insights-active-request-id)

(defun aws-logs--insights-finish-request (request-id)
  "Mark REQUEST-ID as finished in the current buffer."
  (when (= request-id aws-logs--insights-active-request-id)
    (when (timerp aws-logs--insights-active-timer)
      (cancel-timer aws-logs--insights-active-timer))
    (setq aws-logs--insights-active-timer nil)
    (setq aws-logs--insights-active-process nil)
    (setq aws-logs--insights-request-in-flight nil)))

(defun aws-logs--insights-call-cli-json-async (buffer request-id args on-success on-error)
  "Run AWS CLI ARGS asynchronously and parse JSON into ON-SUCCESS.

BUFFER scopes request ownership. REQUEST-ID must match current buffer request.
ON-ERROR is called with one string argument."
  (when (buffer-live-p buffer)
    (let ((output-buffer (generate-new-buffer " *aws-logs-insights-cli*")))
      (condition-case err
          (let ((process
                 (make-process
                  :name (format "aws-logs-insights:%d" request-id)
                  :buffer output-buffer
                  :command (cons aws-logs-cli args)
                  :noquery t
                  :connection-type 'pipe
                  :sentinel
                  (lambda (proc _event)
                    (when (memq (process-status proc) '(exit signal))
                      (let ((exit-code (process-exit-status proc))
                            (output (with-current-buffer output-buffer
                                      (string-trim (buffer-string)))))
                        (unwind-protect
                            (when (buffer-live-p buffer)
                              (with-current-buffer buffer
                                (when (and (= request-id aws-logs--insights-active-request-id)
                                           (eq proc aws-logs--insights-active-process))
                                  (setq aws-logs--insights-active-process nil))
                                (when (and aws-logs--insights-request-in-flight
                                           (= request-id aws-logs--insights-active-request-id))
                                  (if (zerop exit-code)
                                      (condition-case _parse-err
                                          (funcall on-success
                                                   (json-parse-string output :object-type 'alist))
                                        (error
                                         (funcall on-error
                                                  (format "Failed to parse AWS CLI JSON output: %s"
                                                          (if (string-empty-p output)
                                                              "empty output"
                                                            output)))))
                                    (funcall on-error
                                             (format "AWS CLI failed (%s): %s"
                                                     exit-code
                                                     (if (string-empty-p output)
                                                         "no output"
                                                       output)))))))
                          (kill-buffer output-buffer))))))))
            (set-process-query-on-exit-flag process nil)
            (with-current-buffer buffer
              (when (= request-id aws-logs--insights-active-request-id)
                (setq aws-logs--insights-active-process process)))
            process)
        (error
         (kill-buffer output-buffer)
         (funcall on-error
                  (format "Failed to start AWS CLI process: %s"
                          (error-message-string err))))))))

(defun aws-logs--insights-poll-query-results-async
    (buffer request-id query-id global-args poll-count on-success on-error)
  "Poll get-query-results for QUERY-ID asynchronously."
  (if (>= poll-count aws-logs--insights-max-polls)
      (funcall on-error "Timed out waiting for Insights query results")
    (aws-logs--insights-call-cli-json-async
     buffer request-id
     (append global-args
             (list "logs" "get-query-results" "--query-id" query-id))
     (lambda (payload)
       (when (buffer-live-p buffer)
         (with-current-buffer buffer
           (when (and aws-logs--insights-request-in-flight
                      (= request-id aws-logs--insights-active-request-id))
             (let ((status (alist-get 'status payload)))
               (cond
                ((string= status "Complete")
                 (funcall on-success query-id payload))
                ((member status '("Scheduled" "Running" "Unknown"))
                 (setq aws-logs--insights-active-timer
                       (run-at-time
                        aws-logs--insights-poll-delay nil
                        (lambda ()
                          (when (buffer-live-p buffer)
                            (with-current-buffer buffer
                              (when (= request-id aws-logs--insights-active-request-id)
                                (setq aws-logs--insights-active-timer nil)
                                (aws-logs--insights-poll-query-results-async
                                 buffer request-id query-id global-args
                                 (1+ poll-count) on-success on-error))))))))
                (t
                 (funcall on-error
                          (format "Insights query did not complete: %s" status)))))))))
     on-error)))

(defun aws-logs--insights-run-query-window-async
    (buffer request-id start-time end-time global-args log-group query on-success on-error)
  "Run async Insights query for START-TIME..END-TIME.

ON-SUCCESS is called with (QUERY-ID PAYLOAD)."
  (aws-logs--insights-call-cli-json-async
   buffer request-id
   (append global-args
           (list "logs" "start-query"
                 "--log-group-name" log-group
                 "--start-time" (number-to-string start-time)
                 "--end-time" (number-to-string end-time)
                 "--query-string" query))
   (lambda (start-payload)
     (let ((query-id (alist-get 'queryId start-payload)))
       (if (and query-id (stringp query-id))
           (aws-logs--insights-poll-query-results-async
            buffer request-id query-id global-args 0 on-success on-error)
         (funcall on-error
                  (format "Missing queryId from start-query response: %S" start-payload)))))
   on-error))

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

(defun aws-logs--insights-rows->viewer-lines (rows)
  "Convert Insights ROWS into viewer JSON lines for current buffer."
  (mapcar #'aws-logs--insights-row->viewer-line rows))

(defun aws-logs--insights-reset-seen-signatures (rows)
  "Reset row signature cache from ROWS in current buffer."
  (setq aws-logs--insights-seen-signatures (make-hash-table :test 'equal))
  (dolist (row rows)
    (puthash (aws-logs--insights-row-signature row) t aws-logs--insights-seen-signatures)))

(defun aws-logs--insights-apply-replace-result (query-id start-time end-time payload)
  "Replace viewer content from PAYLOAD and update header metadata."
  (let* ((rows (append (alist-get 'results payload) nil))
         (lines (aws-logs--insights-rows->viewer-lines rows)))
    (setq aws-logs--insights-query-id query-id)
    (setq aws-logs--insights-start-time start-time)
    (setq aws-logs--insights-end-time end-time)
    (aws-logs--insights-reset-seen-signatures rows)
    (json-log-viewer-replace-log-lines (current-buffer) lines t)))

(defun aws-logs--insights-apply-since-result (query-id refresh-end payload)
  "Append unseen rows from PAYLOAD and advance since-window to REFRESH-END."
  (unless aws-logs--insights-seen-signatures
    (setq aws-logs--insights-seen-signatures (make-hash-table :test 'equal)))
  (let* ((rows (append (alist-get 'results payload) nil))
         unseen-rows)
    (dolist (row rows)
      (let ((sig (aws-logs--insights-row-signature row)))
        (unless (gethash sig aws-logs--insights-seen-signatures)
          (puthash sig t aws-logs--insights-seen-signatures)
          (push row unseen-rows))))
    (setq unseen-rows (nreverse unseen-rows))
    (setq aws-logs--insights-query-id query-id)
    (setq aws-logs--insights-end-time refresh-end)
    (setq aws-logs--insights-refresh-context
          (plist-put (copy-sequence aws-logs--insights-refresh-context)
                     :last-end refresh-end))
    (if unseen-rows
        (let* ((new-lines (aws-logs--insights-rows->viewer-lines unseen-rows))
               (old-lines (json-log-viewer-current-log-lines (current-buffer))))
          (json-log-viewer-replace-log-lines
           (current-buffer)
           (append old-lines new-lines)
           t))
      ;; Keep header metadata in sync when no rows were appended.
      (json-log-viewer--refresh-header))))

(defun aws-logs--insights-run-window-async (buffer start-time end-time on-success)
  "Run async Insights query in BUFFER for START-TIME..END-TIME."
  (with-current-buffer buffer
    (let* ((request-id (aws-logs--insights-begin-request))
           (log-group (or aws-logs--insights-source-log-group
                          aws-logs--insights-log-group
                          aws-logs-log-group
                          "<unknown>"))
           (query (or aws-logs--insights-source-query aws-logs-query))
           (global-args (or aws-logs--insights-source-global-args
                            (aws-logs--insights-global-args))))
      (unless (and query (not (string-empty-p query)))
        (aws-logs--insights-finish-request request-id)
        (user-error "Missing Insights query for this buffer"))
      (aws-logs--insights-run-query-window-async
       buffer request-id start-time end-time global-args log-group query
       (lambda (query-id payload)
         (when (buffer-live-p buffer)
           (with-current-buffer buffer
             (when (= request-id aws-logs--insights-active-request-id)
               (unwind-protect
                   (condition-case err
                       (progn
                         (funcall on-success query-id payload)
                         (message "Insights query completed for %s" log-group))
                     (error
                      (message "Insights result handling failed for %s: %s"
                               log-group
                               (error-message-string err))))
                 (aws-logs--insights-finish-request request-id))))))
       (lambda (err-msg)
         (when (buffer-live-p buffer)
           (with-current-buffer buffer
             (when (= request-id aws-logs--insights-active-request-id)
               (unwind-protect
                   (message "Insights query failed for %s: %s" log-group err-msg)
                 (aws-logs--insights-finish-request request-id))))))))))

(defun aws-logs--insights-start-refresh-async (buffer)
  "Start async refresh flow for existing Insights BUFFER."
  (with-current-buffer buffer
    (unless aws-logs--insights-refresh-context
      (user-error "Missing Insights refresh context in buffer"))
    (when aws-logs--insights-request-in-flight
      (message "Insights refresh already in progress for %s" aws-logs--insights-log-group))
    (unless aws-logs--insights-request-in-flight
      (pcase (plist-get aws-logs--insights-refresh-context :mode)
        ('since
         (let* ((base-start (or (plist-get aws-logs--insights-refresh-context :start) 0))
                (refresh-end (floor (float-time))))
           (if (<= refresh-end base-start)
               (message "Insights refresh skipped for %s (no new time window)"
                        aws-logs--insights-log-group)
             (aws-logs--insights-run-window-async
              buffer base-start refresh-end
              (lambda (query-id payload)
                (setq aws-logs--insights-refresh-context
                      (plist-put (copy-sequence aws-logs--insights-refresh-context)
                                 :last-end refresh-end))
                (aws-logs--insights-apply-replace-result
                 query-id base-start refresh-end payload))))))
        ('custom-range
         (let* ((refresh-start (plist-get aws-logs--insights-refresh-context :start))
                (refresh-end (plist-get aws-logs--insights-refresh-context :end)))
           (aws-logs--insights-run-window-async
            buffer refresh-start refresh-end
            (lambda (query-id payload)
              (aws-logs--insights-apply-replace-result
               query-id refresh-start refresh-end payload)))))
        (_
         (user-error "Unknown refresh mode: %S"
                     (plist-get aws-logs--insights-refresh-context :mode)))))))

(defun aws-logs--insights-refresh-dispatch (_old-log-lines)
  "Kick off async Insights refresh from json-log-viewer callback."
  (aws-logs--insights-start-refresh-async (current-buffer))
  ;; No synchronous data mutation; async callback updates the buffer when ready.
  :async)

(defun aws-logs-insights-refresh ()
  "Refresh current Insights viewer buffer."
  (interactive)
  (json-log-viewer-refresh))

(defun aws-logs-insights-run ()
  "Run Logs Insights query asynchronously and display formatted results."
  (unless (and aws-logs-log-group (not (string-empty-p aws-logs-log-group)))
    (user-error "Select a log group first"))
  (unless (and aws-logs-query (not (string-empty-p aws-logs-query)))
    (user-error "Set a Logs Insights query first"))
  (let* ((refresh-context (aws-logs--insights-window-context))
         (start-time (plist-get refresh-context :start))
         (end-time (plist-get refresh-context :end))
         (source-global-args (aws-logs--insights-global-args))
         (source-log-group aws-logs-log-group)
         (source-query aws-logs-query)
         (extra-fields (append aws-logs-summary-extra-fields nil))
         (extra-paths (aws-logs--insights-extra-summary-paths extra-fields))
         (buffer-name (format "*AWS insights - %s*" aws-logs-log-group))
         (buffer
          (json-log-viewer-make-buffer
           buffer-name
           :log-lines nil
           :timestamp-path "__summary_timestamp"
           :level-path "__summary_level"
           :message-path "__summary_message"
           :extra-paths extra-paths
           :refresh-function #'aws-logs--insights-refresh-dispatch
           :streaming nil
           :direction 'newest-first
           :header-lines-function #'aws-logs--insights-header-lines)))
    (with-current-buffer buffer
      (add-hook 'kill-buffer-hook #'aws-logs--insights-cancel-active-request nil t)
      (aws-logs--insights-cancel-active-request)
      (setq-local aws-logs--insights-refresh-context (copy-sequence refresh-context))
      (setq-local aws-logs--insights-query-id nil)
      (setq-local aws-logs--insights-start-time start-time)
      (setq-local aws-logs--insights-end-time end-time)
      (setq-local aws-logs--insights-log-group aws-logs-log-group)
      (setq-local aws-logs--insights-source-global-args (append source-global-args nil))
      (setq-local aws-logs--insights-source-log-group source-log-group)
      (setq-local aws-logs--insights-source-query source-query)
      (setq-local aws-logs--insights-extra-fields extra-fields)
      (setq-local aws-logs--insights-extra-paths extra-paths)
      (setq-local aws-logs--insights-seen-signatures (make-hash-table :test 'equal))
      ;; Refresh header now that buffer-local header state is initialized.
      (json-log-viewer--refresh-header))
    (display-buffer buffer)
    (message "Insights query started for %s" aws-logs-log-group)
    (aws-logs--insights-run-window-async
     buffer start-time end-time
     (lambda (query-id payload)
       (with-current-buffer buffer
         (aws-logs--insights-apply-replace-result query-id start-time end-time payload))))))

(provide 'aws-logs-insights)
;;; aws-logs-insights.el ends here
