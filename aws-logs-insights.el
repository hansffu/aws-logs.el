;;; aws-logs-insights.el --- Insights execution and rendering -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Runs CloudWatch Logs Insights queries and renders results in json-log-viewer.
;;
;;; Code:

(require 'json)
(require 'subr-x)
(require 'json-log-viewer)

(defvar aws-logs-cli)
(defvar aws-logs-endpoint)
(defvar aws-logs-region)
(defvar aws-logs-profile)
(defvar aws-logs-log-group)
(defvar aws-logs-query)
(defvar aws-logs-time-range)
(defvar aws-logs-custom-time-range)
(defvar aws-logs-insights-timestamp-path)
(defvar aws-logs-insights-level-path)
(defvar aws-logs-insights-message-path)
(defvar aws-logs-insights-extra-paths)

(defvar-local aws-logs--insights-query-id nil
  "Current query id displayed in the Insights header.")

(defvar-local aws-logs--insights-start-time nil
  "Current start time displayed in the Insights header.")

(defvar-local aws-logs--insights-end-time nil
  "Current end time displayed in the Insights header.")

(defvar-local aws-logs--insights-log-group nil
  "Current log group displayed in the Insights header.")

(defvar-local aws-logs--insights-request-id 0
  "Monotonic request id for current buffer's async Insights lifecycle.")

(defvar-local aws-logs--insights-active-process nil
  "Current active AWS CLI process for this buffer's async Insights request.")

(defvar-local aws-logs--insights-active-timer nil
  "Current poll timer for this buffer's async Insights request.")

(defconst aws-logs--insights-max-polls 120
  "Maximum number of async poll attempts for one Insights query.")

(defconst aws-logs--insights-poll-delay 1
  "Seconds between async poll attempts for one Insights query.")

(define-derived-mode aws-logs-insights-viewer-mode json-log-viewer-mode "AWS-Logs-Insights"
  "Major mode for AWS Logs Insights buffers rendered with `json-log-viewer`."
  :group 'aws-logs)

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

(defun aws-logs--insights-cancel-active-request ()
  "Cancel active async request state in the current buffer."
  (when (timerp aws-logs--insights-active-timer)
    (cancel-timer aws-logs--insights-active-timer))
  (setq aws-logs--insights-active-timer nil)
  (when (process-live-p aws-logs--insights-active-process)
    (delete-process aws-logs--insights-active-process))
  (setq aws-logs--insights-active-process nil))

(defun aws-logs--insights-begin-request ()
  "Start a new async Insights request in the current buffer.

Returns the new request id."
  (aws-logs--insights-cancel-active-request)
  (setq aws-logs--insights-request-id (1+ aws-logs--insights-request-id))
  aws-logs--insights-request-id)

(defun aws-logs--insights-finish-request (request-id)
  "Mark REQUEST-ID as finished in the current buffer."
  (when (= request-id aws-logs--insights-request-id)
    (when (timerp aws-logs--insights-active-timer)
      (cancel-timer aws-logs--insights-active-timer))
    (setq aws-logs--insights-active-timer nil)
    (setq aws-logs--insights-active-process nil)))

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
                                (when (= request-id aws-logs--insights-request-id)
                                  (when (eq proc aws-logs--insights-active-process)
                                    (setq aws-logs--insights-active-process nil))
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
              (when (= request-id aws-logs--insights-request-id)
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
           (when (= request-id aws-logs--insights-request-id)
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
                              (when (= request-id aws-logs--insights-request-id)
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

(defun aws-logs--insights-value->string (value)
  "Convert VALUE into a display string."
  (cond
   ((stringp value) value)
   ((numberp value) (number-to-string value))
   ((eq value t) "true")
   ((eq value :false) "false")
   ((null value) "")
   (t (format "%s" value))))

(defun aws-logs--insights-row-fields (row)
  "Return alist of string field/value pairs from ROW."
  (let (fields)
    (mapc (lambda (cell)
            (let ((name (aws-logs--insights-value->string (alist-get 'field cell)))
                  (value (aws-logs--insights-value->string (alist-get 'value cell))))
              (when name
                (push (cons name value) fields))))
          row)
    (nreverse fields)))

(defun aws-logs--insights-row->json-line (row)
  "Convert Insights ROW to one JSON line."
  (let ((json-object (make-hash-table :test 'equal)))
    (dolist (pair (aws-logs--insights-row-fields row))
      (puthash (car pair) (cdr pair) json-object))
    (json-serialize json-object)))

(defun aws-logs--insights-rows->viewer-lines (rows)
  "Convert Insights ROWS into viewer JSON lines for current buffer."
  (mapcar #'aws-logs--insights-row->json-line rows))

(defun aws-logs--insights-header-lines (_state)
  "Return header lines for the current Insights viewer buffer."
  (list (cons "Query ID" (format "%s" (or aws-logs--insights-query-id "-")))
        (cons "Log group" (or aws-logs--insights-log-group "-"))
        (cons "Time window"
              (format "%s -> %s"
                      (or aws-logs--insights-start-time "-")
                      (or aws-logs--insights-end-time "-")))))

(defun aws-logs--insights-apply-replace-result (query-id start-time end-time payload)
  "Replace viewer content from PAYLOAD and update header metadata."
  (let* ((rows (append (alist-get 'results payload) nil))
         (lines (aws-logs--insights-rows->viewer-lines rows)))
    (setq aws-logs--insights-query-id query-id)
    (setq aws-logs--insights-start-time start-time)
    (setq aws-logs--insights-end-time end-time)
    (json-log-viewer-replace-log-lines (current-buffer) lines t)))

(defun aws-logs--insights-run-window-async
    (buffer start-time end-time global-args log-group query on-success)
  "Run async Insights query in BUFFER for START-TIME..END-TIME."
  (with-current-buffer buffer
    (let ((request-id (aws-logs--insights-begin-request)))
      (aws-logs--insights-run-query-window-async
       buffer request-id start-time end-time global-args log-group query
       (lambda (query-id payload)
         (when (buffer-live-p buffer)
           (with-current-buffer buffer
             (when (= request-id aws-logs--insights-request-id)
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
             (when (= request-id aws-logs--insights-request-id)
               (unwind-protect
                   (message "Insights query failed for %s: %s" log-group err-msg)
                  (aws-logs--insights-finish-request request-id))))))))))

(defun aws-logs-insights-run ()
  "Run Logs Insights query asynchronously and display formatted results."
  (unless (and aws-logs-log-group (not (string-empty-p aws-logs-log-group)))
    (user-error "Select a log group first"))
  (unless (and aws-logs-query (not (string-empty-p aws-logs-query)))
    (user-error "Set a Logs Insights query first"))
  (let* ((time-window (aws-logs--insights-time-window))
         (start-time (nth 0 time-window))
         (end-time (nth 1 time-window))
         (global-args (aws-logs--insights-global-args))
         (log-group aws-logs-log-group)
         (query aws-logs-query)
         (buffer-name (format "*AWS insights - %s*" log-group))
         (buffer
         (json-log-viewer-make-buffer
           buffer-name
           :timestamp-path aws-logs-insights-timestamp-path
           :level-path aws-logs-insights-level-path
           :message-path aws-logs-insights-message-path
           :extra-paths aws-logs-insights-extra-paths
           :mode #'aws-logs-insights-viewer-mode
           :header-lines-function #'aws-logs--insights-header-lines)))
    (with-current-buffer buffer
      (add-hook 'kill-buffer-hook #'aws-logs--insights-cancel-active-request nil t)
      (aws-logs--insights-cancel-active-request)
      (setq-local aws-logs--insights-query-id nil)
      (setq-local aws-logs--insights-start-time start-time)
      (setq-local aws-logs--insights-end-time end-time)
      (setq-local aws-logs--insights-log-group log-group)
      ;; Refresh header now that buffer-local header state is initialized.
      (json-log-viewer--refresh-header))
    (display-buffer buffer)
    (message "Insights query started for %s" log-group)
    (aws-logs--insights-run-window-async
     buffer start-time end-time global-args log-group query
     (lambda (query-id payload)
       (with-current-buffer buffer
         (aws-logs--insights-apply-replace-result query-id start-time end-time payload))))))

(provide 'aws-logs-insights)
;;; aws-logs-insights.el ends here
