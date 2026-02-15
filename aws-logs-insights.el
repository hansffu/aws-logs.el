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

(defvar-local aws-logs--insights-entry-overlays nil
  "Entry overlays in the current Insights results buffer.")

(defvar-local aws-logs--insights-refresh-context nil
  "Refresh context plist for the current Insights results buffer.")

(defvar-local aws-logs--insights-seen-rows nil
  "Hash table of row signatures already rendered in this buffer.")

(defvar-local aws-logs--insights-filter-string nil
  "Current substring filter for Insights entries, or nil.")

(defvar-local aws-logs--insights-current-line-overlay nil
  "Overlay used to highlight current entry in Insights results buffers.")

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

(defun aws-logs--insights-clear-overlays ()
  "Remove all fold and entry overlays in the current buffer."
  (mapc #'delete-overlay aws-logs--insights-overlays)
  (mapc #'delete-overlay aws-logs--insights-entry-overlays)
  (when aws-logs--insights-current-line-overlay
    (delete-overlay aws-logs--insights-current-line-overlay))
  (setq aws-logs--insights-overlays nil)
  (setq aws-logs--insights-entry-overlays nil)
  (setq aws-logs--insights-current-line-overlay nil))

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

(defun aws-logs--insights-entry-overlay-at-point (&optional pos)
  "Return entry overlay at POS, or nil."
  (catch 'found
    (dolist (ov (overlays-at (or pos (point))))
      (when (overlay-get ov 'aws-logs-entry)
        (throw 'found ov)))
    nil))

(defun aws-logs--insights-highlight-current-line ()
  "Update current entry highlight overlay in Insights results buffers."
  (when (derived-mode-p 'aws-logs-insights-results-mode)
    (let* ((pos (point))
           (visible-pos
            (if (invisible-p pos)
                (or (next-single-char-property-change pos 'invisible nil (point-max))
                    (previous-single-char-property-change pos 'invisible nil (point-min))
                    pos)
              pos))
           (entry-ov (or (aws-logs--insights-entry-overlay-at-point visible-pos)
                         (aws-logs--insights-entry-overlay-at-point pos))))
      (if (not entry-ov)
          (when aws-logs--insights-current-line-overlay
            (delete-overlay aws-logs--insights-current-line-overlay)
            (setq aws-logs--insights-current-line-overlay nil))
        (let* ((entry-start (overlay-start entry-ov))
               (entry-end (overlay-end entry-ov))
               (fold-ov (overlay-get entry-ov 'aws-logs-fold-overlay))
               (collapsed (and fold-ov (overlay-get fold-ov 'invisible)))
               (highlight-end (if (and collapsed fold-ov)
                                  (overlay-start fold-ov)
                                entry-end)))
          (unless aws-logs--insights-current-line-overlay
            (setq aws-logs--insights-current-line-overlay (make-overlay entry-start highlight-end nil t t))
            (overlay-put aws-logs--insights-current-line-overlay 'face 'hl-line)
            (overlay-put aws-logs--insights-current-line-overlay 'priority 1000))
          (move-overlay aws-logs--insights-current-line-overlay entry-start highlight-end))))))

(defun aws-logs--insights-entry-filter-text (fields)
  "Build searchable text blob from FIELDS."
  (downcase
   (mapconcat (lambda (pair)
                (format "%s %s" (car pair) (cdr pair)))
              fields
              "\n")))

(defun aws-logs--insights-filter-match-p (entry-overlay needle)
  "Return non-nil when ENTRY-OVERLAY matches NEEDLE."
  (string-match-p
   (regexp-quote needle)
   (or (overlay-get entry-overlay 'aws-logs-filter-text) "")))

(defun aws-logs--insights-apply-filter ()
  "Apply active filter to entry overlays in current buffer."
  (let* ((needle (and aws-logs--insights-filter-string
                      (string-trim aws-logs--insights-filter-string)))
         (normalized (and needle (downcase needle))))
    (dolist (entry-overlay aws-logs--insights-entry-overlays)
      (overlay-put entry-overlay 'invisible
                   (if (and normalized
                            (not (string-empty-p normalized))
                            (not (aws-logs--insights-filter-match-p entry-overlay normalized)))
                       'aws-logs-insights-filter
                     nil)))))

(defun aws-logs-insights-narrow ()
  "Hide entries whose fields do not contain a minibuffer substring."
  (interactive)
  (let ((needle (string-trim
                 (read-string "Narrow Insights to string: "
                              (or aws-logs--insights-filter-string "")))))
    (when (string-empty-p needle)
      (user-error "Narrow string cannot be empty"))
    (setq aws-logs--insights-filter-string needle)
    (aws-logs--insights-apply-filter)
    (let ((visible 0))
      (dolist (entry-overlay aws-logs--insights-entry-overlays)
        (unless (overlay-get entry-overlay 'invisible)
          (setq visible (1+ visible))))
      (message "Insights narrowed to \"%s\" (%d visible row(s))" needle visible))))

(defun aws-logs-insights-widen ()
  "Clear Insights entry filter and show all rows."
  (interactive)
  (setq aws-logs--insights-filter-string nil)
  (aws-logs--insights-apply-filter)
  (message "Insights narrowing cleared"))

(defvar-keymap aws-logs-insights-results-mode-map
  :doc "Keymap for Insights results buffers."
  "TAB" #'aws-logs-insights-toggle-entry
  "<tab>" #'aws-logs-insights-toggle-entry
  "<backtab>" #'aws-logs-insights-toggle-all
  "C-c C-r" #'aws-logs-insights-refresh
  "C-c C-n" #'aws-logs-insights-narrow
  "C-c C-w" #'aws-logs-insights-widen)

(define-derived-mode aws-logs-insights-results-mode special-mode "AWS-Insights"
  "Major mode for formatted AWS Logs Insights query results."
  :group 'aws-logs
  (setq-local truncate-lines t)
  (setq-local line-move-ignore-invisible t)
  (setq-local buffer-invisibility-spec '(t))
  (add-to-invisibility-spec 'aws-logs-insights-filter)
  (add-hook 'post-command-hook #'aws-logs--insights-highlight-current-line nil t))

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
         (filter-text (aws-logs--insights-entry-filter-text fields))
         (summary-start (point))
         details-start details-end fold-ov entry-ov)
    (insert (aws-logs--insights-summary fields sources) "\n")
    (setq details-start (point))
    (dolist (pair fields)
      (insert "  ")
      (insert (propertize (car pair) 'face 'aws-logs-insights-key-face))
      (insert ": " (cdr pair) "\n"))
    (insert "\n")
    (setq details-end (point))
    (setq fold-ov (make-overlay details-start details-end))
    (overlay-put fold-ov 'aws-logs-fold t)
    (overlay-put fold-ov 'invisible t)
    (push fold-ov aws-logs--insights-overlays)
    (setq entry-ov (make-overlay summary-start details-end))
    (overlay-put entry-ov 'aws-logs-entry t)
    (overlay-put entry-ov 'aws-logs-fold-overlay fold-ov)
    (overlay-put entry-ov 'aws-logs-filter-text filter-text)
    (push entry-ov aws-logs--insights-entry-overlays)
    (put-text-property summary-start (1- details-start) 'aws-logs-details-overlay fold-ov)))

(defun aws-logs--insights-row-signature (row)
  "Return a stable signature string for ROW."
  (prin1-to-string row))

(defun aws-logs--insights-mark-seen-rows (rows)
  "Mark ROWS as seen in `aws-logs--insights-seen-rows`."
  (mapc (lambda (row)
          (puthash (aws-logs--insights-row-signature row) t aws-logs--insights-seen-rows))
        rows))

(defun aws-logs--insights-unseen-rows (rows)
  "Return subset of ROWS not previously seen."
  (append (seq-filter (lambda (row)
                        (not (gethash (aws-logs--insights-row-signature row) aws-logs--insights-seen-rows)))
                      rows)
          nil))

(defun aws-logs--insights-row-epoch (row)
  "Return epoch seconds for ROW timestamp, or nil."
  (let* ((fields (aws-logs--insights-row-fields row))
         (sources (aws-logs--insights-json-sources fields))
         (ts (aws-logs--insights-first-match fields sources aws-logs--insights-timestamp-candidates)))
    (when ts
      (let ((parsed (ignore-errors (date-to-time ts))))
        (when parsed
          (floor (float-time parsed)))))))

(defun aws-logs--insights-detect-order (rows)
  "Detect display order from ROWS by timestamp.

Returns `asc` or `desc`. Defaults to `asc` when it cannot be inferred."
  (let* ((timestamps (delq nil (mapcar #'aws-logs--insights-row-epoch (append rows nil))))
         (first (nth 0 timestamps))
         (second (nth 1 timestamps)))
    (if (and first second (> first second))
        'desc
      'asc)))

(defun aws-logs--insights-sort-latest-first (rows)
  "Return ROWS sorted by timestamp descending (latest first)."
  (cl-stable-sort (append rows nil)
                  (lambda (a b)
                    (> (or (aws-logs--insights-row-epoch a) -1)
                       (or (aws-logs--insights-row-epoch b) -1)))))

(defun aws-logs--insights-header-end-position ()
  "Return position where entries start (after header block)."
  (save-excursion
    (goto-char (point-min))
    (if (search-forward "\n\n" nil t)
        (point)
      (point-min))))

(defun aws-logs--insights-insert-new-rows (rows)
  "Insert ROWS into current buffer in latest-first order."
  (let ((inhibit-read-only t))
    (save-excursion
      (goto-char (aws-logs--insights-header-end-position))
      (mapc #'aws-logs--insights-insert-entry
            (aws-logs--insights-sort-latest-first rows))))
  (aws-logs--insights-apply-filter))

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

(defun aws-logs--render-insights-results (buffer query-id start-time end-time payload refresh-context)
  "Render Insights query PAYLOAD into BUFFER."
  (with-current-buffer buffer
    (let ((active-filter aws-logs--insights-filter-string)
          (inhibit-read-only t)
          (rows (aws-logs--insights-sort-latest-first
                 (append (alist-get 'results payload) nil))))
      (aws-logs-insights-results-mode)
      (setq aws-logs--insights-filter-string active-filter)
      (aws-logs--insights-clear-overlays)
      (setq aws-logs--insights-seen-rows (make-hash-table :test 'equal))
      (erase-buffer)
      (insert (format "Insights query id: %s\n" query-id))
      (insert (format "Log group: %s\n" aws-logs-log-group))
      (insert (format "Time window: %s -> %s\n" start-time end-time))
      (insert "TAB: toggle entry  S-TAB: toggle all  C-c C-r: refresh  C-c C-n: narrow  C-c C-w: widen  q: quit\n\n")
      (if (or (null rows) (= (length rows) 0))
          (insert "No results.\n")
        (mapc #'aws-logs--insights-insert-entry rows))
      (aws-logs--insights-mark-seen-rows rows)
      (setq aws-logs--insights-refresh-context (copy-sequence refresh-context))
      (aws-logs--insights-apply-filter))
    (goto-char (point-min))
    (aws-logs--insights-highlight-current-line)
    (display-buffer buffer)))

(defun aws-logs-insights-refresh ()
  "Refresh current Insights buffer.

For since-based runs, load only rows since the last refresh and merge them.
For explicit from/to ranges, reload the full fixed range."
  (interactive)
  (unless aws-logs--insights-refresh-context
    (user-error "No refresh context in this buffer"))
  (message "Refreshing")
  (let ((mode (plist-get aws-logs--insights-refresh-context :mode)))
    (pcase mode
      ('since
       (let* ((start-time (plist-get aws-logs--insights-refresh-context :last-end))
              (end-time (floor (float-time)))
              (result (aws-logs--insights-run-query-window start-time end-time))
              (query-id (car result))
              (payload (cdr result))
              (rows (append (alist-get 'results payload) nil))
              (new-rows (aws-logs--insights-unseen-rows rows)))
         (if (or (null new-rows) (= (length new-rows) 0))
             (message "Insights refresh: no new rows")
           (let ((inhibit-read-only t))
             (save-excursion
               (when (save-excursion
                       (goto-char (point-max))
                       (forward-line -1)
                       (looking-at-p "No results\\.$"))
                 (goto-char (point-max))
                 (forward-line -1)
                 (delete-region (line-beginning-position) (line-end-position))
                 (delete-char 1))
               (aws-logs--insights-insert-new-rows new-rows)))
           (aws-logs--insights-mark-seen-rows new-rows)
           (message "Insights refresh: %d new row(s)" (length new-rows)))
         (let ((new-context (copy-sequence aws-logs--insights-refresh-context)))
           (setq new-context (plist-put new-context :last-end end-time))
           (setq new-context (plist-put new-context :last-query-id query-id))
           (setq aws-logs--insights-refresh-context new-context))))
      ('custom-range
       (let* ((start-time (plist-get aws-logs--insights-refresh-context :start))
              (end-time (plist-get aws-logs--insights-refresh-context :end))
              (result (aws-logs--insights-run-query-window start-time end-time))
              (query-id (car result))
              (payload (cdr result)))
         (aws-logs--render-insights-results
          (current-buffer) query-id start-time end-time payload aws-logs--insights-refresh-context)
         (message "Insights refresh: reloaded fixed time range")))
      (_
       (user-error "Unknown refresh mode: %S" mode)))))

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
         (results (cdr result))
         (buffer (get-buffer-create (format "*AWS insights - %s*" aws-logs-log-group))))
    (aws-logs--render-insights-results buffer query-id start-time end-time results refresh-context)
    (message "Insights query completed for %s" aws-logs-log-group)))

(provide 'aws-logs-insights)
;;; aws-logs-insights.el ends here
