;;; json-log-viewer-async-worker.el --- Async worker for json-log-viewer -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Worker-side entrypoint used by `json-log-viewer' via `async-job-queue'.
;;
;;; Code:

(require 'cl-lib)
(require 'json)
(require 'sqlite)
(require 'subr-x)

(require 'json-log-viewer-shared)

(declare-function json-pretty-print-buffer "json" ())

(defun json-log-viewer-async-worker--normalize-narrow-string (needle)
  "Normalize NEEDLE into downcased substring matching text, or nil."
  (let ((normalized (string-trim (or needle ""))))
    (unless (string-empty-p normalized)
      (downcase normalized))))

(defun json-log-viewer-async-worker--parse-time (value)
  "Return epoch seconds parsed from VALUE, or nil."
  (when (and (stringp value) (not (string-empty-p value)))
    (let ((parsed (ignore-errors (date-to-time value))))
      (when parsed
        (let* ((base (float-time parsed))
               (fraction
                (when (string-match
                       "[T ][0-9][0-9]:[0-9][0-9]:[0-9][0-9][.,]\\([0-9]+\\)"
                       value)
                  (let ((digits (match-string 1 value)))
                    (/ (string-to-number digits)
                       (expt 10.0 (length digits)))))))
          (if (and fraction (= base (truncate base)))
              (+ base fraction)
            base))))))

(defun json-log-viewer-async-worker--json-value->pretty-string (value)
  "Render VALUE as pretty JSON text when possible."
  (let* ((parsed (or (json-log-viewer-shared--parse-json-maybe value) value))
         (normalized (json-log-viewer-shared--normalize-json-value-for-serialize parsed))
         (json (condition-case nil
                   (json-serialize normalized :null-object nil :false-object :false)
                 (error nil))))
    (if (not json)
        (or (json-log-viewer-shared--value->string parsed) "")
      (condition-case nil
          (with-temp-buffer
            (insert json)
            (json-pretty-print-buffer)
            (string-trim-right (buffer-string)))
        (error json)))))

(defun json-log-viewer-async-worker--flatten-node (node json-path-list &optional prefix)
  "Flatten NODE into row plist objects honoring JSON-PATH-LIST and PREFIX."
  (cond
   ((and prefix (member prefix json-path-list))
    (list (list :k prefix
                :v (json-log-viewer-async-worker--json-value->pretty-string node)
                :b t)))
   ((hash-table-p node)
    (let (rows keys)
      (maphash (lambda (key _value)
                 (when-let ((k (json-log-viewer-shared--value->string key)))
                   (push k keys)))
               node)
      (setq keys (sort keys #'string-lessp))
      (if (null keys)
          (when prefix (list (list :k prefix :v "")))
        (dolist (key keys)
          (setq rows
                (append rows
                        (json-log-viewer-async-worker--flatten-node
                         (or (gethash key node)
                             (when-let ((sym (intern-soft key)))
                               (gethash sym node)))
                         json-path-list
                         (json-log-viewer-shared--join-path prefix key)))))
        rows)))
   ((json-log-viewer-shared--alist-like-p node)
    (if (null node)
        (when prefix (list (list :k prefix :v "")))
      (let (rows)
        (dolist (pair node)
          (when (consp pair)
            (when-let ((k (json-log-viewer-shared--value->string (car pair))))
              (setq rows
                    (append rows
                            (json-log-viewer-async-worker--flatten-node
                             (cdr pair)
                             json-path-list
                             (json-log-viewer-shared--join-path prefix k)))))))
        rows)))
   ((listp node)
    (let ((base (or prefix "value")))
      (if (null node)
          (list (list :k base :v "[]"))
        (let ((idx 0)
              rows)
          (dolist (item node)
            (setq rows
                  (append rows
                          (json-log-viewer-async-worker--flatten-node
                           item
                           json-path-list
                           (format "%s[%d]" base idx))))
            (setq idx (1+ idx)))
          rows))))
   (t
    (list (list :k (or prefix "value")
                :v (or (json-log-viewer-shared--value->string node) ""))))))

(defun json-log-viewer-async-worker--json-object-rows (parsed raw-line json-path-list)
  "Return flattened rows from PARSED JSON, or RAW-LINE fallback row."
  (let ((rows (and parsed
                   (json-log-viewer-async-worker--flatten-node parsed json-path-list nil))))
    (if rows
        rows
      (list (list :k "raw" :v (or raw-line ""))))))

(defun json-log-viewer-async-worker--entry-filter-text (rows)
  "Return downcased filter text derived from ROWS."
  (downcase
   (mapconcat (lambda (row)
                (format "%s %s"
                        (or (plist-get row :k) "")
                        (or (plist-get row :v) "")))
              rows
              "\n")))

(defun json-log-viewer-async-worker--rows->storage-json (rows)
  "Serialize ROWS into storage JSON."
  (let (objects)
    (dolist (row rows)
      (let ((obj (list :k (or (plist-get row :k) "")
                       :v (or (plist-get row :v) ""))))
        (when (plist-get row :b)
          (setq obj (append obj (list :b t))))
        (push obj objects)))
    (json-serialize (vconcat (nreverse objects)))))

(defun json-log-viewer-async-worker--line->entry+detail (line id config)
  "Build one entry/detail pair for LINE with synthetic ID using CONFIG."
  (let* ((parsed (json-log-viewer-shared--parse-json-line line))
         (flattened (and parsed
                         (json-log-viewer-shared--flatten-path-values parsed)))
         (timestamp (json-log-viewer-shared--resolve-path
                     parsed (plist-get config :timestamp-path) flattened))
         (timestamp-epoch (json-log-viewer-async-worker--parse-time timestamp))
         (sort-key (or timestamp-epoch (+ 1000000000000.0 id)))
         (level (or (json-log-viewer-shared--resolve-path
                     parsed (plist-get config :level-path) flattened)
                    "-"))
         (message (or (json-log-viewer-shared--resolve-path
                       parsed (plist-get config :message-path) flattened)
                      line
                      "-"))
         (extras nil)
         (rows (json-log-viewer-async-worker--json-object-rows
                parsed line (plist-get config :json-paths)))
         (filter-text (json-log-viewer-async-worker--entry-filter-text rows))
         (fields-json (json-log-viewer-async-worker--rows->storage-json rows))
         (signature (number-to-string id)))
    (dolist (path (plist-get config :extra-paths))
      (when-let ((value (json-log-viewer-shared--resolve-path parsed path flattened)))
        (push value extras)))
    (cons
     (list :id id
           :sort-key sort-key
           :timestamp (or timestamp "-")
           :level level
           :message message
           :extras (nreverse extras)
           :filter-text filter-text
           :storage-populated t)
     (vector signature filter-text fields-json))))

(defun json-log-viewer-async-worker--line->storage-json (line parsed)
  "Return normalized JSON object text persisted for LINE and PARSED payload."
  (let ((normalized
         (cond
          ((or (hash-table-p parsed)
               (json-log-viewer-shared--alist-like-p parsed))
           (json-log-viewer-shared--normalize-json-value-for-serialize parsed))
          ((or (listp parsed) (vectorp parsed))
           (let ((obj (make-hash-table :test 'equal)))
             (puthash
              "value"
              (json-log-viewer-shared--normalize-json-value-for-serialize parsed)
              obj)
             obj))
          (t
           (let ((obj (make-hash-table :test 'equal)))
             (puthash "raw" (or line "") obj)
             obj)))))
    (json-serialize normalized :null-object nil :false-object :false)))

(defun json-log-viewer-async-worker--line->summary (line config)
  "Return summary plist derived from LINE using CONFIG."
  (let* ((parsed (json-log-viewer-shared--parse-json-line line))
         (flattened (and parsed
                         (json-log-viewer-shared--flatten-path-values parsed)))
         (timestamp (json-log-viewer-shared--resolve-path
                     parsed (plist-get config :timestamp-path) flattened))
         (timestamp-epoch (json-log-viewer-async-worker--parse-time timestamp))
         (sort-key (or timestamp-epoch nil))
         (level (or (json-log-viewer-shared--resolve-path
                     parsed (plist-get config :level-path) flattened)
                    "-"))
         (message (or (json-log-viewer-shared--resolve-path
                       parsed (plist-get config :message-path) flattened)
                      line
                      "-"))
         (extra-fields nil))
    (dolist (path (plist-get config :extra-paths))
      (when-let ((value (json-log-viewer-shared--resolve-path parsed path flattened)))
        (push value extra-fields)))
    (list :parsed parsed
          :sort-key sort-key
          :timestamp (or timestamp "-")
          :timestamp-epoch timestamp-epoch
          :level level
          :message message
          :extra-fields (nreverse extra-fields))))

(defun json-log-viewer-async-worker--extra-fields->csv (extra-fields)
  "Serialize EXTRA-FIELDS list into comma-separated string."
  (string-join
   (mapcar (lambda (value) (or (json-log-viewer-shared--value->string value) ""))
           (or extra-fields nil))
   ","))

(defun json-log-viewer-async-worker--csv->extra-fields (csv)
  "Deserialize comma-separated CSV into list of extra field strings."
  (if (or (null csv) (string-empty-p csv))
      nil
    (split-string csv "," t)))

(defun json-log-viewer-async-worker--json-matches-narrow-p (json-text narrow-string)
  "Return non-nil when JSON-TEXT contains NARROW-STRING."
  (or (null narrow-string)
      (and (stringp json-text)
           (string-match-p (regexp-quote narrow-string)
                           (downcase json-text)))))

(defun json-log-viewer-async-worker--sqlite-insert-log-entry (db line config &optional narrow-string)
  "Insert one LINE into DB and return summary plist when it matches NARROW-STRING."
  (let* ((summary (json-log-viewer-async-worker--line->summary line config))
         (parsed (plist-get summary :parsed))
         (sort-key (plist-get summary :sort-key))
         (timestamp-text (plist-get summary :timestamp))
         (level-path (plist-get summary :level))
         (message-path (plist-get summary :message))
         (extra-paths (json-log-viewer-async-worker--extra-fields->csv
                       (plist-get summary :extra-fields)))
         (timestamp-epoch (plist-get summary :timestamp-epoch))
         (storage-json (json-log-viewer-async-worker--line->storage-json line parsed)))
    (sqlite-execute
     db
     (concat
      "INSERT INTO log_entry("
      "timestamp_epoch, timestamp, level_path, message_path, extra_paths, json) "
      "VALUES (?, ?, ?, ?, ?, ?)")
     (vector timestamp-epoch timestamp-text level-path message-path extra-paths storage-json))
    (let ((id (car (car (sqlite-select db "SELECT last_insert_rowid()")))))
      (when (json-log-viewer-async-worker--json-matches-narrow-p storage-json narrow-string)
        (list :id id
              :sort-key (or sort-key (+ 1000000000000.0 id))
              :timestamp timestamp-text
              :level level-path
              :message message-path
              :extra-fields (plist-get summary :extra-fields))))))

(defun json-log-viewer-async-worker--sqlite-insert-stored-entry
    (db line stored-entry &optional narrow-string)
  "Insert one precomputed LINE/STORED-ENTRY row into DB."
  (let* ((sort-key (or (plist-get stored-entry :sort-key) nil))
         (timestamp-text (or (plist-get stored-entry :timestamp) "-"))
         (level-path (or (plist-get stored-entry :level-path) "-"))
         (message-path (or (plist-get stored-entry :message-path) "-"))
         (extra-paths (or (plist-get stored-entry :extra-paths) ""))
         (storage-json (or line "")))
    (sqlite-execute
     db
     (concat
      "INSERT INTO log_entry("
      "timestamp_epoch, timestamp, level_path, message_path, extra_paths, json) "
      "VALUES (?, ?, ?, ?, ?, ?)")
     (vector sort-key timestamp-text level-path message-path extra-paths storage-json))
    (let ((id (car (car (sqlite-select db "SELECT last_insert_rowid()")))))
      (when (json-log-viewer-async-worker--json-matches-narrow-p storage-json narrow-string)
        (list :id id
              :sort-key (or sort-key (+ 1000000000000.0 id))
              :timestamp timestamp-text
              :level level-path
              :message message-path
              :extra-fields (json-log-viewer-async-worker--csv->extra-fields extra-paths))))))

(defun json-log-viewer-async-worker--stored-row->entry (row)
  "Build summary entry from sqlite ROW."
  (let ((id (nth 0 row))
        (sort-key (nth 1 row))
        (timestamp-text (nth 2 row))
        (level-path (nth 3 row))
        (message-path (nth 4 row))
        (extra-paths (nth 5 row)))
    (list :id id
          :sort-key (or sort-key (+ 1000000000000.0 (or id 0)))
          :timestamp (or timestamp-text "-")
          :level (or level-path "-")
          :message (or message-path "-")
          :extra-fields (json-log-viewer-async-worker--csv->extra-fields extra-paths))))

(defun json-log-viewer-async-worker--sqlite-select-entries (db _config &optional narrow-string)
  "Return ordered summary entries from DB using CONFIG and NARROW-STRING."
  (let* ((rows (if narrow-string
                   (sqlite-select
                    db
                    (concat
                     "SELECT id, timestamp_epoch, timestamp, level_path, message_path, extra_paths "
                     "FROM log_entry "
                     "WHERE instr(lower(json), ?) > 0 "
                     "ORDER BY id")
                    (vector narrow-string))
                 (sqlite-select
                  db
                  (concat
                   "SELECT id, timestamp_epoch, timestamp, level_path, message_path, extra_paths "
                   "FROM log_entry ORDER BY id"))))
         entries)
    (dolist (row rows)
      (push (json-log-viewer-async-worker--stored-row->entry row) entries))
    (nreverse entries)))

(defun json-log-viewer-async-worker--sqlite-truncate-oldest (db count)
  "Delete COUNT oldest rows from DB ordered by timestamp then id."
  (when (> count 0)
    (sqlite-execute
     db
     (concat
      "DELETE FROM log_entry "
      "WHERE id IN ("
      "SELECT id FROM log_entry "
      "ORDER BY CASE WHEN timestamp_epoch IS NULL THEN 1 ELSE 0 END, timestamp_epoch, id "
      "LIMIT ?)")
     (vector count)))
  count)

(defconst json-log-viewer-async-worker--sqlite-lock-retries 12
  "Maximum retries when sqlite reports lock contention.")

(defun json-log-viewer-async-worker--sqlite-setup (db)
  "Apply sqlite settings for DB used by async workers."
  ;; WAL allows concurrent reader/writer connections, and busy timeout avoids
  ;; immediate failure when the other async worker holds the write lock.
  (sqlite-execute db "PRAGMA journal_mode = WAL")
  (sqlite-execute db "PRAGMA synchronous = NORMAL")
  (sqlite-execute db "PRAGMA busy_timeout = 5000"))

(defun json-log-viewer-async-worker--sqlite-lock-error-p (err)
  "Return non-nil when ERR indicates sqlite lock contention."
  (string-match-p
   "\\(database is locked\\|database is busy\\|sqlite_busy\\|sqlite_locked\\)"
   (downcase (error-message-string err))))

(defun json-log-viewer-async-worker--sqlite-run-transaction (sqlite-file body-fn)
  "Run BODY-FN inside a sqlite transaction for SQLITE-FILE with lock retries."
  (let ((attempt 0)
        result
        done)
    (while (not done)
      (setq attempt (1+ attempt))
      (let ((db (sqlite-open sqlite-file))
            (committed nil))
        (unwind-protect
            (condition-case err
                (progn
                  (json-log-viewer-async-worker--sqlite-setup db)
                  (sqlite-execute db "BEGIN IMMEDIATE")
                  (setq result (funcall body-fn db))
                  (sqlite-execute db "COMMIT")
                  (setq committed t)
                  (setq done t))
              (error
               (unless committed
                 (ignore-errors (sqlite-execute db "ROLLBACK")))
               (if (and (< attempt json-log-viewer-async-worker--sqlite-lock-retries)
                        (json-log-viewer-async-worker--sqlite-lock-error-p err))
                   (sleep-for (* 0.02 attempt))
                 (signal (car err) (cdr err)))))
          (ignore-errors (sqlite-close db)))))
    result))

(defun json-log-viewer-async-worker--config-from-job (job)
  "Extract reusable config plist from JOB."
  (list :timestamp-path (plist-get job :timestamp-path)
        :level-path (plist-get job :level-path)
        :message-path (plist-get job :message-path)
        :extra-paths (or (plist-get job :extra-paths) nil)
        :sqlite-file (plist-get job :sqlite-file)))

(defun json-log-viewer-async-worker-process-log-ingestor-job (job)
  "Process one LOG-INGESTOR JOB payload."
  (let* ((op (or (plist-get job :op) 'ingest))
         (sqlite-file (plist-get job :sqlite-file))
         (config (json-log-viewer-async-worker--config-from-job job))
         (narrow-string (json-log-viewer-async-worker--normalize-narrow-string
                         (plist-get job :narrow-string))))
    (unless sqlite-file
      (error "Log-ingestor worker missing sqlite support"))
    (pcase op
      ('reset
       (json-log-viewer-async-worker--sqlite-run-transaction
        sqlite-file
        (lambda (db)
          (sqlite-execute db "DELETE FROM log_entry")))
       (list :op 'reset))
      ('ingest
       (let ((line (plist-get job :line)))
         (unless (stringp line)
           (error "Log-ingestor job :line must be a string"))
         (list :op 'ingest
               :entry
               (json-log-viewer-async-worker--sqlite-run-transaction
                sqlite-file
                (lambda (db)
                  (json-log-viewer-async-worker--sqlite-insert-log-entry
                   db line config narrow-string))))))
      ('ingest-stored
       (let ((line (plist-get job :line))
             (stored-entry (plist-get job :stored-entry)))
         (unless (stringp line)
           (error "Log-ingestor job :line must be a string"))
         (unless (listp stored-entry)
           (error "Log-ingestor job :stored-entry must be a plist"))
         (list :op 'ingest
               :entry
               (json-log-viewer-async-worker--sqlite-run-transaction
                sqlite-file
                (lambda (db)
                  (json-log-viewer-async-worker--sqlite-insert-stored-entry
                   db line stored-entry narrow-string))))))
      ('narrow
       (unless narrow-string
         (error "Log-ingestor narrow op requires :narrow-string"))
       (list :op 'narrow
             :entries
             (json-log-viewer-async-worker--sqlite-run-transaction
              sqlite-file
              (lambda (db)
                (json-log-viewer-async-worker--sqlite-select-entries
                 db config narrow-string)))))
      ('widen
       (list :op 'widen
             :entries
             (json-log-viewer-async-worker--sqlite-run-transaction
              sqlite-file
              (lambda (db)
                (json-log-viewer-async-worker--sqlite-select-entries
                 db config nil)))))
      (_
       (error "Unknown log-ingestor op: %S" op)))))

(defun json-log-viewer-async-worker-process-trunkator-job (job)
  "Process one TRUNKATOR JOB payload."
  (let* ((sqlite-file (plist-get job :sqlite-file))
         (count (max 0 (or (plist-get job :count) 0)))
         (deleted 0))
    (unless sqlite-file
      (error "Trunkator worker missing sqlite support"))
    (json-log-viewer-async-worker--sqlite-run-transaction
     sqlite-file
     (lambda (db)
       (setq deleted (json-log-viewer-async-worker--sqlite-truncate-oldest db count))))
    (list :op 'truncate :count deleted)))

(defun json-log-viewer-async-worker-process-entry-details-job (_job)
  "Compatibility no-op retained for older callers."
  (list :op 'noop :count 0))

(defun json-log-viewer-async-worker-process-job (job)
  "Compatibility wrapper that routes JOB to log-ingestor worker."
  (json-log-viewer-async-worker-process-log-ingestor-job job))

(provide 'json-log-viewer-async-worker)
;;; json-log-viewer-async-worker.el ends here
