;;; json-log-viewer-async-worker.el --- Async worker for json-log-viewer -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Worker-side entrypoint used by `json-log-viewer' via `async-job-queue'.
;;
;;; Code:

(require 'cl-lib)
(require 'json)
(require 'subr-x)

(require 'json-log-viewer-shared)
(require 'json-log-viewer-repository)

(declare-function json-pretty-print-buffer "json" ())
(declare-function json-log-viewer-repository-select-summary-entry-by-id
                  "json-log-viewer-repository" (db entry-id))
(declare-function async-job-queue-worker-publish "async-job-queue" (payload))

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
         (timestamp-text (plist-get summary :timestamp))
         (level-path (plist-get summary :level))
         (message-path (plist-get summary :message))
         (extra-paths (json-log-viewer-async-worker--extra-fields->csv
                       (plist-get summary :extra-fields)))
         (timestamp-epoch (plist-get summary :timestamp-epoch))
         (storage-json (json-log-viewer-async-worker--line->storage-json line parsed)))
    (let ((id (json-log-viewer-repository-insert-entry
               db
               timestamp-epoch
               timestamp-text
               level-path
               message-path
               extra-paths
               storage-json)))
      (when (json-log-viewer-async-worker--json-matches-narrow-p storage-json narrow-string)
        (json-log-viewer-async-worker--stored-row->entry
         (json-log-viewer-repository-select-summary-entry-by-id db id))))))

(defun json-log-viewer-async-worker--stored-row->entry (row)
  "Build summary entry from sqlite ROW plist."
  (let ((id (plist-get row :id))
        (sort-key (plist-get row :sort-key))
        (timestamp-text (plist-get row :timestamp))
        (level-path (plist-get row :level-path))
        (message-path (plist-get row :message-path))
        (extra-paths (plist-get row :extra-paths)))
    (list :id id
          :sort-key (or sort-key (+ 1000000000000.0 (or id 0)))
          :timestamp (or timestamp-text "-")
          :level (or level-path "-")
          :message (or message-path "-")
          :extra-fields (json-log-viewer-async-worker--csv->extra-fields extra-paths))))

(defun json-log-viewer-async-worker--sqlite-select-entries (db _config &optional narrow-string)
  "Return ordered summary entries from DB using CONFIG and NARROW-STRING."
  (let ((rows (json-log-viewer-repository-select-summary-entries db narrow-string))
        entries)
    (dolist (row rows)
      (push (json-log-viewer-async-worker--stored-row->entry row) entries))
    (nreverse entries)))

(defun json-log-viewer-async-worker--sqlite-select-entries-chunk
    (db after-id chunk-size &optional narrow-string)
  "Return chunked ordered summary entries from DB.

AFTER-ID is exclusive. CHUNK-SIZE is max row count."
  (let ((rows (json-log-viewer-repository-select-summary-entries-chunk
               db after-id chunk-size narrow-string))
        entries)
    (dolist (row rows)
      (push (json-log-viewer-async-worker--stored-row->entry row) entries))
    (nreverse entries)))

(defun json-log-viewer-async-worker--sqlite-truncate-oldest (db count)
  "Delete COUNT oldest rows from DB ordered by timestamp then id."
  (json-log-viewer-repository-truncate-oldest db count))

(defvar json-log-viewer-async-worker--db nil
  "Worker-local sqlite handle used by queue jobs.")

(defvar json-log-viewer-async-worker--sqlite-file nil
  "Worker-local sqlite file path for current queue session.")

(defvar json-log-viewer-async-worker--max-entries nil
  "Worker-local streaming retention cap, or nil for unbounded.")

(defvar json-log-viewer-async-worker--chunk-size 1
  "Worker-local chunk size used for retention truncation.")

(defvar json-log-viewer-async-worker--render-mode 'all
  "Worker-local render mode: `all' or `narrow'.")

(defvar json-log-viewer-async-worker--render-narrow-string nil
  "Worker-local active narrow string, or nil when rendering all entries.")

(defun json-log-viewer-async-worker--publish-cmd (cmd &rest props)
  "Publish render CMD with PROPS to the main Emacs instance."
  (async-job-queue-worker-publish (append (list :cmd cmd) props)))

(defun json-log-viewer-async-worker--publish-rerender-chunks ()
  "Publish clear/render commands for current worker render mode."
  (let ((narrow-string (and (eq json-log-viewer-async-worker--render-mode 'narrow)
                            json-log-viewer-async-worker--render-narrow-string))
        (after-id 0)
        (chunk-size (max 1 (or json-log-viewer-async-worker--chunk-size 1)))
        done)
    (json-log-viewer-async-worker--publish-cmd 'clear)
    (while (not done)
      (let* ((entries (json-log-viewer-async-worker--sqlite-select-entries-chunk
                       json-log-viewer-async-worker--db
                       after-id
                       chunk-size
                       narrow-string))
             (count (length entries)))
        (when entries
          (json-log-viewer-async-worker--publish-cmd 'render-entries :entries entries)
          (setq after-id (or (plist-get (car (last entries)) :id) after-id)))
        (setq done (< count chunk-size))))))

(defun json-log-viewer-async-worker-init (&optional config)
  "Initialize worker-local sqlite storage from CONFIG plist."
  (json-log-viewer-async-worker-teardown)
  (setq json-log-viewer-async-worker--max-entries
        (plist-get config :max-entries))
  (setq json-log-viewer-async-worker--chunk-size
        (max 1 (or (plist-get config :chunk-size) 1)))
  (setq json-log-viewer-async-worker--render-mode 'all)
  (setq json-log-viewer-async-worker--render-narrow-string nil)
  (let* ((file (make-temp-file "json-log-viewer-worker-" nil ".sqlite"))
         (db (sqlite-open file)))
    (setq json-log-viewer-async-worker--sqlite-file file)
    (setq json-log-viewer-async-worker--db db)
    (json-log-viewer-repository-setup-db db)
    (json-log-viewer-repository-create-schema db)))

(defun json-log-viewer-async-worker-teardown ()
  "Close and remove worker-local sqlite storage."
  (when json-log-viewer-async-worker--db
    (ignore-errors (sqlite-close json-log-viewer-async-worker--db))
    (setq json-log-viewer-async-worker--db nil))
  (when (and json-log-viewer-async-worker--sqlite-file
             (file-exists-p json-log-viewer-async-worker--sqlite-file))
    (ignore-errors (delete-file json-log-viewer-async-worker--sqlite-file)))
  (setq json-log-viewer-async-worker--sqlite-file nil)
  (setq json-log-viewer-async-worker--render-mode 'all)
  (setq json-log-viewer-async-worker--render-narrow-string nil))

(defun json-log-viewer-async-worker--ensure-storage ()
  "Ensure worker-local sqlite storage has been initialized."
  (unless json-log-viewer-async-worker--db
    (error "Worker sqlite storage is not initialized")))

(defun json-log-viewer-async-worker--sqlite-count-entries (db)
  "Return row count in DB."
  (or (car (car (sqlite-select db "SELECT COUNT(*) FROM log_entry")))
      0))

(defun json-log-viewer-async-worker--stream-eviction-drop-count (over-limit)
  "Return chunk-rounded drop count for OVER-LIMIT rows."
  (let* ((chunk-size (max 1 (or json-log-viewer-async-worker--chunk-size 1)))
         (chunks (/ (+ over-limit chunk-size -1) chunk-size)))
    (* chunks chunk-size)))

(defun json-log-viewer-async-worker--enforce-retention (db)
  "Apply worker retention policy in DB and return deleted row count."
  (if (and (integerp json-log-viewer-async-worker--max-entries)
           (> json-log-viewer-async-worker--max-entries 0))
      (let* ((count (json-log-viewer-async-worker--sqlite-count-entries db))
             (over-limit (- count json-log-viewer-async-worker--max-entries)))
        (if (> over-limit 0)
            (let ((drop (json-log-viewer-async-worker--stream-eviction-drop-count
                         over-limit)))
              (json-log-viewer-async-worker--sqlite-truncate-oldest db drop)
              drop)
          0))
    0))

(defun json-log-viewer-async-worker--config-from-job (job)
  "Extract reusable config plist from JOB."
  (list :timestamp-path (plist-get job :timestamp-path)
        :level-path (plist-get job :level-path)
        :message-path (plist-get job :message-path)
        :extra-paths (or (plist-get job :extra-paths) nil)
        :json-paths (or (plist-get job :json-paths) nil)))

(defun json-log-viewer-async-worker--entry-fields (db entry-id config)
  "Return normalized detail rows for ENTRY-ID using CONFIG."
  (when-let ((json-text (json-log-viewer-repository-select-entry-json-by-id db entry-id)))
    (let ((parsed (json-log-viewer-shared--parse-json-line json-text)))
      (json-log-viewer-async-worker--json-object-rows
       parsed
       json-text
       (plist-get config :json-paths)))))

(defun json-log-viewer-async-worker-process-log-ingestor-job (job)
  "Process one LOG-INGESTOR JOB payload."
  (let* ((op (or (plist-get job :op) 'ingest))
         (request-id (plist-get job :request-id))
         (config (json-log-viewer-async-worker--config-from-job job))
         (narrow-string (json-log-viewer-async-worker--normalize-narrow-string
                         (plist-get job :narrow-string))))
    (json-log-viewer-async-worker--ensure-storage)
    (pcase op
      ('reset
       (json-log-viewer-repository-reset-log-entries
        json-log-viewer-async-worker--db)
       (json-log-viewer-async-worker--publish-cmd 'clear)
       (when request-id
         (list :request-id request-id)))
      ('ingest
       (let ((line (plist-get job :line))
             entry)
         (unless (stringp line)
           (error "Log-ingestor job :line must be a string"))
         (setq entry (json-log-viewer-async-worker--sqlite-insert-log-entry
                      json-log-viewer-async-worker--db
                      line
                      config
                      (and (eq json-log-viewer-async-worker--render-mode 'narrow)
                           json-log-viewer-async-worker--render-narrow-string)))
         (json-log-viewer-async-worker--enforce-retention
          json-log-viewer-async-worker--db)
         (when entry
           (json-log-viewer-async-worker--publish-cmd 'render-entries :entries (list entry)))
         (when request-id
           (list :request-id request-id))))
      ('narrow
       (unless narrow-string
         (error "Log-ingestor narrow op requires :narrow-string"))
       (setq json-log-viewer-async-worker--render-mode 'narrow)
       (setq json-log-viewer-async-worker--render-narrow-string narrow-string)
       (json-log-viewer-async-worker--publish-rerender-chunks)
       (when request-id
         (list :request-id request-id)))
      ('rerender
       (if narrow-string
           (progn
             (setq json-log-viewer-async-worker--render-mode 'narrow)
             (setq json-log-viewer-async-worker--render-narrow-string narrow-string))
         (setq json-log-viewer-async-worker--render-mode 'all)
         (setq json-log-viewer-async-worker--render-narrow-string nil))
       (json-log-viewer-async-worker--publish-rerender-chunks)
       (when request-id
         (list :request-id request-id)))
      ((or 'entry-details 'entry-fields)
       (let ((entry-id (plist-get job :entry-id)))
         (unless (integerp entry-id)
           (error "entry-details op requires integer :entry-id"))
         (json-log-viewer-async-worker--publish-cmd
          'expand-details
          :entry-id entry-id
          :fields (json-log-viewer-async-worker--entry-fields
                   json-log-viewer-async-worker--db entry-id config)
          :request-id request-id)
         (when request-id
           (list :request-id request-id))))
      (_
       (error "Unknown log-ingestor op: %S" op)))))

(provide 'json-log-viewer-async-worker)
;;; json-log-viewer-async-worker.el ends here
