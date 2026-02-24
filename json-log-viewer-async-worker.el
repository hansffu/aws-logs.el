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

(declare-function json-pretty-print-buffer "json" ())

(defun json-log-viewer-async-worker--value->string (value)
  "Convert VALUE into worker-safe string or nil."
  (cond
   ((stringp value) value)
   ((numberp value) (number-to-string value))
   ((eq value t) "true")
   ((eq value :false) "false")
   ((null value) nil)
   (t (format "%s" value))))

(defun json-log-viewer-async-worker--parse-line (line)
  "Parse JSON LINE into an alist/list structure, or nil."
  (when (stringp line)
    (condition-case nil
        (json-parse-string line :object-type 'alist :array-type 'list
                           :null-object nil :false-object :false)
      (error nil))))

(defun json-log-viewer-async-worker--parse-maybe (value)
  "Parse VALUE when it looks like JSON text."
  (when (and (stringp value)
             (string-match-p "\\`[[:space:]\n\r\t]*[{\\[]" value))
    (json-log-viewer-async-worker--parse-line value)))

(defun json-log-viewer-async-worker--normalize-json-value-for-serialize (value)
  "Normalize VALUE into a shape `json-serialize' can encode reliably."
  (cond
   ((hash-table-p value)
    (let ((normalized (make-hash-table :test 'equal)))
      (maphash
       (lambda (key child)
         (when-let ((name (json-log-viewer-async-worker--value->string key)))
           (puthash name
                    (json-log-viewer-async-worker--normalize-json-value-for-serialize child)
                    normalized)))
       value)
      normalized))
   ((json-log-viewer-async-worker--alist-like-p value)
    (let ((normalized (make-hash-table :test 'equal)))
      (dolist (pair value)
        (when (consp pair)
          (when-let ((name (json-log-viewer-async-worker--value->string (car pair))))
            (puthash name
                     (json-log-viewer-async-worker--normalize-json-value-for-serialize (cdr pair))
                     normalized))))
      normalized))
   ((vectorp value)
    (vconcat
     (mapcar #'json-log-viewer-async-worker--normalize-json-value-for-serialize
             (append value nil))))
   ((listp value)
    (vconcat
     (mapcar #'json-log-viewer-async-worker--normalize-json-value-for-serialize value)))
   (t value)))

(defun json-log-viewer-async-worker--value->summary-string (value)
  "Convert resolved path VALUE into one-line summary text."
  (cond
   ((or (hash-table-p value)
        (vectorp value)
        (json-log-viewer-async-worker--alist-like-p value)
        (and (listp value) (not (null value))))
    (let ((json (condition-case nil
                    (json-serialize
                     (json-log-viewer-async-worker--normalize-json-value-for-serialize value)
                     :null-object nil
                     :false-object :false)
                  (error nil))))
      (or json (json-log-viewer-async-worker--value->string value))))
   (t
    (json-log-viewer-async-worker--value->string value))))

(defun json-log-viewer-async-worker--array-index (key)
  "Return numeric array index for KEY string, or nil."
  (when (and (stringp key)
             (string-match-p "\\`[0-9]+\\'" key))
    (string-to-number key)))

(defun json-log-viewer-async-worker--alist-like-p (node)
  "Return non-nil when NODE behaves like a JSON object alist."
  (and (listp node)
       (or (null node)
           (let ((first (car node)))
             (and (consp first)
                  (or (stringp (car first))
                      (symbolp (car first))))))))

(defun json-log-viewer-async-worker--get-child (node key)
  "Return child value KEY from NODE."
  (cond
   ((hash-table-p node)
    (or (gethash key node)
        (gethash (intern-soft key) node)))
   ((json-log-viewer-async-worker--alist-like-p node)
    (or (alist-get key node nil nil #'equal)
        (when-let ((sym (intern-soft key)))
          (alist-get sym node))))
   ((listp node)
    (when-let ((idx (json-log-viewer-async-worker--array-index key)))
      (nth idx node)))
   (t nil)))

(defun json-log-viewer-async-worker--get-path (node path)
  "Return value at dot-separated PATH in NODE."
  (let ((parts (split-string path "\\."))
        (current node)
        (failed nil))
    (while (and parts (not failed))
      (when-let ((parsed (json-log-viewer-async-worker--parse-maybe current)))
        (setq current parsed))
      (setq current (json-log-viewer-async-worker--get-child current (car parts)))
      (unless current
        (setq failed t))
      (setq parts (cdr parts)))
    (unless failed
      current)))

(defun json-log-viewer-async-worker--resolve-path (parsed path)
  "Resolve string PATH from PARSED JSON object."
  (when (and parsed
             (stringp path)
             (not (string-empty-p path)))
    (json-log-viewer-async-worker--value->summary-string
     (json-log-viewer-async-worker--get-path parsed path))))

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
  (let* ((parsed (or (json-log-viewer-async-worker--parse-maybe value) value))
         (normalized (json-log-viewer-async-worker--normalize-json-value-for-serialize parsed))
         (json (condition-case nil
                   (json-serialize normalized :null-object nil :false-object :false)
                 (error nil))))
    (if (not json)
        (or (json-log-viewer-async-worker--value->string parsed) "")
      (condition-case nil
          (with-temp-buffer
            (insert json)
            (json-pretty-print-buffer)
            (string-trim-right (buffer-string)))
        (error json)))))

(defun json-log-viewer-async-worker--join-path (prefix part)
  "Join PREFIX and PART using dot notation."
  (if (and prefix (not (string-empty-p prefix)))
      (concat prefix "." part)
    part))

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
                 (when-let ((k (json-log-viewer-async-worker--value->string key)))
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
                         (json-log-viewer-async-worker--join-path prefix key)))))
        rows)))
   ((json-log-viewer-async-worker--alist-like-p node)
    (if (null node)
        (when prefix (list (list :k prefix :v "")))
      (let (rows)
        (dolist (pair node)
          (when (consp pair)
            (when-let ((k (json-log-viewer-async-worker--value->string (car pair))))
              (setq rows
                    (append rows
                            (json-log-viewer-async-worker--flatten-node
                             (cdr pair)
                             json-path-list
                             (json-log-viewer-async-worker--join-path prefix k)))))))
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
                :v (or (json-log-viewer-async-worker--value->string node) ""))))))

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
  (let* ((parsed (json-log-viewer-async-worker--parse-line line))
         (timestamp (json-log-viewer-async-worker--resolve-path
                     parsed (plist-get config :timestamp-path)))
         (timestamp-epoch (json-log-viewer-async-worker--parse-time timestamp))
         (sort-key (or timestamp-epoch (+ 1000000000000.0 id)))
         (level (or (json-log-viewer-async-worker--resolve-path
                     parsed (plist-get config :level-path))
                    "-"))
         (message (or (json-log-viewer-async-worker--resolve-path
                       parsed (plist-get config :message-path))
                      line
                      "-"))
         (extras nil)
         (rows (json-log-viewer-async-worker--json-object-rows
                parsed line (plist-get config :json-paths)))
         (filter-text (json-log-viewer-async-worker--entry-filter-text rows))
         (fields-json (json-log-viewer-async-worker--rows->storage-json rows))
         (signature (number-to-string id)))
    (dolist (path (plist-get config :extra-paths))
      (when-let ((value (json-log-viewer-async-worker--resolve-path parsed path)))
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

(defun json-log-viewer-async-worker--build-batch (lines start-id config)
  "Return plist containing entries/details built from LINES."
  (let ((next-id start-id)
        entries
        details)
    (dolist (line lines)
      (let ((pair (json-log-viewer-async-worker--line->entry+detail line next-id config)))
        (push (car pair) entries)
        (push (cdr pair) details))
      (setq next-id (1+ next-id)))
    (list :entries (nreverse entries)
          :details (nreverse details)
          :next-id next-id)))

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

(defun json-log-viewer-async-worker--sqlite-trim-raw-lines (db max-entries chunk-size)
  "Trim RAW_LINES table on DB to MAX-ENTRIES in CHUNK-SIZE batches."
  (when (and (integerp max-entries)
             (> max-entries 0))
    (let* ((count-row (car (sqlite-select db "SELECT COUNT(*) FROM raw_lines")))
           (count (or (car count-row) 0))
           (over (- count max-entries)))
      (when (> over 0)
        (let* ((chunks (/ (+ over chunk-size -1) chunk-size))
               (drop-target (* chunks chunk-size))
               (rows (sqlite-select db
                                    (format "SELECT seq FROM raw_lines ORDER BY seq LIMIT %d"
                                            drop-target))))
          (when rows
            (let ((last-seq (car (car (last rows)))))
              (sqlite-execute db
                              "DELETE FROM raw_lines WHERE seq <= ?"
                              (vector last-seq)))))))))

(defun json-log-viewer-async-worker--persist-sqlite (op lines details config)
  "Persist worker outputs to sqlite."
  (let ((sqlite-file (plist-get config :sqlite-file)))
    (unless sqlite-file
      (error "sqlite support is required for json-log-viewer worker"))
    (json-log-viewer-async-worker--sqlite-run-transaction
     sqlite-file
     (lambda (db)
       (when (eq op 'replace)
         (sqlite-execute db "DELETE FROM raw_lines")
         (sqlite-execute db "DELETE FROM entry_details"))
       (dolist (line lines)
         (sqlite-execute db
                         "INSERT INTO raw_lines(line) VALUES (?)"
                         (vector line)))
       (dolist (detail details)
         (sqlite-execute
          db
          "INSERT OR REPLACE INTO entry_details(signature, filter_text, fields_json) VALUES (?, ?, ?)"
          detail))
       (when (plist-get config :streaming)
         (json-log-viewer-async-worker--sqlite-trim-raw-lines
          db
          (plist-get config :max-entries)
          (plist-get config :chunk-size)))))))

(defun json-log-viewer-async-worker--config-from-job (job)
  "Extract reusable config plist from JOB."
  (list :timestamp-path (plist-get job :timestamp-path)
        :level-path (plist-get job :level-path)
        :message-path (plist-get job :message-path)
        :extra-paths (or (plist-get job :extra-paths) nil)
        :json-paths (or (plist-get job :json-paths) nil)
        :sqlite-file (plist-get job :sqlite-file)
        :streaming (plist-get job :streaming)
        :max-entries (plist-get job :max-entries)
        :chunk-size (max 1 (or (plist-get job :chunk-size) 1))))

(defun json-log-viewer-async-worker-process-entry-details-job (job)
  "Process one async entry_details JOB payload in subordinate Emacs."
  (let* ((op (plist-get job :op))
         (sqlite-file (plist-get job :sqlite-file))
         (signature (plist-get job :signature))
         (filter-text (or (plist-get job :filter-text) ""))
         (fields-json (or (plist-get job :fields-json) "[]"))
         (signatures (or (plist-get job :signatures) nil)))
    (unless sqlite-file
      (error "Entry-details worker missing sqlite support"))
    (let ((count 0))
      (json-log-viewer-async-worker--sqlite-run-transaction
       sqlite-file
       (lambda (db)
         (pcase op
           ('reset
            (sqlite-execute db "DELETE FROM entry_details"))
           ('upsert
            (when signature
              (setq count 1)
              (sqlite-execute
               db
               "INSERT OR REPLACE INTO entry_details(signature, filter_text, fields_json) VALUES (?, ?, ?)"
               (vector signature filter-text fields-json))))
           ('delete-many
            (dolist (sig signatures)
              (when sig
                (setq count (1+ count))
                (sqlite-execute db
                                "DELETE FROM entry_details WHERE signature = ?"
                                (vector sig)))))
           (_
            (error "Unknown entry_details op: %S" op)))))
      (list :op op :count count))))

(defun json-log-viewer-async-worker-process-job (job)
  "Process one async JOB payload in subordinate Emacs.

Return a plist consumed by main-process callback code."
  (let* ((op (plist-get job :op))
         (lines (or (plist-get job :lines) nil))
         (start-id (or (plist-get job :start-id) 0))
         (preserve-filter (plist-get job :preserve-filter))
         (config (json-log-viewer-async-worker--config-from-job job))
         (batch (json-log-viewer-async-worker--build-batch lines start-id config))
         (entries (plist-get batch :entries))
         (details (plist-get batch :details))
         (next-id (plist-get batch :next-id)))
    (json-log-viewer-async-worker--persist-sqlite op lines details config)
    (list :op op
          :entries entries
          :next-id next-id
          :preserve-filter preserve-filter)))

(provide 'json-log-viewer-async-worker)
;;; json-log-viewer-async-worker.el ends here
