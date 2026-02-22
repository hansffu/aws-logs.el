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

(declare-function json-pretty-print-buffer "json" ())
(declare-function sqlite-open "sqlite" (file))
(declare-function sqlite-close "sqlite" (db))
(declare-function sqlite-execute "sqlite" (db sql &optional values))
(declare-function sqlite-select "sqlite" (db sql &optional values))

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
    (json-log-viewer-async-worker--value->string
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
  (let* ((normalized (or (json-log-viewer-async-worker--parse-maybe value) value))
         (json (condition-case nil
                   (json-serialize normalized :null-object nil :false-object :false)
                 (error nil))))
    (if (not json)
        (or (json-log-viewer-async-worker--value->string normalized) "")
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
  "Persist worker outputs and return resulting raw line count, or nil."
  (let ((storage-backend (plist-get config :storage-backend))
        (sqlite-file (plist-get config :sqlite-file)))
    (when (and (eq storage-backend 'sqlite)
               sqlite-file
               (fboundp 'sqlite-open))
      (let ((db (sqlite-open sqlite-file))
            (ok nil)
            (raw-count nil))
        (unwind-protect
            (progn
              (sqlite-execute db "BEGIN")
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
                 (plist-get config :chunk-size)))
              (setq raw-count
                    (or (car (car (sqlite-select db "SELECT COUNT(*) FROM raw_lines"))) 0))
              (setq ok t)
              (sqlite-execute db "COMMIT"))
          (unless ok
            (ignore-errors (sqlite-execute db "ROLLBACK")))
          (ignore-errors (sqlite-close db)))
        raw-count))))

(defun json-log-viewer-async-worker--config-from-job (job)
  "Extract reusable config plist from JOB."
  (list :timestamp-path (plist-get job :timestamp-path)
        :level-path (plist-get job :level-path)
        :message-path (plist-get job :message-path)
        :extra-paths (or (plist-get job :extra-paths) nil)
        :json-paths (or (plist-get job :json-paths) nil)
        :sqlite-file (plist-get job :sqlite-file)
        :storage-backend (plist-get job :storage-backend)
        :streaming (plist-get job :streaming)
        :max-entries (plist-get job :max-entries)
        :chunk-size (max 1 (or (plist-get job :chunk-size) 1))))

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
         (next-id (plist-get batch :next-id))
         (raw-count (json-log-viewer-async-worker--persist-sqlite
                     op lines details config)))
    (list :op op
          :entries entries
          :next-id next-id
          :raw-count raw-count
          :preserve-filter preserve-filter)))

(provide 'json-log-viewer-async-worker)
;;; json-log-viewer-async-worker.el ends here
