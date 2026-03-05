;;; json-log-viewer-repository.el --- SQLite repository for json-log-viewer -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; SQL-only repository helpers for json-log-viewer storage.
;;
;;; Code:

(require 'sqlite)
(require 'subr-x)

(defconst json-log-viewer-repository-sqlite-lock-retries 12
  "Maximum retries when sqlite reports lock contention.")

(defun json-log-viewer-repository-setup-db (db)
  "Apply sqlite settings for DB connections."
  (sqlite-execute db "PRAGMA journal_mode = WAL")
  (sqlite-execute db "PRAGMA synchronous = NORMAL")
  (sqlite-execute db "PRAGMA busy_timeout = 5000"))

(defun json-log-viewer-repository-create-schema (db)
  "Create tables and indexes in DB."
  (sqlite-execute
   db
   (concat
    "CREATE TABLE log_entry ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "timestamp_epoch REAL, "
    "timestamp TEXT, "
    "level_path TEXT, "
    "message_path TEXT, "
    "extra_paths TEXT, "
    "json TEXT NOT NULL)"))
  (sqlite-execute db "CREATE INDEX log_entry_timestamp_idx ON log_entry(timestamp_epoch, id)"))

(defun json-log-viewer-repository-lock-error-p (err)
  "Return non-nil when ERR indicates sqlite lock contention."
  (string-match-p
   "\\(database is locked\\|database is busy\\|sqlite_busy\\|sqlite_locked\\)"
   (downcase (error-message-string err))))

(defun json-log-viewer-repository-run-transaction (sqlite-file body-fn)
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
                  (json-log-viewer-repository-setup-db db)
                  (sqlite-execute db "BEGIN IMMEDIATE")
                  (setq result (funcall body-fn db))
                  (sqlite-execute db "COMMIT")
                  (setq committed t)
                  (setq done t))
              (error
               (unless committed
                 (ignore-errors (sqlite-execute db "ROLLBACK")))
               (if (and (< attempt json-log-viewer-repository-sqlite-lock-retries)
                        (json-log-viewer-repository-lock-error-p err))
                   (sleep-for (* 0.02 attempt))
                 (signal (car err) (cdr err)))))
          (ignore-errors (sqlite-close db)))))
    result))

(defun json-log-viewer-repository-select-entry-json-by-id (db entry-id)
  "Return raw JSON text for ENTRY-ID from DB."
  (when-let ((row (car (sqlite-select db
                                      "SELECT json FROM log_entry WHERE id = ?"
                                      (vector entry-id)))))
    (car row)))

(defun json-log-viewer-repository-select-entry-by-id (db entry-id)
  "Return stored entry plist for ENTRY-ID from DB."
  (when-let ((row (car (sqlite-select db
                                      (concat
                                       "SELECT timestamp_epoch, timestamp, level_path, "
                                       "message_path, extra_paths, json "
                                       "FROM log_entry WHERE id = ?")
                                      (vector entry-id)))))
    (list :sort-key (nth 0 row)
          :timestamp (nth 1 row)
          :level-path (nth 2 row)
          :message-path (nth 3 row)
          :extra-paths (nth 4 row)
          :json (nth 5 row))))

(defun json-log-viewer-repository-select-max-id (db)
  "Return max log_entry id from DB."
  (or (car (car (sqlite-select db "SELECT COALESCE(MAX(id), 0) FROM log_entry")))
      0))

(defun json-log-viewer-repository-select-matching-ids (db needle)
  "Return row ids from DB whose JSON contains NEEDLE."
  (mapcar #'car
          (sqlite-select db
                         (concat
                          "SELECT id FROM log_entry "
                          "WHERE instr(lower(json), ?) > 0")
                         (vector needle))))

(defun json-log-viewer-repository-select-all-json-lines (db)
  "Return all stored raw JSON lines from DB ordered by id."
  (mapcar #'car
          (sqlite-select db "SELECT json FROM log_entry ORDER BY id")))

(defun json-log-viewer-repository-select-logs-before (db timestamp limit)
  "Return stored rows from DB before TIMESTAMP with optional LIMIT.

Returned rows are ascending plists of shape
\(:id INTEGER :timestamp NUMBER-OR-NIL :json STRING)."
  (let ((rows
         (cond
          ((and timestamp limit)
           (nreverse
            (sqlite-select
             db
             (concat
              "SELECT id, timestamp_epoch, json FROM log_entry "
              "WHERE timestamp_epoch < ? "
              "ORDER BY timestamp_epoch DESC, id DESC LIMIT ?")
             (vector timestamp limit))))
          (timestamp
           (sqlite-select
            db
            (concat
             "SELECT id, timestamp_epoch, json FROM log_entry "
             "WHERE timestamp_epoch < ? "
             "ORDER BY timestamp_epoch ASC, id ASC")
            (vector timestamp)))
          (limit
           (nreverse
            (sqlite-select
             db
             (concat
              "SELECT id, timestamp_epoch, json FROM log_entry "
              "ORDER BY "
              "CASE WHEN timestamp_epoch IS NULL THEN 1 ELSE 0 END, "
              "timestamp_epoch DESC, id DESC LIMIT ?")
             (vector limit))))
          (t
           (sqlite-select
            db
            (concat
             "SELECT id, timestamp_epoch, json FROM log_entry "
             "ORDER BY "
             "CASE WHEN timestamp_epoch IS NULL THEN 1 ELSE 0 END, "
             "timestamp_epoch ASC, id ASC"))))))
    (mapcar
     (lambda (row)
       (list :id (nth 0 row)
             :timestamp (nth 1 row)
             :json (nth 2 row)))
     rows)))

(defun json-log-viewer-repository-insert-entry
    (db timestamp-epoch timestamp level-path message-path extra-paths json-text)
  "Insert one log entry in DB and return its id."
  (sqlite-execute
   db
   (concat
    "INSERT INTO log_entry("
    "timestamp_epoch, timestamp, level_path, message_path, extra_paths, json) "
    "VALUES (?, ?, ?, ?, ?, ?)")
   (vector timestamp-epoch timestamp level-path message-path extra-paths json-text))
  (car (car (sqlite-select db "SELECT last_insert_rowid()"))))

(defun json-log-viewer-repository-select-summary-entries (db &optional narrow-string)
  "Return summary row plists from DB, optionally filtered by NARROW-STRING."
  (mapcar
   (lambda (row)
     (list :id (nth 0 row)
           :sort-key (nth 1 row)
           :timestamp (nth 2 row)
           :level-path (nth 3 row)
           :message-path (nth 4 row)
           :extra-paths (nth 5 row)))
   (if narrow-string
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
       "FROM log_entry ORDER BY id")))))

(defun json-log-viewer-repository-select-summary-entry-by-id (db entry-id)
  "Return summary row plist for ENTRY-ID from DB."
  (when-let ((row
              (car
               (sqlite-select
                db
                (concat
                 "SELECT id, timestamp_epoch, timestamp, level_path, message_path, extra_paths "
                 "FROM log_entry WHERE id = ?")
                (vector entry-id)))))
    (list :id (nth 0 row)
          :sort-key (nth 1 row)
          :timestamp (nth 2 row)
          :level-path (nth 3 row)
          :message-path (nth 4 row)
          :extra-paths (nth 5 row))))

(defun json-log-viewer-repository-reset-log-entries (db)
  "Delete all log rows in DB."
  (sqlite-execute db "DELETE FROM log_entry"))

(defun json-log-viewer-repository-truncate-oldest (db count)
  "Delete COUNT oldest rows in DB and return COUNT."
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

(defun json-log-viewer-repository-copy-source-chunk
    (db source-sqlite-file after-id max-id chunk-size)
  "Copy one chunk from SOURCE-SQLITE-FILE into DB.

Rows are copied for ids in \(AFTER-ID, MAX-ID], limited by CHUNK-SIZE.
Returns plist \(:copied N :next-after-id ID)."
  (let (attached)
    (unwind-protect
        (progn
          (sqlite-execute db
                          "ATTACH DATABASE ? AS source_db"
                          (vector source-sqlite-file))
          (setq attached t)
          (let* ((meta-row
                  (car
                   (sqlite-select
                    db
                    (concat
                     "SELECT COALESCE(MAX(id), ?), COUNT(*) FROM ("
                     "SELECT id FROM source_db.log_entry "
                     "WHERE id > ? AND id <= ? "
                     "ORDER BY id LIMIT ?)")
                    (vector after-id after-id max-id chunk-size))))
                 (next-after-id (or (nth 0 meta-row) after-id))
                 (copied (or (nth 1 meta-row) 0)))
            (when (> copied 0)
              (sqlite-execute
               db
               (concat
                "INSERT INTO log_entry("
                "id, timestamp_epoch, timestamp, level_path, message_path, extra_paths, json) "
                "SELECT NULL, timestamp_epoch, timestamp, level_path, message_path, extra_paths, json "
                "FROM source_db.log_entry "
                "WHERE id > ? AND id <= ? "
                "ORDER BY id LIMIT ?")
               (vector after-id max-id chunk-size)))
            (list :copied copied :next-after-id next-after-id)))
      (when attached
        (ignore-errors (sqlite-execute db "DETACH DATABASE source_db"))))))

(provide 'json-log-viewer-repository)
;;; json-log-viewer-repository.el ends here
