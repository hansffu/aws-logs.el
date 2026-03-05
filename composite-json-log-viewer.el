;;; composite-json-log-viewer.el --- Composite streams for json-log-viewer -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Builds a composite json-log-viewer buffer by subscribing to other
;; json-log-viewer buffers and mirroring their sqlite-backed rows into the
;; composite buffer database.
;;
;;; Code:

(require 'json-log-viewer)
(require 'subr-x)
(require 'async)
(require 'json-log-viewer-repository)

(declare-function json-log-viewer--make-log-ingestor-async-job "json-log-viewer"
                  (op line &optional narrow-string))
(declare-function json-log-viewer--async-submit "json-log-viewer" (job &optional wait-for-callback))
(declare-function json-log-viewer--request-narrow-rebuild "json-log-viewer"
                  (op &optional needle wait-for-callback))

(defvar json-log-viewer--sqlite-db)
(defvar json-log-viewer--sqlite-file)
(defvar json-log-viewer--narrow-rebuild-in-progress)
(defvar json-log-viewer--timestamp-path)
(defvar json-log-viewer--level-path)
(defvar json-log-viewer--message-path)
(defvar json-log-viewer--extra-paths)

(defvar-local composite-json-log-viewer--is-composite nil
  "Non-nil in composite json-log-viewer buffers.")

(defvar-local composite-json-log-viewer--subscription-id nil
  "Subscriber id used to register this composite with source viewers.")

(defvar-local composite-json-log-viewer--source-callback nil
  "Callback used for source viewer subscriptions.")

(defvar-local composite-json-log-viewer--source-buffers nil
  "Source viewer buffers currently subscribed by this composite.")

(defvar-local composite-json-log-viewer--backfill-pending-count 0
  "Count of source backfill tasks still running.")

(defvar-local composite-json-log-viewer--ingested-source-entries nil
  "Hash table of source-entry keys already enqueued for ingestion.")

(defcustom composite-json-log-viewer-backfill-async-chunk-size 200
  "Maximum number of rows fetched per async backfill chunk per source."
  :type 'integer
  :group 'json-log-viewer)

(defun composite-json-log-viewer--source-candidates ()
  "Return live non-composite `json-log-viewer-mode' buffers."
  (let (buffers)
    (dolist (buffer (buffer-list))
      (with-current-buffer buffer
        (when (and (derived-mode-p 'json-log-viewer-mode)
                   (not composite-json-log-viewer--is-composite))
          (push buffer buffers))))
    (nreverse buffers)))

(defun composite-json-log-viewer--normalize-source-buffers (sources)
  "Normalize SOURCES into live non-composite json-log-viewer buffers."
  (let (buffers)
    (dolist (source sources)
      (let ((buffer (cond
                     ((bufferp source) source)
                     ((stringp source) (get-buffer source))
                     (t nil))))
        (when (buffer-live-p buffer)
          (with-current-buffer buffer
            (when (and (derived-mode-p 'json-log-viewer-mode)
                       (not composite-json-log-viewer--is-composite))
              (push buffer buffers))))))
    (nreverse (delete-dups buffers))))

(defun composite-json-log-viewer--source-entry-id (source-overlay)
  "Return source entry id from SOURCE-OVERLAY, or nil."
  (let ((entry-id (or (overlay-get source-overlay 'json-log-viewer-storage-entry-id)
                      (overlay-get source-overlay 'json-log-viewer-log-entry-id))))
    (cond
     ((integerp entry-id) entry-id)
     ((and (stringp entry-id)
           (string-match-p "\\`[0-9]+\\'" entry-id))
      (string-to-number entry-id))
     (t nil))))

(defun composite-json-log-viewer--source-entry-json-by-id (source-buffer entry-id)
  "Return stored source JSON text from SOURCE-BUFFER ENTRY-ID."
  (when (and (buffer-live-p source-buffer)
             (integerp entry-id))
    (with-current-buffer source-buffer
      (when json-log-viewer--sqlite-db
        (json-log-viewer-repository-select-entry-json-by-id
         json-log-viewer--sqlite-db
         entry-id)))))

(defun composite-json-log-viewer--source-summary-config (source-buffer)
  "Return summary path config plist from SOURCE-BUFFER."
  (when (buffer-live-p source-buffer)
    (with-current-buffer source-buffer
      (list :timestamp-path json-log-viewer--timestamp-path
            :level-path json-log-viewer--level-path
            :message-path json-log-viewer--message-path
            :extra-paths (append json-log-viewer--extra-paths nil)))))

(defun composite-json-log-viewer--enqueue-source-entry (source-buffer entry-id)
  "Enqueue SOURCE-BUFFER ENTRY-ID into current composite ingestion queue."
  (when (and (integerp entry-id)
             (buffer-live-p source-buffer))
    (let* ((seen (or composite-json-log-viewer--ingested-source-entries
                     (setq-local composite-json-log-viewer--ingested-source-entries
                                 (make-hash-table :test 'equal))))
           (key (cons source-buffer entry-id)))
      (unless (gethash key seen)
       (puthash key t seen)
        (when-let* ((json-line (composite-json-log-viewer--source-entry-json-by-id
                                source-buffer entry-id))
                    (source-config (composite-json-log-viewer--source-summary-config
                                    source-buffer)))
          (let ((job (json-log-viewer--make-log-ingestor-async-job 'ingest json-line)))
            ;; Keep composite queue/db/filter behavior but parse summary fields
            ;; using source buffer paths so live and backfilled rows match.
            (setq job (plist-put job :timestamp-path (plist-get source-config :timestamp-path)))
            (setq job (plist-put job :level-path (plist-get source-config :level-path)))
            (setq job (plist-put job :message-path (plist-get source-config :message-path)))
            (setq job (plist-put job :extra-paths (plist-get source-config :extra-paths)))
            (json-log-viewer--async-submit job)))))))

(defun composite-json-log-viewer--source-max-entry-id (source-buffer)
  "Return max sqlite entry id currently present in SOURCE-BUFFER."
  (with-current-buffer source-buffer
    (if json-log-viewer--sqlite-db
        (json-log-viewer-repository-select-max-id json-log-viewer--sqlite-db)
      0)))

(defun composite-json-log-viewer--backfill-chunk-size ()
  "Return normalized row count per backfill chunk."
  (max 1 (or composite-json-log-viewer-backfill-async-chunk-size 1)))

(defun composite-json-log-viewer--copy-source-rows-async-chunk
    (source-sqlite-file target-sqlite-file after-id max-id callback)
  "Copy one source row chunk into target sqlite and invoke CALLBACK.

Rows are copied from SOURCE-SQLITE-FILE into TARGET-SQLITE-FILE for ids in
\(AFTER-ID, MAX-ID], bounded by `composite-json-log-viewer--backfill-chunk-size`.
CALLBACK receives plist (:copied N :next-after-id ID) on success or
(:error MESSAGE) on failure."
  (let* ((repo-file
          (or (locate-library "json-log-viewer-repository")
              (when-let ((composite-file (locate-library "composite-json-log-viewer")))
                (expand-file-name "json-log-viewer-repository.el"
                                  (file-name-directory composite-file)))
              (when (file-readable-p "json-log-viewer-repository.el")
                (expand-file-name "json-log-viewer-repository.el"))))
         (repo-dir (and repo-file (file-name-directory repo-file))))
    (async-start
     `(lambda ()
        (when ,repo-dir
          (add-to-list 'load-path ,repo-dir))
        (unless (fboundp 'json-log-viewer-repository-run-transaction)
          (unless (or (require 'json-log-viewer-repository nil t)
                      (and ,repo-file (file-readable-p ,repo-file) (load ,repo-file nil t)))
            (error "Cannot load json-log-viewer-repository in async backfill worker")))
        (condition-case err
            (json-log-viewer-repository-run-transaction
             ,target-sqlite-file
             (lambda (db)
               (json-log-viewer-repository-copy-source-chunk
                db
                ,source-sqlite-file
                ,after-id
                ,max-id
                ,(composite-json-log-viewer--backfill-chunk-size))))
          (error
           (list :error (error-message-string err)))))
     callback)))

(defun composite-json-log-viewer--finish-backfill-source ()
  "Mark one source backfill as completed in current composite buffer."
  (setq composite-json-log-viewer--backfill-pending-count
        (max 0 (1- composite-json-log-viewer--backfill-pending-count)))
  (when (= composite-json-log-viewer--backfill-pending-count 0)
    ;; Render everything already stored in composite sqlite, including updates
    ;; received while backfill was running.
    (json-log-viewer--request-narrow-rebuild 'widen nil)))

(defun composite-json-log-viewer--continue-backfill-source
    (composite-buffer source-buffer source-sqlite-file target-sqlite-file max-id after-id)
  "Continue async chunked backfill for SOURCE-BUFFER into COMPOSITE-BUFFER."
  (composite-json-log-viewer--copy-source-rows-async-chunk
   source-sqlite-file target-sqlite-file after-id max-id
   (lambda (result)
     (when (buffer-live-p composite-buffer)
       (with-current-buffer composite-buffer
         (when composite-json-log-viewer--is-composite
           (if-let ((err (plist-get result :error)))
               (progn
                 (message "composite-json-log-viewer backfill failed for %s: %s"
                          (buffer-name source-buffer) err)
                 (composite-json-log-viewer--finish-backfill-source))
             (let ((copied (or (plist-get result :copied) 0))
                   (next-after-id (or (plist-get result :next-after-id) after-id)))
               (if (< copied (composite-json-log-viewer--backfill-chunk-size))
                   (composite-json-log-viewer--finish-backfill-source)
                 (composite-json-log-viewer--continue-backfill-source
                  composite-buffer
                  source-buffer
                  source-sqlite-file
                  target-sqlite-file
                  max-id
                  next-after-id))))))))))

(defun composite-json-log-viewer--start-backfill (source-buffers)
  "Start initial sqlite backfill for SOURCE-BUFFERS into current composite."
  (let (source-snapshots)
    (dolist (source-buffer source-buffers)
      (when (buffer-live-p source-buffer)
        (with-current-buffer source-buffer
          (when (and json-log-viewer--sqlite-file
                     (file-readable-p json-log-viewer--sqlite-file))
            (push (list source-buffer
                        json-log-viewer--sqlite-file
                        (composite-json-log-viewer--source-max-entry-id source-buffer))
                  source-snapshots)))))
    (setq-local composite-json-log-viewer--backfill-pending-count (length source-snapshots))
    (if (null source-snapshots)
        (json-log-viewer--request-narrow-rebuild 'widen nil)
      (let ((composite-buffer (current-buffer)))
        (dolist (snapshot source-snapshots)
          (let ((source-buffer (nth 0 snapshot))
                (source-sqlite-file (nth 1 snapshot))
                (max-id (nth 2 snapshot)))
            (composite-json-log-viewer--continue-backfill-source
             composite-buffer
             source-buffer
             source-sqlite-file
             json-log-viewer--sqlite-file
             max-id
             0)))))))

(defun composite-json-log-viewer--make-source-callback (composite-buffer)
  "Return source subscriber callback for COMPOSITE-BUFFER."
  (lambda (action source-buffer entry-overlays)
    (when (and (buffer-live-p composite-buffer)
               (buffer-live-p source-buffer)
               (eq action 'append)
               entry-overlays)
      (with-current-buffer composite-buffer
        (when composite-json-log-viewer--is-composite
          (dolist (entry-overlay entry-overlays)
            (when-let ((entry-id
                        (composite-json-log-viewer--source-entry-id entry-overlay)))
              (composite-json-log-viewer--enqueue-source-entry
               source-buffer entry-id))))))))

(defun composite-json-log-viewer--unsubscribe-source (source-buffer subscription-id)
  "Unsubscribe SUBSCRIPTION-ID from SOURCE-BUFFER."
  (when (and (buffer-live-p source-buffer)
             subscription-id)
    (with-current-buffer source-buffer
      (json-log-viewer-unsubscribe subscription-id))))

(defun composite-json-log-viewer--cleanup ()
  "Cleanup all source subscriptions for current composite buffer."
  (let ((subscription-id composite-json-log-viewer--subscription-id))
    (dolist (source-buffer (append composite-json-log-viewer--source-buffers nil))
      (composite-json-log-viewer--unsubscribe-source source-buffer subscription-id)))
  (setq-local composite-json-log-viewer--source-buffers nil)
  (setq-local composite-json-log-viewer--backfill-pending-count 0))

(defun composite-json-log-viewer-add-source (composite-buffer-or-name source-buffer-or-name)
  "Subscribe COMPOSITE-BUFFER-OR-NAME to SOURCE-BUFFER-OR-NAME."
  (let ((composite-buffer (json-log-viewer-get-buffer composite-buffer-or-name))
        (source-buffer (json-log-viewer-get-buffer source-buffer-or-name)))
    (with-current-buffer composite-buffer
      (unless composite-json-log-viewer--is-composite
        (user-error "Not a composite buffer: %s" (buffer-name composite-buffer)))
      (let ((subscription-id composite-json-log-viewer--subscription-id)
            (callback composite-json-log-viewer--source-callback))
        (unless (memq source-buffer composite-json-log-viewer--source-buffers)
          (push source-buffer composite-json-log-viewer--source-buffers)
          (with-current-buffer source-buffer
            (json-log-viewer-subscribe subscription-id callback)))))
    source-buffer))

(defun composite-json-log-viewer-remove-source (composite-buffer-or-name source-buffer-or-name)
  "Unsubscribe COMPOSITE-BUFFER-OR-NAME from SOURCE-BUFFER-OR-NAME."
  (let ((composite-buffer (json-log-viewer-get-buffer composite-buffer-or-name))
        (source-buffer (json-log-viewer-get-buffer source-buffer-or-name)))
    (with-current-buffer composite-buffer
      (unless composite-json-log-viewer--is-composite
        (user-error "Not a composite buffer: %s" (buffer-name composite-buffer)))
      (setq-local composite-json-log-viewer--source-buffers
                  (delq source-buffer composite-json-log-viewer--source-buffers))
      (composite-json-log-viewer--unsubscribe-source
       source-buffer
       composite-json-log-viewer--subscription-id))
    source-buffer))

(defun composite-json-log-viewer-create (buffer-name source-buffers)
  "Create composite BUFFER-NAME from SOURCE-BUFFERS."
  (when-let ((existing (get-buffer buffer-name)))
    (with-current-buffer existing
      (when composite-json-log-viewer--is-composite
        (composite-json-log-viewer--cleanup))))
  (let ((buffer (json-log-viewer-make-buffer
                 buffer-name
                 :log-lines nil
                 :timestamp-path "timestamp"
                 :level-path "level"
                 :message-path "message"
                 :streaming t
                 :direction 'oldest-first)))
    (with-current-buffer buffer
      (setq-local composite-json-log-viewer--is-composite t)
      (setq-local composite-json-log-viewer--source-buffers nil)
      (setq-local composite-json-log-viewer--subscription-id
                  (list 'composite-json-log-viewer buffer))
      (setq-local composite-json-log-viewer--source-callback
                  (composite-json-log-viewer--make-source-callback buffer))
      (setq-local composite-json-log-viewer--ingested-source-entries
                  (make-hash-table :test 'equal))
      ;; Keep ingest callbacks from rendering until the initial backfill ends.
      (setq-local json-log-viewer--narrow-rebuild-in-progress t)
      (add-hook 'kill-buffer-hook #'composite-json-log-viewer--cleanup nil t)
      (dolist (source-buffer source-buffers)
        (composite-json-log-viewer-add-source buffer source-buffer))
      (composite-json-log-viewer--start-backfill source-buffers))
    (display-buffer buffer)
    buffer))

(defun composite-json-log-viewer-setup (buffer-name source-buffer-names)
  "Interactively create a composite BUFFER-NAME from SOURCE-BUFFER-NAMES."
  (interactive
   (let* ((candidates (composite-json-log-viewer--source-candidates))
          (candidate-names (mapcar #'buffer-name candidates)))
     (unless candidate-names
       (user-error "No json-log-viewer buffers are available"))
     (list
      (read-string "Composite buffer name: " "*Composite JSON Logs*")
      (completing-read-multiple "Source buffers: " candidate-names nil t))))
  (let ((sources (composite-json-log-viewer--normalize-source-buffers source-buffer-names)))
    (unless sources
      (user-error "Select at least one source json-log-viewer buffer"))
    (composite-json-log-viewer-create buffer-name sources)))

(provide 'composite-json-log-viewer)
;;; composite-json-log-viewer.el ends here
