;;; json-log-viewer.el --- Generic foldable JSON log viewer -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Displays foldable JSON log entries with optional refresh callbacks.
;;
;;; Code:

(require 'cl-lib)
(require 'json)
(require 'subr-x)
(require 'sqlite)
(require 'async-job-queue)

(require 'json-log-viewer-shared)
(require 'json-log-viewer-repository)

(declare-function json-pretty-print-buffer "json" ())
(declare-function async-job-queue-create "async-job-queue" (process-func callback-func))
(declare-function async-job-queue-push "async-job-queue" (queue element))
(declare-function async-job-queue-stop "async-job-queue" (queue))
(declare-function json-log-viewer-async-worker-process-log-ingestor-job
                  "json-log-viewer-async-worker"
                  (job))
(declare-function json-log-viewer-async-worker-process-trunkator-job
                  "json-log-viewer-async-worker"
                  (job))

(defgroup json-log-viewer nil
  "Foldable JSON log viewer buffers."
  :group 'tools)

(defface json-log-viewer-key-face
  '((t :inherit font-lock-keyword-face))
  "Face for keys in expanded log entry details."
  :group 'json-log-viewer)

(defface json-log-viewer-header-key-face
  '((t :inherit font-lock-keyword-face :weight bold))
  "Face for header keys in log viewer buffers."
  :group 'json-log-viewer)

(defface json-log-viewer-header-value-face
  '((t :inherit default))
  "Face for header values in log viewer buffers."
  :group 'json-log-viewer)

(defface json-log-viewer-keybinding-face
  '((t :inherit font-lock-constant-face :weight bold))
  "Face for keybinding tokens in log viewer headers."
  :group 'json-log-viewer)

(defface json-log-viewer-timestamp-face
  '((t :inherit shadow))
  "Face for timestamp segments in summary lines."
  :group 'json-log-viewer)

(defface json-log-viewer-level-face
  '((t :inherit font-lock-constant-face))
  "Face for level segments in summary lines."
  :group 'json-log-viewer)

(defface json-log-viewer-message-face
  '((t :inherit default))
  "Face for message segments in summary lines."
  :group 'json-log-viewer)

(defface json-log-viewer-extra-face
  '((t :inherit font-lock-variable-name-face))
  "Face for bracketed extra segments in summary lines."
  :group 'json-log-viewer)

(defcustom json-log-viewer-enable-evil-bindings t
  "When non-nil, load optional Evil integration for json-log-viewer."
  :type 'boolean
  :group 'json-log-viewer)

(defcustom json-log-viewer-stream-max-entries 15000
  "Maximum entries retained in streaming buffers.

When nil, streaming buffers are unbounded."
  :type '(choice (const :tag "Unbounded" nil) integer)
  :group 'json-log-viewer)

(defcustom json-log-viewer-stream-chunk-size 100
  "Chunk size used for streaming storage and batch eviction."
  :type 'integer
  :group 'json-log-viewer)

(defcustom json-log-viewer-json-syntax-mode 'json-ts-mode
  "Major mode function used to fontify pretty JSON detail blocks.

When the configured mode is unavailable or fails, json-log-viewer falls back
to `js-mode`."
  :type 'symbol
  :group 'json-log-viewer)

(defvar-local json-log-viewer--fold-overlays nil
  "Detail overlays for expanded entries in the current viewer buffer.")

(defvar-local json-log-viewer--entry-overlays nil
  "Entry overlays in the current viewer buffer.")

(defvar-local json-log-viewer--current-line-overlay nil
  "Overlay used to highlight current entry.")

(defvar-local json-log-viewer--seen-signatures nil
  "Hash table of entry signatures already rendered in this buffer.")

(defvar-local json-log-viewer--filter-string nil
  "Current substring filter for rendered entries, or nil.")

(defvar-local json-log-viewer--context nil
  "Opaque refresh context owned by the caller.")

(defvar-local json-log-viewer--metadata nil
  "Opaque header metadata owned by the caller.")

(defvar-local json-log-viewer--entry-fields-function nil
  "Callback: (ENTRY) -> alist of field/value pairs.")

(defvar-local json-log-viewer--summary-function nil
  "Callback: (ENTRY FIELDS) -> summary string.")

(defvar-local json-log-viewer--refresh-function nil
  "Callback: (STATE) -> plist refresh payload.")

(defvar-local json-log-viewer--header-function nil
  "Callback: (STATE) -> alist of (KEY . VALUE) header lines.")

(defvar-local json-log-viewer--signature-function nil
  "Callback: (ENTRY) -> stable entry signature string.")

(defvar-local json-log-viewer--sort-key-function nil
  "Callback: (ENTRY) -> sortable key for ordering.")

(defvar-local json-log-viewer--streaming nil
  "Non-nil means streaming mode for this buffer.")

(defvar-local json-log-viewer--direction 'newest-first
  "Non-streaming direction: `newest-first' or `oldest-first'.")

(defvar-local json-log-viewer--narrow-rebuild-in-progress nil
  "Non-nil while async narrow/widen rebuild is replacing rendered rows.")

(defvar-local json-log-viewer--sqlite-db nil
  "SQLite database handle for storage backend, or nil.")

(defvar-local json-log-viewer--sqlite-file nil
  "Path of sqlite file used by current viewer buffer, or nil.")

(defvar-local json-log-viewer--log-ingestor-async-queue nil
  "Per-buffer async queue that ingests one log line per job.")

(defvar-local json-log-viewer--log-ingestor-async-pending-count 0
  "Count of queued log-ingestor jobs awaiting callbacks.")

(defvar-local json-log-viewer--trunkator-async-queue nil
  "Per-buffer async queue that truncates old sqlite rows.")

(defvar-local json-log-viewer--trunkator-async-pending-count 0
  "Count of queued trunkator jobs awaiting callbacks.")

(defvar-local json-log-viewer--entry-count 0
  "Cached count of rendered entry overlays.")

(defvar-local json-log-viewer--stream-assume-ordered nil
  "Non-nil means streaming entries are assumed to arrive in order.")

(defvar-local json-log-viewer--stream-max-entries nil
  "Maximum rendered entries retained for this buffer, or nil for unbounded.")

(defvar-local json-log-viewer--next-entry-id 0
  "Next synthetic entry id for JSON-line based buffers.")

(defvar-local json-log-viewer--timestamp-path nil
  "JSON path used for timestamp summary rendering.")

(defvar-local json-log-viewer--level-path nil
  "JSON path used for level summary rendering.")

(defvar-local json-log-viewer--message-path nil
  "JSON path used for message summary rendering.")

(defvar-local json-log-viewer--extra-paths nil
  "List of JSON paths used for extra summary segments.")

(defvar-local json-log-viewer--json-paths nil
  "List of JSON paths rendered as pretty JSON detail blocks.")

(defconst json-log-viewer--source-directory
  (let ((source-file (or load-file-name
                         (and (boundp 'byte-compile-current-file)
                              byte-compile-current-file)
                         (buffer-file-name))))
    (and source-file (file-name-directory source-file)))
  "Directory that contains json-log-viewer source files.")

(defvar-local json-log-viewer--json-refresh-log-lines-function nil
  "Callback: (OLD-LOG-LINES) -> NEW-LOG-LINES for JSON-line buffers.")

(defvar-local json-log-viewer--json-header-lines-function nil
  "Optional callback: (STATE) -> additional header lines for JSON-line buffers.")

(defvar-local json-log-viewer--auto-follow nil
  "Non-nil means keep point at newest entry while streaming.")

(defvar-local json-log-viewer--auto-follow-point-before-command nil
  "Point value captured in `pre-command-hook' for follow disabling logic.")

(defvar-local json-log-viewer--auto-follow-internal-move nil
  "Non-nil while viewer code moves point for auto-follow housekeeping.")

(defvar json-log-viewer--keybindings-function nil
  "Optional callback returning popup keybindings for `json-log-viewer-show-info`.")

(defconst json-log-viewer--self-subscriber-id :json-log-viewer-self
  "Reserved subscriber id used for viewer self-subscription.")

(defvar-local json-log-viewer--subscribers nil
  "Alist of (ID . CALLBACK) subscribers for current viewer buffer.")

(defun json-log-viewer-get-buffer (buffer-name)
  "Return validated json-log-viewer buffer from BUFFER-NAME.

BUFFER-NAME can be a live buffer object or a buffer name string."
  (let ((buffer (cond
                 ((bufferp buffer-name) buffer-name)
                 ((stringp buffer-name) (get-buffer buffer-name))
                 (t nil))))
    (unless (buffer-live-p buffer)
      (user-error "Buffer not found: %S" buffer-name))
    (with-current-buffer buffer
      (unless (derived-mode-p 'json-log-viewer-mode)
        (user-error "Not a json-log-viewer buffer: %s" (buffer-name buffer))))
    buffer))

(defun json-log-viewer-subscribe (id callback)
  "Subscribe ID with CALLBACK in current viewer buffer.

CALLBACK is called as (ACTION SOURCE-BUFFER ENTRY-OVERLAYS)."
  (unless (derived-mode-p 'json-log-viewer-mode)
    (user-error "Not a json-log-viewer buffer: %s" (buffer-name (current-buffer))))
  (unless id
    (user-error "Subscriber id cannot be nil"))
  (unless (functionp callback)
    (user-error "Subscriber callback must be a function, got: %S" callback))
  (setq-local json-log-viewer--subscribers
              (assoc-delete-all id json-log-viewer--subscribers))
  (push (cons id callback) json-log-viewer--subscribers)
  id)

(defun json-log-viewer-unsubscribe (id)
  "Unsubscribe ID from current viewer buffer."
  (unless (derived-mode-p 'json-log-viewer-mode)
    (user-error "Not a json-log-viewer buffer: %s" (buffer-name (current-buffer))))
  (setq-local json-log-viewer--subscribers
              (assoc-delete-all id json-log-viewer--subscribers))
  id)

(defun json-log-viewer--normalize-fields (fields)
  "Normalize FIELDS into an alist of (string . string)."
  (let (normalized)
    (dolist (pair fields)
      (when (consp pair)
        (let ((key (json-log-viewer-shared--value->string (car pair))))
          (when key
            (push (cons key
                        (or (json-log-viewer-shared--value->string (cdr pair)) ""))
                  normalized)))))
    (nreverse normalized)))

(defun json-log-viewer--fields->storage-json (fields)
  "Serialize normalized FIELDS into JSON for storage."
  (let (rows)
    (dolist (pair fields)
      (let* ((key (or (car pair) ""))
             (value (or (cdr pair) ""))
             (json-block (and (> (length value) 0)
                              (get-text-property 0 'json-log-viewer-json-block value)))
             (obj (list :k key :v (substring-no-properties value))))
        (when json-block
          (setq obj (append obj (list :b t))))
        (push obj rows)))
    (json-serialize (vconcat (nreverse rows)))))

(defun json-log-viewer--storage-json->fields (value)
  "Deserialize field VALUE JSON from storage."
  (condition-case nil
      (let ((rows (json-parse-string value :object-type 'alist :array-type 'list
                                     :null-object nil :false-object :false))
            fields)
        (dolist (row rows)
          (let* ((key (or (json-log-viewer-shared--value->string
                           (json-log-viewer-shared--json-get-child row "k"))
                          ""))
                 (text (or (json-log-viewer-shared--value->string
                            (json-log-viewer-shared--json-get-child row "v"))
                           ""))
                 (json-block (eq (json-log-viewer-shared--json-get-child row "b") t))
                 (rendered (if json-block
                               (propertize (json-log-viewer--fontify-json-string text)
                                           'json-log-viewer-json-block t)
                             text)))
            (push (cons key rendered) fields)))
        (nreverse fields))
    (error nil)))

(defun json-log-viewer--storage-init ()
  "Initialize sqlite storage for current buffer."
  (json-log-viewer--stop-async-queue)
  (json-log-viewer--storage-close)
  (let* ((file (make-temp-file "json-log-viewer-" nil ".sqlite"))
         (db (sqlite-open file)))
    (setq-local json-log-viewer--sqlite-file file)
    (setq-local json-log-viewer--sqlite-db db)
    (json-log-viewer-repository-setup-db db)
    (json-log-viewer-repository-create-schema db)))

(defun json-log-viewer--storage-close ()
  "Close and cleanup current buffer storage resources."
  (when json-log-viewer--sqlite-db
    (ignore-errors (sqlite-close json-log-viewer--sqlite-db))
    (setq-local json-log-viewer--sqlite-db nil))
  (when (and json-log-viewer--sqlite-file
             (file-exists-p json-log-viewer--sqlite-file))
    (ignore-errors (delete-file json-log-viewer--sqlite-file)))
  (setq-local json-log-viewer--sqlite-file nil))

(defun json-log-viewer--storage-reset-entry-details ()
  "Reset sqlite log entries for current buffer."
  (json-log-viewer--ensure-async-queue-running)
  (json-log-viewer--async-submit
   (json-log-viewer--make-log-ingestor-async-job 'reset nil)
   t))

(defun json-log-viewer--storage-put-entry-details (_signature _filter-text _fields)
  "Compatibility no-op; entry details are loaded from sqlite on expand.")

(defun json-log-viewer--storage-remove-entry-details-batch (_signatures)
  "Compatibility no-op; truncation is handled by trunkator queue.")

(defun json-log-viewer--storage-entry-filter-text (entry-overlay)
  "Return normalized filter text for ENTRY-OVERLAY."
  (when (and (overlay-buffer entry-overlay)
             (overlay-start entry-overlay))
    (with-current-buffer (overlay-buffer entry-overlay)
      (downcase
       (buffer-substring-no-properties
        (overlay-start entry-overlay)
        (save-excursion
          (goto-char (overlay-start entry-overlay))
          (line-end-position)))))))

(defun json-log-viewer--entry-storage-buffer (entry-overlay)
  "Return storage buffer used by ENTRY-OVERLAY."
  (let ((storage-buffer (overlay-get entry-overlay 'json-log-viewer-storage-buffer)))
    (if (buffer-live-p storage-buffer)
        storage-buffer
      (overlay-buffer entry-overlay))))

(defun json-log-viewer--entry-storage-id (entry-overlay)
  "Return storage entry id used by ENTRY-OVERLAY, or nil."
  (let ((entry-id (or (overlay-get entry-overlay 'json-log-viewer-storage-entry-id)
                      (overlay-get entry-overlay 'json-log-viewer-log-entry-id))))
    (cond
     ((integerp entry-id) entry-id)
     ((and (stringp entry-id)
           (string-match-p "\\`[0-9]+\\'" entry-id))
      (string-to-number entry-id))
     (t
      (let ((signature (or (overlay-get entry-overlay 'json-log-viewer-storage-signature)
                           (overlay-get entry-overlay 'json-log-viewer-signature))))
        (when (and (stringp signature)
                   (string-match-p "\\`[0-9]+\\'" signature))
          (string-to-number signature)))))))

(defun json-log-viewer--storage-entry-fields (entry-overlay)
  "Return normalized fields for ENTRY-OVERLAY."
  (let ((storage-buffer (json-log-viewer--entry-storage-buffer entry-overlay))
        (entry-id (json-log-viewer--entry-storage-id entry-overlay)))
    (when (and (buffer-live-p storage-buffer)
               entry-id)
      (with-current-buffer storage-buffer
        (when-let* ((json-text (and json-log-viewer--sqlite-db
                                    (json-log-viewer-repository-select-entry-json-by-id
                                     json-log-viewer--sqlite-db
                                     entry-id))))
          (json-log-viewer--normalize-fields
           (json-log-viewer--json-object-fields
            (json-log-viewer-shared--parse-json-line json-text)
            json-text
            json-log-viewer--json-paths)))))))

(defun json-log-viewer--storage-matching-signatures (needle)
  "Return hash table of matching entry signatures for NEEDLE from sqlite storage."
  (when (and json-log-viewer--sqlite-db
             (stringp needle)
             (not (string-empty-p needle)))
    (let ((matches (make-hash-table :test 'equal)))
      (dolist (id (json-log-viewer-repository-select-matching-ids
                   json-log-viewer--sqlite-db
                   needle))
        (puthash (number-to-string id) t matches))
      matches)))

(defun json-log-viewer--async-worker-file ()
  "Return a readable path to `json-log-viewer-async-worker.el'."
  (let* ((candidate-from-source
          (and json-log-viewer--source-directory
               (expand-file-name "json-log-viewer-async-worker.el"
                                 json-log-viewer--source-directory)))
         (main-file (locate-library "json-log-viewer"))
         (candidate-from-library
          (and main-file
               (expand-file-name "json-log-viewer-async-worker.el"
                                 (file-name-directory main-file)))))
    (cond
     ((and (stringp candidate-from-source)
           (file-readable-p candidate-from-source))
      candidate-from-source)
     ((and (stringp candidate-from-library)
           (file-readable-p candidate-from-library))
      candidate-from-library)
     ((file-readable-p "json-log-viewer-async-worker.el")
      (expand-file-name "json-log-viewer-async-worker.el"))
     (t
      (user-error "Cannot find json-log-viewer-async-worker.el")))))

(defun json-log-viewer--async-await-pending-count (target-count)
  "Wait until pending async count reaches TARGET-COUNT, with timeout."
  (let ((deadline (+ (float-time) 15.0)))
    (while (and (> json-log-viewer--log-ingestor-async-pending-count target-count)
                (< (float-time) deadline))
      (accept-process-output nil 0.01))
    (when (> json-log-viewer--log-ingestor-async-pending-count target-count)
      (error "Timed out waiting for async queue callback"))))

(defun json-log-viewer--worker-entry->entry (worker-entry)
  "Normalize WORKER-ENTRY into a renderable entry plist."
  (let* ((id (plist-get worker-entry :id))
         (timestamp (or (plist-get worker-entry :timestamp) "-"))
         (level (or (plist-get worker-entry :level) "-"))
         (message (or (plist-get worker-entry :message) "-"))
         (extra-fields (or (plist-get worker-entry :extra-fields)
                           (plist-get worker-entry :extras)
                           nil))
         (sort-key (or (plist-get worker-entry :sort-key)
                       (json-log-viewer--parse-time timestamp)
                       (+ 1000000000000.0 (or id 0)))))
    (list :id id
          :sort-key sort-key
          :timestamp timestamp
          :level level
          :message message
          :extras extra-fields
          :extra-fields extra-fields
          :storage-populated t)))

(defun json-log-viewer--worker-entries->entries (worker-entries)
  "Normalize WORKER-ENTRIES list into renderable entry plists."
  (mapcar #'json-log-viewer--worker-entry->entry (or worker-entries nil)))

(defun json-log-viewer--async-handle-result (result)
  "Handle log-ingestor queue RESULT in current viewer buffer."
  (if (and (listp result) (eq (car result) :error))
      (progn
        (setq json-log-viewer--narrow-rebuild-in-progress nil)
        (message "json-log-viewer async worker error: %s" (or (cadr result) "unknown error")))
    (pcase (plist-get result :op)
      ('reset nil)
      ('ingest
       (when (and (not json-log-viewer--narrow-rebuild-in-progress)
                  (plist-get result :entry))
         (json-log-viewer-append-entries
          (list (json-log-viewer--worker-entry->entry
                 (plist-get result :entry))))))
      ('narrow
       (setq json-log-viewer--narrow-rebuild-in-progress nil)
       (json-log-viewer-replace-entries
        (json-log-viewer--worker-entries->entries (plist-get result :entries))
        t
        t))
      ('widen
       (setq json-log-viewer--narrow-rebuild-in-progress nil)
       (json-log-viewer-replace-entries
        (json-log-viewer--worker-entries->entries (plist-get result :entries))
        nil
        t))
      (op
       (message "json-log-viewer async worker returned unknown op: %S" op)))))

(defun json-log-viewer--trunkator-async-handle-result (result)
  "Handle trunkator async queue RESULT in current viewer buffer."
  (when (and (listp result) (eq (car result) :error))
    (message "json-log-viewer trunkator worker error: %s"
             (or (cadr result) "unknown error"))))

(defun json-log-viewer--make-log-ingestor-queue-process-func ()
  "Return a serialization-safe PROCESS-FUNC for log-ingestor queue."
  (eval
   '(lambda (job)
      (let ((worker-file (plist-get job :worker-file)))
        (unless (and (stringp worker-file)
                     (file-readable-p worker-file))
          (error "Async worker file is unreadable: %S" worker-file))
        (let ((worker-dir (file-name-directory worker-file)))
          (when worker-dir
            (add-to-list 'load-path worker-dir)))
        (unless (fboundp 'json-log-viewer-async-worker-process-log-ingestor-job)
          (load worker-file nil t)
          (unless (fboundp 'json-log-viewer-async-worker-process-log-ingestor-job)
            (error "Missing log-ingestor worker entrypoint in %s" worker-file)))
        (json-log-viewer-async-worker-process-log-ingestor-job job)))))

(defun json-log-viewer--make-trunkator-queue-process-func ()
  "Return a serialization-safe PROCESS-FUNC for trunkator queue."
  (eval
   '(lambda (job)
      (let ((worker-file (plist-get job :worker-file)))
        (unless (and (stringp worker-file)
                     (file-readable-p worker-file))
          (error "Async worker file is unreadable: %S" worker-file))
        (let ((worker-dir (file-name-directory worker-file)))
          (when worker-dir
            (add-to-list 'load-path worker-dir)))
        (unless (fboundp 'json-log-viewer-async-worker-process-trunkator-job)
          (load worker-file nil t)
          (unless (fboundp 'json-log-viewer-async-worker-process-trunkator-job)
            (error "Missing trunkator worker entrypoint in %s" worker-file)))
        (json-log-viewer-async-worker-process-trunkator-job job)))))

(defun json-log-viewer--stop-async-queue ()
  "Stop async queue for current buffer."
  (when json-log-viewer--log-ingestor-async-queue
    (ignore-errors (async-job-queue-stop json-log-viewer--log-ingestor-async-queue))
    (setq json-log-viewer--log-ingestor-async-queue nil))
  (when json-log-viewer--trunkator-async-queue
    (ignore-errors (async-job-queue-stop json-log-viewer--trunkator-async-queue))
    (setq json-log-viewer--trunkator-async-queue nil))
  (setq json-log-viewer--log-ingestor-async-pending-count 0)
  (setq json-log-viewer--trunkator-async-pending-count 0))

(defun json-log-viewer--start-async-queue ()
  "Start log-ingestor and trunkator async queues for current buffer."
  (json-log-viewer--stop-async-queue)
  (let ((buffer (current-buffer)))
    (setq-local json-log-viewer--log-ingestor-async-queue
                (async-job-queue-create
                 (json-log-viewer--make-log-ingestor-queue-process-func)
                 (lambda (result)
                   (when (buffer-live-p buffer)
                     (with-current-buffer buffer
                       (setq json-log-viewer--log-ingestor-async-pending-count
                             (max 0 (1- json-log-viewer--log-ingestor-async-pending-count)))
                       (json-log-viewer--async-handle-result result))))))
    (setq-local json-log-viewer--trunkator-async-queue
                (async-job-queue-create
                 (json-log-viewer--make-trunkator-queue-process-func)
                 (lambda (result)
                   (when (buffer-live-p buffer)
                     (with-current-buffer buffer
                       (setq json-log-viewer--trunkator-async-pending-count
                             (max 0 (1- json-log-viewer--trunkator-async-pending-count)))
                       (json-log-viewer--trunkator-async-handle-result result))))))))

(defun json-log-viewer--normalize-narrow-string (&optional needle)
  "Normalize NEEDLE into a downcased substring filter, or nil when empty."
  (let ((normalized (string-trim (or needle ""))))
    (unless (string-empty-p normalized)
      (downcase normalized))))

(defun json-log-viewer--make-log-ingestor-async-job (op line &optional narrow-string)
  "Build log-ingestor async queue payload for OP and LINE."
  (list :op op
        :line line
        :narrow-string (json-log-viewer--normalize-narrow-string
                        (or narrow-string json-log-viewer--filter-string))
        :worker-file (json-log-viewer--async-worker-file)
        :timestamp-path json-log-viewer--timestamp-path
        :level-path json-log-viewer--level-path
        :message-path json-log-viewer--message-path
        :extra-paths json-log-viewer--extra-paths
        :sqlite-file json-log-viewer--sqlite-file))

(defun json-log-viewer--make-log-ingestor-async-stored-job (line stored-entry)
  "Build log-ingestor async payload for LINE with STORED-ENTRY summary.

STORED-ENTRY is a plist with keys:
`:sort-key', `:timestamp', `:level-path', `:message-path', and `:extra-paths'."
  (list :op 'ingest-stored
        :line line
        :stored-entry stored-entry
        :worker-file (json-log-viewer--async-worker-file)
        :sqlite-file json-log-viewer--sqlite-file))

(defun json-log-viewer--make-trunkator-async-job (count)
  "Build trunkator async queue payload for COUNT."
  (list :count count
        :worker-file (json-log-viewer--async-worker-file)
        :sqlite-file json-log-viewer--sqlite-file))

(defun json-log-viewer--make-async-job (op payload &optional _preserve-filter)
  "Compatibility wrapper that builds log-ingestor async jobs."
  (pcase op
    ('reset
     (json-log-viewer--make-log-ingestor-async-job 'reset nil))
    ('ingest
     (json-log-viewer--make-log-ingestor-async-job 'ingest payload))
    ('append
     (json-log-viewer--make-log-ingestor-async-job
      'ingest
      (if (listp payload) (car payload) payload)))
    ('narrow
     (json-log-viewer--make-log-ingestor-async-job 'narrow nil payload))
    ('widen
     (json-log-viewer--make-log-ingestor-async-job 'widen nil nil))
    ('replace
     (json-log-viewer--make-log-ingestor-async-job 'reset nil))
    (_
     (list :op op
          :worker-file (json-log-viewer--async-worker-file)
          :sqlite-file json-log-viewer--sqlite-file))))

(defun json-log-viewer--async-submit (job &optional wait-for-callback)
  "Submit log-ingestor JOB to current buffer queue.

When WAIT-FOR-CALLBACK is non-nil, block until callback has applied."
  (unless json-log-viewer--log-ingestor-async-queue
    (error "Log-ingestor queue is not running for this buffer"))
  (let ((before json-log-viewer--log-ingestor-async-pending-count))
    (setq json-log-viewer--log-ingestor-async-pending-count
          (1+ json-log-viewer--log-ingestor-async-pending-count))
    (async-job-queue-push json-log-viewer--log-ingestor-async-queue job)
    (when (or wait-for-callback noninteractive)
      (json-log-viewer--async-await-pending-count before))))

(defun json-log-viewer--trunkator-async-await-pending-count (target-count)
  "Wait until trunkator pending async count reaches TARGET-COUNT."
  (let ((deadline (+ (float-time) 15.0)))
    (while (and (> json-log-viewer--trunkator-async-pending-count target-count)
                (< (float-time) deadline))
      (accept-process-output nil 0.01))
    (when (> json-log-viewer--trunkator-async-pending-count target-count)
      (error "Timed out waiting for trunkator async queue callback"))))

(defun json-log-viewer--trunkator-async-submit (job &optional wait-for-callback)
  "Submit trunkator JOB to current buffer queue."
  (unless json-log-viewer--trunkator-async-queue
    (error "Trunkator async queue is not running for this buffer"))
  (let ((before json-log-viewer--trunkator-async-pending-count))
    (setq json-log-viewer--trunkator-async-pending-count
          (1+ json-log-viewer--trunkator-async-pending-count))
    (async-job-queue-push json-log-viewer--trunkator-async-queue job)
    (when (or wait-for-callback noninteractive)
      (json-log-viewer--trunkator-async-await-pending-count before))))

(defun json-log-viewer--ensure-async-queue-running ()
  "Ensure current buffer has active async queues."
  (when (or (not json-log-viewer--log-ingestor-async-queue)
            (not json-log-viewer--trunkator-async-queue))
    (user-error
     "json-log-viewer async queues are not running")))

(defun json-log-viewer--normalize-path-list (paths source)
  "Validate PATHS from SOURCE and return normalized path list."
  (unless (or (null paths)
              (and (listp paths)
                   (cl-every #'stringp paths)))
    (user-error "%s must be a list of strings, got: %S" source paths))
  (let (normalized)
    (dolist (path paths)
      (let ((trimmed (string-trim path)))
        (when (not (string-empty-p trimmed))
          (push trimmed normalized))))
    (nreverse (delete-dups normalized))))

(defun json-log-viewer--fontify-json-string (value)
  "Return VALUE with JSON syntax highlighting."
  (with-temp-buffer
    (insert value)
    (delay-mode-hooks
      (let ((warning-suppress-types (cons '(treesit) warning-suppress-types)))
        (let* ((mode json-log-viewer-json-syntax-mode)
               (can-use-mode
                (cond
                 ((not (symbolp mode)) nil)
                 ((not (fboundp mode)) nil)
                 ((and (eq mode 'json-ts-mode)
                       (fboundp 'treesit-language-available-p))
                  (ignore-errors (treesit-language-available-p 'json)))
                 (t t)))
               (ok (and can-use-mode
                        (condition-case nil
                            (progn (funcall mode) t)
                          (error nil)))))
          (unless ok
            (if (fboundp 'js-mode)
                (js-mode)
              (fundamental-mode))))))
    (if (fboundp 'font-lock-ensure)
        (font-lock-ensure (point-min) (point-max))
      (font-lock-fontify-region (point-min) (point-max)))
    (buffer-substring (point-min) (point-max))))

(defun json-log-viewer--json-value->pretty-string (value)
  "Render VALUE as pretty, syntax-highlighted JSON text."
  (let* ((parsed (or (json-log-viewer-shared--parse-json-maybe value) value))
         (normalized (json-log-viewer-shared--normalize-json-value-for-serialize parsed))
         (json (condition-case nil
                   (json-serialize normalized :null-object nil :false-object :false)
                 (error nil))))
    (if (not json)
        (or (json-log-viewer-shared--value->string parsed) "")
      (let ((pretty (condition-case nil
                        (with-temp-buffer
                          (insert json)
                          (json-pretty-print-buffer)
                          (buffer-string))
                      (error json))))
        (propertize (json-log-viewer--fontify-json-string (string-trim-right pretty))
                    'json-log-viewer-json-block t)))))

(defun json-log-viewer--json-object-fields (parsed raw-line json-paths)
  "Build detail fields alist from PARSED JSON and RAW-LINE.

JSON-PATHS is a list of paths to render as JSON blocks instead of flattening."
  (cl-labels
      ((flatten (node &optional prefix)
         (cond
          ((and prefix (member prefix json-paths))
           (list (cons prefix (json-log-viewer--json-value->pretty-string node))))
          ((hash-table-p node)
           (let (fields keys)
             (maphash (lambda (key _value)
                        (let ((k (json-log-viewer-shared--value->string key)))
                          (when k
                            (push k keys))))
                      node)
             (setq keys (sort keys #'string-lessp))
             (if (null keys)
                 (when prefix (list (cons prefix "")))
               (dolist (key keys)
                 (setq fields
                       (append fields
                               (flatten (or (gethash key node)
                                            (when-let ((sym (intern-soft key)))
                                              (gethash sym node)))
                                        (json-log-viewer-shared--join-path prefix key)))))
               fields)))
          ((json-log-viewer-shared--alist-like-p node)
           (if (null node)
               (when prefix (list (cons prefix "")))
             (let (fields)
               (dolist (pair node)
                 (when (consp pair)
                   (let ((k (json-log-viewer-shared--value->string (car pair))))
                     (when k
                       (setq fields
                             (append fields
                                     (flatten (cdr pair)
                                              (json-log-viewer-shared--join-path prefix k))))))))
               fields)))
          ((listp node)
           (let ((base (or prefix "value")))
             (if (null node)
                 (list (cons base "[]"))
               (let ((idx 0)
                     fields)
                 (dolist (item node)
                   (setq fields
                         (append fields
                                 (flatten item (format "%s[%d]" base idx))))
                   (setq idx (1+ idx)))
                 fields))))
          (t
           (list (cons (or prefix "value")
                       (or (json-log-viewer-shared--value->string node) "")))))))
    (let ((fields (and parsed (flatten parsed nil))))
      (if fields
          fields
        (list (cons "raw" (or raw-line "")))))))

(defun json-log-viewer--parse-time (value)
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
          ;; `date-to-time' can ignore sub-second precision in some formats.
          (if (and fraction (= base (truncate base)))
              (+ base fraction)
            base))))))

(defun json-log-viewer--level-face (level)
  "Return face symbol suitable for LEVEL."
  (let ((normalized (downcase (or level ""))))
    (cond
     ((string-match-p "\\`\\(error\\|fatal\\|crit\\|panic\\)" normalized) 'error)
     ((string-match-p "\\`\\(warn\\|warning\\)" normalized) 'warning)
     ((string-match-p "\\`\\(debug\\|trace\\)" normalized) 'font-lock-doc-face)
     (t 'json-log-viewer-level-face))))

(defun json-log-viewer--truncate (value limit)
  "Truncate VALUE to LIMIT characters."
  (if (> (length value) limit)
      (concat (substring value 0 (max 0 (- limit 3))) "...")
    value))

(defun json-log-viewer--normalize-direction (direction)
  "Normalize DIRECTION symbol to `newest-first' or `oldest-first'."
  (pcase direction
    ((or 'newest-first 'desc 'descending) 'newest-first)
    ((or 'oldest-first 'asc 'ascending) 'oldest-first)
    (_
     (user-error "Invalid direction: %S (expected newest-first/oldest-first or asc/desc)"
                 direction))))

(defun json-log-viewer--ensure-log-lines (log-lines source)
  "Validate LOG-LINES from SOURCE and return a copied list."
  (unless (or (null log-lines)
              (and (listp log-lines)
                   (cl-every #'stringp log-lines)))
    (user-error "%s must be a list of JSON strings, got: %S" source log-lines))
  (append log-lines nil))

(defun json-log-viewer--json-line->entry (line)
  "Convert one JSON log LINE into a viewer entry plist."
  (let* ((entry-id json-log-viewer--next-entry-id)
         (parsed (json-log-viewer-shared--parse-json-line line))
         (timestamp (json-log-viewer-shared--resolve-path parsed json-log-viewer--timestamp-path))
         (timestamp-epoch (json-log-viewer--parse-time timestamp))
         (sort-key (or timestamp-epoch (+ 1000000000000.0 entry-id))))
    (setq json-log-viewer--next-entry-id (1+ json-log-viewer--next-entry-id))
    (list :id entry-id
          :raw line
          :parsed parsed
          :fields (json-log-viewer--json-object-fields
                   parsed line json-log-viewer--json-paths)
          :sort-key sort-key)))

(defun json-log-viewer--json-line->entry-with-config (line entry-id timestamp-path &optional json-paths)
  "Convert LINE into an entry plist using ENTRY-ID and TIMESTAMP-PATH.

When JSON-PATHS is non-nil, selected paths render as pretty JSON blocks."
  (let* ((parsed (json-log-viewer-shared--parse-json-line line))
         (timestamp (json-log-viewer-shared--resolve-path parsed timestamp-path))
         (timestamp-epoch (json-log-viewer--parse-time timestamp))
         (sort-key (or timestamp-epoch (+ 1000000000000.0 entry-id))))
    (list :id entry-id
          :raw line
          :parsed parsed
          :fields (json-log-viewer--json-object-fields parsed line json-paths)
          :sort-key sort-key)))

(defun json-log-viewer--json-lines->entries (lines timestamp-path start-id &optional json-paths)
  "Convert LINES into entries using TIMESTAMP-PATH, starting at START-ID.

Returns cons cell (ENTRIES . NEXT-ID)."
  (let ((next-id start-id)
        entries)
    (dolist (line lines)
      (push (json-log-viewer--json-line->entry-with-config
             line next-id timestamp-path json-paths)
            entries)
      (setq next-id (1+ next-id)))
    (cons (nreverse entries) next-id)))

(defun json-log-viewer--json-entry-fields (entry)
  "Return detail fields from JSON-line ENTRY."
  (plist-get entry :fields))

(defun json-log-viewer--json-entry-signature (entry)
  "Return stable signature string for JSON-line ENTRY."
  (number-to-string (or (plist-get entry :id) 0)))

(defun json-log-viewer--json-entry-sort-key (entry)
  "Return sort key for JSON-line ENTRY."
  (plist-get entry :sort-key))

(defun json-log-viewer--json-summary (entry _fields)
  "Return formatted summary line for JSON-line ENTRY."
  (let* ((parsed (plist-get entry :parsed))
         (raw (or (plist-get entry :raw) ""))
         (flattened-fields (and parsed (json-log-viewer-shared--flatten-path-values parsed)))
         (timestamp (or (plist-get entry :timestamp)
                        (json-log-viewer-shared--resolve-path
                         parsed json-log-viewer--timestamp-path flattened-fields)
                        "-"))
         (level (or (plist-get entry :level)
                    (json-log-viewer-shared--resolve-path
                     parsed json-log-viewer--level-path flattened-fields)
                    "-"))
         (message (or (plist-get entry :message)
                      (json-log-viewer-shared--resolve-path
                       parsed json-log-viewer--message-path flattened-fields)
                      raw
                      "-"))
         (extras (or (plist-get entry :extra-fields)
                     (plist-get entry :extras)
                     nil)))
    (unless (or extras (not parsed))
      (dolist (path json-log-viewer--extra-paths)
        (when-let ((value (json-log-viewer-shared--resolve-path parsed path flattened-fields)))
          (push value extras)))
      (setq extras (nreverse extras)))
    (concat
     (propertize timestamp 'face 'json-log-viewer-timestamp-face)
     " "
     (propertize (upcase level) 'face (json-log-viewer--level-face level))
     (if extras
         (concat " "
                 (mapconcat (lambda (value)
                              (propertize
                               (format "[%s]" (json-log-viewer--truncate value 80))
                               'face 'json-log-viewer-extra-face))
                            extras
                            " "))
       "")
     " "
     (propertize (json-log-viewer--truncate message 240)
                 'face 'json-log-viewer-message-face))))

(defun json-log-viewer--json-header-lines (state)
  "Return header lines for current JSON-line buffer and STATE."
  (append
   (list (cons "Mode" (if (plist-get state :streaming) "streaming" "non-streaming"))
         (cons "Direction" (symbol-name (plist-get state :direction)))
         (cons "Auto follow" (if (plist-get state :auto-follow) "on" "off")))
   (when (functionp json-log-viewer--json-header-lines-function)
     (or (funcall json-log-viewer--json-header-lines-function state) nil))))

(defun json-log-viewer--json-refresh (_state)
  "Refresh callback used for JSON-line buffers."
  (unless (functionp json-log-viewer--json-refresh-log-lines-function)
    (user-error "No JSON-line refresh function configured"))
  (let* ((old-lines (json-log-viewer--raw-lines-list))
         (refresh-value (funcall json-log-viewer--json-refresh-log-lines-function old-lines)))
    (if (eq refresh-value :async)
        ;; Async refresh function will mutate the buffer later.
        (list :entries nil :replace nil)
      (let ((new-lines (json-log-viewer--ensure-log-lines
                        refresh-value
                        "json-log-viewer refresh-function return value")))
        (json-log-viewer-replace-log-lines (current-buffer) new-lines t)
        (list :entries nil :replace nil)))))

(defun json-log-viewer--entry-signature (entry)
  "Return stable signature for ENTRY."
  (if json-log-viewer--signature-function
      (or (funcall json-log-viewer--signature-function entry)
          (prin1-to-string entry))
    (prin1-to-string entry)))

(defun json-log-viewer--sort-key< (a b)
  "Return non-nil when sortable key A is strictly before B."
  (cond
   ((and (numberp a) (numberp b)) (< a b))
   ((and (stringp a) (stringp b)) (string-lessp a b))
   (t (string-lessp (format "%s" a) (format "%s" b)))))

(defun json-log-viewer--sort-entries (entries)
  "Return ENTRIES in display order for current buffer mode."
  (let ((ordered (append entries nil)))
    (if (not json-log-viewer--sort-key-function)
        ordered
      (let ((ascending (if json-log-viewer--streaming
                           t
                         (eq json-log-viewer--direction 'oldest-first))))
        (cl-stable-sort
         ordered
         (lambda (a b)
           (let ((ka (funcall json-log-viewer--sort-key-function a))
                 (kb (funcall json-log-viewer--sort-key-function b)))
             (cond
              ((and (null ka) (null kb)) nil)
              ((null ka) nil)
              ((null kb) t)
              (ascending (json-log-viewer--sort-key< ka kb))
              (t (json-log-viewer--sort-key< kb ka))))))))))

(defun json-log-viewer--state ()
  "Return current viewer state plist for callbacks."
  (list :context json-log-viewer--context
        :metadata json-log-viewer--metadata
        :streaming json-log-viewer--streaming
        :direction json-log-viewer--direction
        :auto-follow json-log-viewer--auto-follow
        :filter json-log-viewer--filter-string
        :row-count json-log-viewer--entry-count
        :visible-row-count (json-log-viewer--visible-entry-count)))

(defun json-log-viewer--self-subscriber (_action _source-buffer _entry-overlays)
  "Default no-op subscriber used for self-subscription.")

(defun json-log-viewer--publish (action entry-overlays)
  "Publish ACTION and ENTRY-OVERLAYS to current buffer subscribers."
  (let ((source-buffer (current-buffer)))
    (dolist (subscriber (append json-log-viewer--subscribers nil))
      (let ((id (car subscriber))
            (callback (cdr subscriber)))
        (when (functionp callback)
          (condition-case err
              (funcall callback action source-buffer entry-overlays)
            (error
             (message "json-log-viewer subscriber %S failed: %s"
                      id
                      (error-message-string err)))))))))

(defun json-log-viewer--set-point-to-latest-entry ()
  "Move point and all visible windows for current buffer to latest entry."
  (let ((target (point-max)))
    (setq json-log-viewer--auto-follow-internal-move t)
    (unwind-protect
        (progn
          (goto-char target)
          (dolist (window (get-buffer-window-list (current-buffer) nil t))
            (set-window-point window target)))
      (setq json-log-viewer--auto-follow-internal-move nil))))

(defun json-log-viewer--cleanup-storage-on-kill ()
  "Cleanup persistent storage resources for current viewer buffer."
  (json-log-viewer--stop-async-queue)
  (json-log-viewer--storage-close))

(defun json-log-viewer--remember-point-before-command ()
  "Record current point for auto-follow cursor-move detection."
  (setq json-log-viewer--auto-follow-point-before-command (point)))

(defun json-log-viewer--maybe-disable-auto-follow-after-command ()
  "Disable auto-follow when cursor moved by user command."
  (when (and json-log-viewer--auto-follow
             (not json-log-viewer--auto-follow-internal-move)
             (integer-or-marker-p json-log-viewer--auto-follow-point-before-command)
             (/= (point) json-log-viewer--auto-follow-point-before-command)
             (not (eq this-command 'json-log-viewer-toggle-auto-follow)))
    (setq json-log-viewer--auto-follow nil)
    (json-log-viewer--refresh-header)
    (message "Auto-follow disabled (cursor moved)")))

(defun json-log-viewer--clear-overlays ()
  "Remove all fold and entry overlays in the current buffer."
  (mapc #'delete-overlay json-log-viewer--fold-overlays)
  (mapc #'delete-overlay json-log-viewer--entry-overlays)
  (when json-log-viewer--current-line-overlay
    (delete-overlay json-log-viewer--current-line-overlay))
  (setq json-log-viewer--fold-overlays nil)
  (setq json-log-viewer--entry-overlays nil)
  (setq json-log-viewer--entry-count 0)
  (setq json-log-viewer--current-line-overlay nil))

(defun json-log-viewer--clear-rendered-buffer ()
  "Clear rendered entries while preserving sqlite storage."
  (let ((inhibit-read-only t))
    (json-log-viewer--clear-overlays)
    (erase-buffer)
    (setq json-log-viewer--seen-signatures (make-hash-table :test 'equal))
    (json-log-viewer--refresh-header)
    (goto-char (point-min))
    (json-log-viewer--highlight-current-line)))

(defun json-log-viewer--entry-summary-end (entry-overlay)
  "Return end position of ENTRY-OVERLAY summary line."
  (save-excursion
    (goto-char (overlay-start entry-overlay))
    (end-of-line)
    (if (< (point) (point-max))
        (1+ (point))
      (point))))

(defun json-log-viewer--insert-entry-details-lines (fields)
  "Insert detail lines for normalized FIELD pairs."
  (dolist (pair fields)
    (let* ((key (car pair))
           (value (or (cdr pair) ""))
           (prefix (format "  %s: " key))
           (continuation (make-string (length prefix) ?\s))
           (lines (split-string value "\n" nil)))
      ;; (insert "  ")
      (insert (propertize key 'face 'json-log-viewer-key-face))
      (if (get-text-property 0 'json-log-viewer-json-block value)
          (progn
            (insert ":\n")
            (insert value "\n"))
        (insert ": ")
        (if (null lines)
            (insert "\n")
          (insert (car lines) "\n")
          (dolist (line (cdr lines))
            (insert continuation line "\n"))))))
  (insert "\n"))

(defun json-log-viewer--entry-expand (entry-overlay)
  "Insert details for ENTRY-OVERLAY when currently collapsed."
  (unless (overlay-get entry-overlay 'json-log-viewer-entry-expanded)
    (let ((inhibit-read-only t))
      (save-excursion
        (goto-char (json-log-viewer--entry-summary-end entry-overlay))
        (let ((details-start (point))
              (fields (or (json-log-viewer--storage-entry-fields entry-overlay)
                          '())))
          (json-log-viewer--insert-entry-details-lines fields)
          (let ((details-end (point))
                (fold-ov (make-overlay details-start (point))))
            (overlay-put fold-ov 'json-log-viewer-fold t)
            (overlay-put fold-ov 'invisible (overlay-get entry-overlay 'invisible))
            (push fold-ov json-log-viewer--fold-overlays)
            (overlay-put entry-overlay 'json-log-viewer-fold-overlay fold-ov)
            (overlay-put entry-overlay 'json-log-viewer-entry-expanded t)
            (move-overlay entry-overlay
                          (overlay-start entry-overlay)
                          details-end)))))))

(defun json-log-viewer--entry-collapse (entry-overlay)
  "Remove details for ENTRY-OVERLAY when currently expanded."
  (when (overlay-get entry-overlay 'json-log-viewer-entry-expanded)
    (let ((inhibit-read-only t)
          (fold-ov (overlay-get entry-overlay 'json-log-viewer-fold-overlay)))
      (when (and (overlayp fold-ov)
                 (overlay-buffer fold-ov)
                 (overlay-start fold-ov)
                 (overlay-end fold-ov))
        (delete-region (overlay-start fold-ov) (overlay-end fold-ov))
        (setq json-log-viewer--fold-overlays
              (delq fold-ov json-log-viewer--fold-overlays))
        (delete-overlay fold-ov))
      (overlay-put entry-overlay 'json-log-viewer-fold-overlay nil)
      (overlay-put entry-overlay 'json-log-viewer-entry-expanded nil)
      (move-overlay entry-overlay
                    (overlay-start entry-overlay)
                    (json-log-viewer--entry-summary-end entry-overlay)))))

(defun json-log-viewer-toggle-entry ()
  "Toggle fold state for current log entry."
  (interactive)
  (when-let ((entry-ov (json-log-viewer--entry-overlay-at-point)))
    (if (overlay-get entry-ov 'json-log-viewer-entry-expanded)
        (json-log-viewer--entry-collapse entry-ov)
      (json-log-viewer--entry-expand entry-ov))
    (json-log-viewer--highlight-current-line)))

(defun json-log-viewer--entry-overlays-in-buffer-order (&optional visible-only)
  "Return entry overlays sorted by buffer position.

When VISIBLE-ONLY is non-nil, return only currently visible entries."
  (let (rows)
    (dolist (entry-ov json-log-viewer--entry-overlays)
      (when (and (overlay-buffer entry-ov)
                 (overlay-start entry-ov)
                 (overlay-end entry-ov)
                 (or (not visible-only)
                     (not (overlay-get entry-ov 'invisible))))
        (push entry-ov rows)))
    (sort rows (lambda (a b) (< (overlay-start a) (overlay-start b))))))

(defun json-log-viewer-toggle-all ()
  "Toggle fold state for all log entries."
  (interactive)
  (let ((expand-any nil)
        (entries (json-log-viewer--entry-overlays-in-buffer-order t)))
    (dolist (entry-ov entries)
      (unless (overlay-get entry-ov 'json-log-viewer-entry-expanded)
        (setq expand-any t)))
    (dolist (entry-ov (reverse entries))
      (if expand-any
          (json-log-viewer--entry-expand entry-ov)
        (json-log-viewer--entry-collapse entry-ov)))
    (json-log-viewer--highlight-current-line)))

(defun json-log-viewer--entry-overlay-at-point (&optional pos)
  "Return entry overlay at POS, or nil."
  (catch 'found
    (dolist (ov (overlays-at (or pos (point))))
      (when (overlay-get ov 'json-log-viewer-entry)
        (throw 'found ov)))
    nil))

(defun json-log-viewer--highlight-current-line ()
  "Update current entry highlight overlay in viewer buffers."
  (when (derived-mode-p 'json-log-viewer-mode)
    (let* ((entries-start (json-log-viewer--header-end-position))
           (pos (point))
           (visible-pos
            (if (invisible-p pos)
                (or (next-single-char-property-change pos 'invisible nil (point-max))
                    (previous-single-char-property-change pos 'invisible nil (point-min))
                    pos)
              pos))
           (_ (when (< visible-pos entries-start)
                (setq visible-pos pos)))
           (entry-ov (or (json-log-viewer--entry-overlay-at-point visible-pos)
                         (json-log-viewer--entry-overlay-at-point pos))))
      (if (or (not entry-ov)
              (< pos entries-start))
          (when json-log-viewer--current-line-overlay
            (delete-overlay json-log-viewer--current-line-overlay)
            (setq json-log-viewer--current-line-overlay nil))
        (let* ((entry-start (max entries-start (overlay-start entry-ov)))
               (entry-end (overlay-end entry-ov))
               (expanded (overlay-get entry-ov 'json-log-viewer-entry-expanded))
               (highlight-end (if expanded
                                  entry-end
                                (max entries-start
                                     (json-log-viewer--entry-summary-end entry-ov)))))
          (if (<= highlight-end entry-start)
              (when json-log-viewer--current-line-overlay
                (delete-overlay json-log-viewer--current-line-overlay)
                (setq json-log-viewer--current-line-overlay nil))
            (unless json-log-viewer--current-line-overlay
              (setq json-log-viewer--current-line-overlay
                    (make-overlay entry-start highlight-end nil t t))
              (overlay-put json-log-viewer--current-line-overlay 'face 'hl-line)
              (overlay-put json-log-viewer--current-line-overlay 'priority 1000))
            (move-overlay json-log-viewer--current-line-overlay
                          entry-start highlight-end)))))))

(defun json-log-viewer--entry-filter-text (fields)
  "Build searchable text blob from FIELDS."
  (downcase
   (mapconcat (lambda (pair)
                (format "%s %s" (car pair) (cdr pair)))
              fields
              "\n")))

(defun json-log-viewer--filter-match-p (entry-overlay needle)
  "Return non-nil when ENTRY-OVERLAY matches NEEDLE."
  (string-match-p
   (regexp-quote needle)
   (or (json-log-viewer--storage-entry-filter-text entry-overlay) "")))

(defun json-log-viewer--filter-managed-by-ingestor-p ()
  "Return non-nil when active narrowing is handled by async log ingestor."
  (and json-log-viewer--sqlite-file
       json-log-viewer--log-ingestor-async-queue))

(defun json-log-viewer--apply-filter ()
  "Apply active filter to entry overlays in current buffer."
  (let* ((needle (and json-log-viewer--filter-string
                      (string-trim json-log-viewer--filter-string)))
         (normalized (and needle (downcase needle)))
         (active (and normalized (not (string-empty-p normalized))))
         (matching-signatures (and active
                                   (json-log-viewer--storage-matching-signatures normalized))))
    (dolist (entry-overlay json-log-viewer--entry-overlays)
      (let ((invisible
             (if (and active
                      (not (json-log-viewer--filter-managed-by-ingestor-p))
                      (if matching-signatures
                          (not (gethash (overlay-get entry-overlay 'json-log-viewer-signature)
                                        matching-signatures))
                        (not (json-log-viewer--filter-match-p entry-overlay normalized))))
                 'json-log-viewer-filter
               nil)))
        (overlay-put entry-overlay 'invisible invisible)
        (when-let ((fold-ov (overlay-get entry-overlay 'json-log-viewer-fold-overlay)))
          (overlay-put fold-ov 'invisible invisible))))))

(defun json-log-viewer--apply-filter-to-overlays (overlays)
  "Apply current filter to OVERLAYS only."
  (let* ((needle (and json-log-viewer--filter-string
                      (string-trim json-log-viewer--filter-string)))
         (normalized (and needle (downcase needle)))
         (active (and normalized (not (string-empty-p normalized))))
         (matching-signatures (and active
                                   (json-log-viewer--storage-matching-signatures normalized))))
    (dolist (entry-overlay overlays)
      (let ((invisible
             (if (and active
                      (not (json-log-viewer--filter-managed-by-ingestor-p))
                      (if matching-signatures
                          (not (gethash (overlay-get entry-overlay 'json-log-viewer-signature)
                                        matching-signatures))
                        (not (json-log-viewer--filter-match-p entry-overlay normalized))))
                 'json-log-viewer-filter
               nil)))
        (overlay-put entry-overlay 'invisible invisible)
        (when-let ((fold-ov (overlay-get entry-overlay 'json-log-viewer-fold-overlay)))
          (overlay-put fold-ov 'invisible invisible))))))

(defun json-log-viewer--set-filter (needle)
  "Set viewer filter to NEEDLE and apply it."
  (let ((normalized (string-trim (or needle ""))))
    (setq json-log-viewer--filter-string
          (unless (string-empty-p normalized) normalized))
    (json-log-viewer--apply-filter)
    (json-log-viewer--highlight-current-line)))

(defun json-log-viewer--request-narrow-rebuild (op &optional needle wait-for-callback)
  "Request async log-ingestor OP rebuild with NEEDLE.

When WAIT-FOR-CALLBACK is non-nil, block until callback is applied."
  (json-log-viewer--ensure-async-queue-running)
  (setq json-log-viewer--narrow-rebuild-in-progress t)
  (json-log-viewer--clear-rendered-buffer)
  (json-log-viewer--async-submit
   (json-log-viewer--make-log-ingestor-async-job op nil needle)
   wait-for-callback))

(defun json-log-viewer--visible-entry-count ()
  "Return number of visible rendered entries in current buffer."
  (let ((visible 0))
    (dolist (entry-overlay json-log-viewer--entry-overlays)
      (unless (overlay-get entry-overlay 'invisible)
        (setq visible (1+ visible))))
    visible))

(defun json-log-viewer--filter-summary ()
  "Return display string for active filter."
  (if (and json-log-viewer--filter-string
           (not (string-empty-p json-log-viewer--filter-string)))
      (format "\"%s\"" json-log-viewer--filter-string)
    "(none)"))

(defun json-log-viewer--stream-chunk-size ()
  "Return normalized streaming chunk size."
  (max 1 (or json-log-viewer-stream-chunk-size 1)))

(defun json-log-viewer--stream-eviction-drop-count (over-limit)
  "Return chunk-rounded drop count for OVER-LIMIT rows."
  (let* ((chunk-size (json-log-viewer--stream-chunk-size))
         (chunks (/ (+ over-limit chunk-size -1) chunk-size)))
    (* chunks chunk-size)))

(defun json-log-viewer--raw-lines-list ()
  "Return flattened copy of raw lines from sqlite storage."
  (json-log-viewer-repository-select-all-json-lines json-log-viewer--sqlite-db))

(defun json-log-viewer--get-logs-since (timestamp limit)
  "Return stored log rows before TIMESTAMP from current buffer sqlite storage.

TIMESTAMP is an epoch float upper bound and rows are returned strictly
before it.
When TIMESTAMP is nil, all stored rows are eligible.

LIMIT controls how many of the newest matching rows are returned.  When LIMIT
is nil, all matching rows are returned.

Return value is always sorted in ascending order and each row is a plist:
  (:id INTEGER :timestamp NUMBER-OR-NIL :json STRING)."
  (unless json-log-viewer--sqlite-db
    (user-error "No sqlite storage is active for this buffer"))
  (unless (or (null timestamp) (numberp timestamp))
    (user-error "TIMESTAMP must be a number or nil, got: %S" timestamp))
  (unless (or (null limit)
              (and (integerp limit) (> limit 0)))
    (user-error "LIMIT must be a positive integer or nil, got: %S" limit))
  (json-log-viewer-repository-select-logs-before
   json-log-viewer--sqlite-db
   timestamp
   limit))

(defun json-log-viewer--info-line (key value)
  "Return formatted popup info line from KEY and VALUE."
  (concat
   (propertize (format "%-12s" (concat key ":"))
               'face 'json-log-viewer-header-key-face)
   " "
   (propertize value 'face 'json-log-viewer-header-value-face)))

(defun json-log-viewer--pad-right (text width)
  "Return TEXT padded with spaces to WIDTH."
  (concat text (make-string (max 0 (- width (string-width text))) ? )))

(defun json-log-viewer--binding-line (binding)
  "Return formatted popup line for one key BINDING."
  (concat
   (propertize (format "%-10s" (car binding))
               'face 'json-log-viewer-keybinding-face)
   (propertize (format " %s" (cdr binding))
               'face 'json-log-viewer-header-value-face)))

(defun json-log-viewer--default-keybindings ()
  "Return default keybindings shown in the viewer info popup."
  (append
   '(("TAB" . "toggle entry")
     ("S-TAB" . "toggle all")
     ("C-c C-n" . "narrow")
     ("C-c C-w" . "widen")
     ("C-c C-f" . "toggle follow")
     ("?" . "show info")
     ("q" . "quit"))
   (when json-log-viewer--refresh-function
     '(("C-c C-r" . "refresh")))))

(defun json-log-viewer--keybindings ()
  "Return keybindings shown in the viewer info popup."
  (if (functionp json-log-viewer--keybindings-function)
      (or (funcall json-log-viewer--keybindings-function)
          (json-log-viewer--default-keybindings))
    (json-log-viewer--default-keybindings)))

(defun json-log-viewer--info-lines (&optional row-count)
  "Return viewer info lines for popup display using ROW-COUNT."
  (let ((state (json-log-viewer--state)))
    (append
     (or (and json-log-viewer--header-function
              (funcall json-log-viewer--header-function state))
         nil)
     (list
     (cons "Messages"
           (number-to-string
            (or row-count
                 json-log-viewer--entry-count)))
      (cons "Narrow filter"
            (json-log-viewer--filter-summary))))))

(defun json-log-viewer-show-info ()
  "Show current viewer context and keys in a popup."
  (interactive)
  (let ((source-buffer (current-buffer))
        (lines (json-log-viewer--info-lines))
        (bindings (json-log-viewer--keybindings)))
    (let* ((binding-lines (mapcar #'json-log-viewer--binding-line bindings))
           (info-lines
            (delq nil
                  (mapcar
                   (lambda (line)
                     (let ((key (json-log-viewer-shared--value->string (car line)))
                           (value (json-log-viewer-shared--value->string (cdr line))))
                       (when key
                         (json-log-viewer--info-line key (or value "")))))
                   lines)))
           (column-separator "  |  ")
           (bindings-title (propertize "Bindings" 'face 'json-log-viewer-header-key-face))
           (info-title (propertize "Info" 'face 'json-log-viewer-header-key-face))
           (bindings-width
            (max (string-width bindings-title)
                 (if binding-lines
                     (apply #'max (mapcar #'string-width binding-lines))
                   0)))
           (row-count (max (length binding-lines) (length info-lines))))
      (with-help-window (help-buffer)
        (princ (format "JSON Log Viewer: %s\n\n" (buffer-name source-buffer)))
        (princ (json-log-viewer--pad-right bindings-title bindings-width))
        (princ column-separator)
        (princ info-title)
        (princ "\n")
        (princ (make-string bindings-width ?-))
        (princ column-separator)
        (princ "----")
        (princ "\n")
        (dotimes (idx row-count)
          (let ((binding-line (or (nth idx binding-lines) ""))
                (info-line (or (nth idx info-lines) "")))
            (princ (json-log-viewer--pad-right binding-line bindings-width))
            (princ column-separator)
            (princ info-line)
            (princ "\n")))))))

(defun json-log-viewer--header-end-position ()
  "Return position where entries start (no header is rendered)."
  (point-min))

(defun json-log-viewer--header-line-string ()
  "Return header-line text for current viewer buffer."
  (let ((messages (format "Messages: %d" json-log-viewer--entry-count))
        (follow (format "Follow: %s" (if json-log-viewer--auto-follow "on" "off")))
        (direction (format "Direction: %s" (symbol-name json-log-viewer--direction)))
        (needle (and json-log-viewer--filter-string
                     (string-trim json-log-viewer--filter-string))))
    (concat
     " " messages
     "  |  " follow
     (if json-log-viewer--streaming
         ""
       (concat "  |  " direction))
     (if (and needle (not (string-empty-p needle)))
         (format "  |  Narrow: \"%s\"" needle)
       ""))))

(defun json-log-viewer--refresh-header ()
  "Refresh `header-line-format` for current viewer buffer."
  (setq-local header-line-format
              (propertize (json-log-viewer--header-line-string)
                          'face 'json-log-viewer-header-value-face)))

(defun json-log-viewer-narrow ()
  "Narrow rendered entries to rows whose stored JSON contains a substring."
  (interactive)
  (let ((needle (string-trim
                 (read-string "Narrow to string: "
                              (or json-log-viewer--filter-string "")))))
    (when (string-empty-p needle)
      (user-error "Narrow string cannot be empty"))
    (setq json-log-viewer--filter-string needle)
    (json-log-viewer--refresh-header)
    (json-log-viewer--request-narrow-rebuild 'narrow needle)
    (message "Narrowing to \"%s\"..." needle)))

(defun json-log-viewer-widen ()
  "Clear active narrowing and replay all stored entries."
  (interactive)
  (setq json-log-viewer--filter-string nil)
  (json-log-viewer--refresh-header)
  (json-log-viewer--request-narrow-rebuild 'widen nil)
  (message "Widening..."))

(defun json-log-viewer-toggle-auto-follow ()
  "Toggle automatic scrolling to newest entries."
  (interactive)
  (setq json-log-viewer--auto-follow (not json-log-viewer--auto-follow))
  (when json-log-viewer--auto-follow
    (json-log-viewer--set-point-to-latest-entry))
  (json-log-viewer--refresh-header)
  (message "Auto-follow %s" (if json-log-viewer--auto-follow "enabled" "disabled")))

(defun json-log-viewer--insert-entry (entry)
  "Insert one foldable ENTRY."
  (let* ((storage-populated (plist-get entry :storage-populated))
         (entry-id (plist-get entry :id))
         (raw-fields (unless storage-populated
                       (funcall json-log-viewer--entry-fields-function entry)))
         (fields (and raw-fields (json-log-viewer--normalize-fields raw-fields)))
         (summary (funcall json-log-viewer--summary-function entry fields))
         (filter-text (or (plist-get entry :filter-text)
                          (and fields (json-log-viewer--entry-filter-text fields))
                          ""))
         (signature (json-log-viewer--entry-signature entry))
         (summary-start (point))
         entry-ov)
    (insert (or (json-log-viewer-shared--value->string summary) "-") "\n")
    ;; Front-advance keeps older entry overlays stable when a newer line is
    ;; inserted at the buffer start (non-streaming newest-first updates).
    (setq entry-ov (make-overlay summary-start (point) nil t nil))
    (overlay-put entry-ov 'json-log-viewer-entry t)
    (overlay-put entry-ov 'json-log-viewer-entry-expanded nil)
    (overlay-put entry-ov 'json-log-viewer-fold-overlay nil)
    (overlay-put entry-ov 'json-log-viewer-log-entry-id entry-id)
    (overlay-put entry-ov 'json-log-viewer-storage-buffer (current-buffer))
    (overlay-put entry-ov 'json-log-viewer-storage-entry-id entry-id)
    (unless storage-populated
      (json-log-viewer--storage-put-entry-details signature filter-text fields))
    (overlay-put entry-ov 'json-log-viewer-signature signature)
    (overlay-put entry-ov 'json-log-viewer-storage-signature signature)
    (push entry-ov json-log-viewer--entry-overlays)
    entry-ov))

(defun json-log-viewer--mark-seen-entries (entries)
  "Mark ENTRIES as seen in current buffer."
  (dolist (entry entries)
    (puthash (json-log-viewer--entry-signature entry) t json-log-viewer--seen-signatures)))

(defun json-log-viewer--unseen-entries (entries)
  "Return subset of ENTRIES not previously seen in current buffer."
  (cl-remove-if
   (lambda (entry)
     (gethash (json-log-viewer--entry-signature entry) json-log-viewer--seen-signatures))
   entries))

(defun json-log-viewer--delete-no-results-placeholder ()
  "Delete a `No results.` placeholder line when present."
  (save-excursion
    (goto-char (json-log-viewer--header-end-position))
    (when (looking-at "No results\\.\n")
      (delete-region (match-beginning 0) (match-end 0)))))

(defun json-log-viewer--evict-oldest-entries-if-needed ()
  "Evict oldest rendered entries when streaming retention cap is exceeded."
  (let ((max-entries json-log-viewer--stream-max-entries))
    (when (and json-log-viewer--streaming
               (integerp max-entries)
               (> max-entries 0)
               (> json-log-viewer--entry-count max-entries))
      (let* ((drop (min json-log-viewer--entry-count
                        (json-log-viewer--stream-chunk-size)))
             (keep (- json-log-viewer--entry-count drop))
             kept
             victims
             (victim-folds nil))
        ;; Avoid `cl-subseq` on long lists to prevent deep recursive list copying.
        (let ((idx 0))
          (dolist (entry-overlay json-log-viewer--entry-overlays)
            (if (< idx keep)
                (push entry-overlay kept)
              (push entry-overlay victims))
            (setq idx (1+ idx))))
        (setq kept (nreverse kept))
        (setq victims (nreverse victims))
        (setq json-log-viewer--entry-overlays kept)
        (setq json-log-viewer--entry-count keep)
        (dolist (entry-overlay victims)
          (let ((fold-ov (overlay-get entry-overlay 'json-log-viewer-fold-overlay))
                (sig (overlay-get entry-overlay 'json-log-viewer-signature)))
            (when (overlayp fold-ov)
              (push fold-ov victim-folds))
            (when sig
              (remhash sig json-log-viewer--seen-signatures))
            (when (and (overlay-buffer entry-overlay)
                       (overlay-start entry-overlay)
                       (overlay-end entry-overlay))
              (delete-region (overlay-start entry-overlay)
                             (overlay-end entry-overlay)))
            (when (overlay-buffer entry-overlay)
              (delete-overlay entry-overlay))
            (when (overlayp fold-ov)
              (delete-overlay fold-ov))))
        (setq json-log-viewer--fold-overlays
              (cl-remove-if (lambda (ov) (memq ov victim-folds))
                            json-log-viewer--fold-overlays))
        (json-log-viewer--trunkator-async-submit
         (json-log-viewer--make-trunkator-async-job drop))))))

(defun json-log-viewer-replace-entries (entries &optional preserve-filter storage-prepared)
  "Replace rendered entries with ENTRIES.

When PRESERVE-FILTER is non-nil, keep the current active filter.
When STORAGE-PREPARED is non-nil, skip resetting sqlite entry-details."
  (let ((active-filter (and preserve-filter json-log-viewer--filter-string))
        (inhibit-read-only t)
        (ordered (json-log-viewer--sort-entries entries))
        (inserted-overlays nil))
    (setq json-log-viewer--filter-string active-filter)
    (json-log-viewer--clear-overlays)
    (unless storage-prepared
      (json-log-viewer--storage-reset-entry-details))
    (setq json-log-viewer--seen-signatures (make-hash-table :test 'equal))
    (erase-buffer)
    (if (null ordered)
        (insert "No results.\n")
      (dolist (entry ordered)
        (push (json-log-viewer--insert-entry entry) inserted-overlays)))
    (setq inserted-overlays (nreverse inserted-overlays))
    (setq json-log-viewer--entry-count (length ordered))
    (json-log-viewer--mark-seen-entries ordered)
    (json-log-viewer--apply-filter)
    (json-log-viewer--refresh-header)
    (if json-log-viewer--auto-follow
        (json-log-viewer--set-point-to-latest-entry)
      (goto-char (point-min)))
    (json-log-viewer--highlight-current-line)
    (json-log-viewer--publish 'replace inserted-overlays)))

(defun json-log-viewer-append-entries (entries)
  "Append ENTRIES into current viewer buffer.

In streaming mode new entries are always appended to the bottom.
In non-streaming mode the append position follows configured direction."
  (let* ((skip-sort (and json-log-viewer--streaming
                         json-log-viewer--stream-assume-ordered))
         (candidate-entries (if skip-sort
                                entries
                              (json-log-viewer--unseen-entries entries)))
         (ordered (if skip-sort
                      candidate-entries
                    (json-log-viewer--sort-entries candidate-entries)))
         (append-at-bottom (or json-log-viewer--streaming
                               (eq json-log-viewer--direction 'oldest-first)))
         (inhibit-read-only t)
         (inserted-overlays nil))
    (when ordered
      (save-excursion
        (json-log-viewer--delete-no-results-placeholder)
        (if append-at-bottom
            (goto-char (point-max))
          (goto-char (json-log-viewer--header-end-position)))
        (dolist (entry ordered)
          (push (json-log-viewer--insert-entry entry) inserted-overlays)))
      (setq inserted-overlays (nreverse inserted-overlays))
      (setq json-log-viewer--entry-count
            (+ json-log-viewer--entry-count (length ordered)))
      (json-log-viewer--mark-seen-entries ordered)
      (json-log-viewer--evict-oldest-entries-if-needed)
      (json-log-viewer--apply-filter-to-overlays inserted-overlays)
      (json-log-viewer--refresh-header)
      (when (and json-log-viewer--auto-follow append-at-bottom)
        (json-log-viewer--set-point-to-latest-entry))
      (json-log-viewer--highlight-current-line)
      (json-log-viewer--publish 'append inserted-overlays))
    ordered))

(defun json-log-viewer-refresh ()
  "Refresh current viewer buffer via configured refresh callback."
  (interactive)
  (unless json-log-viewer--refresh-function
    (user-error "No refresh callback configured for this buffer"))
  (let* ((result (funcall json-log-viewer--refresh-function (json-log-viewer--state)))
         (entries (or (plist-get result :entries) nil))
         (replace (if (plist-member result :replace)
                      (plist-get result :replace)
                    (not json-log-viewer--streaming)))
         (message-text (plist-get result :message)))
    (when (plist-member result :context)
      (setq json-log-viewer--context (plist-get result :context)))
    (when (plist-member result :metadata)
      (setq json-log-viewer--metadata (plist-get result :metadata)))
    (if replace
        (json-log-viewer-replace-entries entries t)
      (json-log-viewer-append-entries entries))
    (json-log-viewer--refresh-header)
    (when message-text
      (message "%s" message-text))))

(defvar-keymap json-log-viewer-mode-map
  :doc "Keymap for `json-log-viewer-mode'."
  "TAB" #'json-log-viewer-toggle-entry
  "<tab>" #'json-log-viewer-toggle-entry
  "<backtab>" #'json-log-viewer-toggle-all
  "?" #'json-log-viewer-show-info
  "C-c C-r" #'json-log-viewer-refresh
  "C-c C-n" #'json-log-viewer-narrow
  "C-c C-f" #'json-log-viewer-toggle-auto-follow
  "C-c C-w" #'json-log-viewer-widen)

(define-derived-mode json-log-viewer-mode special-mode "JsonLogs"
  "Major mode for foldable JSON log entries."
  :group 'json-log-viewer
  (buffer-disable-undo)
  (setq-local truncate-lines t)
  (setq-local line-move-ignore-invisible t)
  (setq-local buffer-invisibility-spec '(t))
  (add-to-invisibility-spec 'json-log-viewer-filter)
  (add-hook 'kill-buffer-hook #'json-log-viewer--cleanup-storage-on-kill nil t)
  (add-hook 'pre-command-hook #'json-log-viewer--remember-point-before-command nil t)
  (add-hook 'post-command-hook #'json-log-viewer--maybe-disable-auto-follow-after-command nil t)
  (add-hook 'post-command-hook #'json-log-viewer--highlight-current-line t t))

(defun json-log-viewer--maybe-load-evil-bindings ()
  "Conditionally load and initialize optional Evil bindings."
  (when (and json-log-viewer-enable-evil-bindings
             (featurep 'evil)
             (require 'json-log-viewer-evil nil t)
             (fboundp 'json-log-viewer-setup-evil))
    (json-log-viewer-setup-evil)))

(with-eval-after-load 'evil
  (json-log-viewer--maybe-load-evil-bindings))

(when (featurep 'evil)
  (json-log-viewer--maybe-load-evil-bindings))

(cl-defun json-log-viewer-make-buffer (buffer-name
                                       &key
                                       log-lines
                                       timestamp-path
                                       level-path
                                       message-path
                                       extra-paths
                                       json-paths
                                       refresh-function
                                       streaming
                                       (max-entries json-log-viewer-stream-max-entries)
                                       (direction 'newest-first)
                                       header-lines-function)
  "Create BUFFER-NAME for JSON LOG-LINES.

Summary rendering is configured with explicit JSON paths.

LOG-LINES is a list of JSON strings.

TIMESTAMP-PATH, LEVEL-PATH, MESSAGE-PATH are dot-separated JSON paths used for
summary rendering. EXTRA-PATHS is a list of additional paths rendered as
bracketed segments. JSON-PATHS is a list of paths rendered as pretty JSON
blocks in entry details instead of flattened subfields.

REFRESH-FUNCTION, when non-nil, is called with the current old list of raw log
lines and must return the new full list of raw log lines.

STREAMING controls append semantics:
- non-nil: new data is appended to the bottom (use `json-log-viewer-push`)
- nil: ordering follows DIRECTION
  (`newest-first`/`oldest-first` or `desc`/`asc`)

MAX-ENTRIES caps retained rows in streaming mode. Nil disables capping.

Returns the created buffer."
  (unless (stringp buffer-name)
    (user-error "json-log-viewer-make-buffer requires BUFFER-NAME to be a string"))
  (let* ((normalized-extra-paths (json-log-viewer--normalize-path-list
                                  extra-paths
                                  "json-log-viewer-make-buffer :extra-paths"))
         (normalized-json-paths (json-log-viewer--normalize-path-list
                                 json-paths
                                 "json-log-viewer-make-buffer :json-paths"))
         (normalized-lines (json-log-viewer--ensure-log-lines
                            log-lines "json-log-viewer-make-buffer :log-lines"))
         (normalized-direction (json-log-viewer--normalize-direction direction))
         (target (get-buffer-create buffer-name)))
    (with-current-buffer target
      ;; Reinitializing an existing viewer buffer can lose old queue handles if
      ;; mode setup resets locals first. Stop/close previous resources upfront.
      (json-log-viewer--stop-async-queue)
      (json-log-viewer--storage-close)
      (json-log-viewer-mode)
      (setq-local json-log-viewer--entry-fields-function #'json-log-viewer--json-entry-fields)
      (setq-local json-log-viewer--summary-function #'json-log-viewer--json-summary)
      (setq-local json-log-viewer--refresh-function
                  (when (functionp refresh-function)
                    #'json-log-viewer--json-refresh))
      (setq-local json-log-viewer--header-function #'json-log-viewer--json-header-lines)
      (setq-local json-log-viewer--signature-function #'json-log-viewer--json-entry-signature)
      (setq-local json-log-viewer--sort-key-function #'json-log-viewer--json-entry-sort-key)
      (setq-local json-log-viewer--streaming streaming)
      (setq-local json-log-viewer--direction normalized-direction)
      (setq-local json-log-viewer--context nil)
      (setq-local json-log-viewer--metadata nil)
      (setq-local json-log-viewer--narrow-rebuild-in-progress nil)
      (setq-local json-log-viewer--entry-count 0)
      (setq-local json-log-viewer--stream-assume-ordered streaming)
      (setq-local json-log-viewer--stream-max-entries max-entries)
      (setq-local json-log-viewer--next-entry-id 0)
      (setq-local json-log-viewer--timestamp-path timestamp-path)
      (setq-local json-log-viewer--level-path level-path)
      (setq-local json-log-viewer--message-path message-path)
      (setq-local json-log-viewer--extra-paths normalized-extra-paths)
      (setq-local json-log-viewer--json-paths normalized-json-paths)
      (setq-local json-log-viewer--json-refresh-log-lines-function refresh-function)
      (setq-local json-log-viewer--json-header-lines-function header-lines-function)
      (setq-local json-log-viewer--seen-signatures (make-hash-table :test 'equal))
      (setq-local json-log-viewer--subscribers nil)
      (json-log-viewer-subscribe json-log-viewer--self-subscriber-id
                                 #'json-log-viewer--self-subscriber)
      (json-log-viewer--storage-init)
      (json-log-viewer--start-async-queue)
      (json-log-viewer--ensure-async-queue-running)
      (json-log-viewer-replace-entries nil nil t)
      (when normalized-lines
        (json-log-viewer-replace-log-lines target normalized-lines nil))
    target)))

(defun json-log-viewer-push (buffer-or-name log-lines)
  "Push LOG-LINES into BUFFER-OR-NAME for streaming updates.

BUFFER-OR-NAME must identify a live `json-log-viewer-mode` buffer created by
`json-log-viewer-make-buffer` with `:streaming` non-nil."
  (let ((target (json-log-viewer-get-buffer buffer-or-name)))
    (with-current-buffer target
      (unless json-log-viewer--streaming
        (user-error "json-log-viewer-push requires a buffer with streaming mode enabled"))
      (let ((normalized-lines (json-log-viewer--ensure-log-lines
                               log-lines "json-log-viewer-push")))
        (json-log-viewer--ensure-async-queue-running)
        (dolist (line normalized-lines)
          (json-log-viewer--async-submit
           (json-log-viewer--make-log-ingestor-async-job 'ingest line)))))))

(defun json-log-viewer-current-log-lines (buffer-or-name)
  "Return a copy of current raw log lines from BUFFER-OR-NAME."
  (let ((target (json-log-viewer-get-buffer buffer-or-name)))
    (with-current-buffer target
      (json-log-viewer--raw-lines-list))))

(defun json-log-viewer-replace-log-lines (buffer-or-name log-lines &optional preserve-filter)
  "Replace raw LOG-LINES in BUFFER-OR-NAME.

When PRESERVE-FILTER is non-nil, keep the current active filter."
  (let ((target (json-log-viewer-get-buffer buffer-or-name)))
    (with-current-buffer target
      (let ((normalized-lines (json-log-viewer--ensure-log-lines
                               log-lines "json-log-viewer-replace-log-lines")))
        (json-log-viewer--ensure-async-queue-running)
        (json-log-viewer--async-submit
         (json-log-viewer--make-log-ingestor-async-job 'reset nil)
         t)
        (json-log-viewer-replace-entries nil preserve-filter t)
        (dolist (line normalized-lines)
          (json-log-viewer--async-submit
           (json-log-viewer--make-log-ingestor-async-job 'ingest line)))))))

(provide 'json-log-viewer)
;;; json-log-viewer.el ends here
