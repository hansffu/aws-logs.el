;;; kafka-logs.el --- Kafka logs transient UI -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Transient-driven Kafka logs viewer built on top of json-log-viewer.
;; Supports:
;; - Preconfigured Kafka connections (`kafka-logs-make-connection`)
;; - Topic selection from cluster metadata
;; - Streaming new messages
;; - Time-span lookups
;; - Regex filtering
;; - auth-source based credential resolution
;;
;;; Code:

(require 'auth-source)
(require 'cl-lib)
(require 'json)
(require 'subr-x)
(require 'transient)
(require 'url)
(require 'url-parse)
(require 'url-util)

(require 'composite-log-viewer)
(require 'json-log-viewer)

(declare-function json-log-viewer-make-buffer "json-log-viewer"
                  (buffer-name &rest args))
(declare-function json-log-viewer-ingest-wrapper-executable "json-log-viewer" ())
(declare-function json-log-viewer-worker-socket-path "json-log-viewer"
                  (&optional buffer-or-name))
(declare-function json-log-viewer-run-when-ready "json-log-viewer"
                  (buffer-or-name function))
(declare-function json-log-viewer-composite-buffer-p "composite-log-viewer"
                  (&optional buffer-or-name))
(declare-function json-log-viewer-register-source-config "json-log-viewer"
                  (buffer-or-name source &rest args))
(declare-function json-log-viewer-push "json-log-viewer"
                  (buffer-or-name log-lines))
(declare-function json-log-viewer-replace-log-lines "json-log-viewer"
                  (buffer-or-name log-lines &optional preserve-filter))
(declare-function org-read-date "org"
                  (&optional with-time to-time from-string prompt default-time default-input))

(define-derived-mode kafka-logs-viewer-mode json-log-viewer-mode "Kafka-Logs"
  "Major mode for Kafka log buffers rendered with `json-log-viewer`."
  :group 'kafka-logs)

(defgroup kafka-logs nil
  "Kafka logs transient UI and rendering."
  :group 'tools)

(defcustom kafka-logs-kcat "kcat"
  "kcat executable used by kafka-logs."
  :type 'string
  :group 'kafka-logs)

(defcustom kafka-logs-default-connection nil
  "Default named connection for new Emacs sessions."
  :type '(choice (const :tag "None" nil) string)
  :group 'kafka-logs)

(defcustom kafka-logs-default-topic nil
  "Default Kafka topic for new Emacs sessions."
  :type '(choice (const :tag "None" nil) string)
  :group 'kafka-logs)

(defcustom kafka-logs-default-stream t
  "Default mode for new Emacs sessions.

When non-nil, start in stream mode (new messages).
When nil, start in time-span lookup mode."
  :type 'boolean
  :group 'kafka-logs)

(defcustom kafka-logs-default-since nil
  "Default relative lookup range for new Emacs sessions.

Values use the same format as aws-logs, e.g. 10m, 2h, 1d, 30s, 1w."
  :type '(choice (const :tag "None" nil) string)
  :group 'kafka-logs)

(defcustom kafka-logs-default-time-range nil
  "Default explicit lookup range for new Emacs sessions.

Value is nil or a cons cell (FROM . TO), where each value is a date-time
string parseable by Emacs `date-to-time` or an epoch millisecond string."
  :type '(choice (const :tag "None" nil) (cons :tag "From/To" string string))
  :group 'kafka-logs)

(defcustom kafka-logs-default-filter nil
  "Default regex filter for kcat output.

When non-nil, output is piped through grep with this regex."
  :type '(choice (const :tag "No filter" nil) string)
  :group 'kafka-logs)

(defcustom kafka-logs-default-max-messages nil
  "Default maximum message count in time-span mode.

When nil, kafka-logs does not set `-c` for kcat."
  :type '(choice (const :tag "No limit" nil) integer)
  :group 'kafka-logs)

(defcustom kafka-logs-default-value-format 'auto
  "Default Kafka value wire format for new Emacs sessions.

When set to `auto`, kafka-logs checks Schema Registry for the selected topic
and uses Avro when a conventional `<topic>-value` Avro subject exists.
When set to `avro`, kafka-logs asks kcat to deserialize values with Schema
Registry.  `json` and `string` keep kcat's raw payload and differ only in the
default payload rendering choice."
  :type '(choice (const :tag "Auto" auto)
                 (const :tag "JSON" json)
                 (const :tag "Avro" avro)
                 (const :tag "String/raw" string))
  :group 'kafka-logs)

(defcustom kafka-logs-default-payload-format 'json
  "Default payload rendering format for new Emacs sessions.

When set to `json`, kafka-logs attempts to parse string payloads as JSON and
stores parsed objects in the viewer `payload` field.
When nil, keep payloads as-is."
  :type '(choice (const :tag "As string (default)" nil)
                 (const :tag "JSON" json))
  :group 'kafka-logs)

(defcustom kafka-logs-default-json-paths nil
  "Default JSON detail paths rendered as formatted blocks.

Values are dot-separated paths understood by json-log-viewer, for example
`payload` or `payload.log`."
  :type '(repeat string)
  :group 'kafka-logs)

(defcustom kafka-logs-default-extra-paths
  '("topic" "key" "partition")
  "Default JSON paths rendered as summary extra segments."
  :type '(repeat string)
  :group 'kafka-logs)

(defcustom kafka-logs-default-message-path "message"
  "Default JSON path used for summary message rendering.

Examples:
- `message`
- `payload`
- `payload.data.name`

When the resolved value is a JSON object/array, it is rendered on one line."
  :type 'string
  :group 'kafka-logs)

(defcustom kafka-logs-stream-drain-interval 0.05
  "Seconds between stream queue drain ticks.

Lower values reduce display latency at the cost of more frequent UI work."
  :type 'number
  :group 'kafka-logs)

(defcustom kafka-logs-stream-max-lines-per-batch 250
  "Maximum streamed lines rendered per drain tick."
  :type 'integer
  :group 'kafka-logs)

(defvar kafka-logs-connection kafka-logs-default-connection
  "Selected Kafka connection name for this Emacs session.")

(defvar kafka-logs-topic kafka-logs-default-topic
  "Selected Kafka topic for this Emacs session.")

(defvar kafka-logs-stream kafka-logs-default-stream
  "Non-nil means stream new Kafka messages.")

(defvar kafka-logs-since kafka-logs-default-since
  "Selected relative time range (e.g. 10m) for this Emacs session.")

(defvar kafka-logs-time-range kafka-logs-default-time-range
  "Selected explicit FROM/TO time range for this Emacs session.

Value is nil or (FROM . TO), where both are date-time strings.")

(defvar kafka-logs-filter kafka-logs-default-filter
  "Regex filter for kcat output in this Emacs session, or nil.")

(defvar kafka-logs-max-messages kafka-logs-default-max-messages
  "Maximum message count for this Emacs session in time-span mode, or nil.")

(defvar kafka-logs-value-format kafka-logs-default-value-format
  "Kafka value wire format for this Emacs session.

Allowed values are `auto`, `json`, `avro`, and `string`.")

(defvar kafka-logs--detected-value-format nil
  "Detected Kafka value wire format for the selected topic, or nil.")

(defvar kafka-logs-payload-format kafka-logs-default-payload-format
  "Payload rendering format for this Emacs session, or nil.")

(defvar kafka-logs-json-paths (append kafka-logs-default-json-paths nil)
  "JSON detail paths rendered as formatted blocks in this Emacs session.")

(defvar kafka-logs-extra-paths (append kafka-logs-default-extra-paths nil)
  "JSON paths rendered as summary extra segments in this Emacs session.")

(defvar kafka-logs-message-path kafka-logs-default-message-path
  "JSON path used for summary message rendering in this Emacs session.")

(defvar kafka-logs-viewer-buffer nil
  "Selected json-log-viewer buffer for Kafka ingestion, or nil.

When nil, kafka-logs creates its normal dedicated viewer buffer.")

(defvar kafka-logs-connections nil
  "Alist of named Kafka connections.

Each element has the form (NAME . PLIST).")

(defvar-local kafka-logs--process nil
  "Process associated with current kafka-logs viewer buffer.")

(defvar-local kafka-logs--pending-fragment ""
  "Trailing incomplete process output fragment for streaming buffers.")

(defvar-local kafka-logs--once-output-buffer nil
  "Temporary process output buffer for one-shot asynchronous fetches.")

(defvar-local kafka-logs--stream-chunks-in nil
  "LIFO queue of pending stream output chunks waiting for reversal.")

(defvar-local kafka-logs--stream-chunks-out nil
  "FIFO queue of pending stream output chunks ready for draining.")

(defvar-local kafka-logs--stream-pending-lines nil
  "Parsed full lines waiting to be converted and rendered.")

(defvar-local kafka-logs--stream-drain-timer nil
  "Per-buffer timer used to drain queued stream data incrementally.")

(defvar-local kafka-logs--viewer-connection nil
  "Connection name shown in current viewer buffer header.")

(defvar-local kafka-logs--viewer-topic nil
  "Topic shown in current viewer buffer header.")

(defvar-local kafka-logs--viewer-stream nil
  "Stream state shown in current viewer buffer header.")

(defvar-local kafka-logs--viewer-time-range nil
  "Time range shown in current viewer buffer header.")

(defvar-local kafka-logs--viewer-filter nil
  "Filter shown in current viewer buffer header.")

(defvar-local kafka-logs--viewer-payload-format nil
  "Payload format shown in current viewer buffer header.")

(defvar-local kafka-logs--viewer-value-format nil
  "Selected value wire format shown in current viewer buffer header.")

(defvar-local kafka-logs--viewer-detected-value-format nil
  "Detected value wire format shown in current viewer buffer header.")

(defvar-local kafka-logs--viewer-json-paths nil
  "JSON detail paths shown in current viewer buffer header.")

(defvar-local kafka-logs--viewer-message-path nil
  "Message path shown in current viewer buffer header.")

(defconst kafka-logs--connection-keys
  '(:brokers :security-protocol :sasl-mechanisms :username :password
    :auth-source :properties :kcat-args :description
    :schema-registry-url :schema-registry-username
    :schema-registry-password :schema-registry-auth-source)
  "Allowed keys for `kafka-logs-make-connection`.")

(defconst kafka-logs--avro-envelope-format
  "{\"topic\":\"%t\",\"partition\":%p,\"offset\":%o,\"ts\":%T,\"key_size\":%K,\"key\":\"%k\",\"payload\":%s}\\n"
  "kcat format string used to wrap Avro-decoded payloads as JSON lines.")

(defun kafka-logs--transient-reprompt ()
  "Refresh transient so UI reflects current backing fields."
  (transient-quit-one)
  (transient-setup 'kafka-logs-transient))

(defun kafka-logs--normalize-json-paths (paths &optional source)
  "Validate and normalize JSON PATHS.

SOURCE is an optional user-facing origin label."
  (unless (and (listp paths) (cl-every #'stringp paths))
    (user-error "%s must be a list of strings, got: %S"
                (or source "JSON paths")
                paths))
  (let ((seen (make-hash-table :test 'equal))
        normalized)
    (dolist (path paths)
      (let ((trimmed (string-trim path)))
        (unless (or (string-empty-p trimmed)
                    (gethash trimmed seen))
          (puthash trimmed t seen)
          (push trimmed normalized))))
    (nreverse normalized)))

(defun kafka-logs--normalize-message-path (path &optional source)
  "Validate and normalize message PATH from SOURCE."
  (unless (stringp path)
    (user-error "%s must be a string, got: %S"
                (or source "Message path")
                path))
  (let ((trimmed (string-trim path)))
    (when (string-empty-p trimmed)
      (user-error "%s cannot be empty" (or source "Message path")))
    trimmed))

(defun kafka-logs--normalize-extra-paths (paths &optional source)
  "Validate and normalize summary extra PATHS from SOURCE."
  (kafka-logs--normalize-json-paths
   paths
   (or source "Extra paths")))

(defun kafka-logs--json-paths-display (paths)
  "Return one-line display label for JSON PATHS."
  (if (and paths (> (length paths) 0))
      (string-join paths ",")
    "none"))

(defun kafka-logs--normalize-value-format (value &optional source)
  "Validate and normalize wire format VALUE from SOURCE."
  (let ((format (or value 'string)))
    (unless (memq format '(auto json avro string))
      (user-error "%s must be one of auto, json, avro, or string; got: %S"
                  (or source "Value format")
                  value))
    format))

(defun kafka-logs--value-format-display (value detected)
  "Return display label for selected VALUE and DETECTED format."
  (let ((format (kafka-logs--normalize-value-format value)))
    (if (eq format 'auto)
        (format "auto -> %s" (or detected "json"))
      (symbol-name format))))

(defun kafka-logs--normalize-brokers (brokers)
  "Normalize BROKERS to a comma-separated string."
  (cond
   ((stringp brokers) (string-trim brokers))
   ((and (listp brokers) (cl-every #'stringp brokers))
    (string-join brokers ","))
   (t
    (user-error "Connection :brokers must be a string or string list"))))

(defun kafka-logs--first-broker-host-port (brokers)
  "Return (HOST . PORT) for first entry in BROKERS string."
  (let* ((first (car (split-string brokers "," t "[[:space:]]*")))
         (host nil)
         (port nil))
    (cond
     ((and first
           (string-match "\\`\\[\\([^]]+\\)\\]\\(?::\\([0-9]+\\)\\)?\\'" first))
      (setq host (match-string 1 first))
      (setq port (match-string 2 first)))
     ((and first
           (string-match "\\`\\([^:]+\\):\\([0-9]+\\)\\'" first))
      (setq host (match-string 1 first))
      (setq port (match-string 2 first)))
     (t
      (setq host first)))
    (cons host port)))

(defun kafka-logs--connection-plist-valid-p (plist)
  "Return non-nil if PLIST is valid for `kafka-logs-make-connection`."
  (let ((cursor plist))
    (while cursor
      (let ((key (car cursor)))
        (unless (keywordp key)
          (user-error "Connection key must be a keyword, got: %S" key))
        (unless (memq key kafka-logs--connection-keys)
          (user-error "Unsupported connection key: %S" key)))
      (setq cursor (cddr cursor))))
  t)

(defun kafka-logs-make-connection (name &rest options)
  "Create or replace named Kafka connection NAME with OPTIONS plist.

Supported keys:
- `:brokers` (required): string or list of broker endpoints.
- `:security-protocol`: value for librdkafka `security.protocol`.
- `:sasl-mechanisms`: value for librdkafka `sasl.mechanisms`.
- `:username`: SASL username (optional).
- `:password`: SASL password (optional).
- `:auth-source`: nil, t, or plist used with `auth-source-search`.
  When non-nil, credentials are resolved from auth-source.
- `:properties`: list of additional librdkafka properties.
  Elements may be strings like \"key=value\" or cons cells (KEY . VALUE).
- `:kcat-args`: extra string args appended to all kcat commands.
- `:schema-registry-url`: Schema Registry URL for Avro detection/decoding.
- `:schema-registry-username`: Schema Registry username (optional).
- `:schema-registry-password`: Schema Registry password (optional).
- `:schema-registry-auth-source`: nil, t, or plist used with
  `auth-source-search` for Schema Registry credentials.
- `:description`: optional UI description string."
  (let ((connection-name (if (symbolp name) (symbol-name name) name)))
    (unless (stringp connection-name)
      (user-error "Connection name must be a string or symbol, got: %S" name))
    (unless (zerop (% (length options) 2))
      (user-error "Connection options must be key/value pairs"))
    (kafka-logs--connection-plist-valid-p options)
    (unless (plist-member options :brokers)
      (user-error "Connection %s is missing required :brokers" connection-name))
    (kafka-logs--normalize-brokers (plist-get options :brokers))
    (setq kafka-logs-connections
          (assoc-delete-all connection-name kafka-logs-connections))
    (push (cons connection-name options) kafka-logs-connections)
    (unless kafka-logs-connection
      (setq kafka-logs-connection connection-name))
    (car kafka-logs-connections)))

(defun kafka-logs--connection-names ()
  "Return list of known connection names."
  (mapcar #'car kafka-logs-connections))

(defun kafka-logs--connection-plist (&optional connection-name)
  "Return connection plist for CONNECTION-NAME or current selection."
  (let* ((name (or connection-name kafka-logs-connection))
         (entry (and name (assoc name kafka-logs-connections))))
    (unless name
      (user-error "Select a connection first"))
    (unless entry
      (user-error "Connection not found: %s" name))
    (cdr entry)))

(defun kafka-logs--normalize-properties (properties)
  "Normalize connection PROPERTIES into a list of \"key=value\" strings."
  (let ((result nil))
    (dolist (item properties)
      (cond
       ((stringp item)
        (unless (string-match-p "=" item)
          (user-error "Property string must be key=value, got: %S" item))
        (push item result))
       ((consp item)
        (let ((key (format "%s" (car item)))
              (value (cdr item)))
          (push (format "%s=%s" key value) result)))
       (t
        (user-error "Unsupported property entry: %S" item))))
    (nreverse result)))

(defun kafka-logs--auth-query (connection brokers)
  "Return auth-source query plist for CONNECTION using BROKERS."
  (let ((spec (plist-get connection :auth-source)))
    (when spec
      (let* ((host+port (kafka-logs--first-broker-host-port brokers))
             (default-host (car host+port))
             (default-port (cdr host+port))
             (query (list :max 1 :require '(:secret))))
        (when default-host
          (setq query (plist-put query :host default-host)))
        (when default-port
          (setq query (plist-put query :port default-port)))
        (when (plist-member connection :username)
          (setq query (plist-put query :user (plist-get connection :username))))
        (when (listp spec)
          (let ((cursor spec))
            (while cursor
              (setq query (plist-put query (car cursor) (cadr cursor)))
              (setq cursor (cddr cursor)))))
        query))))

(defun kafka-logs--auth-entry (connection brokers)
  "Resolve auth-source entry for CONNECTION using BROKERS."
  (when-let ((query (kafka-logs--auth-query connection brokers)))
    (car (apply #'auth-source-search query))))

(defun kafka-logs--auth-secret (entry)
  "Return secret string from auth-source ENTRY."
  (let ((secret (plist-get entry :secret)))
    (cond
     ((functionp secret) (funcall secret))
     ((stringp secret) secret)
     (t nil))))

(defun kafka-logs--connection-credentials (connection brokers)
  "Return (USERNAME PASSWORD) for CONNECTION using BROKERS."
  (let* ((entry (kafka-logs--auth-entry connection brokers))
         (username (or (plist-get connection :username)
                       (plist-get entry :user)))
         (password (or (plist-get connection :password)
                       (kafka-logs--auth-secret entry))))
    (list username password)))

(defun kafka-logs--schema-registry-url (&optional connection)
  "Return Schema Registry URL for CONNECTION or current connection."
  (when-let ((url (plist-get (or connection (kafka-logs--connection-plist))
                             :schema-registry-url)))
    (let ((trimmed (string-trim url)))
      (unless (string-empty-p trimmed)
        trimmed))))

(defun kafka-logs--url-host-port (url)
  "Return (HOST . PORT) parsed from URL."
  (let* ((parsed (url-generic-parse-url url))
         (port (when (string-match
                      "\\`[[:alpha:]][[:alnum:]+.-]*://\\(?:[^/@]+@\\)?\\(?:\\[[^]]+\\]\\|[^/:?#]+\\):\\([0-9]+\\)"
                      url)
                 (match-string 1 url))))
    (cons (url-host parsed)
          port)))

(defun kafka-logs--schema-registry-auth-query (connection registry-url)
  "Return auth-source query plist for CONNECTION using REGISTRY-URL."
  (let ((spec (plist-get connection :schema-registry-auth-source)))
    (when spec
      (let* ((host+port (kafka-logs--url-host-port registry-url))
             (default-host (car host+port))
             (default-port (cdr host+port))
             (query (list :max 1 :require '(:secret))))
        (when default-host
          (setq query (plist-put query :host default-host)))
        (when default-port
          (setq query (plist-put query :port default-port)))
        (when (plist-member connection :schema-registry-username)
          (setq query (plist-put query :user
                                 (plist-get connection
                                            :schema-registry-username))))
        (when (listp spec)
          (let ((cursor spec))
            (while cursor
              (setq query (plist-put query (car cursor) (cadr cursor)))
              (setq cursor (cddr cursor)))))
        query))))

(defun kafka-logs--schema-registry-auth-entry (connection registry-url)
  "Resolve auth-source entry for CONNECTION using REGISTRY-URL."
  (when-let ((query (kafka-logs--schema-registry-auth-query connection
                                                            registry-url)))
    (or (car (apply #'auth-source-search query))
        (when (plist-member query :port)
          (let ((fallback-query (cl-copy-list query)))
            (cl-remf fallback-query :port)
            (car (apply #'auth-source-search fallback-query)))))))

(defun kafka-logs--schema-registry-auth-configured-p (connection)
  "Return non-nil when CONNECTION has Schema Registry auth settings."
  (or (plist-member connection :schema-registry-auth-source)
      (plist-member connection :schema-registry-username)
      (plist-member connection :schema-registry-password)))

(defun kafka-logs--schema-registry-credentials (connection registry-url)
  "Return (USERNAME PASSWORD) for CONNECTION using REGISTRY-URL."
  (let* ((entry (kafka-logs--schema-registry-auth-entry connection registry-url))
         (username (or (plist-get connection :schema-registry-username)
                       (plist-get entry :user)))
         (password (or (plist-get connection :schema-registry-password)
                       (kafka-logs--auth-secret entry))))
    (when (and (kafka-logs--schema-registry-auth-configured-p connection)
               (not (and username password)))
      (user-error
       "Schema Registry credentials not found for %s; check :schema-registry-auth-source or authinfo"
       registry-url))
    (list username password)))

(defun kafka-logs--schema-registry-basic-auth-header (connection registry-url)
  "Return Basic Auth header value for CONNECTION and REGISTRY-URL, or nil."
  (pcase-let ((`(,username ,password)
               (kafka-logs--schema-registry-credentials connection registry-url)))
    (when (and username password)
      (concat "Basic "
              (base64-encode-string (format "%s:%s" username password) t)))))

(defun kafka-logs--schema-registry-kcat-url (&optional connection)
  "Return Schema Registry URL for kcat, including credentials when configured."
  (let* ((conn (or connection (kafka-logs--connection-plist)))
         (registry-url (kafka-logs--schema-registry-url conn)))
    (when registry-url
      (pcase-let ((`(,username ,password)
                   (kafka-logs--schema-registry-credentials conn registry-url)))
        (if (and username password)
            (let ((parsed (url-generic-parse-url registry-url)))
              ;; libserdes-backed kcat builds commonly expect raw URL userinfo
              ;; here; percent-encoded Schema Registry secrets can be sent
              ;; literally and cause 401s.
              (setf (url-user parsed) username)
              (setf (url-password parsed) password)
              (url-recreate-url parsed))
          registry-url)))))

(defun kafka-logs--connection-base-args ()
  "Build base kcat args from current selected connection."
  (let* ((connection (kafka-logs--connection-plist))
         (brokers (kafka-logs--normalize-brokers (plist-get connection :brokers)))
         (credentials (kafka-logs--connection-credentials connection brokers))
         (username (car credentials))
         (password (cadr credentials))
         (properties
          (append
           (when-let ((value (plist-get connection :security-protocol)))
             (list (format "security.protocol=%s" value)))
           (when-let ((value (plist-get connection :sasl-mechanisms)))
             (list (format "sasl.mechanisms=%s" value)))
           (when username
             (list (format "sasl.username=%s" username)))
           (when password
             (list (format "sasl.password=%s" password)))
           (kafka-logs--normalize-properties (plist-get connection :properties))))
         (kcat-args (plist-get connection :kcat-args)))
    (unless (and brokers (not (string-empty-p brokers)))
      (user-error "Connection %s has empty :brokers" kafka-logs-connection))
    (when (and kcat-args
               (not (and (listp kcat-args) (cl-every #'stringp kcat-args))))
      (user-error "Connection :kcat-args must be a list of strings"))
    (append
     (list "-b" brokers)
     (cl-mapcan (lambda (prop) (list "-X" prop)) properties)
     kcat-args)))

(defun kafka-logs--run-kcat-lines (args)
  "Run kcat with ARGS and return output lines.

Signals `user-error` on failure."
  (with-temp-buffer
    (let* ((exit-code (apply #'call-process kafka-logs-kcat nil t nil args))
           (output (string-trim-right (buffer-string))))
      (unless (zerop exit-code)
        (user-error "kcat failed (%s): %s"
                    exit-code
                    (if (string-empty-p output) "no output" output)))
      (split-string output "\n" t))))

(defun kafka-logs--alist-get-any (node key)
  "Return KEY from alist-like NODE using string/symbol lookup."
  (when (listp node)
    (or (alist-get key node nil nil #'equal)
        (when-let ((sym (intern-soft key)))
          (alist-get sym node)))))

(defun kafka-logs--parse-json-maybe (value)
  "Parse VALUE as JSON object/list when possible."
  (when (and (stringp value)
             (string-match-p "\\`[[:space:]\n\r\t]*[{\\[]" value))
    (condition-case nil
        (json-parse-string value :object-type 'alist :array-type 'list
                           :null-object nil :false-object :false)
      (error nil))))

(defun kafka-logs--alist-like-p (value)
  "Return non-nil when VALUE is an alist-like JSON object."
  (and (listp value)
       (or (null value)
           (let ((first (car value)))
             (and (consp first)
                  (or (stringp (car first))
                      (symbolp (car first))))))))

(defun kafka-logs--normalize-json-value (value)
  "Normalize VALUE into a shape `json-serialize' handles reliably."
  (cond
   ((hash-table-p value)
    (let ((normalized (make-hash-table :test 'equal)))
      (maphash
       (lambda (key child)
         (when-let ((name (kafka-logs--value->string key)))
           (puthash name
                    (kafka-logs--normalize-json-value child)
                    normalized)))
       value)
      normalized))
   ((kafka-logs--alist-like-p value)
    (let ((normalized (make-hash-table :test 'equal)))
      (dolist (pair value)
        (when (consp pair)
          (when-let ((name (kafka-logs--value->string (car pair))))
            (puthash name
                     (kafka-logs--normalize-json-value (cdr pair))
                     normalized))))
      normalized))
   ((vectorp value)
    (vconcat
     (mapcar #'kafka-logs--normalize-json-value
             (append value nil))))
   ((listp value)
    (vconcat
     (mapcar #'kafka-logs--normalize-json-value value)))
   (t value)))

(defun kafka-logs--list-topics ()
  "Return available topic names for current connection."
  (let* ((args (append (kafka-logs--connection-base-args) '("-L" "-J" "-q")))
         (lines (kafka-logs--run-kcat-lines args))
         (doc (kafka-logs--parse-json-maybe (string-join lines "\n")))
         (topics (kafka-logs--alist-get-any doc "topics"))
         (names nil))
    (unless topics
      (user-error "Unable to parse topic metadata from kcat"))
    (dolist (topic topics)
      (when-let ((name (kafka-logs--alist-get-any topic "topic")))
        (push name names)))
    (sort (delete-dups names) #'string-lessp)))

(defun kafka-logs--schema-registry-subject-path (subject)
  "Return Schema Registry latest-version path for SUBJECT."
  (format "/subjects/%s/versions/latest" (url-hexify-string subject)))

(defun kafka-logs--schema-registry-request-json (path)
  "Fetch Schema Registry PATH and return (STATUS . JSON).

JSON is parsed as an alist.  Signals `user-error` when no Schema Registry URL
is configured for the current connection."
  (let* ((connection (kafka-logs--connection-plist))
         (registry-url (kafka-logs--schema-registry-url connection)))
    (unless registry-url
      (user-error "Connection %s has no :schema-registry-url"
                  kafka-logs-connection))
    (let* ((base (string-remove-suffix "/" registry-url))
           (url-request-extra-headers
            (let ((auth (kafka-logs--schema-registry-basic-auth-header
                         connection registry-url)))
              (when auth
                `(("Authorization" . ,auth)))))
           (buffer (url-retrieve-synchronously
                    (concat base path) t t 5)))
      (unless buffer
        (user-error "Schema Registry request failed: %s%s" base path))
      (unwind-protect
          (with-current-buffer buffer
            (goto-char (point-min))
            (unless (re-search-forward
                     "\\`HTTP/[0-9.]+ \\([0-9]+\\)" nil t)
              (user-error "Unable to parse Schema Registry response"))
            (let ((status (string-to-number (match-string 1)))
                  (body nil))
              (goto-char (point-min))
              (when (re-search-forward "\r?\n\r?\n" nil t)
                (setq body (buffer-substring-no-properties (point) (point-max))))
              (cons status
                    (when (and body (not (string-empty-p (string-trim body))))
                      (json-parse-string body :object-type 'alist
                                         :array-type 'list
                                         :null-object nil
                                         :false-object :false)))))
        (kill-buffer buffer)))))

(defun kafka-logs--schema-registry-fetch-subject (subject)
  "Fetch latest Schema Registry SUBJECT.

Return parsed subject metadata, nil for 404, and signal on other failures."
  (let* ((response (kafka-logs--schema-registry-request-json
                    (kafka-logs--schema-registry-subject-path subject)))
         (status (car response))
         (body (cdr response)))
    (cond
     ((and (>= status 200) (< status 300)) body)
     ((= status 404) nil)
     (t
      (user-error "Schema Registry subject lookup failed (%s): %s"
                  status subject)))))

(defun kafka-logs--schema-registry-topic-value-subject (topic)
  "Return conventional Schema Registry value subject for TOPIC."
  (format "%s-value" topic))

(defun kafka-logs--schema-registry-avro-subject-p (subject)
  "Return non-nil when SUBJECT exists and is Avro."
  (when-let ((metadata (kafka-logs--schema-registry-fetch-subject subject)))
    (let ((schema-type (kafka-logs--alist-get-any metadata "schemaType")))
      (or (null schema-type)
          (equal (upcase (kafka-logs--value->string schema-type)) "AVRO")))))

(defun kafka-logs--detect-topic-value-format (topic)
  "Detect value wire format for TOPIC.

Only conventional `<topic>-value` Avro subjects are detected.  Detection
failures fall back to `json` and are reported as messages."
  (if (not (kafka-logs--schema-registry-url))
      'json
    (condition-case err
        (if (kafka-logs--schema-registry-avro-subject-p
             (kafka-logs--schema-registry-topic-value-subject topic))
            'avro
          'json)
      (error
       (message "kafka-logs Schema Registry detection failed: %s"
                (error-message-string err))
       'json))))

(defun kafka-logs--effective-value-format ()
  "Return currently effective value wire format."
  (let ((format (kafka-logs--normalize-value-format kafka-logs-value-format)))
    (if (eq format 'auto)
        (or kafka-logs--detected-value-format 'json)
      format)))

(defun kafka-logs--apply-topic-selection (topic)
  "Set selected TOPIC and update detected value format when needed."
  (let ((trimmed (string-trim topic)))
    (when (string-empty-p trimmed)
      (user-error "Topic cannot be empty"))
    (setq kafka-logs-topic trimmed)
    (setq kafka-logs--detected-value-format nil)
    (when (eq (kafka-logs--normalize-value-format kafka-logs-value-format)
              'auto)
      (setq kafka-logs--detected-value-format
            (kafka-logs--detect-topic-value-format trimmed))
      ;; Auto mode treats both JSON and Avro as structured payloads.
      (setq kafka-logs-payload-format 'json))
    trimmed))

(defun kafka-logs--time-string->ms (value label)
  "Parse VALUE into epoch milliseconds for LABEL."
  (unless (and value (not (string-empty-p value)))
    (user-error "%s cannot be empty" label))
  (if (string-match-p "\\`[0-9]+\\'" value)
      (string-to-number value)
    (condition-case err
        (truncate (* 1000.0 (float-time (date-to-time value))))
      (error
       (user-error "Invalid %s time %S: %s" label value (error-message-string err))))))

(defun kafka-logs--resolved-time-range-ms ()
  "Return resolved (FROM-MS . TO-MS) for current session.

When TO is omitted, treat it as current time."
  (unless kafka-logs-time-range
    (user-error "Set a FROM/TO range first"))
  (let* ((from (car kafka-logs-time-range))
         (to (cdr kafka-logs-time-range))
         (from-ms (kafka-logs--time-string->ms from "FROM"))
         (to-ms (if (and to (not (string-empty-p to)))
                    (kafka-logs--time-string->ms to "TO")
                  (truncate (* 1000.0 (float-time))))))
    (when (>= from-ms to-ms)
      (user-error "FROM must be earlier than TO"))
    (cons from-ms to-ms)))

(defun kafka-logs--value-format-args ()
  "Return kcat args for the effective value wire format."
  (pcase (kafka-logs--effective-value-format)
    ('avro
     (let ((registry-url (kafka-logs--schema-registry-kcat-url)))
       (unless registry-url
         (user-error "Avro value format requires :schema-registry-url"))
       ;; Do not combine Avro with -J.  Many kcat builds support JSON and
       ;; Avro but lack JSON-verbatim support, which is required to place the
       ;; decoded Avro JSON value inside kcat's native -J envelope.
       (list "-s" "value=avro"
             "-r" registry-url
             "-f" kafka-logs--avro-envelope-format)))
    (_
     (list "-J"))))

(defun kafka-logs--consume-args ()
  "Build kcat consumer args from current backing fields."
  (unless (and kafka-logs-topic (not (string-empty-p kafka-logs-topic)))
    (user-error "Select a topic first"))
  (append
   (kafka-logs--connection-base-args)
   (list "-C" "-u" "-q" "-t" kafka-logs-topic)
   (kafka-logs--value-format-args)
   (if kafka-logs-stream
       (list "-o" "end")
     (let* ((range (kafka-logs--resolved-time-range-ms))
            (from-ms (car range))
            (to-ms (cdr range)))
       (append
        (list "-o" (format "s@%s" from-ms)
              "-o" (format "e@%s" to-ms)
              "-e")
        (when kafka-logs-max-messages
          (list "-c" (number-to-string kafka-logs-max-messages))))))))

(defun kafka-logs--command-with-filter (args &optional line-buffered)
  "Return process command list for kcat ARGS with optional grep filter.

When LINE-BUFFERED is non-nil and a filter is set, use grep --line-buffered."
  (let ((regex (and kafka-logs-filter
                    (not (string-empty-p kafka-logs-filter))
                    kafka-logs-filter)))
    (if (not regex)
        (cons kafka-logs-kcat args)
      (let* ((kcat-cmd (string-join (mapcar #'shell-quote-argument
                                            (cons kafka-logs-kcat args))
                                    " "))
             (grep-cmd (string-join
                        (append
                         (list "grep")
                         (when line-buffered (list "--line-buffered"))
                         (list "-E" (shell-quote-argument regex)))
                        " "))
             (full (format "%s | %s" kcat-cmd grep-cmd)))
        (list shell-file-name shell-command-switch full)))))

(defun kafka-logs--wrapper-command (socket-path command)
  "Return Rust ingestion wrapper command for SOCKET-PATH and source COMMAND."
  (append
   (list (json-log-viewer-ingest-wrapper-executable)
         "--socket" socket-path
         "kafka"
         "--connection" (or kafka-logs--viewer-connection kafka-logs-connection "")
         "--topic" (or kafka-logs--viewer-topic kafka-logs-topic "")
         "--payload-format" (if (eq kafka-logs--viewer-payload-format 'json)
                                "json"
                              "raw")
         "--")
   command))

(defun kafka-logs--viewer-buffer-name ()
  "Return viewer buffer name for current connection/topic."
  (format "*Kafka logs - %s/%s*"
          (or kafka-logs-connection "-")
          (or kafka-logs-topic "-")))

(defun kafka-logs--process-name ()
  "Return process name for current connection/topic."
  (format "kafka-logs:%s:%s"
          (or kafka-logs-connection "-")
          (or kafka-logs-topic "-")))

(defun kafka-logs--time-range-display (range)
  "Return one-line display label for RANGE."
  (if (and range (car range) (cdr range))
      (format "%s -> %s" (car range) (cdr range))
    "none"))

(defun kafka-logs--viewer-header-lines (_state)
  "Return header lines for current kafka-logs viewer buffer."
  (list
   (cons "Connection" (or kafka-logs--viewer-connection "-"))
   (cons "Topic" (or kafka-logs--viewer-topic "-"))
   (cons "Mode" (if kafka-logs--viewer-stream "stream (new messages)" "time span"))
   (cons "Range" (if kafka-logs--viewer-stream
                     "start at topic end"
                   (kafka-logs--time-range-display kafka-logs--viewer-time-range)))
   (cons "Filter" (or kafka-logs--viewer-filter "none"))
   (cons "Value format" (kafka-logs--value-format-display
                         kafka-logs--viewer-value-format
                         kafka-logs--viewer-detected-value-format))
   (cons "Payload rendering" (if (eq kafka-logs--viewer-payload-format 'json)
                                 "json"
                               "string"))
   (cons "Message path" (or kafka-logs--viewer-message-path "message"))
   (cons "JSON paths" (kafka-logs--json-paths-display kafka-logs--viewer-json-paths))))

(defun kafka-logs--install-viewer-keymap ()
  "Install buffer-local keymap tweaks for kafka logs viewer buffers."
  (let ((map (copy-keymap (current-local-map))))
    (define-key map (kbd "q") #'kafka-logs-quit-process-and-window)
    (use-local-map map)))

(defun kafka-logs--kill-buffer-process (buffer)
  "Stop process and cleanup state associated with BUFFER, if any."
  (when (buffer-live-p buffer)
    (with-current-buffer buffer
      (let ((proc (or kafka-logs--process
                      (and (derived-mode-p 'kafka-logs-viewer-mode)
                           (get-buffer-process buffer)))))
        (when (process-live-p proc)
          (delete-process proc))
        (setq kafka-logs--process nil))
      (when (timerp kafka-logs--stream-drain-timer)
        (cancel-timer kafka-logs--stream-drain-timer))
      (setq kafka-logs--stream-drain-timer nil)
      (setq kafka-logs--stream-chunks-in nil)
      (setq kafka-logs--stream-chunks-out nil)
      (setq kafka-logs--stream-pending-lines nil)
      (setq kafka-logs--pending-fragment "")
      (when (buffer-live-p kafka-logs--once-output-buffer)
        (kill-buffer kafka-logs--once-output-buffer))
      (setq kafka-logs--once-output-buffer nil))))

(defun kafka-logs-quit-process-and-window ()
  "Stop Kafka process for current buffer and close the window."
  (interactive)
  (kafka-logs--kill-buffer-process (current-buffer))
  (quit-window t))

(defun kafka-logs--selected-viewer-buffer-p ()
  "Return non-nil when kafka-logs should use a selected viewer buffer."
  (and kafka-logs-viewer-buffer
       (not (string-empty-p kafka-logs-viewer-buffer))))

(defun kafka-logs--selected-viewer-buffer ()
  "Return selected kafka-logs viewer buffer, or nil when unset."
  (when (kafka-logs--selected-viewer-buffer-p)
    (json-log-viewer-get-buffer kafka-logs-viewer-buffer)))

(defun kafka-logs--selected-composite-viewer-buffer-p ()
  "Return non-nil when the selected viewer is a composite log viewer."
  (and (kafka-logs--selected-viewer-buffer-p)
       (json-log-viewer-composite-buffer-p kafka-logs-viewer-buffer)))

(defun kafka-logs--register-composite-source-config (buffer)
  "Register current Kafka formatting for composite BUFFER."
  (when (json-log-viewer-composite-buffer-p buffer)
    (json-log-viewer-register-source-config
     buffer
     "kafka"
     :timestamp-path "timestamp"
     :level-path "level"
     :message-path (or kafka-logs-message-path kafka-logs-default-message-path "message")
     :extra-paths kafka-logs-extra-paths
     :json-paths kafka-logs-json-paths)))

(defun kafka-logs--initialize-viewer-buffer
    (buffer message-path json-paths &optional install-keymap)
  "Initialize kafka-logs state in BUFFER.

MESSAGE-PATH and JSON-PATHS are the normalized rendering paths for the current
session.  When INSTALL-KEYMAP is non-nil, install kafka-logs key bindings."
  (with-current-buffer buffer
    (setq-local kafka-logs--process nil)
    (setq-local kafka-logs--pending-fragment "")
    (setq-local kafka-logs--once-output-buffer nil)
    (setq-local kafka-logs--stream-chunks-in nil)
    (setq-local kafka-logs--stream-chunks-out nil)
    (setq-local kafka-logs--stream-pending-lines nil)
    (setq-local kafka-logs--stream-drain-timer nil)
    (setq-local kafka-logs--viewer-connection kafka-logs-connection)
    (setq-local kafka-logs--viewer-topic kafka-logs-topic)
    (setq-local kafka-logs--viewer-stream kafka-logs-stream)
    (setq-local kafka-logs--viewer-time-range kafka-logs-time-range)
    (setq-local kafka-logs--viewer-filter kafka-logs-filter)
    (setq-local kafka-logs--viewer-payload-format kafka-logs-payload-format)
    (setq-local kafka-logs--viewer-value-format kafka-logs-value-format)
    (setq-local kafka-logs--viewer-detected-value-format
                kafka-logs--detected-value-format)
    (setq-local kafka-logs--viewer-message-path message-path)
    (setq-local kafka-logs--viewer-json-paths json-paths)
    (add-hook 'kill-buffer-hook
              (lambda ()
                (kafka-logs--kill-buffer-process (current-buffer)))
              nil t)
    (when install-keymap
      (kafka-logs--install-viewer-keymap))))

(defun kafka-logs--value->string (value)
  "Convert VALUE into a display string."
  (cond
   ((stringp value) value)
   ((numberp value) (number-to-string value))
   ((eq value t) "true")
   ((eq value :false) "false")
   ((null value) nil)
   (t (format "%s" value))))

(defun kafka-logs--extract-first-field (node candidates)
  "Extract first non-empty field from NODE matching CANDIDATES."
  (catch 'found
    (dolist (candidate candidates)
      (when-let ((value (kafka-logs--value->string
                         (kafka-logs--alist-get-any node candidate))))
        (unless (string-empty-p value)
          (throw 'found value))))
    nil))

(defun kafka-logs--normalize-headers (headers)
  "Normalize kcat HEADERS into a viewer-friendly JSON object.

Native kcat JSON output represents headers as a flat array of alternating
header names and values.  Duplicate header names are valid in Kafka, so
duplicates are preserved as arrays."
  (cond
   ((null headers) nil)
   ((or (hash-table-p headers)
        (kafka-logs--alist-like-p headers))
    headers)
   ((listp headers)
    (let ((cursor headers)
          (sentinel (make-symbol "missing"))
          (table (make-hash-table :test 'equal))
          keys)
      (while cursor
        (let ((name (kafka-logs--value->string (car cursor)))
              (value (cadr cursor)))
          (when name
            (let ((existing (gethash name table sentinel)))
              (when (eq existing sentinel)
                (push name keys))
              (puthash name
                       (cond
                        ((eq existing sentinel) value)
                        ((and (listp existing)
                              (not (kafka-logs--alist-like-p existing)))
                         (append existing (list value)))
                        (t
                         (list existing value)))
                       table))))
        (setq cursor (cddr cursor)))
      (when keys
        (mapcar (lambda (key)
                  (cons key (gethash key table)))
                (nreverse keys)))))
   (t headers)))

(defun kafka-logs--epoch-ms->iso8601 (ms)
  "Convert epoch milliseconds MS to UTC ISO-8601 string."
  (format-time-string "%Y-%m-%dT%H:%M:%S.%3NZ"
                      (seconds-to-time (/ (float ms) 1000.0))
                      t))

(defun kafka-logs--line->json-line (line)
  "Convert one kcat JSON envelope LINE into viewer JSON line."
  (let ((clean (string-trim-right (or line "") "\r")))
    (unless (string-empty-p clean)
      (let* ((envelope (kafka-logs--parse-json-maybe clean))
             (topic (or (kafka-logs--value->string
                         (kafka-logs--alist-get-any envelope "topic"))
                        kafka-logs-topic))
             (partition (kafka-logs--alist-get-any envelope "partition"))
             (offset (kafka-logs--alist-get-any envelope "offset"))
             (ts (kafka-logs--alist-get-any envelope "ts"))
             (timestamp
              (when (numberp ts)
                (kafka-logs--epoch-ms->iso8601 ts)))
             (key-size (kafka-logs--alist-get-any envelope "key_size"))
             (key (unless (and (numberp key-size) (< key-size 0))
                    (kafka-logs--alist-get-any envelope "key")))
             (headers (kafka-logs--normalize-headers
                       (kafka-logs--alist-get-any envelope "headers")))
             (payload (kafka-logs--alist-get-any envelope "payload"))
             (payload-node
              (cond
               ((listp payload) payload)
               ((stringp payload) (kafka-logs--parse-json-maybe payload))
               (t nil)))
             (display-payload
              (if (and (eq kafka-logs-payload-format 'json)
                       payload-node)
                  payload-node
                payload))
             (level (and payload-node
                         (kafka-logs--extract-first-field
                          payload-node
                          '("level" "severity" "logLevel" "lvl"))))
             (obj (make-hash-table :test 'equal)))
        (when timestamp
          (puthash "timestamp" timestamp obj))
        (when level
          (puthash "level" level obj))
        (puthash "source" "kafka" obj)
        (puthash "raw" clean obj)
        (puthash "connection" (or kafka-logs--viewer-connection kafka-logs-connection "") obj)
        (when topic
          (puthash "topic" topic obj))
        (when partition
          (puthash "partition" partition obj))
        (when offset
          (puthash "offset" offset obj))
        (when key
          (puthash "key" key obj))
        (when headers
          (puthash "headers" headers obj))
        (when display-payload
          (puthash "payload" display-payload obj))
        (json-serialize (kafka-logs--normalize-json-value obj))))))

(defun kafka-logs--lines->json-lines (lines)
  "Convert kcat output LINES to json-log-viewer JSON lines."
  (delq nil (mapcar #'kafka-logs--line->json-line lines)))

(defun kafka-logs--make-viewer-buffer (&optional on-ready)
  "Create kafka logs viewer buffer.
ON-READY is called once the async worker is ready to receive jobs."
  (let* ((buffer-name (kafka-logs--viewer-buffer-name))
         (extra-paths
          (kafka-logs--normalize-extra-paths
           kafka-logs-extra-paths
           "kafka-logs-extra-paths"))
         (json-paths
          (kafka-logs--normalize-json-paths
           kafka-logs-json-paths
           "kafka-logs-json-paths"))
         (message-path
          (kafka-logs--normalize-message-path
           kafka-logs-message-path
           "kafka-logs-message-path"))
         buffer)
    (if-let ((selected (kafka-logs--selected-viewer-buffer)))
        (if (json-log-viewer-composite-buffer-p selected)
            (progn
              (setq buffer selected)
              (when on-ready
                (json-log-viewer-run-when-ready buffer on-ready)))
          (kafka-logs--kill-buffer-process selected)
          (setq buffer selected)
          (kafka-logs--initialize-viewer-buffer buffer message-path json-paths)
          (when on-ready
            (json-log-viewer-run-when-ready buffer on-ready)))
      (let ((existing (get-buffer buffer-name)))
        (when existing
          (kafka-logs--kill-buffer-process existing))
        (setq buffer
              (json-log-viewer-make-buffer
               buffer-name
               :timestamp-path "timestamp"
               :message-path message-path
               :extra-paths extra-paths
               :json-paths json-paths
               :mode #'kafka-logs-viewer-mode
               :header-lines-function #'kafka-logs--viewer-header-lines
               :on-ready on-ready))
        (kafka-logs--initialize-viewer-buffer buffer message-path json-paths t)))
    buffer))

(defun kafka-logs--consume-chunk-lines (chunk)
  "Consume process CHUNK and return complete lines in current buffer."
  (let* ((combined (concat kafka-logs--pending-fragment chunk))
         (has-newline (string-suffix-p "\n" combined))
         (parts (split-string combined "\n"))
         (complete-lines (if has-newline parts (butlast parts)))
         (rest (if has-newline "" (car (last parts)))))
    (setq kafka-logs--pending-fragment (or rest ""))
    complete-lines))

(defun kafka-logs--stream-queue-empty-p ()
  "Return non-nil when no stream output is waiting to be rendered."
  (and (null kafka-logs--stream-chunks-in)
       (null kafka-logs--stream-chunks-out)
       (null kafka-logs--stream-pending-lines)))

(defun kafka-logs--stream-cancel-drain-timer ()
  "Cancel and clear stream drain timer for current buffer."
  (when (timerp kafka-logs--stream-drain-timer)
    (cancel-timer kafka-logs--stream-drain-timer))
  (setq kafka-logs--stream-drain-timer nil))

(defun kafka-logs--stream-drain-on-timer (buffer)
  "Drain queued stream output for BUFFER from timer callbacks."
  (if (not (buffer-live-p buffer))
      nil
    (with-current-buffer buffer
      (condition-case err
          (kafka-logs--stream-drain nil)
        (error
         (kafka-logs--stream-cancel-drain-timer)
         (message "kafka-logs drain failed: %s" (error-message-string err)))))))

(defun kafka-logs--stream-schedule-drain ()
  "Ensure periodic draining is scheduled for current buffer."
  (unless (timerp kafka-logs--stream-drain-timer)
    (let ((interval (max 0.01 (or kafka-logs-stream-drain-interval 0.05))))
      (setq kafka-logs--stream-drain-timer
            (run-at-time interval interval
                         #'kafka-logs--stream-drain-on-timer
                         (current-buffer))))))

(defun kafka-logs--stream-enqueue-chunk (chunk)
  "Queue one process output CHUNK for later incremental rendering."
  (when (and (stringp chunk) (> (length chunk) 0))
    (push chunk kafka-logs--stream-chunks-in)
    (kafka-logs--stream-schedule-drain)))

(defun kafka-logs--stream-pop-chunk ()
  "Pop the next queued process output chunk, or nil."
  (unless kafka-logs--stream-chunks-out
    (when kafka-logs--stream-chunks-in
      (setq kafka-logs--stream-chunks-out (nreverse kafka-logs--stream-chunks-in))
      (setq kafka-logs--stream-chunks-in nil)))
  (prog1 (car kafka-logs--stream-chunks-out)
    (setq kafka-logs--stream-chunks-out (cdr kafka-logs--stream-chunks-out))))

(defun kafka-logs--stream-pop-lines (max-lines)
  "Pop up to MAX-LINES complete streamed lines in order."
  (let ((lines nil)
        (count 0))
    (while (< count max-lines)
      (unless kafka-logs--stream-pending-lines
        (if-let ((chunk (kafka-logs--stream-pop-chunk)))
            (setq kafka-logs--stream-pending-lines
                  (kafka-logs--consume-chunk-lines chunk))
          (setq count max-lines)))
      (while (and kafka-logs--stream-pending-lines
                  (< count max-lines))
        (push (pop kafka-logs--stream-pending-lines) lines)
        (setq count (1+ count))))
    (nreverse lines)))

(defun kafka-logs--stream-drain (&optional drain-all)
  "Render queued streamed output in batches.

When DRAIN-ALL is non-nil, consume the full queue in one call."
  (let ((batch-size (max 1 (or kafka-logs-stream-max-lines-per-batch 250)))
        (more t))
    (while more
      (let* ((limit (if drain-all most-positive-fixnum batch-size))
             (lines (kafka-logs--stream-pop-lines limit))
             (json-lines (kafka-logs--lines->json-lines lines)))
        (when json-lines
          (json-log-viewer-push (current-buffer) json-lines))
        (setq more (and drain-all
                        (not (kafka-logs--stream-queue-empty-p))))))
    (when (kafka-logs--stream-queue-empty-p)
      (kafka-logs--stream-cancel-drain-timer))))

(defun kafka-logs--flush-pending-fragment ()
  "Flush pending trailing fragment in current buffer."
  (when (and kafka-logs--pending-fragment
             (not (string-empty-p kafka-logs--pending-fragment)))
    (let ((line kafka-logs--pending-fragment))
      (setq kafka-logs--pending-fragment "")
      (when-let ((json-line (kafka-logs--line->json-line line)))
        (json-log-viewer-push (current-buffer) (list json-line))))))

(defun kafka-logs--stream-process-filter (process output)
  "Process filter for streaming kcat PROCESS OUTPUT."
  (let ((buffer (process-buffer process)))
    (when (buffer-live-p buffer)
      (with-current-buffer buffer
        ;; Keep process filter lightweight: queue output and render on timer ticks.
        (kafka-logs--stream-enqueue-chunk output)))))

(defun kafka-logs--wrapper-process-filter (_process output)
  "Report low-volume wrapper diagnostics from OUTPUT."
  (let ((text (string-trim output)))
    (unless (string-empty-p text)
      (message "kafka-logs wrapper: %s" text))))

(defun kafka-logs--stream-process-sentinel (process event)
  "Process sentinel for streaming kcat PROCESS EVENT."
  (let ((buffer (process-buffer process)))
    (when (buffer-live-p buffer)
      (with-current-buffer buffer
        (kafka-logs--stream-drain t)
        (kafka-logs--flush-pending-fragment)
        (kafka-logs--stream-cancel-drain-timer)
        (setq kafka-logs--process nil)))
    (when (and (memq (process-status process) '(exit signal))
               (not (zerop (process-exit-status process)))
               (not (and kafka-logs-filter
                         (= (process-exit-status process) 1))))
      (message "kcat exited (%s): %s"
               (process-exit-status process)
               (string-trim event)))))

(defun kafka-logs--run-once ()
  "Fetch Kafka messages once asynchronously and render in json-log-viewer."
  (let* ((append-to-existing (kafka-logs--selected-composite-viewer-buffer-p))
         (buffer (kafka-logs--make-viewer-buffer))
         (_ (kafka-logs--register-composite-source-config buffer))
         (args (kafka-logs--consume-args))
         (command (kafka-logs--command-with-filter args nil))
         (output-buffer (generate-new-buffer " *kafka-logs-once*"))
         (label (format "%s/%s"
                        (or kafka-logs-connection "-")
                        (or kafka-logs-topic "-")))
         (process
          (make-process
           :name (kafka-logs--process-name)
           :buffer output-buffer
           :command command
           :noquery t
           :connection-type 'pipe
           :sentinel
           (lambda (proc event)
             (when (memq (process-status proc) '(exit signal))
               (let ((exit-code (process-exit-status proc))
                     (output (with-current-buffer output-buffer
                               (buffer-string))))
                 (unwind-protect
                     (when (buffer-live-p buffer)
                       (with-current-buffer buffer
                         (when (eq kafka-logs--process proc)
                           (setq kafka-logs--process nil))
                         (when (eq kafka-logs--once-output-buffer output-buffer)
                           (setq kafka-logs--once-output-buffer nil))
                         (if (or (zerop exit-code)
                                 (and kafka-logs-filter (= exit-code 1)))
                             (let* ((raw-lines (split-string output "\n" t))
                                    (json-lines (kafka-logs--lines->json-lines raw-lines)))
                               (if append-to-existing
                                   (json-log-viewer-push buffer json-lines)
                                 (json-log-viewer-replace-log-lines buffer json-lines nil))
                               (message "Fetched Kafka messages for %s" label))
                           (message "kcat failed (%s): %s"
                                    exit-code
                                    (if (string-empty-p (string-trim output))
                                        (string-trim event)
                                      (string-trim output))))))
                   (kill-buffer output-buffer))))))))
    (with-current-buffer buffer
      (setq-local kafka-logs--once-output-buffer output-buffer))
    (display-buffer buffer)
    (message "Fetching Kafka messages for %s..." label)
    (set-process-query-on-exit-flag process nil)
    (with-current-buffer buffer
      (setq-local kafka-logs--process process))))

(defun kafka-logs--run-stream ()
  "Start Kafka stream and render in json-log-viewer."
  (let* ((args (kafka-logs--consume-args))
         (command (kafka-logs--command-with-filter args t))
         (label (format "%s/%s"
                        (or kafka-logs-connection "-")
                        (or kafka-logs-topic "-")))
         buffer)
    (setq buffer
          (kafka-logs--make-viewer-buffer
           (lambda ()
             (let* ((viewer-buffer (current-buffer))
                    (_ (kafka-logs--register-composite-source-config viewer-buffer))
                    (socket-path (json-log-viewer-worker-socket-path viewer-buffer))
                    (wrapper-command
                     (kafka-logs--wrapper-command socket-path command))
                    (process (make-process
                              :name (kafka-logs--process-name)
                              :buffer viewer-buffer
                              :command wrapper-command
                              :noquery t
                              :connection-type 'pipe
                              :filter #'kafka-logs--wrapper-process-filter)))
               (set-process-sentinel process #'kafka-logs--stream-process-sentinel)
               (set-process-query-on-exit-flag process nil)
               (with-current-buffer viewer-buffer
                 (setq-local kafka-logs--process process))
               (message "Started Kafka stream for %s" label)))))
    (display-buffer buffer)))

(defun kafka-logs-run ()
  "Run kcat consume using current session selections."
  (interactive)
  (unless (and kafka-logs-connection
               (assoc kafka-logs-connection kafka-logs-connections))
    (user-error "Select a configured connection first"))
  (unless (and kafka-logs-topic (not (string-empty-p kafka-logs-topic)))
    (user-error "Select a topic first"))
  (when (and kafka-logs-max-messages
             (<= kafka-logs-max-messages 0))
    (user-error "Max messages must be a positive integer"))
  (unless kafka-logs-stream
    (kafka-logs--resolved-time-range-ms))
  (if kafka-logs-stream
      (kafka-logs--run-stream)
    (kafka-logs--run-once)))

(transient-define-suffix kafka-logs-select-connection ()
  "Set Kafka connection."
  :description (lambda ()
                 (format "Connection: %s" (or kafka-logs-connection "-")))
  :transient t
  (interactive)
  (unless kafka-logs-connections
    (user-error "No connections configured; use `kafka-logs-make-connection`"))
  (let ((value (completing-read "Connection: " (kafka-logs--connection-names) nil t)))
    (setq kafka-logs-connection value)
    ;; Topic validity depends on selected connection.
    (setq kafka-logs-topic nil)
    (setq kafka-logs--detected-value-format nil)
    (kafka-logs--transient-reprompt)))

(transient-define-suffix kafka-logs-select-topic ()
  "Set Kafka topic."
  :description (lambda ()
                 (format "Topic: %s" (or kafka-logs-topic "-")))
  :transient t
  (interactive)
  (unless (and kafka-logs-connection
               (assoc kafka-logs-connection kafka-logs-connections))
    (user-error "Select a configured connection first"))
  (let* ((choices (ignore-errors (kafka-logs--list-topics)))
         (value
          (if (and choices (listp choices) (> (length choices) 0))
              (completing-read "Topic: " choices nil t)
            (string-trim (read-string "Topic: ")))))
    (kafka-logs--apply-topic-selection value)
    (kafka-logs--transient-reprompt)))

(transient-define-suffix kafka-logs-toggle-stream ()
  "Toggle stream mode."
  :description (lambda ()
                 (format "Mode: %s"
                         (if kafka-logs-stream "stream (new)" "time span")))
  :transient t
  (interactive)
  (setq kafka-logs-stream (not kafka-logs-stream))
  (kafka-logs--transient-reprompt))

(defun kafka-logs--set-time-range (from to)
  "Set session range to FROM and TO, preserving nil semantics."
  (setq kafka-logs-time-range
        (when (or from to)
          (cons from to))))

(defun kafka-logs--time-string->time (value)
  "Parse VALUE into an Emacs time value, or nil when parsing fails."
  (when (and value (not (string-empty-p value)))
    (if (string-match-p "\\`[0-9]+\\'" value)
        (seconds-to-time (/ (string-to-number value) 1000.0))
      (ignore-errors (date-to-time value)))))

(defun kafka-logs--read-org-time (prompt &optional initial)
  "Read timestamp with Org date picker using PROMPT and INITIAL time string."
  (require 'org)
  (let* ((initial-time (kafka-logs--time-string->time initial))
         (selected (org-read-date nil t nil prompt initial-time)))
    (format-time-string "%Y-%m-%dT%H:%M:%S%z" selected)))

(transient-define-suffix kafka-logs-set-range-from ()
  "Set range FROM value with Org timestamp picker."
  :description (lambda ()
                 (format "From: %s"
                         (or (and kafka-logs-time-range (car kafka-logs-time-range))
                             "-")))
  :transient t
  (interactive)
  (let* ((current (and kafka-logs-time-range (car kafka-logs-time-range)))
         (to (and kafka-logs-time-range (cdr kafka-logs-time-range))))
    (kafka-logs--set-time-range
     (kafka-logs--read-org-time "From: " current)
     to)
    (kafka-logs--transient-reprompt)))

(transient-define-suffix kafka-logs-set-range-to ()
  "Set range TO value with Org timestamp picker."
  :description (lambda ()
                 (format "To: %s"
                         (or (and kafka-logs-time-range (cdr kafka-logs-time-range))
                             "-")))
  :transient t
  (interactive)
  (let* ((current (and kafka-logs-time-range (cdr kafka-logs-time-range)))
         (from (and kafka-logs-time-range (car kafka-logs-time-range))))
    (kafka-logs--set-time-range
     from
     (kafka-logs--read-org-time "To: " current))
    (kafka-logs--transient-reprompt)))

(transient-define-suffix kafka-logs-set-filter ()
  "Set regex filter."
  :description (lambda ()
                 (format "Filter: %s" (or kafka-logs-filter "none")))
  :transient t
  (interactive)
  (let ((input (string-trim (read-string "Filter regex (empty=none): "
                                         (or kafka-logs-filter "")))))
    (setq kafka-logs-filter (unless (string-empty-p input) input))
    (kafka-logs--transient-reprompt)))

(transient-define-suffix kafka-logs-set-max-messages ()
  "Set max messages in time-span mode."
  :description (lambda ()
                 (format "Max messages: %s"
                         (if kafka-logs-max-messages
                             (number-to-string kafka-logs-max-messages)
                           "none")))
  :transient t
  (interactive)
  (let* ((initial (if kafka-logs-max-messages
                      (number-to-string kafka-logs-max-messages)
                    ""))
         (input (string-trim (read-string "Max messages (empty=none): " initial))))
    (setq kafka-logs-max-messages
          (unless (string-empty-p input)
            (let ((n (string-to-number input)))
              (when (<= n 0)
                (user-error "Max messages must be a positive integer"))
              n)))
    (kafka-logs--transient-reprompt)))

(transient-define-suffix kafka-logs-set-json-paths ()
  "Set JSON detail paths rendered as formatted blocks."
  :description (lambda ()
                 (format "JSON paths: %s"
                         (kafka-logs--json-paths-display kafka-logs-json-paths)))
  :transient t
  (interactive)
  (let* ((initial (if kafka-logs-json-paths
                      (string-join kafka-logs-json-paths ",")
                    ""))
         (input (string-trim (read-string "JSON paths (comma separated, empty=none): "
                                          initial))))
    (setq kafka-logs-json-paths
          (if (string-empty-p input)
              nil
            (kafka-logs--normalize-json-paths
             (split-string input "," t)
             "JSON paths")))
    (kafka-logs--transient-reprompt)))

(transient-define-suffix kafka-logs-set-message-path ()
  "Set JSON path used for summary message rendering."
  :description (lambda ()
                 (format "Message path: %s"
                         (or kafka-logs-message-path "message")))
  :transient t
  (interactive)
  (let* ((initial (or kafka-logs-message-path kafka-logs-default-message-path "message"))
         (input (string-trim (read-string
                              "Message path (empty=default): "
                              initial))))
    (setq kafka-logs-message-path
          (kafka-logs--normalize-message-path
           (if (string-empty-p input)
               (or kafka-logs-default-message-path "message")
             input)
           "Message path"))
    (kafka-logs--transient-reprompt)))

(transient-define-suffix kafka-logs-toggle-payload-format ()
  "Toggle payload rendering format."
  :description (lambda ()
                 (format "Payload rendering: %s"
                         (if (eq kafka-logs-payload-format 'json)
                             "json"
                           "string")))
  :transient t
  (interactive)
  (setq kafka-logs-payload-format
        (if (eq kafka-logs-payload-format 'json) nil 'json))
  (kafka-logs--transient-reprompt))

(transient-define-suffix kafka-logs-set-value-format ()
  "Set Kafka value wire format."
  :description (lambda ()
                 (format "Value format: %s"
                         (kafka-logs--value-format-display
                          kafka-logs-value-format
                          kafka-logs--detected-value-format)))
  :transient t
  (interactive)
  (let* ((choices '("auto" "json" "avro" "string"))
         (current (symbol-name
                   (kafka-logs--normalize-value-format
                    kafka-logs-value-format)))
         (input (completing-read "Value format: " choices nil t nil nil current))
         (format (intern input)))
    (setq kafka-logs-value-format
          (kafka-logs--normalize-value-format format))
    (setq kafka-logs--detected-value-format nil)
    (pcase kafka-logs-value-format
      ('string (setq kafka-logs-payload-format nil))
      ((or 'json 'avro) (setq kafka-logs-payload-format 'json))
      ('auto
       (when kafka-logs-topic
         (kafka-logs--apply-topic-selection kafka-logs-topic))))
    (kafka-logs--transient-reprompt)))

(defun kafka-logs--set-viewer-buffer-from-current-buffer ()
  "Use current composite log viewer as selected viewer, or clear selection."
  (setq kafka-logs-viewer-buffer
        (when (json-log-viewer-composite-buffer-p (current-buffer))
          (buffer-name (current-buffer)))))

(transient-define-suffix kafka-logs-action-run ()
  "Run Kafka logs viewer with current selections."
  :transient nil
  (interactive)
  (kafka-logs-run))

(transient-define-prefix kafka-logs-transient ()
  "Transient menu for selecting and running Kafka logs."
  :remember-value 'exit
  [["Config"
    ("c" kafka-logs-select-connection)
    ("t" kafka-logs-select-topic)
    ("-f" kafka-logs-toggle-stream)
    ("-F" kafka-logs-set-filter)
    ("-v" kafka-logs-set-value-format)
    ("-M" kafka-logs-set-message-path)
    ("-j" kafka-logs-set-json-paths)
    ("-p" kafka-logs-toggle-payload-format)
    ("-m" kafka-logs-set-max-messages)]
   ["Range (time-span mode)"
    ("a" kafka-logs-set-range-from)
    ("z" kafka-logs-set-range-to)]]
  [[4 :description (lambda ()
                     (format "Active target: %s/%s"
                             (or kafka-logs-connection "-")
                             (or kafka-logs-topic "-")))]]
  [["Actions"
    ("RET" "Run logs" kafka-logs-action-run)]]
  (interactive)
  (kafka-logs--set-viewer-buffer-from-current-buffer)
  (transient-setup 'kafka-logs-transient))

(defun kafka-logs ()
  "Open kafka-logs transient UI."
  (interactive)
  (call-interactively #'kafka-logs-transient))

(provide 'kafka-logs)
;;; kafka-logs.el ends here
