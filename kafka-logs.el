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

(require 'json-log-viewer)

(declare-function json-log-viewer-make-buffer "json-log-viewer"
                  (buffer-name &rest args))
(declare-function json-log-viewer-push "json-log-viewer"
                  (buffer-or-name log-lines))
(declare-function json-log-viewer-replace-log-lines "json-log-viewer"
                  (buffer-or-name log-lines &optional preserve-filter))

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

(defcustom kafka-logs-default-time-range nil
  "Default lookup range for new Emacs sessions.

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

(defcustom kafka-logs-default-payload-format nil
  "Default payload rendering format for new Emacs sessions.

When set to `json`, kafka-logs attempts to parse string payloads as JSON and
stores parsed objects in the viewer `payload` field.
When nil, keep payloads as-is."
  :type '(choice (const :tag "As string (default)" nil)
                 (const :tag "JSON" json))
  :group 'kafka-logs)

(defvar kafka-logs-connection kafka-logs-default-connection
  "Selected Kafka connection name for this Emacs session.")

(defvar kafka-logs-topic kafka-logs-default-topic
  "Selected Kafka topic for this Emacs session.")

(defvar kafka-logs-stream kafka-logs-default-stream
  "Non-nil means stream new Kafka messages.")

(defvar kafka-logs-time-range kafka-logs-default-time-range
  "Selected explicit FROM/TO time range for this Emacs session.

Value is nil or (FROM . TO), where both are date-time strings.")

(defvar kafka-logs-filter kafka-logs-default-filter
  "Regex filter for kcat output in this Emacs session, or nil.")

(defvar kafka-logs-max-messages kafka-logs-default-max-messages
  "Maximum message count for this Emacs session in time-span mode, or nil.")

(defvar kafka-logs-payload-format kafka-logs-default-payload-format
  "Payload rendering format for this Emacs session, or nil.")

(defvar kafka-logs-connections nil
  "Alist of named Kafka connections.

Each element has the form (NAME . PLIST).")

(defvar-local kafka-logs--process nil
  "Process associated with current kafka-logs viewer buffer.")

(defvar-local kafka-logs--pending-fragment ""
  "Trailing incomplete process output fragment for streaming buffers.")

(defvar-local kafka-logs--once-output-buffer nil
  "Temporary process output buffer for one-shot asynchronous fetches.")

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

(defconst kafka-logs--connection-keys
  '(:brokers :security-protocol :sasl-mechanisms :username :password
    :auth-source :properties :kcat-args :description)
  "Allowed keys for `kafka-logs-make-connection`.")

(defun kafka-logs--transient-reprompt ()
  "Refresh transient so UI reflects current backing fields."
  (transient-quit-one)
  (transient-setup 'kafka-logs-transient))

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
  "Return resolved (FROM-MS . TO-MS) for current session."
  (unless kafka-logs-time-range
    (user-error "Set a FROM/TO range first"))
  (let* ((from (car kafka-logs-time-range))
         (to (cdr kafka-logs-time-range))
         (from-ms (kafka-logs--time-string->ms from "FROM"))
         (to-ms (kafka-logs--time-string->ms to "TO")))
    (when (>= from-ms to-ms)
      (user-error "FROM must be earlier than TO"))
    (cons from-ms to-ms)))

(defun kafka-logs--consume-args ()
  "Build kcat consumer args from current backing fields."
  (unless (and kafka-logs-topic (not (string-empty-p kafka-logs-topic)))
    (user-error "Select a topic first"))
  (append
   (kafka-logs--connection-base-args)
   (list "-C" "-J" "-u" "-q" "-t" kafka-logs-topic)
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
   (cons "Payload format" (if (eq kafka-logs--viewer-payload-format 'json)
                              "json"
                            "string"))))

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
                      (get-buffer-process buffer))))
        (when (process-live-p proc)
          (delete-process proc))
        (setq kafka-logs--process nil))
      (setq kafka-logs--pending-fragment "")
      (when (buffer-live-p kafka-logs--once-output-buffer)
        (kill-buffer kafka-logs--once-output-buffer))
      (setq kafka-logs--once-output-buffer nil))))

(defun kafka-logs-quit-process-and-window ()
  "Stop Kafka process for current buffer and close the window."
  (interactive)
  (kafka-logs--kill-buffer-process (current-buffer))
  (quit-window t))

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
             (key (kafka-logs--alist-get-any envelope "key"))
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
             (display-message
              (or (and payload-node
                       (kafka-logs--extract-first-field
                        payload-node
                        '("message" "msg" "log" "@message")))
                  (and (stringp payload) payload)
                  (and payload (kafka-logs--value->string payload))
                  clean))
             (obj (make-hash-table :test 'equal)))
        (when timestamp
          (puthash "timestamp" timestamp obj))
        (when level
          (puthash "level" level obj))
        (puthash "message" (or display-message "") obj)
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
        (when display-payload
          (puthash "payload" display-payload obj))
        (message "%s" (json-serialize obj))
        (json-serialize obj)))))

(defun kafka-logs--lines->json-lines (lines)
  "Convert kcat output LINES to json-log-viewer JSON lines."
  (delq nil (mapcar #'kafka-logs--line->json-line lines)))

(defun kafka-logs--make-viewer-buffer (initial-lines streaming)
  "Create kafka logs viewer buffer using INITIAL-LINES.

When STREAMING is non-nil, configure buffer for incremental pushes."
  (let* ((buffer-name (kafka-logs--viewer-buffer-name))
         (existing (get-buffer buffer-name))
         buffer)
    (when existing
      (kafka-logs--kill-buffer-process existing))
    (setq buffer
          (json-log-viewer-make-buffer
           buffer-name
           :log-lines (or initial-lines nil)
           :timestamp-path "timestamp"
           :level-path "level"
           :message-path "message"
           :extra-paths '("connection" "topic" "partition" "offset" "key")
           :streaming streaming
           :direction 'oldest-first
           :header-lines-function #'kafka-logs--viewer-header-lines))
    (with-current-buffer buffer
      (setq-local kafka-logs--process nil)
      (setq-local kafka-logs--pending-fragment "")
      (setq-local kafka-logs--once-output-buffer nil)
      (setq-local kafka-logs--viewer-connection kafka-logs-connection)
      (setq-local kafka-logs--viewer-topic kafka-logs-topic)
      (setq-local kafka-logs--viewer-stream kafka-logs-stream)
      (setq-local kafka-logs--viewer-time-range kafka-logs-time-range)
      (setq-local kafka-logs--viewer-filter kafka-logs-filter)
      (setq-local kafka-logs--viewer-payload-format kafka-logs-payload-format)
      (add-hook 'kill-buffer-hook
                (lambda ()
                  (kafka-logs--kill-buffer-process (current-buffer)))
                nil t)
      (kafka-logs--install-viewer-keymap))
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
        (let* ((lines (kafka-logs--consume-chunk-lines output))
               (json-lines (kafka-logs--lines->json-lines lines)))
          (when json-lines
            (json-log-viewer-push buffer json-lines)))))))

(defun kafka-logs--stream-process-sentinel (process event)
  "Process sentinel for streaming kcat PROCESS EVENT."
  (let ((buffer (process-buffer process)))
    (when (buffer-live-p buffer)
      (with-current-buffer buffer
        (kafka-logs--flush-pending-fragment)
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
  (let* ((buffer (kafka-logs--make-viewer-buffer nil nil))
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
                               (json-log-viewer-replace-log-lines buffer json-lines nil)
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
  (let* ((buffer (kafka-logs--make-viewer-buffer nil t))
         (args (kafka-logs--consume-args))
         (command (kafka-logs--command-with-filter args t))
         (process (make-process
                   :name (kafka-logs--process-name)
                   :buffer buffer
                   :command command
                   :noquery t
                   :connection-type 'pipe)))
    (with-current-buffer buffer
      (setq-local kafka-logs--process process)
      (setq-local kafka-logs--pending-fragment "")
      (display-buffer buffer))
    (set-process-filter process #'kafka-logs--stream-process-filter)
    (set-process-sentinel process #'kafka-logs--stream-process-sentinel)
    (set-process-query-on-exit-flag process nil)
    (message "Started Kafka stream for %s/%s"
             (or kafka-logs-connection "-")
             (or kafka-logs-topic "-"))))

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
    (when (string-empty-p value)
      (user-error "Topic cannot be empty"))
    (setq kafka-logs-topic value)
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

(transient-define-suffix kafka-logs-set-range-from ()
  "Set range FROM value."
  :description (lambda ()
                 (format "From: %s"
                         (or (and kafka-logs-time-range (car kafka-logs-time-range))
                             "-")))
  :transient t
  (interactive)
  (let* ((current (and kafka-logs-time-range (car kafka-logs-time-range)))
         (input (string-trim (read-string "From (datetime or epoch ms, empty=clear): "
                                          (or current ""))))
         (to (and kafka-logs-time-range (cdr kafka-logs-time-range))))
    (kafka-logs--set-time-range
     (unless (string-empty-p input) input)
     to)
    (kafka-logs--transient-reprompt)))

(transient-define-suffix kafka-logs-set-range-to ()
  "Set range TO value."
  :description (lambda ()
                 (format "To: %s"
                         (or (and kafka-logs-time-range (cdr kafka-logs-time-range))
                             "-")))
  :transient t
  (interactive)
  (let* ((current (and kafka-logs-time-range (cdr kafka-logs-time-range)))
         (input (string-trim (read-string "To (datetime or epoch ms, empty=clear): "
                                          (or current ""))))
         (from (and kafka-logs-time-range (car kafka-logs-time-range))))
    (kafka-logs--set-time-range
     from
     (unless (string-empty-p input) input))
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

(transient-define-suffix kafka-logs-toggle-payload-format ()
  "Toggle payload rendering format."
  :description (lambda ()
                 (format "Payload format: %s"
                         (if (eq kafka-logs-payload-format 'json)
                             "json"
                           "string")))
  :transient t
  (interactive)
  (setq kafka-logs-payload-format
        (if (eq kafka-logs-payload-format 'json) nil 'json))
  (kafka-logs--transient-reprompt))

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
  (transient-setup 'kafka-logs-transient))

(defun kafka-logs ()
  "Open kafka-logs transient UI."
  (interactive)
  (call-interactively #'kafka-logs-transient))

(provide 'kafka-logs)
;;; kafka-logs.el ends here
