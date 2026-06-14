;;; kafka-logs-test.el --- kafka-logs unit tests -*- lexical-binding: t; -*-

(require 'ert)
(require 'cl-lib)
(require 'json)

(require 'kafka-logs)
(require 'json-log-viewer-shared)

(defvar kafka-logs-connection)
(defvar kafka-logs-topic)
(defvar kafka-logs-stream)
(defvar kafka-logs-time-range)
(defvar kafka-logs-filter)
(defvar kafka-logs-max-messages)
(defvar kafka-logs-value-format)
(defvar kafka-logs--detected-value-format)
(defvar kafka-logs-payload-format)
(defvar kafka-logs-json-paths)
(defvar kafka-logs-extra-paths)
(defvar kafka-logs-message-path)
(defvar kafka-logs-stream-max-lines-per-batch)
(defvar kafka-logs-connections)

(ert-deftest kafka-logs-connection-base-args-with-auth-source-test ()
  (let ((kafka-logs-connection "prod")
        (kafka-logs-connections
         '(("prod" . (:brokers "kafka.example.com:9093"
                     :security-protocol "SASL_SSL"
                     :sasl-mechanisms "SCRAM-SHA-512"
                     :auth-source t
                     :properties (("client.id" . "emacs")))))))
    (cl-letf (((symbol-function 'auth-source-search)
               (lambda (&rest spec)
                 (should (equal (plist-get spec :host) "kafka.example.com"))
                 (should (equal (plist-get spec :port) "9093"))
                 (list '(:user "alice"
                         :secret (lambda () "pw"))))))
      (should (equal (kafka-logs--connection-base-args)
                     '("-b" "kafka.example.com:9093"
                       "-X" "security.protocol=SASL_SSL"
                       "-X" "sasl.mechanisms=SCRAM-SHA-512"
                       "-X" "sasl.username=alice"
                       "-X" "sasl.password=pw"
                       "-X" "client.id=emacs"))))))

(ert-deftest kafka-logs-consume-args-stream-test ()
  (let ((kafka-logs-topic "orders")
        (kafka-logs-stream t)
        (kafka-logs-time-range nil)
        (kafka-logs-max-messages nil)
        (kafka-logs-value-format 'json)
        (kafka-logs--detected-value-format nil))
    (cl-letf (((symbol-function 'kafka-logs--connection-base-args)
               (lambda () '("-b" "localhost:9092"))))
      (should (equal (kafka-logs--consume-args)
                     '("-b" "localhost:9092"
                       "-C" "-u" "-q" "-t" "orders"
                       "-J"
                       "-o" "end"))))))

(ert-deftest kafka-logs-stream-to-buffer-starts-at-topic-end-test ()
  (let ((buffer (generate-new-buffer "*kafka-composite-stream-test*"))
        (kafka-logs-connections '(("prod" . (:brokers "localhost:9092"))))
        captured-command)
    (unwind-protect
        (progn
          (with-current-buffer buffer
            (composite-log-viewer-mode)
            (setq-local json-log-viewer--async-queue nil))
          (cl-letf (((symbol-function 'json-log-viewer-run-when-ready)
                     (lambda (target function)
                       (with-current-buffer target
                         (funcall function))))
                    ((symbol-function 'json-log-viewer-worker-socket-path)
                     (lambda (&optional _buffer) "/tmp/socket"))
                    ((symbol-function 'json-log-viewer-ingest-wrapper-executable)
                     (lambda () "/tmp/ingest-wrapper"))
                    ((symbol-function 'make-process)
                     (lambda (&rest args)
                       (setq captured-command (plist-get args :command))
                       'kafka-process))
                    ((symbol-function 'set-process-sentinel)
                     (lambda (&rest _args) nil))
                    ((symbol-function 'set-process-query-on-exit-flag)
                     (lambda (&rest _args) nil)))
            (kafka-logs-stream-to-buffer
             buffer
             '(:connection "prod" :topic "orders" :value-format json)))
          (should (equal (member "--" captured-command)
                         '("--" "kcat" "-b" "localhost:9092"
                           "-C" "-u" "-q" "-t" "orders"
                           "-J" "-o" "end"))))
      (when (buffer-live-p buffer)
        (kill-buffer buffer)))))

(ert-deftest kafka-logs-consume-args-time-span-test ()
  (let ((kafka-logs-topic "orders")
        (kafka-logs-stream nil)
        (kafka-logs-time-range '("1000" . "2000"))
        (kafka-logs-max-messages 75)
        (kafka-logs-value-format 'json)
        (kafka-logs--detected-value-format nil))
    (cl-letf (((symbol-function 'kafka-logs--connection-base-args)
               (lambda () '("-b" "localhost:9092"))))
      (should (equal (kafka-logs--consume-args)
                     '("-b" "localhost:9092"
                       "-C" "-u" "-q" "-t" "orders"
                       "-J"
                       "-o" "s@1000"
                       "-o" "e@2000"
                       "-e"
                       "-c" "75"))))))

(ert-deftest kafka-logs-consume-args-time-span-to-defaults-to-now-test ()
  (let ((kafka-logs-topic "orders")
        (kafka-logs-stream nil)
        (kafka-logs-time-range '("1000"))
        (kafka-logs-max-messages nil)
        (kafka-logs-value-format 'json)
        (kafka-logs--detected-value-format nil))
    (cl-letf (((symbol-function 'kafka-logs--connection-base-args)
               (lambda () '("-b" "localhost:9092")))
              ((symbol-function 'float-time)
               (lambda (&optional _time) 2.0)))
      (should (equal (kafka-logs--consume-args)
                     '("-b" "localhost:9092"
                       "-C" "-u" "-q" "-t" "orders"
                       "-J"
                       "-o" "s@1000"
                       "-o" "e@2000"
                       "-e"))))))

(ert-deftest kafka-logs-line->json-line-json-payload-test ()
  (with-temp-buffer
    (setq-local kafka-logs--viewer-connection "prod")
    (let* ((kafka-logs-payload-format nil)
           (line
            (concat
             "{\"topic\":\"orders\",\"partition\":2,\"offset\":9,"
             "\"ts\":1700000000123,\"key\":\"order-1\","
             "\"payload\":\"{\\\"level\\\":\\\"warn\\\",\\\"message\\\":\\\"boom\\\"}\"}"))
           (json-line (kafka-logs--line->json-line line))
           (parsed (json-parse-string json-line :object-type 'alist)))
      (should (equal (alist-get 'connection parsed) "prod"))
      (should (equal (alist-get 'source parsed) "kafka"))
      (should (equal (alist-get 'topic parsed) "orders"))
      (should (equal (alist-get 'partition parsed) 2))
      (should (equal (alist-get 'offset parsed) 9))
      (should (equal (alist-get 'key parsed) "order-1"))
      (should (equal (alist-get 'level parsed) "warn"))
      (should-not (assoc 'message parsed))
      (should (equal (alist-get 'payload parsed)
                     "{\"level\":\"warn\",\"message\":\"boom\"}"))
      (should (equal (alist-get 'timestamp parsed)
                     (kafka-logs--epoch-ms->iso8601 1700000000123))))))

(ert-deftest kafka-logs-schema-registry-auth-source-test ()
  (let ((kafka-logs-connection "prod")
        (kafka-logs-connections
         '(("prod" . (:brokers "kafka.example.com:9093"
                     :schema-registry-url "https://sr.example.com:8081"
                     :schema-registry-auth-source t)))))
    (cl-letf (((symbol-function 'auth-source-search)
               (lambda (&rest spec)
                 (should (equal (plist-get spec :host) "sr.example.com"))
                 (should (equal (plist-get spec :port) "8081"))
                 (list '(:user "alice"
                         :secret (lambda () "pw"))))))
      (should (equal (kafka-logs--schema-registry-basic-auth-header
                      (kafka-logs--connection-plist)
                      "https://sr.example.com:8081")
                     "Basic YWxpY2U6cHc="))
      (should (equal (kafka-logs--schema-registry-kcat-url)
                     "https://alice:pw@sr.example.com:8081")))))

(ert-deftest kafka-logs-schema-registry-kcat-url-uses-raw-userinfo-test ()
  (let ((kafka-logs-connection "prod")
        (kafka-logs-connections
         '(("prod" . (:brokers "kafka.example.com:9093"
                     :schema-registry-url "https://sr.example.com"
                     :schema-registry-username "api-key"
                     :schema-registry-password "a+b/c=")))))
    (should (equal (kafka-logs--schema-registry-kcat-url)
                   "https://api-key:a+b/c=@sr.example.com"))))

(ert-deftest kafka-logs-schema-registry-auth-source-omits-default-port-test ()
  (let ((kafka-logs-connection "prod")
        (kafka-logs-connections
         '(("prod" . (:brokers "kafka.example.com:9093"
                     :schema-registry-url "https://sr.example.com"
                     :schema-registry-username "alice"
                     :schema-registry-auth-source t))))
        seen)
    (cl-letf (((symbol-function 'auth-source-search)
               (lambda (&rest spec)
                 (push spec seen)
                 (when (not (plist-member spec :port))
                   (list '(:user "alice"
                           :secret (lambda () "pw")))))))
      (should (equal (kafka-logs--schema-registry-credentials
                      (kafka-logs--connection-plist)
                      "https://sr.example.com")
                     '("alice" "pw")))
      (should (= (length seen) 1))
      (should-not (plist-member (car seen) :port)))))

(ert-deftest kafka-logs-schema-registry-auth-source-missing-secret-errors-test ()
  (let ((kafka-logs-connection "prod")
        (kafka-logs-connections
         '(("prod" . (:brokers "kafka.example.com:9093"
                     :schema-registry-url "https://sr.example.com"
                     :schema-registry-username "alice"
                     :schema-registry-auth-source t)))))
    (cl-letf (((symbol-function 'auth-source-search)
               (lambda (&rest _spec) nil)))
      (should-error
       (kafka-logs--schema-registry-basic-auth-header
        (kafka-logs--connection-plist)
        "https://sr.example.com")
       :type 'user-error))))

(ert-deftest kafka-logs-consume-args-avro-test ()
  (let ((kafka-logs-connection "prod")
        (kafka-logs-connections
         '(("prod" . (:brokers "localhost:9092"
                     :schema-registry-url "https://sr.example.com:8081"
                     :schema-registry-username "alice"
                     :schema-registry-password "pw"))))
        (kafka-logs-topic "orders")
        (kafka-logs-stream t)
        (kafka-logs-time-range nil)
        (kafka-logs-max-messages nil)
        (kafka-logs-value-format 'avro)
        (kafka-logs--detected-value-format nil))
    (should (equal (kafka-logs--consume-args)
                   '("-b" "localhost:9092"
                     "-C" "-u" "-q" "-t" "orders"
                     "-s" "value=avro"
                     "-r" "https://alice:pw@sr.example.com:8081"
                     "-f"
                     "{\"topic\":\"%t\",\"partition\":%p,\"offset\":%o,\"ts\":%T,\"key_size\":%K,\"key\":\"%k\",\"payload\":%s}\\n"
                     "-o" "end")))))

(ert-deftest kafka-logs-detect-topic-value-format-avro-test ()
  (let ((kafka-logs-connection "prod")
        (kafka-logs-connections
         '(("prod" . (:brokers "localhost:9092"
                     :schema-registry-url "http://sr:8081"))))
        captured-subject)
    (cl-letf (((symbol-function 'kafka-logs--schema-registry-fetch-subject)
               (lambda (subject)
                 (setq captured-subject subject)
                 '((schemaType . "AVRO")))))
      (should (eq (kafka-logs--detect-topic-value-format "orders") 'avro))
      (should (equal captured-subject "orders-value")))))

(ert-deftest kafka-logs-apply-topic-selection-switches-auto-format-test ()
  (let ((kafka-logs-topic nil)
        (kafka-logs-value-format 'auto)
        (kafka-logs--detected-value-format nil)
        (kafka-logs-payload-format nil)
        (formats '(avro json)))
    (cl-letf (((symbol-function 'kafka-logs--detect-topic-value-format)
               (lambda (_topic)
                 (pop formats))))
      (kafka-logs--apply-topic-selection "orders")
      (should (equal kafka-logs-topic "orders"))
      (should (eq kafka-logs--detected-value-format 'avro))
      (should (eq kafka-logs-payload-format 'json))
      (kafka-logs--apply-topic-selection "payments")
      (should (equal kafka-logs-topic "payments"))
      (should (eq kafka-logs--detected-value-format 'json))
      (should (eq kafka-logs-payload-format 'json)))))

(ert-deftest kafka-logs-line->json-line-json-payload-with-message-path-test ()
  (with-temp-buffer
    (let ((kafka-logs-message-path "payload.message")
          (line
           (concat
            "{\"topic\":\"orders\",\"partition\":2,\"offset\":9,"
            "\"ts\":1700000000123,\"key\":\"order-1\","
            "\"payload\":\"{\\\"level\\\":\\\"warn\\\",\\\"message\\\":\\\"boom\\\"}\"}")))
      (let* ((json-line (kafka-logs--line->json-line line))
             (parsed (json-parse-string json-line :object-type 'alist))
             (flattened (json-log-viewer-shared--flatten-path-values parsed)))
        (should (equal (json-log-viewer-shared--resolve-path
                        parsed kafka-logs-message-path flattened)
                       "boom"))))))

(ert-deftest kafka-logs-line->json-line-array-payload-test ()
  (with-temp-buffer
    (setq-local kafka-logs--viewer-connection "prod")
    (let* ((line
            (concat
             "{\"topic\":\"orders\",\"partition\":2,\"offset\":9,"
             "\"payload\":[{\"externalTransactionId\":\"c12345d6789\","
             "\"loyaltyAccountId\":1}]}"))
           (json-line (kafka-logs--line->json-line line))
           (parsed (json-parse-string json-line :object-type 'alist :array-type 'list))
           (payload (alist-get 'payload parsed))
           (first (car payload)))
      (should (equal (alist-get 'topic parsed) "orders"))
      (should (listp payload))
      (should (equal (alist-get 'externalTransactionId first) "c12345d6789"))
      (should (equal (alist-get 'loyaltyAccountId first) 1)))))

(ert-deftest kafka-logs-line->json-line-headers-test ()
  (with-temp-buffer
    (let* ((line
            (concat
             "{\"topic\":\"orders\",\"partition\":2,\"offset\":9,"
             "\"key\":\"order-1\","
             "\"headers\":[\"trace-id\",\"abc\","
             "\"empty\",\"\",\"nil-header\",null,"
             "\"trace-id\",\"def\"],"
             "\"payload\":\"ok\"}"))
           (json-line (kafka-logs--line->json-line line))
           (parsed (json-parse-string json-line :object-type 'alist
                                      :array-type 'list))
           (headers (alist-get 'headers parsed)))
      (should (equal (alist-get 'key parsed) "order-1"))
      (should (equal (alist-get 'trace-id headers) '("abc" "def")))
      (should (equal (alist-get 'empty headers) ""))
      (should (assoc 'nil-header headers))
      (should (null (alist-get 'nil-header headers))))))

(ert-deftest kafka-logs-line->json-line-avro-envelope-key-test ()
  (with-temp-buffer
    (let* ((line
            (concat
             "{\"topic\":\"orders\",\"partition\":2,\"offset\":9,"
             "\"ts\":1700000000123,\"key_size\":7,\"key\":\"order-1\","
             "\"payload\":{\"level\":\"warn\",\"message\":\"boom\"}}"))
           (json-line (kafka-logs--line->json-line line))
           (parsed (json-parse-string json-line :object-type 'alist)))
      (should (equal (alist-get 'key parsed) "order-1"))
      (should (equal (alist-get 'level parsed) "warn"))
      (should (equal (alist-get 'payload parsed)
                     '((level . "warn") (message . "boom")))))))

(ert-deftest kafka-logs-line->json-line-avro-envelope-null-key-test ()
  (with-temp-buffer
    (let* ((line
            (concat
             "{\"topic\":\"orders\",\"partition\":2,\"offset\":9,"
             "\"ts\":1700000000123,\"key_size\":-1,\"key\":\"\","
             "\"payload\":{\"level\":\"warn\",\"message\":\"boom\"}}"))
           (json-line (kafka-logs--line->json-line line))
           (parsed (json-parse-string json-line :object-type 'alist)))
      (should-not (assoc 'key parsed))
      (should (equal (alist-get 'level parsed) "warn"))
      (should (equal (alist-get 'payload parsed)
                     '((level . "warn") (message . "boom")))))))

(ert-deftest kafka-logs-list-topics-parses-metadata-json-test ()
  (let ((kafka-logs-connection "dev")
        (kafka-logs-connections '(("dev" . (:brokers "localhost:9092")))))
    (cl-letf (((symbol-function 'kafka-logs--connection-base-args)
               (lambda () '("-b" "localhost:9092")))
              ((symbol-function 'kafka-logs--run-kcat-lines)
               (lambda (_args)
                 '("{\"topics\":[{\"topic\":\"zeta\"},{\"topic\":\"alpha\"},{\"topic\":\"alpha\"}]}"))))
      (should (equal (kafka-logs--list-topics)
                     '("alpha" "zeta"))))))

(ert-deftest kafka-logs-make-connection-replaces-existing-test ()
  (let ((kafka-logs-connections nil)
        (kafka-logs-connection nil))
    (kafka-logs-make-connection "dev" :brokers "localhost:9092")
    (kafka-logs-make-connection "dev" :brokers "localhost:9093")
    (should (equal kafka-logs-connection "dev"))
    (should (equal (length kafka-logs-connections) 1))
    (should (equal (plist-get (cdr (assoc "dev" kafka-logs-connections)) :brokers)
                   "localhost:9093"))))

(ert-deftest kafka-logs-normalize-json-paths-test ()
  (should (equal (kafka-logs--normalize-json-paths
                  '(" payload " "payload.log" "payload"))
                 '("payload" "payload.log")))
  (should-error (kafka-logs--normalize-json-paths "payload")
                :type 'user-error))

(ert-deftest kafka-logs-normalize-message-path-test ()
  (should (equal (kafka-logs--normalize-message-path " payload.data.name ")
                 "payload.data.name"))
  (should-error (kafka-logs--normalize-message-path "")
                :type 'user-error)
  (should-error (kafka-logs--normalize-message-path nil)
                :type 'user-error))

(ert-deftest kafka-logs-normalize-extra-paths-test ()
  (should (equal (kafka-logs--normalize-extra-paths
                  '(" topic " "payload.service" "topic"))
                 '("topic" "payload.service")))
  (should-error (kafka-logs--normalize-extra-paths "topic")
                :type 'user-error))

(ert-deftest kafka-logs-make-viewer-buffer-passes-json-paths-test ()
  (let ((kafka-logs-connection "prod")
        (kafka-logs-topic "orders")
        (kafka-logs-stream t)
        (kafka-logs-time-range nil)
        (kafka-logs-filter nil)
        (kafka-logs-payload-format nil)
        (kafka-logs-json-paths '("payload" "payload.log"))
        (kafka-logs-extra-paths '("topic" "payload.service"))
        (kafka-logs-message-path "payload.data.name")
        captured-args
        viewer-buffer)
    (cl-letf (((symbol-function 'json-log-viewer-make-buffer)
               (lambda (_buffer-name &rest args)
                 (setq captured-args args)
                 (setq viewer-buffer (generate-new-buffer "*kafka-logs-viewer-test*"))
                 (with-current-buffer viewer-buffer
                   (special-mode))
                 viewer-buffer)))
      (unwind-protect
          (let ((buffer (kafka-logs--make-viewer-buffer)))
            (should (eq buffer viewer-buffer))
            (should (equal (plist-get captured-args :message-path)
                           "payload.data.name"))
            (should (equal (plist-get captured-args :extra-paths)
                           '("topic" "payload.service")))
            (should (equal (plist-get captured-args :json-paths)
                           '("payload" "payload.log")))
            (with-current-buffer buffer
              (should (equal kafka-logs--viewer-message-path
                             "payload.data.name"))
              (should (equal kafka-logs--viewer-json-paths
                             '("payload" "payload.log")))))
        (when (buffer-live-p viewer-buffer)
          (kill-buffer viewer-buffer))))))

(ert-deftest kafka-logs-make-viewer-buffer-uses-selected-viewer-test ()
  (let* ((viewer (generate-new-buffer "*kafka-logs-shared-viewer-test*"))
         (kafka-logs-viewer-buffer (buffer-name viewer))
         (kafka-logs-connection "prod")
         (kafka-logs-topic "orders")
         (kafka-logs-stream t)
         (kafka-logs-time-range nil)
         (kafka-logs-filter nil)
         (kafka-logs-payload-format 'json)
         (kafka-logs-value-format 'json)
         (kafka-logs--detected-value-format nil)
         (kafka-logs-json-paths '("payload"))
         (kafka-logs-extra-paths '("topic"))
         (kafka-logs-message-path "payload.message")
         ready-buffer)
    (unwind-protect
        (progn
          (with-current-buffer viewer
            (json-log-viewer-mode))
          (cl-letf (((symbol-function 'json-log-viewer-make-buffer)
                     (lambda (&rest _args)
                       (error "should not create a dedicated viewer")))
                    ((symbol-function 'json-log-viewer-run-when-ready)
                     (lambda (buffer function)
                       (setq ready-buffer buffer)
                       (with-current-buffer buffer
                         (funcall function)))))
            (let ((buffer (kafka-logs--make-viewer-buffer
                           (lambda ()
                             (setq-local kafka-logs--process 'ready)))))
              (should (eq buffer viewer))
              (should (eq ready-buffer viewer))
              (with-current-buffer viewer
                (should (eq kafka-logs--process 'ready))
                (should (equal kafka-logs--viewer-connection "prod"))
                (should (equal kafka-logs--viewer-topic "orders"))
                (should (equal kafka-logs--viewer-message-path
                               "payload.message"))))))
      (when (buffer-live-p viewer)
        (kill-buffer viewer)))))

(ert-deftest kafka-logs-stream-to-buffer-uses-source-payload-format-test ()
  (let* ((viewer (generate-new-buffer "*kafka-logs-composite-source-format-test*"))
         (kafka-logs-connections '(("prod" :brokers "localhost:9092")))
         (kafka-logs-kcat "kcat")
         wrapper-command)
    (unwind-protect
        (progn
          (with-current-buffer viewer
            (composite-log-viewer-mode)
            (setq-local kafka-logs--viewer-payload-format nil))
          (cl-letf (((symbol-function 'kafka-logs--consume-args)
                     (lambda () '("-b" "localhost:9092" "-t" "orders")))
                    ((symbol-function 'kafka-logs--command-with-filter)
                     (lambda (args &optional _line-buffered)
                       (cons "kcat" args)))
                    ((symbol-function 'json-log-viewer-run-when-ready)
                     (lambda (buffer function)
                       (with-current-buffer buffer
                         (funcall function))))
                    ((symbol-function 'json-log-viewer-worker-socket-path)
                     (lambda (&optional _buffer) "/tmp/kafka-test.sock"))
                    ((symbol-function 'make-process)
                     (lambda (&rest args)
                       (setq wrapper-command (plist-get args :command))
                       :fake-process))
                    ((symbol-function 'set-process-sentinel) #'ignore)
                    ((symbol-function 'set-process-query-on-exit-flag) #'ignore)
                    ((symbol-function 'kafka-logs--add-buffer-process) #'ignore)
                    ((symbol-function 'message) #'ignore))
            (kafka-logs-stream-to-buffer
             viewer
             '(:connection "prod"
               :topic "orders"
               :value-format json
               :payload-format json
               :message-path "payload.type"
               :extra-paths ("topic" "key" "payload.type")
               :json-paths ("payload")))
            (should (member "--payload-format" wrapper-command))
            (should (equal (cadr (member "--payload-format" wrapper-command))
                           "json"))
            (should-not (member "--source" wrapper-command))
            (should (equal (cadr (member "--source-id" wrapper-command))
                           "kafka:prod/orders"))))
      (when (buffer-live-p viewer)
        (kill-buffer viewer)))))

(ert-deftest kafka-logs-stream-to-buffer-disambiguates-duplicate-source-id-test ()
  (let* ((viewer (generate-new-buffer "*kafka-logs-composite-source-id-test*"))
         (kafka-logs-connections '(("prod" :brokers "localhost:9092")))
         (kafka-logs-kcat "kcat")
         commands)
    (unwind-protect
        (progn
          (with-current-buffer viewer
            (composite-log-viewer-mode))
          (cl-letf (((symbol-function 'kafka-logs--consume-args)
                     (lambda () '("-b" "localhost:9092" "-t" "orders")))
                    ((symbol-function 'kafka-logs--command-with-filter)
                     (lambda (args &optional _line-buffered)
                       (cons "kcat" args)))
                    ((symbol-function 'json-log-viewer-run-when-ready)
                     (lambda (buffer function)
                       (with-current-buffer buffer
                         (funcall function))))
                    ((symbol-function 'json-log-viewer-worker-socket-path)
                     (lambda (&optional _buffer) "/tmp/kafka-test.sock"))
                    ((symbol-function 'make-process)
                     (lambda (&rest args)
                       (push (plist-get args :command) commands)
                       :fake-process))
                    ((symbol-function 'set-process-sentinel) #'ignore)
                    ((symbol-function 'set-process-query-on-exit-flag) #'ignore)
                    ((symbol-function 'kafka-logs--add-buffer-process) #'ignore)
                    ((symbol-function 'message) #'ignore))
            (kafka-logs-stream-to-buffer
             viewer
             '(:connection "prod"
               :topic "orders"
               :value-format json
               :payload-format json
               :message-path "payload.type"))
            (kafka-logs-stream-to-buffer
             viewer
             '(:connection "prod"
               :topic "orders"
               :value-format json
               :payload-format json
               :message-path "payload")))
          (setq commands (nreverse commands))
          (should (equal (cadr (member "--source-id" (nth 0 commands)))
                         "kafka:prod/orders"))
          (should (equal (cadr (member "--source-id" (nth 1 commands)))
                         "kafka:prod/orders#2"))
          (with-current-buffer viewer
            (should (equal
                     (plist-get
                      (gethash "kafka:prod/orders" json-log-viewer--source-configs)
                      :message-path)
                     "payload.type"))
            (should (equal
                     (plist-get
                      (gethash "kafka:prod/orders#2" json-log-viewer--source-configs)
                      :message-path)
                     "payload"))))
      (when (buffer-live-p viewer)
        (kill-buffer viewer)))))

(ert-deftest kafka-logs-composite-real-worker-uses-per-topic-message-path-test ()
  (let* ((viewer (json-log-viewer-make-buffer
                  "*kafka-logs-composite-real-worker-test*"
                  :timestamp-path "timestamp"
                  :level-path "level"
                  :message-path "message"
                  :mode #'composite-log-viewer-mode))
         (kafka-logs-connections '(("cluster-a" :brokers "localhost:9092")))
         (line-a (json-serialize
                  '(:topic "topic-a"
                    :ts 1781447361217
                    :key "record-key"
                    :payload "{\"type\":\"ExampleEvent\",\"requestId\":\"request-2\"}")))
         (line-b (json-serialize
                  '(:topic "topic-b"
                    :ts 1781447361218
                    :key "record-key"
                    :payload "{\"message\":\"payload message\"}")))
         (call 0))
    (unwind-protect
        (progn
          (cl-letf (((symbol-function 'kafka-logs--consume-args)
                     (lambda () nil))
                    ((symbol-function 'kafka-logs--command-with-filter)
                     (lambda (_args &optional _line-buffered)
                       (setq call (1+ call))
                       (list "printf" "%s\n" (if (= call 1) line-a line-b))))
                    ((symbol-function 'message) #'ignore))
            (kafka-logs-stream-to-buffer
             viewer
             '(:connection "cluster-a"
               :topic "topic-a"
               :value-format json
               :payload-format json
               :json-paths ("payload")
               :message-path "payload.type"
               :extra-paths ("topic" "key" "payload.type")))
            (kafka-logs-stream-to-buffer
             viewer
             '(:connection "cluster-a"
               :topic "topic-b"
               :value-format json
               :payload-format json
               :message-path "payload"
               :extra-paths ("topic" "key"))))
          (dotimes (_ 20)
            (accept-process-output nil 0.05))
          (with-current-buffer viewer
            (json-log-viewer--await-pull-complete)
            (let ((summary (buffer-substring-no-properties (point-min) (point-max))))
              (should (string-match-p
                       "\\[topic-a\\].*\\[record-key\\].*\\[ExampleEvent\\] ExampleEvent"
                       summary))
              (should (string-match-p
                       "\\[topic-b\\].*\\[record-key\\].*{\"message\":\"payload message\"}"
                       summary)))))
      (when (buffer-live-p viewer)
        (kill-buffer viewer)))))

(ert-deftest kafka-logs-transient-viewer-buffer-follows-current-composite-test ()
  (let ((viewer (generate-new-buffer "*kafka-logs-composite-current-test*"))
        (kafka-logs-viewer-buffer "stale"))
    (unwind-protect
        (progn
          (with-current-buffer viewer
            (composite-log-viewer-mode)
            (kafka-logs--set-viewer-buffer-from-current-buffer))
          (should (equal kafka-logs-viewer-buffer (buffer-name viewer)))
          (with-temp-buffer
            (json-log-viewer-mode)
            (kafka-logs--set-viewer-buffer-from-current-buffer))
          (should-not kafka-logs-viewer-buffer))
      (when (buffer-live-p viewer)
        (kill-buffer viewer)))))

(ert-deftest kafka-logs-stream-drain-batches-output-test ()
  (let ((kafka-logs-stream-max-lines-per-batch 2)
        (captured nil)
        (buffer (generate-new-buffer "*kafka-logs-stream-drain-test*")))
    (unwind-protect
        (with-current-buffer buffer
          (setq-local kafka-logs--pending-fragment "")
          (setq-local kafka-logs--stream-chunks-in nil)
          (setq-local kafka-logs--stream-chunks-out nil)
          (setq-local kafka-logs--stream-pending-lines nil)
          (setq-local kafka-logs--stream-drain-timer nil)
          (cl-letf (((symbol-function 'kafka-logs--stream-schedule-drain)
                    (lambda () nil))
                    ((symbol-function 'kafka-logs--line->json-line)
                     (lambda (line)
                       (unless (equal line "")
                         line)))
                    ((symbol-function 'json-log-viewer-push)
                     (lambda (_buffer lines)
                       (push lines captured))))
            (kafka-logs--stream-enqueue-chunk "line-1\nline-2\nline-3\n")
            (should (equal captured nil))
            (kafka-logs--stream-drain nil)
            (should (equal (car captured) '("line-1" "line-2")))
            (kafka-logs--stream-drain nil)
            (should (equal (car captured) '("line-3")))
            (should (kafka-logs--stream-queue-empty-p))))
      (when (buffer-live-p buffer)
        (with-current-buffer buffer
          (when (timerp kafka-logs--stream-drain-timer)
            (cancel-timer kafka-logs--stream-drain-timer)))
        (kill-buffer buffer)))))

(provide 'kafka-logs-test)
;;; kafka-logs-test.el ends here
