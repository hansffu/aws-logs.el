;;; kafka-logs-test.el --- kafka-logs unit tests -*- lexical-binding: t; -*-

(require 'ert)
(require 'cl-lib)
(require 'json)

(require 'kafka-logs)

(defvar kafka-logs-connection)
(defvar kafka-logs-topic)
(defvar kafka-logs-stream)
(defvar kafka-logs-time-range)
(defvar kafka-logs-filter)
(defvar kafka-logs-max-messages)
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
        (kafka-logs-max-messages nil))
    (cl-letf (((symbol-function 'kafka-logs--connection-base-args)
               (lambda () '("-b" "localhost:9092"))))
      (should (equal (kafka-logs--consume-args)
                     '("-b" "localhost:9092"
                       "-C" "-J" "-u" "-q" "-t" "orders"
                       "-o" "end"))))))

(ert-deftest kafka-logs-consume-args-time-span-test ()
  (let ((kafka-logs-topic "orders")
        (kafka-logs-stream nil)
        (kafka-logs-time-range '("1000" . "2000"))
        (kafka-logs-max-messages 75))
    (cl-letf (((symbol-function 'kafka-logs--connection-base-args)
               (lambda () '("-b" "localhost:9092"))))
      (should (equal (kafka-logs--consume-args)
                     '("-b" "localhost:9092"
                       "-C" "-J" "-u" "-q" "-t" "orders"
                       "-o" "s@1000"
                       "-o" "e@2000"
                       "-e"
                       "-c" "75"))))))

(ert-deftest kafka-logs-line->json-line-json-payload-test ()
  (with-temp-buffer
    (setq-local kafka-logs--viewer-connection "prod")
    (let* ((line
            (concat
             "{\"topic\":\"orders\",\"partition\":2,\"offset\":9,"
             "\"ts\":1700000000123,\"key\":\"order-1\","
             "\"payload\":\"{\\\"level\\\":\\\"warn\\\",\\\"message\\\":\\\"boom\\\"}\"}"))
           (json-line (kafka-logs--line->json-line line))
           (parsed (json-parse-string json-line :object-type 'alist)))
      (should (equal (alist-get 'connection parsed) "prod"))
      (should (equal (alist-get 'topic parsed) "orders"))
      (should (equal (alist-get 'partition parsed) 2))
      (should (equal (alist-get 'offset parsed) 9))
      (should (equal (alist-get 'key parsed) "order-1"))
      (should (equal (alist-get 'level parsed) "warn"))
      (should (equal (alist-get 'message parsed) "boom"))
      (should (equal (alist-get 'timestamp parsed)
                     (kafka-logs--epoch-ms->iso8601 1700000000123))))))

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

(provide 'kafka-logs-test)
;;; kafka-logs-test.el ends here
