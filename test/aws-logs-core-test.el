;;; aws-logs-core-test.el --- Core unit tests -*- lexical-binding: t; -*-

(require 'ert)
(require 'cl-lib)

(require 'json-log-viewer)
(require 'json-log-viewer-async-worker)
(require 'aws-logs-insights)
(require 'aws-logs-tail)

(defvar aws-logs-custom-time-range)
(defvar aws-logs-time-range)
(defvar aws-logs-query)

(defun aws-logs-core-test--make-viewer (buffer-name initial-lines &rest args)
  "Create json-log-viewer BUFFER-NAME, optionally seeded with INITIAL-LINES."
  (let ((buf (apply #'json-log-viewer-make-buffer buffer-name args)))
    (when initial-lines
      (json-log-viewer-push buf initial-lines)
      (with-current-buffer buf
        (json-log-viewer--async-await-pending-count 0)))
    buf))

(ert-deftest aws-logs-insights-parse-time-range-to-seconds-test ()
  (should (= 30 (aws-logs--insights-parse-time-range-to-seconds "30s")))
  (should (= 600 (aws-logs--insights-parse-time-range-to-seconds "10m")))
  (should (= 7200 (aws-logs--insights-parse-time-range-to-seconds "2h")))
  (should (= 86400 (aws-logs--insights-parse-time-range-to-seconds "1d")))
  (should (= 1209600 (aws-logs--insights-parse-time-range-to-seconds "2w")))
  (should-not (aws-logs--insights-parse-time-range-to-seconds ""))
  (should-not (aws-logs--insights-parse-time-range-to-seconds "10"))
  (should-not (aws-logs--insights-parse-time-range-to-seconds "foo")))

(ert-deftest aws-logs-insights-time-window-since-test ()
  (let ((aws-logs-custom-time-range nil)
        (aws-logs-time-range "10m"))
    (cl-letf (((symbol-function 'float-time) (lambda (&optional _time) 1000.0)))
      (let ((window (aws-logs--insights-time-window)))
        (should (= (nth 0 window) 400))
        (should (= (nth 1 window) 1000))))))

(ert-deftest aws-logs-insights-time-window-custom-range-test ()
  (let ((aws-logs-custom-time-range '("2026-01-01T00:00:00+0000" . "2026-01-01T00:10:00+0000"))
        (aws-logs-time-range "10m"))
    (let ((window (aws-logs--insights-time-window)))
      (should (< (nth 0 window) (nth 1 window))))))

(ert-deftest aws-logs-tail-ecs-normalize-lines-with-pending-test ()
  (let* ((first (aws-logs--tail-ecs-normalize-lines-with-pending
                 '("2026-01-01T00:00:00Z /aws/ecs/demo {\"message\":\"hello\",")
                 nil))
         (second (aws-logs--tail-ecs-normalize-lines-with-pending
                  '("\"level\":\"info\"}")
                  (cdr first)))
         (normalized (car second))
         (parsed (json-parse-string (car normalized) :object-type 'alist)))
    (should (null (car first)))
    (should (= (length normalized) 1))
    (should (equal (alist-get 'message parsed) "hello"))
    (should (equal (alist-get 'level parsed) "info"))
    (should-not (cdr second))))

(ert-deftest json-log-viewer-normalize-direction-test ()
  (should (eq (json-log-viewer--normalize-direction 'newest-first) 'newest-first))
  (should (eq (json-log-viewer--normalize-direction 'desc) 'newest-first))
  (should (eq (json-log-viewer--normalize-direction 'oldest-first) 'oldest-first))
  (should (eq (json-log-viewer--normalize-direction 'asc) 'oldest-first))
  (should-error (json-log-viewer--normalize-direction 'invalid) :type 'user-error))

(ert-deftest json-log-viewer-json-lines->entries-test ()
  (let* ((lines '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"a\"}"
                  "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"msg\":\"b\"}"))
         (entries+next (json-log-viewer--json-lines->entries lines "timestamp" 10))
         (entries (car entries+next))
         (next-id (cdr entries+next)))
    (should (= 2 (length entries)))
    (should (= 12 next-id))
    (should (= 10 (plist-get (car entries) :id)))
    (should (= 11 (plist-get (cadr entries) :id)))))

(ert-deftest json-log-viewer-resolve-path-renders-json-on-one-line-test ()
  (let* ((parsed (json-parse-string
                  "{\"payload\":{\"data\":{\"name\":\"alice\"},\"events\":[{\"id\":1},{\"id\":2}]}}"
                  :object-type 'alist
                  :array-type 'list))
         (payload (json-log-viewer-shared--resolve-path parsed "payload"))
         (data (json-log-viewer-shared--resolve-path parsed "payload.data"))
         (events (json-log-viewer-shared--resolve-path parsed "payload.events"))
         (name (json-log-viewer-shared--resolve-path parsed "payload.data.name")))
    (should (string-match-p "\\`{.*}\\'" payload))
    (should (string-match-p "\"name\":\"alice\"" payload))
    (should-not (string-match-p "\n" payload))
    (should (string-match-p "\\`{.*}\\'" data))
    (should (string-match-p "\"name\":\"alice\"" data))
    (should-not (string-match-p "\n" data))
    (should (string-match-p "\\`\\[.*\\]\\'" events))
    (should (string-match-p "\"id\":1" events))
    (should-not (string-match-p "\n" events))
    (should (equal name "alice"))))

(ert-deftest json-log-viewer-resolve-path-supports-escaped-dot-keys-test ()
  (let* ((parsed (json-parse-string
                  "{\"payload\":{\"@timestamp\":\"2026-02-25T16:14:14.455Z\",\"log.level\":\"INFO\",\"service.name\":\"my-service\",\"log.logger\":\"com.example.MyClass\",\"log\":{\"origin\":{\"file\":{\"name\":\"MyClass.kt\"}}}}}"
                  :object-type 'alist
                  :array-type 'list))
         (level (json-log-viewer-shared--resolve-path parsed "payload.log\\.level"))
         (service (json-log-viewer-shared--resolve-path parsed "payload.service\\.name"))
         (logger (json-log-viewer-shared--resolve-path parsed "payload.log\\.logger"))
         (origin-file (json-log-viewer-shared--resolve-path parsed "payload.log.origin.file.name")))
    (should (equal level "INFO"))
    (should (equal service "my-service"))
    (should (equal logger "com.example.MyClass"))
    (should (equal origin-file "MyClass.kt"))))

(ert-deftest json-log-viewer-resolve-path-matches-details-path-for-dotted-keys-test ()
  (let* ((dotted (json-parse-string
                  "{\"payload\":{\"log.level\":\"INFO\"}}"
                  :object-type 'alist
                  :array-type 'list))
         (nested (json-parse-string
                  "{\"payload\":{\"log\":{\"level\":\"INFO\"}}}"
                  :object-type 'alist
                  :array-type 'list)))
    (should (equal (json-log-viewer-shared--resolve-path dotted "payload.log.level") "INFO"))
    (should (equal (json-log-viewer-shared--resolve-path nested "payload.log.level") "INFO"))))

(ert-deftest json-log-viewer-async-worker-resolves-escaped-dot-keys-test ()
  (let* ((line "{\"timestamp\":\"2026-02-25T16:14:14.455Z\",\"payload\":{\"log.level\":\"INFO\",\"message\":\"This is the log message\",\"service.name\":\"my-service\",\"log.logger\":\"com.example.MyClass\"}}")
         (config '(:timestamp-path "timestamp"
                   :level-path "payload.log\\.level"
                   :message-path "payload.message"
                   :extra-paths ("payload.service\\.name" "payload.log\\.logger")
                   :json-paths nil))
         (pair (json-log-viewer-async-worker--line->entry+detail line 42 config))
         (entry (car pair)))
    (should (equal (plist-get entry :level) "INFO"))
    (should (equal (plist-get entry :message) "This is the log message"))
    (should (equal (plist-get entry :extras)
                   '("my-service" "com.example.MyClass")))))

(ert-deftest json-log-viewer-async-worker-resolves-unescaped-dotted-keys-test ()
  (let* ((config '(:timestamp-path "timestamp"
                   :level-path "payload.log.level"
                   :message-path "payload.message"
                   :extra-paths nil
                   :json-paths nil))
         (entry-dotted
          (car (json-log-viewer-async-worker--line->entry+detail
                "{\"timestamp\":\"2026-02-25T16:14:14.455Z\",\"payload\":{\"log.level\":\"INFO\",\"message\":\"msg\"}}"
                1
                config)))
         (entry-nested
          (car (json-log-viewer-async-worker--line->entry+detail
                "{\"timestamp\":\"2026-02-25T16:14:14.455Z\",\"payload\":{\"log\":{\"level\":\"INFO\"},\"message\":\"msg\"}}"
                2
                config))))
    (should (equal (plist-get entry-dotted :level) "INFO"))
    (should (equal (plist-get entry-nested :level) "INFO"))))

(ert-deftest json-log-viewer-async-worker-rerender-caps-published-entries-test ()
  (let (published)
    (cl-letf (((symbol-function 'async-job-queue-worker-publish)
               (lambda (payload) (push payload published))))
      (unwind-protect
          (progn
            (json-log-viewer-async-worker-init '(:max-entries 3 :chunk-size 2))
            (dotimes (i 5)
              (json-log-viewer-async-worker-process-log-ingestor-job
               (list :op 'ingest
                     :line (format "{\"timestamp\":\"2026-01-01T00:00:0%dZ\",\"msg\":\"m-%d\"}"
                                   i i)
                     :timestamp-path "timestamp"
                     :message-path "msg")))
            (setq published nil)
            (json-log-viewer-async-worker-process-log-ingestor-job
             (list :op 'rerender
                   :timestamp-path "timestamp"
                   :message-path "msg"))
            (setq published (nreverse published))
            (should (equal (plist-get (car published) :cmd) 'clear))
            (let* ((commands (cdr published))
                   (entries (apply #'append
                                   (mapcar (lambda (cmd) (plist-get cmd :entries))
                                           commands)))
                   (messages (mapcar (lambda (entry) (plist-get entry :message))
                                     entries)))
              (should (= (length entries) 3))
              (should (equal messages '("m-2" "m-3" "m-4")))))
        (ignore-errors (json-log-viewer-async-worker-teardown))))))

(ert-deftest json-log-viewer-async-worker-rerender-orders-by-timestamp-test ()
  (let (published)
    (cl-letf (((symbol-function 'async-job-queue-worker-publish)
               (lambda (payload) (push payload published))))
      (unwind-protect
          (progn
            (json-log-viewer-async-worker-init '(:max-entries 10 :chunk-size 10))
            ;; Ingest out of timestamp order; replay should order by timestamp.
            (json-log-viewer-async-worker-process-log-ingestor-job
             (list :op 'ingest
                   :line "{\"timestamp\":\"2026-01-01T00:00:10Z\",\"msg\":\"late\"}"
                   :timestamp-path "timestamp"
                   :message-path "msg"))
            (json-log-viewer-async-worker-process-log-ingestor-job
             (list :op 'ingest
                   :line "{\"timestamp\":\"2026-01-01T00:00:05Z\",\"msg\":\"early\"}"
                   :timestamp-path "timestamp"
                   :message-path "msg"))
            (json-log-viewer-async-worker-process-log-ingestor-job
             (list :op 'ingest
                   :line "{\"timestamp\":\"2026-01-01T00:00:08Z\",\"msg\":\"middle\"}"
                   :timestamp-path "timestamp"
                   :message-path "msg"))
            (setq published nil)
            (json-log-viewer-async-worker-process-log-ingestor-job
             (list :op 'rerender
                   :timestamp-path "timestamp"
                   :message-path "msg"))
            (setq published (nreverse published))
            (let* ((entries (apply #'append
                                   (mapcar (lambda (cmd) (plist-get cmd :entries))
                                           (cdr published))))
                   (messages (mapcar (lambda (entry) (plist-get entry :message))
                                     entries)))
              (should (equal messages '("early" "middle" "late")))))
        (ignore-errors (json-log-viewer-async-worker-teardown))))))

(ert-deftest json-log-viewer-parse-time-preserves-subsecond-order-test ()
  (let ((t1 (json-log-viewer--parse-time "2026-01-01T00:00:00.123Z"))
        (t2 (json-log-viewer--parse-time "2026-01-01T00:00:00.124Z")))
    (should (numberp t1))
    (should (numberp t2))
    (should (< t1 t2))
    (should (> (- t2 t1) 0.0009))
    (should (< (- t2 t1) 0.0011))))

(ert-deftest json-log-viewer-json-syntax-mode-default-test ()
  (should (eq json-log-viewer-json-syntax-mode 'json-ts-mode)))

(ert-deftest json-log-viewer-json-syntax-mode-fallback-test ()
  (let ((json-log-viewer-json-syntax-mode 'definitely-not-a-real-mode))
    (let ((value (json-log-viewer--json-value->pretty-string
                  '((service . "orders") (log . ((level . "ERROR")))))))
      (should (string-match-p "\"service\": \"orders\"" value))
      (should (get-text-property 0 'json-log-viewer-json-block value)))
    (let ((value (json-log-viewer--json-value->pretty-string
                  '(((externalTransactionId . "c12345d6789")
                     (loyaltyAccountId . 1))))))
      (should (string-match-p "\"externalTransactionId\": \"c12345d6789\"" value))
      (should (string-match-p "\"loyaltyAccountId\": 1" value))
      (should (get-text-property 0 'json-log-viewer-json-block value)))))

(ert-deftest json-log-viewer-sqlite-storage-offloads-hidden-data-test ()
  (let* ((buf (aws-logs-core-test--make-viewer
               "*json-log-viewer-sqlite-storage-test*"
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"hello\",\"payload\":{\"service\":\"orders\",\"log\":{\"level\":\"ERROR\"}}}")
               :timestamp-path "timestamp"
               :level-path "payload.log.level"
               :message-path "msg"
               :json-paths '("payload")))
         sqlite-file)
    (unwind-protect
        (with-current-buffer buf
          (setq sqlite-file json-log-viewer--sqlite-file)
          (should (stringp sqlite-file))
          (should (file-exists-p sqlite-file))
          (let ((entry-ov (car json-log-viewer--entry-overlays)))
            (should entry-ov)
            (should-not (overlay-get entry-ov 'json-log-viewer-entry-fields))
            (should-not (overlay-get entry-ov 'json-log-viewer-filter-text))
            (json-log-viewer-toggle-entry)
            (let ((text (buffer-substring-no-properties (point-min) (point-max))))
              (should (string-match-p "payload:\n{" text))
              (should (string-match-p "\"service\": \"orders\"" text)))))
      (when (buffer-live-p buf)
        (kill-buffer buf))
      (when sqlite-file
        (should-not (file-exists-p sqlite-file))))))

(ert-deftest json-log-viewer-stream-push-respects-auto-follow-test ()
  (let* ((buf (aws-logs-core-test--make-viewer
               "*json-log-viewer-follow-test*"
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"a\"}")
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg")))
    (unwind-protect
        (with-current-buffer buf
          (goto-char (point-min))
          (setq-local json-log-viewer--auto-follow nil)
          (json-log-viewer-push
           buf
           '("{\"timestamp\":\"2026-01-01T00:00:01Z\",\"msg\":\"b\"}"))
          (should (< (point) (point-max)))
          (goto-char (point-min))
          (setq-local json-log-viewer--auto-follow t)
          (json-log-viewer-push
           buf
           '("{\"timestamp\":\"2026-01-01T00:00:02Z\",\"msg\":\"c\"}"))
          (should (= (point) (point-max))))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest json-log-viewer-stream-message-path-object-renders-json-test ()
  (let* ((buf (aws-logs-core-test--make-viewer
               "*json-log-viewer-stream-message-path-test*"
               nil
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "payload.data")))
    (unwind-protect
        (with-current-buffer buf
          (json-log-viewer-push
           buf
           '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"payload\":{\"data\":{\"name\":\"alice\",\"roles\":[\"admin\"]}}}"))
          (goto-char (point-min))
          (let ((summary (buffer-substring-no-properties
                          (line-beginning-position)
                          (line-end-position))))
            (should (string-match-p "\"name\":\"alice\"" summary))
            (should (string-match-p "\"roles\":\\[\"admin\"\\]" summary))
            (should-not (string-match-p "\n" summary))))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest json-log-viewer-log-ingestor-job-carries-line-test ()
  (let ((buf (aws-logs-core-test--make-viewer
              "*json-log-viewer-reserve-ids-test*"
              nil
              :timestamp-path "timestamp"
              :level-path "level"
              :message-path "msg")))
    (unwind-protect
        (with-current-buffer buf
          (let* ((line-a "{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"a\"}")
                 (line-b "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"msg\":\"b\"}")
                 (job-a (json-log-viewer--make-log-ingestor-async-job 'ingest line-a))
                 (job-b (json-log-viewer--make-log-ingestor-async-job 'ingest line-b)))
            (should (eq (plist-get job-a :op) 'ingest))
            (should (equal (plist-get job-a :line) line-a))
            (should (equal (plist-get job-b :line) line-b))
            (should (stringp (plist-get job-a :worker-file)))))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest json-log-viewer-lazy-details-render-on-toggle-test ()
  (let* ((buf (aws-logs-core-test--make-viewer
               "*json-log-viewer-lazy-details-test*"
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"level\":\"info\",\"msg\":\"hello\",\"meta\":{\"service\":\"orders\"}}")
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg")))
    (unwind-protect
        (with-current-buffer buf
          (goto-char (point-min))
          (let ((entry-ov (car json-log-viewer--entry-overlays)))
            (should entry-ov)
            (should-not (overlay-get entry-ov 'json-log-viewer-entry-expanded))
            (should-not (string-match-p "meta\\.service: orders"
                                        (buffer-substring-no-properties (point-min) (point-max))))
            (json-log-viewer-toggle-entry)
            (should (overlay-get entry-ov 'json-log-viewer-entry-expanded))
            (should (string-match-p "meta\\.service: orders"
                                    (buffer-substring-no-properties (point-min) (point-max))))
            (json-log-viewer-toggle-entry)
            (should-not (overlay-get entry-ov 'json-log-viewer-entry-expanded))
            (should-not (string-match-p "meta\\.service: orders"
                                        (buffer-substring-no-properties (point-min) (point-max))))))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest json-log-viewer-narrow-replays-matches-from-stored-json-test ()
  (let* ((buf (aws-logs-core-test--make-viewer
               "*json-log-viewer-narrow-replay-test*"
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"level\":\"info\",\"msg\":\"alpha\",\"payload\":{\"db\":\"orders\"}}"
                 "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"level\":\"info\",\"msg\":\"hidden-db-hit\",\"payload\":{\"db\":\"needle\"}}"
                 "{\"timestamp\":\"2026-01-01T00:00:02Z\",\"level\":\"info\",\"msg\":\"needle-visible\",\"payload\":{\"db\":\"payments\"}}")
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg")))
    (unwind-protect
        (with-current-buffer buf
          (setq json-log-viewer--filter-string "needle")
          (json-log-viewer--request-narrow-rebuild 'narrow "needle" t)
          (should (= json-log-viewer--entry-count 2))
          (let ((text (buffer-substring-no-properties (point-min) (point-max))))
            ;; Verifies narrowing uses stored JSON body, not only visible summary.
            (should (string-match-p "hidden-db-hit" text))
            (should (string-match-p "needle-visible" text))
            (should-not (string-match-p "alpha" text)))
          (json-log-viewer-push
           buf
           '("{\"timestamp\":\"2026-01-01T00:00:03Z\",\"level\":\"info\",\"msg\":\"hidden-db-new\",\"payload\":{\"db\":\"needle\"}}"
             "{\"timestamp\":\"2026-01-01T00:00:04Z\",\"level\":\"info\",\"msg\":\"ignore-new\",\"payload\":{\"db\":\"payments\"}}"))
          (should (= json-log-viewer--entry-count 3))
          (let ((text (buffer-substring-no-properties (point-min) (point-max))))
            (should (string-match-p "hidden-db-new" text))
            (should-not (string-match-p "ignore-new" text))))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest json-log-viewer-widen-replays-all-stored-json-rows-test ()
  (let* ((buf (aws-logs-core-test--make-viewer
               "*json-log-viewer-widen-replay-test*"
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"level\":\"info\",\"msg\":\"alpha\"}"
                 "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"level\":\"info\",\"msg\":\"hidden-db-hit\",\"payload\":{\"db\":\"needle\"}}"
                 "{\"timestamp\":\"2026-01-01T00:00:02Z\",\"level\":\"info\",\"msg\":\"gamma\"}")
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg")))
    (unwind-protect
        (with-current-buffer buf
          (setq json-log-viewer--filter-string "needle")
          (json-log-viewer--request-narrow-rebuild 'narrow "needle" t)
          (should (= json-log-viewer--entry-count 1))
          (json-log-viewer-widen)
          (should (= json-log-viewer--entry-count 3))
          (let ((text (buffer-substring-no-properties (point-min) (point-max))))
            (should (string-match-p "alpha" text))
            (should (string-match-p "hidden-db-hit" text))
            (should (string-match-p "gamma" text))))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest json-log-viewer-json-path-renders-payload-block-test ()
  (let* ((buf (aws-logs-core-test--make-viewer
               "*json-log-viewer-json-path-payload-test*"
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"hello\",\"payload\":\"{\\\"service\\\":\\\"orders\\\",\\\"log\\\":{\\\"level\\\":\\\"ERROR\\\"}}\"}")
               :timestamp-path "timestamp"
               :level-path "payload.log.level"
               :message-path "msg"
               :extra-paths '("payload.service")
               :json-paths '("payload"))))
    (unwind-protect
        (with-current-buffer buf
          (goto-char (point-min))
          (let ((summary-line (buffer-substring-no-properties
                               (line-beginning-position)
                               (line-end-position))))
            (should (string-match-p "ERROR" summary-line))
            (should (string-match-p "\\[orders\\]" summary-line)))
          (json-log-viewer-toggle-entry)
          (let ((text (buffer-substring-no-properties (point-min) (point-max))))
            (should (string-match-p "payload:\n{" text))
            (should (string-match-p "\"service\": \"orders\"" text))
            (should-not (string-match-p "payload\\.service:" text))
            (should-not (string-match-p "payload\\.log\\.level:" text))))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest json-log-viewer-json-path-renders-nested-block-test ()
  (let* ((buf (aws-logs-core-test--make-viewer
               "*json-log-viewer-json-path-nested-test*"
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"hello\",\"payload\":{\"service\":\"orders\",\"log\":{\"level\":\"ERROR\"}}}")
               :timestamp-path "timestamp"
               :level-path "payload.log.level"
               :message-path "msg"
               :json-paths '("payload.log"))))
    (unwind-protect
        (with-current-buffer buf
          (goto-char (point-min))
          (let ((summary-line (buffer-substring-no-properties
                               (line-beginning-position)
                               (line-end-position))))
            (should (string-match-p "ERROR" summary-line)))
          (json-log-viewer-toggle-entry)
          (let ((text (buffer-substring-no-properties (point-min) (point-max))))
            (should (string-match-p "payload\\.log:\n{" text))
            (should (string-match-p "\"level\": \"ERROR\"" text))
            (should-not (string-match-p "payload\\.log\\.level:" text))
            (should (string-match-p "payload\\.service: orders" text))))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest json-log-viewer-json-path-renders-array-block-test ()
  (let* ((buf (aws-logs-core-test--make-viewer
               "*json-log-viewer-json-path-array-test*"
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"hello\",\"payload\":[{\"externalTransactionId\":\"c12345d6789\",\"loyaltyAccountId\":1}]}")
               :timestamp-path "timestamp"
               :message-path "msg"
               :json-paths '("payload"))))
    (unwind-protect
        (with-current-buffer buf
          (goto-char (point-min))
          (json-log-viewer-toggle-entry)
          (let ((text (buffer-substring-no-properties (point-min) (point-max))))
            (should (string-match-p "payload:\n\\[" text))
            (should (string-match-p "\"externalTransactionId\": \"c12345d6789\"" text))
            (should (string-match-p "\"loyaltyAccountId\": 1" text))))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest json-log-viewer-stream-evicts-locally-at-cap-test ()
  (let* ((json-log-viewer-stream-chunk-size 50)
         (lines (let (acc)
                  (dotimes (i 301)
                    (push (format "{\"timestamp\":\"2026-01-01T%02d:%02d:%02dZ\",\"msg\":\"m-%d\"}"
                                  (mod (/ i 3600) 24)
                                  (mod (/ i 60) 60)
                                  (mod i 60)
                                  i)
                          acc))
                  (nreverse acc)))
         (buf (json-log-viewer-make-buffer
               "*json-log-viewer-chunk-evict-test*"
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg"
               :max-entries 300)))
    (unwind-protect
        (with-current-buffer buf
          (json-log-viewer-push buf lines)
          (should (= json-log-viewer--entry-count 251))
          (let ((text (buffer-substring-no-properties (point-min) (point-max))))
            (should (string-match-p "m-50" text))
            (should (string-match-p "m-300" text))
            (should-not (string-match-p "m-49" text))))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest json-log-viewer-cursor-move-disables-auto-follow-test ()
  (let* ((buf (aws-logs-core-test--make-viewer
               "*json-log-viewer-follow-disable-test*"
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"a\"}")
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg")))
    (unwind-protect
        (with-current-buffer buf
          (setq-local json-log-viewer--auto-follow t)
          (goto-char (point-min))
          (setq-local json-log-viewer--auto-follow-point-before-command (point))
          (goto-char (point-max))
          (let ((this-command 'next-line))
            (json-log-viewer--maybe-disable-auto-follow-after-command))
          (should-not json-log-viewer--auto-follow))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest json-log-viewer-shows-info-in-popup-instead-of-header-test ()
  (let* ((buf (aws-logs-core-test--make-viewer
               "*json-log-viewer-info-test*"
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"hello\"}")
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg"
               :header-lines-function (lambda (_state)
                                        (list (cons "Log group" "/aws/demo")))))
         (help-buffer-name (help-buffer))
         help-text)
    (unwind-protect
        (with-current-buffer buf
          (let ((buffer-text (buffer-substring-no-properties (point-min) (point-max))))
            (should (string-match-p "hello" buffer-text))
            (should-not (string-match-p "^Mode:" buffer-text))
            (should-not (string-match-p "^Messages:" buffer-text)))
          (json-log-viewer-show-info)
          (with-current-buffer help-buffer-name
            (setq help-text (buffer-substring-no-properties (point-min) (point-max))))
          (should (string-match-p "Bindings[[:space:]]+|[[:space:]]+Info" help-text))
          (should (string-match-p "Mode:[[:space:]]+streaming" help-text))
          (should (string-match-p "Log group:[[:space:]]+/aws/demo" help-text))
          (should (string-match-p "Messages:[[:space:]]+1" help-text))
          (should (string-match-p "show info" help-text)))
      (when (buffer-live-p buf)
        (kill-buffer buf))
      (when (buffer-live-p (get-buffer help-buffer-name))
        (kill-buffer help-buffer-name)))))

(ert-deftest json-log-viewer-popup-keybindings-can-replace-defaults-test ()
  (let* ((buf (aws-logs-core-test--make-viewer
               "*json-log-viewer-keys-test*"
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"hello\"}")
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg"))
         (help-buffer-name (help-buffer))
         help-text)
    (unwind-protect
        (with-current-buffer buf
          (let ((json-log-viewer--keybindings-function
                 (lambda () '(("zz" . "custom action")))))
            (json-log-viewer-show-info)
            (with-current-buffer help-buffer-name
              (setq help-text (buffer-substring-no-properties (point-min) (point-max)))))
          (should (string-match-p "zz[[:space:]]+custom action" help-text))
          (should-not (string-match-p "TAB[[:space:]]+toggle entry" help-text)))
      (when (buffer-live-p buf)
        (kill-buffer buf))
      (when (buffer-live-p (get-buffer help-buffer-name))
        (kill-buffer help-buffer-name)))))

(provide 'aws-logs-core-test)
;;; aws-logs-core-test.el ends here
