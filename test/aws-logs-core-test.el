;;; aws-logs-core-test.el --- Core unit tests -*- lexical-binding: t; -*-

(require 'ert)
(require 'cl-lib)

(require 'json-log-viewer)
(require 'aws-logs-insights)
(require 'aws-logs-tail)

(defvar aws-logs-custom-time-range)
(defvar aws-logs-time-range)
(defvar aws-logs-query)
(defvar aws-logs-insights-refresh-overlap-seconds)

(ert-deftest aws-logs-insights-parse-time-range-to-seconds-test ()
  (should (= 30 (aws-logs--insights-parse-time-range-to-seconds "30s")))
  (should (= 600 (aws-logs--insights-parse-time-range-to-seconds "10m")))
  (should (= 7200 (aws-logs--insights-parse-time-range-to-seconds "2h")))
  (should (= 86400 (aws-logs--insights-parse-time-range-to-seconds "1d")))
  (should (= 1209600 (aws-logs--insights-parse-time-range-to-seconds "2w")))
  (should-not (aws-logs--insights-parse-time-range-to-seconds ""))
  (should-not (aws-logs--insights-parse-time-range-to-seconds "10"))
  (should-not (aws-logs--insights-parse-time-range-to-seconds "foo")))

(ert-deftest aws-logs-insights-window-context-since-test ()
  (let ((aws-logs-custom-time-range nil)
        (aws-logs-time-range "10m"))
    (cl-letf (((symbol-function 'float-time) (lambda (&optional _time) 1000.0)))
      (let ((ctx (aws-logs--insights-window-context)))
        (should (eq (plist-get ctx :mode) 'since))
        (should (= (plist-get ctx :start) 400))
        (should (= (plist-get ctx :end) 1000))
        (should (= (plist-get ctx :last-end) 1000))))))

(ert-deftest aws-logs-insights-window-context-custom-range-test ()
  (let ((aws-logs-custom-time-range '("2026-01-01T00:00:00+0000" . "2026-01-01T00:10:00+0000"))
        (aws-logs-time-range "10m"))
    (let ((ctx (aws-logs--insights-window-context)))
      (should (eq (plist-get ctx :mode) 'custom-range))
      (should (< (plist-get ctx :start) (plist-get ctx :end))))))

(ert-deftest aws-logs-insights-extra-summary-paths-test ()
  (should (equal (aws-logs--insights-extra-summary-paths '("service" "class"))
                 '("__summary_extra_0" "__summary_extra_1"))))

(ert-deftest aws-logs-insights-since-refresh-uses-base-start-test ()
  (with-temp-buffer
    (setq-local aws-logs--insights-refresh-context '(:mode since :start 100 :last-end 200))
    (setq-local aws-logs--insights-request-in-flight nil)
    (setq-local aws-logs--insights-log-group "group-a")
    (let ((aws-logs-insights-refresh-overlap-seconds 20)
          captured)
      (cl-letf (((symbol-function 'float-time) (lambda (&optional _time) 300.0))
                ((symbol-function 'aws-logs--insights-run-window-async)
                 (lambda (_buffer start end _on-success)
                   (setq captured (list start end)))))
        (aws-logs--insights-start-refresh-async (current-buffer)))
      (should (equal captured '(100 300))))))

(ert-deftest aws-logs-insights-run-window-uses-frozen-context-test ()
  (with-temp-buffer
    (setq-local aws-logs--insights-source-log-group "frozen-group")
    (setq-local aws-logs--insights-source-query "fields @timestamp | limit 5")
    (setq-local aws-logs--insights-source-global-args '("--region=us-east-1" "--profile=frozen"))
    (setq aws-logs-query "fields @timestamp | limit 1")
    (let (captured)
      (cl-letf (((symbol-function 'aws-logs--insights-run-query-window-async)
                 (lambda (_buffer _request-id _start _end global-args log-group query on-success _on-error)
                   (setq captured (list global-args log-group query))
                   (funcall on-success "qid" '((results . nil)))))
                ((symbol-function 'message) (lambda (&rest _args) nil)))
        (aws-logs--insights-run-window-async (current-buffer) 10 20
                                             (lambda (_query-id _payload) nil)))
      (should (equal captured
                     '(("--region=us-east-1" "--profile=frozen")
                       "frozen-group"
                       "fields @timestamp | limit 5"))))))

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
      (should (get-text-property 0 'json-log-viewer-json-block value)))))

(ert-deftest json-log-viewer-sqlite-storage-offloads-hidden-data-test ()
  (skip-unless (and (fboundp 'sqlite-available-p)
                    (sqlite-available-p)))
  (let* ((buf (json-log-viewer-make-buffer
               "*json-log-viewer-sqlite-storage-test*"
               :log-lines
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"hello\",\"payload\":{\"service\":\"orders\",\"log\":{\"level\":\"ERROR\"}}}")
               :timestamp-path "timestamp"
               :level-path "payload.log.level"
               :message-path "msg"
               :json-paths '("payload")
               :storage-backend 'sqlite
               :streaming t))
         sqlite-file)
    (unwind-protect
        (with-current-buffer buf
          (setq sqlite-file json-log-viewer--sqlite-file)
          (should (eq json-log-viewer--storage-backend 'sqlite))
          (should (stringp sqlite-file))
          (should (file-exists-p sqlite-file))
          (let ((entry-ov (car json-log-viewer--entry-overlays)))
            (should entry-ov)
            (should-not (overlay-get entry-ov 'json-log-viewer-entry-fields))
            (should-not (overlay-get entry-ov 'json-log-viewer-filter-text))
            (json-log-viewer-toggle-entry)
            (let ((text (buffer-substring-no-properties (point-min) (point-max))))
              (should (string-match-p "payload:\n{" text))
              (should (string-match-p "\"service\": \"orders\"" text))))
          (should (= (length (json-log-viewer-current-log-lines buf)) 1)))
      (when (buffer-live-p buf)
        (kill-buffer buf))
      (when sqlite-file
        (should-not (file-exists-p sqlite-file))))))

(ert-deftest json-log-viewer-json-refresh-async-sentinel-test ()
  (let* ((buf (json-log-viewer-make-buffer
               "*json-log-viewer-test*"
               :log-lines '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"a\"}")
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg"
               :refresh-function (lambda (_old-lines) :async)
               :streaming nil))
         result)
    (unwind-protect
        (with-current-buffer buf
          (setq result (json-log-viewer--json-refresh nil))
          (should (equal (plist-get result :entries) nil))
          (should (eq (plist-get result :replace) nil))
          (should (= (length (json-log-viewer-current-log-lines buf)) 1)))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest json-log-viewer-stream-push-respects-auto-follow-test ()
  (let* ((buf (json-log-viewer-make-buffer
               "*json-log-viewer-follow-test*"
               :log-lines '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"a\"}")
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg"
               :streaming t)))
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

(ert-deftest json-log-viewer-lazy-details-render-on-toggle-test ()
  (let* ((buf (json-log-viewer-make-buffer
               "*json-log-viewer-lazy-details-test*"
               :log-lines
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"level\":\"info\",\"msg\":\"hello\",\"meta\":{\"service\":\"orders\"}}")
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg"
               :streaming nil)))
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

(ert-deftest json-log-viewer-json-path-renders-payload-block-test ()
  (let* ((buf (json-log-viewer-make-buffer
               "*json-log-viewer-json-path-payload-test*"
               :log-lines
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"hello\",\"payload\":\"{\\\"service\\\":\\\"orders\\\",\\\"log\\\":{\\\"level\\\":\\\"ERROR\\\"}}\"}")
               :timestamp-path "timestamp"
               :level-path "payload.log.level"
               :message-path "msg"
               :extra-paths '("payload.service")
               :json-paths '("payload")
               :streaming nil)))
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
  (let* ((buf (json-log-viewer-make-buffer
               "*json-log-viewer-json-path-nested-test*"
               :log-lines
               '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"hello\",\"payload\":{\"service\":\"orders\",\"log\":{\"level\":\"ERROR\"}}}")
               :timestamp-path "timestamp"
               :level-path "payload.log.level"
               :message-path "msg"
               :json-paths '("payload.log")
               :streaming nil)))
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

(ert-deftest json-log-viewer-stream-evicts-in-chunks-test ()
  (let* ((json-log-viewer-stream-chunk-size 100)
         (lines (let (acc)
                  (dotimes (i 1001)
                    (push (format "{\"timestamp\":\"2026-01-01T00:00:%02dZ\",\"msg\":\"m-%d\"}"
                                  (mod i 60) i)
                          acc))
                  (nreverse acc)))
         (buf (json-log-viewer-make-buffer
               "*json-log-viewer-chunk-evict-test*"
               :log-lines nil
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg"
               :streaming t
               :max-entries 1000)))
    (unwind-protect
        (with-current-buffer buf
          (json-log-viewer-push buf lines)
          (should (= json-log-viewer--entry-count 901))
          (should (= json-log-viewer--raw-log-lines-count 901))
          (should (= (length json-log-viewer--raw-log-line-chunks) 10)))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest json-log-viewer-cursor-move-disables-auto-follow-test ()
  (let* ((buf (json-log-viewer-make-buffer
               "*json-log-viewer-follow-disable-test*"
               :log-lines '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"a\"}")
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg"
               :streaming t)))
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
  (let* ((buf (json-log-viewer-make-buffer
               "*json-log-viewer-info-test*"
               :log-lines '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"hello\"}")
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg"
               :streaming nil
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
          (should (string-match-p "Mode:[[:space:]]+non-streaming" help-text))
          (should (string-match-p "Log group:[[:space:]]+/aws/demo" help-text))
          (should (string-match-p "Messages:[[:space:]]+1" help-text))
          (should (string-match-p "show info" help-text)))
      (when (buffer-live-p buf)
        (kill-buffer buf))
      (when (buffer-live-p (get-buffer help-buffer-name))
        (kill-buffer help-buffer-name)))))

(ert-deftest json-log-viewer-popup-keybindings-can-replace-defaults-test ()
  (let* ((buf (json-log-viewer-make-buffer
               "*json-log-viewer-keys-test*"
               :log-lines '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"msg\":\"hello\"}")
               :timestamp-path "timestamp"
               :level-path "level"
               :message-path "msg"
               :streaming nil))
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
