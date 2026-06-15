;;; kube-logs-test.el --- kube-logs unit tests -*- lexical-binding: t; -*-

(require 'ert)
(require 'cl-lib)
(require 'json)

(require 'kube-logs)

(defvar kube-logs-context)
(defvar kube-logs-namespace)
(defvar kube-logs-namespace-enabled)
(defvar kube-logs-target-kind)
(defvar kube-logs-target)
(defvar kube-logs-follow)
(defvar kube-logs-tail-lines)
(defvar kube-logs-since)
(defvar kube-logs-filter)
(defvar kube-logs-debug-process-buffer)
(defvar kube-logs-presets)

(ert-deftest kube-logs-target-ref-test ()
  (let ((kube-logs-target-kind "pod")
        (kube-logs-target "api-0"))
    (should (equal (kube-logs--target-ref) "pod/api-0")))
  (let ((kube-logs-target-kind "deployment")
        (kube-logs-target "payments"))
    (should (equal (kube-logs--target-ref) "deployment/payments")))
  (let ((kube-logs-target-kind "pod")
        (kube-logs-target nil))
    (should-error (kube-logs--target-ref) :type 'user-error)))

(ert-deftest kube-logs-logs-args-test ()
  (let ((kube-logs-context "prod-cluster")
        (kube-logs-namespace "payments")
        (kube-logs-namespace-enabled t)
        (kube-logs-target-kind "deployment")
        (kube-logs-target "payments-api")
        (kube-logs-follow t)
        (kube-logs-tail-lines 150)
        (kube-logs-since "10m"))
    (should (equal (kube-logs--logs-args)
                   '("--context=prod-cluster"
                     "logs" "deployment/payments-api"
                     "--all-pods"
                     "--namespace" "payments"
                     "--prefix"
                     "--timestamps"
                     "--follow"
                     "--tail=150"
                     "--since=10m")))))

(ert-deftest kube-logs-logs-args-with-namespace-override-disabled-test ()
  (let ((kube-logs-context "prod-cluster")
        (kube-logs-namespace "payments")
        (kube-logs-namespace-enabled nil)
        (kube-logs-target-kind "deployment")
        (kube-logs-target "payments-api")
        (kube-logs-follow nil)
        (kube-logs-tail-lines nil)
        (kube-logs-since nil))
    (should (equal (kube-logs--logs-args)
                   '("--context=prod-cluster"
                     "logs" "deployment/payments-api"
                     "--all-pods"
                     "--prefix"
                     "--timestamps")))))

(ert-deftest kube-logs-stream-to-buffer-starts-at-tail-zero-test ()
  (let ((buffer (generate-new-buffer "*kube-composite-stream-test*"))
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
                       'kube-process))
                    ((symbol-function 'set-process-sentinel)
                     (lambda (&rest _args) nil))
                    ((symbol-function 'set-process-query-on-exit-flag)
                     (lambda (&rest _args) nil)))
            (kube-logs-stream-to-buffer
             buffer
             '(:context "prod-cluster"
               :namespace "payments"
               :target-kind deployment
               :target "payments-api"
               :stream-backend kubectl)))
          (should (member "--tail=0" captured-command))
          (should (member "--follow" captured-command))
          (should (equal (cadr (member "--source-id" captured-command))
                         "kube:payments/deployment/payments-api"))
          (should-not (cl-some (lambda (arg)
                                 (string-prefix-p "--since" arg))
                               captured-command)))
      (when (buffer-live-p buffer)
        (kill-buffer buffer)))))

(ert-deftest kube-logs-stream-retry-schedules-restart-test ()
  (let ((buffer (generate-new-buffer "*kube-stream-retry-test*"))
        (proc nil)
        captured-delay
        captured-timer-fn
        restarted-attempt)
    (unwind-protect
        (progn
          (setq proc
                (make-process
                 :name "kube-stream-retry-test"
                 :buffer nil
                 :command '("sh" "-c" "sleep 60")
                 :noquery t))
          (process-put proc 'kube-logs-restart-fn
                       (lambda (attempt)
                         (setq restarted-attempt attempt)))
          (process-put proc 'kube-logs-retry-attempt 0)
          (process-put proc 'kube-logs-description "deployment/payments-api")
          (process-put proc 'kube-logs-started-at (float-time))
          (with-current-buffer buffer
            (setq-local kube-logs--stream-retry-timers nil))
          (let ((kube-logs-stream-retry-enabled t)
                (kube-logs-stream-retry-max-delay 30)
                (kube-logs-stream-retry-reset-after 60))
            (cl-letf (((symbol-function 'run-at-time)
                       (lambda (delay _repeat function &rest args)
                         (setq captured-delay delay)
                         (setq captured-timer-fn
                               (lambda ()
                                 (apply function args)))
                         'retry-timer))
                      ((symbol-function 'message)
                       (lambda (&rest _args) nil)))
              (kube-logs--schedule-process-retry
               buffer proc "exited abnormally\n")))
          (should (= captured-delay 2))
          (with-current-buffer buffer
            (should (equal kube-logs--stream-retry-timers '(retry-timer))))
          (funcall captured-timer-fn)
          (should (= restarted-attempt 1))
          (with-current-buffer buffer
            (should-not kube-logs--stream-retry-timers)))
      (when (process-live-p proc)
        (process-put proc 'kube-logs-stop-requested t)
        (delete-process proc))
      (when (buffer-live-p buffer)
        (kill-buffer buffer)))))

(ert-deftest kube-logs-supervisor-command-test ()
  (let ((kube-logs-context "prod-cluster")
        (kube-logs-namespace "payments")
        (kube-logs-namespace-enabled t)
        (kube-logs-target-kind "deployment")
        (kube-logs-target "payments-api")
        (kube-logs-tail-lines 150)
        (kube-logs-since "10m")
        (kube-logs-filter "ERROR|WARN"))
    (cl-letf (((symbol-function 'json-log-viewer-kube-log-supervisor-executable)
               (lambda () "/tmp/kube-log-supervisor")))
      (should (equal (kube-logs--supervisor-command "/tmp/socket")
                     '("/tmp/kube-log-supervisor"
                       "--socket" "/tmp/socket"
                       "--context" "prod-cluster"
                       "--namespace" "payments"
                       "--target-kind" "deployment"
                       "--target" "payments-api"
                       "--source-id" "kube"
                       "--tail" "150"
                       "--since" "10m"
                       "--filter" "ERROR|WARN"))))))

(ert-deftest kube-logs-process-log-buffer-disabled-by-default-test ()
  (let ((kube-logs-namespace "payments")
        (kube-logs-namespace-enabled t)
        (kube-logs-target "payments-api")
        (kube-logs-debug-process-buffer nil))
    (should-not (kube-logs--make-process-log-buffer))))

(ert-deftest kube-logs-process-log-buffer-created-when-debug-enabled-test ()
  (let ((kube-logs-namespace "payments")
        (kube-logs-namespace-enabled t)
        (kube-logs-target "payments-api")
        (kube-logs-debug-process-buffer t))
    (let ((buffer (kube-logs--make-process-log-buffer)))
      (unwind-protect
          (with-current-buffer buffer
            (should (equal (buffer-name) "*Kube logs process - payments/payments-api*"))
            (should (equal (buffer-string) "")))
        (when (buffer-live-p buffer)
          (kill-buffer buffer))))))

(ert-deftest kube-logs-supervisor-process-filter-messages-lines-test ()
  (let ((viewer (generate-new-buffer "*kube-logs-filter-viewer-test*"))
        captured)
    (unwind-protect
        (with-current-buffer viewer
          (setq-local kube-logs--process-log-pending-fragment "")
          (cl-letf (((symbol-function 'message)
                     (lambda (fmt &rest args)
                       (push (apply #'format fmt args) captured))))
            (kube-logs--supervisor-process-filter viewer nil "first")
            (should-not captured)
            (kube-logs--supervisor-process-filter viewer nil " line\nsecond line\n")
            (should (equal (nreverse captured)
                           '("first line" "second line")))))
      (when (buffer-live-p viewer)
        (kill-buffer viewer)))))

(ert-deftest kube-logs-line->json-line-json-message-test ()
  (with-temp-buffer
    (setq-local kube-logs--viewer-namespace "payments")
    (setq-local kube-logs--viewer-target-kind "deployment")
    (setq-local kube-logs--viewer-target "payments-api")
    (let* ((line "2026-01-01T12:00:00Z {\"level\":\"warn\",\"message\":\"boom\"}")
           (json-line (kube-logs--line->json-line line))
           (parsed (json-parse-string json-line :object-type 'alist))
           (payload (alist-get 'payload parsed)))
      (should (equal (alist-get 'timestamp parsed) "2026-01-01T12:00:00Z"))
      (should (equal (alist-get 'source parsed) "kube"))
      (should-not (alist-get 'level parsed))
      (should-not (alist-get 'message parsed))
      (should (equal (alist-get 'level payload) "warn"))
      (should (equal (alist-get 'message payload) "boom"))
      (should (equal (alist-get 'namespace parsed) "payments"))
      (should (equal (alist-get 'target parsed) "payments-api")))))

(ert-deftest kube-logs-line->json-line-strips-kubectl-prefix-test ()
  (with-temp-buffer
    (setq-local kube-logs--viewer-namespace "payments")
    (setq-local kube-logs--viewer-target-kind "deployment")
    (setq-local kube-logs--viewer-target "payments-api")
    (let* ((line "payments-api-7bbf4c app 2026-01-01T12:00:00Z {\"level\":\"info\",\"message\":\"ok\"}")
           (json-line (kube-logs--line->json-line line))
           (parsed (json-parse-string json-line :object-type 'alist))
           (payload (alist-get 'payload parsed)))
      (should (equal (alist-get 'timestamp parsed) "2026-01-01T12:00:00Z"))
      (should-not (alist-get 'level parsed))
      (should-not (alist-get 'message parsed))
      (should (equal (alist-get 'level payload) "info"))
      (should (equal (alist-get 'message payload) "ok"))
      (should (equal (alist-get 'raw parsed)
                     "2026-01-01T12:00:00Z {\"level\":\"info\",\"message\":\"ok\"}")))))

(ert-deftest kube-logs-line->json-line-plain-test ()
  (with-temp-buffer
    (setq-local kube-logs--viewer-namespace "default")
    (setq-local kube-logs--viewer-target-kind "pod")
    (setq-local kube-logs--viewer-target "web-123")
    (let* ((line "plain message")
           (json-line (kube-logs--line->json-line line))
           (parsed (json-parse-string json-line :object-type 'alist)))
      (should-not (alist-get 'timestamp parsed))
      (should (equal (alist-get 'source parsed) "kube"))
      (should-not (alist-get 'message parsed))
      (should (equal (alist-get 'payload parsed) "plain message"))
      (should (equal (alist-get 'raw parsed) "plain message"))
      (should (equal (alist-get 'kind parsed) "pod")))))

(ert-deftest kube-logs-list-targets-parses-prefixed-lines-test ()
  (let ((kube-logs-context "dev")
        (kube-logs-namespace "payments")
        (kube-logs-namespace-enabled t)
        (kube-logs-target-kind "pod"))
    (cl-letf (((symbol-function 'kube-logs--run-kubectl-lines)
               (lambda (_args)
                 '("pod/zeta-0" "pod/alpha-1"))))
      (should (equal (kube-logs--list-targets)
                     '("alpha-1" "zeta-0"))))))

(ert-deftest kube-logs-list-targets-with-namespace-override-disabled-test ()
  (let ((kube-logs-context "dev")
        (kube-logs-namespace "payments")
        (kube-logs-namespace-enabled nil)
        (kube-logs-target-kind "pod")
        captured-args)
    (cl-letf (((symbol-function 'kube-logs--run-kubectl-lines)
               (lambda (args)
                 (setq captured-args args)
                 '("pod/api-0"))))
      (should (equal (kube-logs--list-targets) '("api-0")))
      (should (equal captured-args
                     '("--context=dev" "get" "pods" "-o" "name"))))))

(ert-deftest kube-logs-preset-apply-test ()
  (let ((kube-logs-presets nil)
        (kube-logs-context nil)
        (kube-logs-namespace "default")
        (kube-logs-namespace-enabled t)
        (kube-logs-target-kind "pod")
        (kube-logs-target nil)
        (kube-logs-follow nil)
        (kube-logs-tail-lines 200)
        (kube-logs-since nil))
    (kube-logs-make-preset
     "prod-tail"
     :context "prod"
     :namespace "payments"
     :namespace-enabled nil
     :target-kind "deployment"
     :target "payments-api"
     :follow t
     :tail-lines 500
     :since "15m")
    (kube-logs--apply-preset-plist (cdr (assoc "prod-tail" kube-logs-presets)))
    (should (equal kube-logs-context "prod"))
    (should (equal kube-logs-namespace "payments"))
    (should-not kube-logs-namespace-enabled)
    (should (equal kube-logs-target-kind "deployment"))
    (should (equal kube-logs-target "payments-api"))
    (should (equal kube-logs-follow t))
    (should (= kube-logs-tail-lines 500))
    (should (equal kube-logs-since "15m"))))

(ert-deftest kube-logs-make-viewer-buffer-uses-selected-viewer-test ()
  (let* ((viewer (generate-new-buffer "*kube-logs-shared-viewer-test*"))
         (kube-logs-viewer-buffer (buffer-name viewer))
         (kube-logs-context "dev")
         (kube-logs-namespace "payments")
         (kube-logs-namespace-enabled t)
         (kube-logs-target-kind "deployment")
         (kube-logs-target "api")
         (kube-logs-follow t)
         (kube-logs-tail-lines 100)
         (kube-logs-since "5m")
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
                         (funcall function))))
                    ((symbol-function 'json-log-viewer-replace-log-lines)
                     (lambda (&rest _args) nil)))
            (let ((buffer (kube-logs--make-viewer-buffer
                           (lambda ()
                             (setq-local kube-logs--process 'ready)))))
              (should (eq buffer viewer))
              (should (eq ready-buffer viewer))
              (with-current-buffer viewer
                (should (eq kube-logs--process 'ready))
                (should (equal kube-logs--viewer-namespace "payments"))
                (should (equal kube-logs--viewer-target "api"))))))
      (when (buffer-live-p viewer)
        (kill-buffer viewer)))))

(ert-deftest kube-logs-make-viewer-buffer-preserves-selected-viewer-processes-test ()
  (let* ((viewer (generate-new-buffer "*kube-logs-shared-processes-test*"))
         (kube-logs-viewer-buffer (buffer-name viewer))
         (kube-logs-context "dev")
         (kube-logs-namespace "payments")
         (kube-logs-namespace-enabled t)
         (kube-logs-target-kind "deployment")
         (kube-logs-target "api")
         (kube-logs-follow t)
         (kube-logs-tail-lines 100)
         (kube-logs-since "5m"))
    (unwind-protect
        (progn
          (with-current-buffer viewer
            (composite-log-viewer-mode)
            (setq-local kube-logs--initialized-p t)
            (setq-local kube-logs--process 'existing)
            (setq-local kube-logs--processes '(existing)))
          (cl-letf (((symbol-function 'kube-logs--kill-buffer-process)
                     (lambda (&rest _args)
                       (error "should not stop existing selected viewer sources")))
                    ((symbol-function 'json-log-viewer-run-when-ready)
                     (lambda (buffer function)
                       (with-current-buffer buffer
                         (funcall function)))))
            (let ((buffer (kube-logs--make-viewer-buffer
                           (lambda ()
                             (kube-logs--add-buffer-process (current-buffer) 'new)))))
              (should (eq buffer viewer))
              (with-current-buffer viewer
                (should (eq kube-logs--process 'new))
                (should (memq 'existing kube-logs--processes))
                (should (memq 'new kube-logs--processes))))))
      (when (buffer-live-p viewer)
        (kill-buffer viewer)))))

(ert-deftest kube-logs-transient-viewer-buffer-follows-current-composite-test ()
  (let ((viewer (generate-new-buffer "*kube-logs-composite-current-test*"))
        (kube-logs-viewer-buffer "stale"))
    (unwind-protect
        (progn
          (with-current-buffer viewer
            (composite-log-viewer-mode)
            (kube-logs--set-viewer-buffer-from-current-buffer))
          (should (equal kube-logs-viewer-buffer (buffer-name viewer)))
          (with-temp-buffer
            (json-log-viewer-mode)
            (kube-logs--set-viewer-buffer-from-current-buffer))
          (should-not kube-logs-viewer-buffer))
      (when (buffer-live-p viewer)
        (kill-buffer viewer)))))

(ert-deftest kube-logs-make-viewer-buffer-replaces-selected-non-composite-test ()
  (let* ((viewer (generate-new-buffer "*kube-logs-replace-viewer-test*"))
         (kube-logs-viewer-buffer (buffer-name viewer))
         (kube-logs-context "dev")
         (kube-logs-namespace "payments")
         (kube-logs-namespace-enabled t)
         (kube-logs-target-kind "deployment")
         (kube-logs-target "api")
         (kube-logs-follow t)
         (kube-logs-tail-lines 100)
         (kube-logs-since "5m")
         killed-buffer
         ready-buffer
         cleared-buffer)
    (unwind-protect
        (progn
          (with-current-buffer viewer
            (json-log-viewer-mode))
          (cl-letf (((symbol-function 'kube-logs--kill-buffer-process)
                     (lambda (buffer)
                       (setq killed-buffer buffer)))
                    ((symbol-function 'json-log-viewer-run-when-ready)
                     (lambda (buffer function)
                       (setq ready-buffer buffer)
                       (with-current-buffer buffer
                         (funcall function))))
                    ((symbol-function 'json-log-viewer-replace-log-lines)
                     (lambda (buffer lines &optional _preserve-filter)
                       (should-not lines)
                       (setq cleared-buffer buffer))))
            (let ((buffer (kube-logs--make-viewer-buffer
                           (lambda ()
                             (setq-local kube-logs--process 'ready)))))
              (should (eq buffer viewer))
              (should (eq killed-buffer viewer))
              (should (eq ready-buffer viewer))
              (should (eq cleared-buffer viewer))
              (with-current-buffer viewer
                (should (eq kube-logs--process 'ready))))))
      (when (buffer-live-p viewer)
        (kill-buffer viewer)))))

(ert-deftest kube-logs-make-viewer-buffer-initializes-before-on-ready-test ()
  (let ((created-buffer nil)
        (ready-buffer nil)
        (ready-namespace nil)
        (kube-logs-viewer-buffer nil)
        (kube-logs-context "dev")
        (kube-logs-namespace "payments")
        (kube-logs-namespace-enabled t)
        (kube-logs-target-kind "deployment")
        (kube-logs-target "api")
        (kube-logs-follow t)
        (kube-logs-tail-lines 100)
        (kube-logs-since "5m"))
    (unwind-protect
        (cl-letf (((symbol-function 'json-log-viewer-make-buffer)
                   (lambda (buffer-name &rest args)
                     (when (plist-get args :on-ready)
                       (error "on-ready should be registered after initialization"))
                     (setq created-buffer (generate-new-buffer buffer-name))
                     (with-current-buffer created-buffer
                       (use-local-map (make-sparse-keymap)))
                     created-buffer))
                  ((symbol-function 'json-log-viewer-run-when-ready)
                   (lambda (buffer function)
                     (setq ready-buffer buffer)
                     (with-current-buffer buffer
                       (funcall function)))))
          (let ((buffer (kube-logs--make-viewer-buffer
                         (lambda ()
                           (setq ready-namespace kube-logs--viewer-namespace)))))
            (should (eq buffer created-buffer))
            (should (eq ready-buffer created-buffer))
            (should (equal ready-namespace "payments"))))
      (when (buffer-live-p created-buffer)
        (kill-buffer created-buffer)))))

(provide 'kube-logs-test)
;;; kube-logs-test.el ends here
