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
                     "--namespace" "payments"
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
                     "--timestamps")))))

(ert-deftest kube-logs-line->json-line-json-message-test ()
  (with-temp-buffer
    (setq-local kube-logs--viewer-namespace "payments")
    (setq-local kube-logs--viewer-target-kind "deployment")
    (setq-local kube-logs--viewer-target "payments-api")
    (let* ((line "2026-01-01T12:00:00Z {\"level\":\"warn\",\"message\":\"boom\"}")
           (json-line (kube-logs--line->json-line line))
           (parsed (json-parse-string json-line :object-type 'alist)))
      (should (equal (alist-get 'timestamp parsed) "2026-01-01T12:00:00Z"))
      (should (equal (alist-get 'level parsed) "warn"))
      (should (equal (alist-get 'message parsed) "boom"))
      (should (equal (alist-get 'namespace parsed) "payments"))
      (should (equal (alist-get 'target parsed) "payments-api")))))

(ert-deftest kube-logs-line->json-line-plain-test ()
  (with-temp-buffer
    (setq-local kube-logs--viewer-namespace "default")
    (setq-local kube-logs--viewer-target-kind "pod")
    (setq-local kube-logs--viewer-target "web-123")
    (let* ((line "plain message")
           (json-line (kube-logs--line->json-line line))
           (parsed (json-parse-string json-line :object-type 'alist)))
      (should-not (alist-get 'timestamp parsed))
      (should (equal (alist-get 'message parsed) "plain message"))
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

(provide 'kube-logs-test)
;;; kube-logs-test.el ends here
