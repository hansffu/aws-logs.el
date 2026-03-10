;;; async-job-queue-test.el --- Async job queue tests -*- lexical-binding: t; -*-

(require 'ert)
(require 'cl-lib)

(require 'async-job-queue)

(defun async-job-queue-test--wait-until (predicate &optional process timeout-seconds)
  "Wait until PREDICATE returns non-nil.
When PROCESS is live, pump process output while waiting.
Return the predicate value when available, nil on timeout."
  (let ((deadline (+ (float-time) (or timeout-seconds 3.0)))
        result)
    (while (and (not result)
                (< (float-time) deadline))
      (setq result (funcall predicate))
      (unless result
        (if (and process (process-live-p process))
            (accept-process-output process 0.05)
          (sleep-for 0.01))))
    result))

(ert-deftest async-job-queue-init-error-calls-callback-and-skips-teardown-test ()
  (let ((events nil)
        (teardown-marker (make-temp-name "/tmp/async-job-queue-teardown-should-not-run-"))
        queue)
    (unwind-protect
        (progn
          (setq queue
                (async-job-queue-create
                 (lambda (x) x)
                 (lambda (result) (push result events))
                 :init-func (lambda () (error "boom-init"))
                 :teardown-func (lambda ()
                                  (with-temp-file teardown-marker
                                    (insert "teardown-ran")))))
          (should (async-job-queue-test--wait-until
                   (lambda () events)
                   (async-job-queue-process queue)))
          (should (equal (car events) '(:error "INIT-FUNC failed: boom-init")))
          (should-not (file-exists-p teardown-marker))
          (should (async-job-queue-test--wait-until
                   (lambda () (not (process-live-p (async-job-queue-process queue))))
                   (async-job-queue-process queue))))
      (when (and queue (process-live-p (async-job-queue-process queue)))
        (async-job-queue-stop queue))
      (when (file-exists-p teardown-marker)
        (delete-file teardown-marker)))))

(ert-deftest async-job-queue-teardown-error-calls-callback-test ()
  (let ((events nil)
        queue)
    (unwind-protect
        (progn
          (setq queue
                (async-job-queue-create
                 (lambda (x) x)
                 (lambda (result) (push result events))
                 :teardown-func (lambda () (error "boom-teardown"))))
          (async-job-queue-stop queue)
          (should (async-job-queue-test--wait-until
                   (lambda () events)
                   (async-job-queue-process queue)))
          (should (equal (car events) '(:error "TEARDOWN-FUNC failed: boom-teardown")))
          (should (async-job-queue-test--wait-until
                   (lambda () (not (process-live-p (async-job-queue-process queue))))
                   (async-job-queue-process queue))))
      (when (and queue (process-live-p (async-job-queue-process queue)))
        (async-job-queue-stop queue)))))

(ert-deftest async-job-queue-init-and-teardown-happy-path-test ()
  (let ((events nil)
        (marker-path (make-temp-name "/tmp/async-job-queue-lifecycle-"))
        queue)
    (unwind-protect
        (progn
          (setq queue
                (async-job-queue-create
                 (lambda (x)
                   (list x (file-exists-p marker-path)))
                 (lambda (result) (push result events))
                 :init-func (lambda ()
                              (with-temp-file marker-path
                                (insert "ready")))
                 :teardown-func (lambda ()
                                  (when (file-exists-p marker-path)
                                    (delete-file marker-path)))))
          (async-job-queue-push queue 42)
          (should (async-job-queue-test--wait-until
                   (lambda () events)
                   (async-job-queue-process queue)))
          (should (equal (car events) '(42 t)))
          (should (file-exists-p marker-path))
          (async-job-queue-stop queue)
          (should (async-job-queue-test--wait-until
                   (lambda () (not (process-live-p (async-job-queue-process queue))))
                   (async-job-queue-process queue)))
          (should-not (file-exists-p marker-path)))
      (when (and queue (process-live-p (async-job-queue-process queue)))
        (async-job-queue-stop queue))
      (when (file-exists-p marker-path)
        (delete-file marker-path)))))

