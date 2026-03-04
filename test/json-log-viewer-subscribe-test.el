;;; json-log-viewer-subscribe-test.el --- json-log-viewer subscription tests -*- lexical-binding: t; -*-

(require 'ert)
(require 'json-log-viewer)

(ert-deftest json-log-viewer-subscribe-and-unsubscribe-test ()
  (let ((buffer nil)
        (events nil))
    (unwind-protect
        (progn
          (setq buffer
                (json-log-viewer-make-buffer
                 "*json-log-viewer-subscribe-test*"
                 :log-lines nil
                 :timestamp-path "timestamp"
                 :level-path "level"
                 :message-path "message"
                 :streaming t
                 :direction 'oldest-first))
          (with-current-buffer buffer
            (json-log-viewer-subscribe
             'test-subscriber
             (lambda (action _source-buffer entry-overlays)
               (push (list action (length entry-overlays)) events))))
          (json-log-viewer-push
           buffer
           '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"level\":\"info\",\"message\":\"one\"}"))
          (with-current-buffer buffer
            (json-log-viewer--async-await-pending-count 0))
          (should (equal (reverse events) '((append 1))))
          (with-current-buffer buffer
            (json-log-viewer-unsubscribe 'test-subscriber))
          (json-log-viewer-push
           buffer
           '("{\"timestamp\":\"2026-01-01T00:00:01Z\",\"level\":\"info\",\"message\":\"two\"}"))
          (with-current-buffer buffer
            (json-log-viewer--async-await-pending-count 0))
          (should (equal (reverse events) '((append 1)))))
      (when (buffer-live-p buffer)
        (kill-buffer buffer)))))

(provide 'json-log-viewer-subscribe-test)
;;; json-log-viewer-subscribe-test.el ends here
