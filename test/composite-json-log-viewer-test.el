;;; composite-json-log-viewer-test.el --- composite viewer tests -*- lexical-binding: t; -*-

(require 'ert)
(require 'json-log-viewer)
(require 'composite-json-log-viewer)

(defvar json-log-viewer--entry-overlays)

(ert-deftest composite-json-log-viewer-streams-new-entries-only-test ()
  (let ((source nil)
        (composite nil))
    (unwind-protect
        (progn
          (setq source
                (json-log-viewer-make-buffer
                 "*json-log-viewer-source-test*"
                 :log-lines nil
                 :timestamp-path "timestamp"
                 :level-path "level"
                 :message-path "message"
                 :streaming t
                 :direction 'oldest-first))
          (json-log-viewer-push
           source
           '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"level\":\"info\",\"message\":\"old\"}"))
          (with-current-buffer source
            (json-log-viewer--async-await-pending-count 0))
          (setq composite
                (composite-json-log-viewer-create
                 "*composite-json-log-viewer-test*"
                 (list source)))
          (with-current-buffer composite
            (should (equal (length json-log-viewer--entry-overlays) 0)))
          (json-log-viewer-push
           source
           '("{\"timestamp\":\"2026-01-01T00:00:01Z\",\"level\":\"info\",\"message\":\"new\"}"))
          (with-current-buffer source
            (json-log-viewer--async-await-pending-count 0))
          (with-current-buffer composite
            (should (equal (length json-log-viewer--entry-overlays) 1))
            (let ((entry-overlay (car json-log-viewer--entry-overlays)))
              (should (eq (overlay-get entry-overlay 'json-log-viewer-storage-buffer) source))
              (should (get-text-property (overlay-start entry-overlay) 'face))
              (json-log-viewer--entry-expand entry-overlay)
              (should (string-match-p
                       "new"
                       (buffer-substring-no-properties (point-min) (point-max)))))))
      (when (buffer-live-p composite)
        (kill-buffer composite))
      (when (buffer-live-p source)
        (kill-buffer source)))))

(provide 'composite-json-log-viewer-test)
;;; composite-json-log-viewer-test.el ends here
