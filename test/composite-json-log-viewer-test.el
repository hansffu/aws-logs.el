;;; composite-json-log-viewer-test.el --- composite viewer tests -*- lexical-binding: t; -*-

(require 'ert)
(require 'cl-lib)
(require 'json-log-viewer)
(require 'composite-json-log-viewer)

(defvar json-log-viewer--entry-overlays)

(defun composite-json-log-viewer-test--wait-for (predicate &optional timeout)
  "Wait until PREDICATE returns non-nil, up to TIMEOUT seconds."
  (let ((deadline (+ (float-time) (or timeout 5.0)))
        result)
    (while (and (null result)
                (< (float-time) deadline))
      (setq result (funcall predicate))
      (unless result
        (accept-process-output nil 0.01)))
    result))

(ert-deftest composite-json-log-viewer-loads-history-and-streams-new-entries-test ()
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
          (should (composite-json-log-viewer-test--wait-for
                   (lambda ()
                     (with-current-buffer composite
                       (= (length json-log-viewer--entry-overlays) 1)))))
          (with-current-buffer composite
            (should (equal (length json-log-viewer--entry-overlays) 1))
            (should (string-match-p
                     "old"
                     (buffer-substring-no-properties (point-min) (point-max)))))
          (json-log-viewer-push
           source
           '("{\"timestamp\":\"2026-01-01T00:00:01Z\",\"level\":\"info\",\"message\":\"new\"}"))
          (with-current-buffer source
            (json-log-viewer--async-await-pending-count 0))
          (should (composite-json-log-viewer-test--wait-for
                   (lambda ()
                     (with-current-buffer composite
                       (= (length json-log-viewer--entry-overlays) 2)))))
          (with-current-buffer composite
            (should (equal (length json-log-viewer--entry-overlays) 2))
            (should (string-match-p
                     "new"
                     (buffer-substring-no-properties (point-min) (point-max))))
            (let ((entry-overlay
                   (cl-find-if
                    (lambda (ov)
                      (= (or (overlay-get ov 'json-log-viewer-storage-entry-id) -1) 2))
                    json-log-viewer--entry-overlays)))
              (should entry-overlay)
              (should (get-text-property (overlay-start entry-overlay) 'face))
              (json-log-viewer--entry-expand entry-overlay)
              (should (string-match-p
                       "new"
                       (buffer-substring-no-properties (point-min) (point-max)))))))
      (when (buffer-live-p composite)
        (kill-buffer composite))
      (when (buffer-live-p source)
        (kill-buffer source)))))

(ert-deftest composite-json-log-viewer-history-is-merged-by-timestamp-test ()
  (let ((source-a nil)
        (source-b nil)
        (composite nil))
    (unwind-protect
        (progn
          (setq source-a
                (json-log-viewer-make-buffer
                 "*json-log-viewer-source-a-test*"
                 :log-lines nil
                 :timestamp-path "timestamp"
                 :level-path "level"
                 :message-path "message"
                 :streaming t
                 :direction 'oldest-first))
          (setq source-b
                (json-log-viewer-make-buffer
                 "*json-log-viewer-source-b-test*"
                 :log-lines nil
                 :timestamp-path "timestamp"
                 :level-path "level"
                 :message-path "message"
                 :streaming t
                 :direction 'oldest-first))
          (json-log-viewer-push
           source-a
           '("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"level\":\"info\",\"message\":\"a-0\"}"
             "{\"timestamp\":\"2026-01-01T00:00:02Z\",\"level\":\"info\",\"message\":\"a-2\"}"))
          (json-log-viewer-push
           source-b
           '("{\"timestamp\":\"2026-01-01T00:00:01Z\",\"level\":\"info\",\"message\":\"b-1\"}"))
          (with-current-buffer source-a
            (json-log-viewer--async-await-pending-count 0))
          (with-current-buffer source-b
            (json-log-viewer--async-await-pending-count 0))
          (setq composite
                (composite-json-log-viewer-create
                 "*composite-json-log-viewer-merge-test*"
                 (list source-a source-b)))
          (should (composite-json-log-viewer-test--wait-for
                   (lambda ()
                     (with-current-buffer composite
                       (= (length json-log-viewer--entry-overlays) 3)))))
          (with-current-buffer composite
            (let ((text (buffer-substring-no-properties (point-min) (point-max))))
              (should (string-match-p "a-0" text))
              (should (string-match-p "b-1" text))
              (should (string-match-p "a-2" text))
              (should (< (string-match "a-0" text) (string-match "b-1" text)))
              (should (< (string-match "b-1" text) (string-match "a-2" text))))))
      (when (buffer-live-p composite)
        (kill-buffer composite))
      (when (buffer-live-p source-a)
        (kill-buffer source-a))
      (when (buffer-live-p source-b)
        (kill-buffer source-b)))))

(provide 'composite-json-log-viewer-test)
;;; composite-json-log-viewer-test.el ends here
