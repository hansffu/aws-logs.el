;;; composite-log-viewer.el --- Shared multi-source JSON log viewer -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Composite log viewer mode for combining multiple log sources in one
;; json-log-viewer buffer.

;;; Code:

(require 'json-log-viewer)

(define-derived-mode composite-log-viewer-mode json-log-viewer-mode "CompositeLogs"
  "Major mode for a shared JSON log viewer with multiple sources."
  :group 'json-log-viewer)

(defun json-log-viewer-composite-buffer-p (&optional buffer-or-name)
  "Return non-nil when BUFFER-OR-NAME is a composite log viewer buffer.

When BUFFER-OR-NAME is nil, inspect the current buffer."
  (let ((buffer (cond
                 ((null buffer-or-name) (current-buffer))
                 ((bufferp buffer-or-name) buffer-or-name)
                 ((stringp buffer-or-name) (get-buffer buffer-or-name))
                 (t nil))))
    (and (buffer-live-p buffer)
         (with-current-buffer buffer
           (derived-mode-p 'composite-log-viewer-mode)))))

(defun composite-log-viewer (&optional buffer-name)
  "Create or reset a composite log viewer buffer.

With prefix argument, prompt for BUFFER-NAME."
  (interactive
   (list (when current-prefix-arg
           (read-string "Composite log viewer buffer: " "*Composite logs*"))))
  (let ((buffer (json-log-viewer-make-buffer
                 (or buffer-name "*Composite logs*")
                 :timestamp-path "timestamp"
                 :level-path "level"
                 :message-path "message"
                 :mode #'composite-log-viewer-mode)))
    (display-buffer buffer)
    buffer))

(provide 'composite-log-viewer)
;;; composite-log-viewer.el ends here
