;;; composite-log-viewer.el --- Shared multi-source JSON log viewer -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Composite log viewer mode for combining multiple log sources in one
;; json-log-viewer buffer.

;;; Code:

(require 'subr-x)

(require 'json-log-viewer)

(declare-function kafka-logs-stream-to-buffer "kafka-logs" (buffer source))
(declare-function kube-logs-stream-to-buffer "kube-logs" (buffer source))

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
  (let ((buffer (composite-log-viewer--make-buffer
                 (or buffer-name "*Composite logs*"))))
    (display-buffer buffer)
    buffer))

(defun composite-log-viewer--make-buffer (buffer-name)
  "Create or reset composite log viewer BUFFER-NAME."
  (json-log-viewer-make-buffer
   buffer-name
   :timestamp-path "timestamp"
   :level-path "level"
   :message-path "message"
   :mode #'composite-log-viewer-mode))

(defun composite-log-viewer--source-type (source)
  "Return normalized source type for SOURCE plist."
  (let ((type (plist-get source :type)))
    (cond
     ((eq type :kafka) 'kafka)
     ((eq type :kube) 'kube)
     ((symbolp type) type)
     ((stringp type) (intern (downcase type)))
     (t type))))

(defun composite-log-viewer-create (spec)
  "Create a composite log viewer from SPEC and start its sources.

SPEC is a plist with these keys:

- `:name': buffer name.  Defaults to \"*Composite logs*\".
- `:sources': list of source plists.

Each source plist must include `:type' with value `kafka' or `kube'.  Kafka
sources are passed to `kafka-logs-stream-to-buffer'.  Kubernetes sources are
passed to `kube-logs-stream-to-buffer'.  Both source starters stream from now:
Kafka starts at the topic end, and kube follows with `--tail=0' and no
`--since'."
  (unless (and (listp spec) (or (null spec) (keywordp (car spec))))
    (user-error "composite-log-viewer-create requires a plist spec"))
  (let* ((buffer-name (or (plist-get spec :name) "*Composite logs*"))
         (sources (plist-get spec :sources)))
    (unless (stringp buffer-name)
      (user-error ":name must be a string, got: %S" buffer-name))
    (unless (and (listp sources) sources)
      (user-error ":sources must be a non-empty list"))
    (when-let ((existing (get-buffer buffer-name)))
      (kill-buffer existing))
    (let ((buffer (composite-log-viewer--make-buffer buffer-name)))
      (dolist (source sources)
        (unless (and (listp source) (or (null source) (keywordp (car source))))
          (user-error "Composite source must be a plist, got: %S" source))
        (pcase (composite-log-viewer--source-type source)
          ('kafka
           (require 'kafka-logs)
           (kafka-logs-stream-to-buffer buffer source))
          ('kube
           (require 'kube-logs)
           (kube-logs-stream-to-buffer buffer source))
          (type
           (user-error "Unsupported composite source type: %S" type))))
      (display-buffer buffer)
      buffer)))

(provide 'composite-log-viewer)
;;; composite-log-viewer.el ends here
