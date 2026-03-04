;;; composite-json-log-viewer.el --- Composite streams for json-log-viewer -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Builds a composite json-log-viewer buffer by subscribing to other
;; json-log-viewer buffers and copying rendered summary rows.
;;
;;; Code:

(require 'json-log-viewer)
(require 'subr-x)

(declare-function json-log-viewer--delete-no-results-placeholder "json-log-viewer" ())
(declare-function json-log-viewer--evict-oldest-entries-if-needed "json-log-viewer" ())
(declare-function json-log-viewer--apply-filter-to-overlays "json-log-viewer" (overlays))
(declare-function json-log-viewer--refresh-header "json-log-viewer" ())
(declare-function json-log-viewer--set-point-to-latest-entry "json-log-viewer" ())
(declare-function json-log-viewer--highlight-current-line "json-log-viewer" ())

(defvar json-log-viewer--entry-overlays)
(defvar json-log-viewer--entry-count)
(defvar json-log-viewer--seen-signatures)
(defvar json-log-viewer--auto-follow)

(defvar-local composite-json-log-viewer--is-composite nil
  "Non-nil in composite json-log-viewer buffers.")

(defvar-local composite-json-log-viewer--subscription-id nil
  "Subscriber id used to register this composite with source viewers.")

(defvar-local composite-json-log-viewer--source-callback nil
  "Callback used for source viewer subscriptions.")

(defvar-local composite-json-log-viewer--source-buffers nil
  "Source viewer buffers currently subscribed by this composite.")

(defvar-local composite-json-log-viewer--next-signature-id 0
  "Next synthetic signature id for composite overlays.")

(defun composite-json-log-viewer--source-candidates ()
  "Return live non-composite `json-log-viewer-mode' buffers."
  (let (buffers)
    (dolist (buffer (buffer-list))
      (with-current-buffer buffer
        (when (and (derived-mode-p 'json-log-viewer-mode)
                   (not composite-json-log-viewer--is-composite))
          (push buffer buffers))))
    (nreverse buffers)))

(defun composite-json-log-viewer--normalize-source-buffers (sources)
  "Normalize SOURCES into live non-composite json-log-viewer buffers."
  (let (buffers)
    (dolist (source sources)
      (let ((buffer (cond
                     ((bufferp source) source)
                     ((stringp source) (get-buffer source))
                     (t nil))))
        (when (buffer-live-p buffer)
          (with-current-buffer buffer
            (when (and (derived-mode-p 'json-log-viewer-mode)
                       (not composite-json-log-viewer--is-composite))
              (push buffer buffers))))))
    (nreverse (delete-dups buffers))))

(defun composite-json-log-viewer--next-signature ()
  "Return next unique signature for current composite buffer."
  (setq composite-json-log-viewer--next-signature-id
        (1+ composite-json-log-viewer--next-signature-id))
  (format "composite:%d" composite-json-log-viewer--next-signature-id))

(defun composite-json-log-viewer--overlay-snapshot (source-buffer source-overlay)
  "Return rendered snapshot for SOURCE-OVERLAY in SOURCE-BUFFER."
  (when (buffer-live-p source-buffer)
    (with-current-buffer source-buffer
      (when (and (overlay-buffer source-overlay)
                 (overlay-start source-overlay)
                 (overlay-end source-overlay))
        (list :text (buffer-substring (overlay-start source-overlay)
                                      (overlay-end source-overlay))
              :entry-id (or (overlay-get source-overlay 'json-log-viewer-storage-entry-id)
                            (overlay-get source-overlay 'json-log-viewer-log-entry-id))
              :storage-signature
              (or (overlay-get source-overlay 'json-log-viewer-storage-signature)
                  (overlay-get source-overlay 'json-log-viewer-signature)))))))

(defun composite-json-log-viewer--insert-snapshot (source-buffer snapshot)
  "Insert SNAPSHOT from SOURCE-BUFFER into current composite buffer."
  (let ((text (plist-get snapshot :text))
        (entry-id (plist-get snapshot :entry-id))
        (storage-signature (plist-get snapshot :storage-signature)))
    (when (and (stringp text) (> (length text) 0))
      (let ((start (point))
            (signature (composite-json-log-viewer--next-signature))
            entry-ov)
        (insert text)
        (unless (string-suffix-p "\n" text)
          (insert "\n"))
        (setq entry-ov (make-overlay start (point) nil t nil))
        (overlay-put entry-ov 'json-log-viewer-entry t)
        (overlay-put entry-ov 'json-log-viewer-entry-expanded nil)
        (overlay-put entry-ov 'json-log-viewer-fold-overlay nil)
        (overlay-put entry-ov 'json-log-viewer-log-entry-id entry-id)
        (overlay-put entry-ov 'json-log-viewer-signature signature)
        (overlay-put entry-ov 'json-log-viewer-storage-buffer source-buffer)
        (overlay-put entry-ov 'json-log-viewer-storage-entry-id entry-id)
        (overlay-put entry-ov 'json-log-viewer-storage-signature storage-signature)
        (puthash signature t json-log-viewer--seen-signatures)
        (push entry-ov json-log-viewer--entry-overlays)
        entry-ov))))

(defun composite-json-log-viewer--append-source-overlays (source-buffer source-overlays)
  "Append SOURCE-OVERLAYS from SOURCE-BUFFER into current composite."
  (let ((inhibit-read-only t)
        (inserted-overlays nil))
    (when source-overlays
      (save-excursion
        (json-log-viewer--delete-no-results-placeholder)
        (goto-char (point-max))
        (dolist (source-overlay source-overlays)
          (when-let ((snapshot (composite-json-log-viewer--overlay-snapshot
                                source-buffer source-overlay)))
            (when-let ((entry-ov (composite-json-log-viewer--insert-snapshot
                                  source-buffer snapshot)))
              (push entry-ov inserted-overlays)))))
      (setq inserted-overlays (nreverse inserted-overlays))
      (when inserted-overlays
        (setq json-log-viewer--entry-count
              (+ json-log-viewer--entry-count (length inserted-overlays)))
        (json-log-viewer--evict-oldest-entries-if-needed)
        (json-log-viewer--apply-filter-to-overlays inserted-overlays)
        (json-log-viewer--refresh-header)
        (when json-log-viewer--auto-follow
          (json-log-viewer--set-point-to-latest-entry))
        (json-log-viewer--highlight-current-line)))))

(defun composite-json-log-viewer--make-source-callback (composite-buffer)
  "Return source subscriber callback for COMPOSITE-BUFFER."
  (lambda (_action source-buffer entry-overlays)
    (when (and (buffer-live-p composite-buffer)
               (buffer-live-p source-buffer)
               entry-overlays)
      (with-current-buffer composite-buffer
        (when composite-json-log-viewer--is-composite
          (composite-json-log-viewer--append-source-overlays
           source-buffer entry-overlays))))))

(defun composite-json-log-viewer--unsubscribe-source (source-buffer subscription-id)
  "Unsubscribe SUBSCRIPTION-ID from SOURCE-BUFFER."
  (when (and (buffer-live-p source-buffer)
             subscription-id)
    (with-current-buffer source-buffer
      (json-log-viewer-unsubscribe subscription-id))))

(defun composite-json-log-viewer--cleanup ()
  "Cleanup all source subscriptions for current composite buffer."
  (let ((subscription-id composite-json-log-viewer--subscription-id))
    (dolist (source-buffer (append composite-json-log-viewer--source-buffers nil))
      (composite-json-log-viewer--unsubscribe-source source-buffer subscription-id)))
  (setq-local composite-json-log-viewer--source-buffers nil))

(defun composite-json-log-viewer-add-source (composite-buffer-or-name source-buffer-or-name)
  "Subscribe COMPOSITE-BUFFER-OR-NAME to SOURCE-BUFFER-OR-NAME."
  (let ((composite-buffer (json-log-viewer-get-buffer composite-buffer-or-name))
        (source-buffer (json-log-viewer-get-buffer source-buffer-or-name)))
    (with-current-buffer composite-buffer
      (unless composite-json-log-viewer--is-composite
        (user-error "Not a composite buffer: %s" (buffer-name composite-buffer)))
      (let ((subscription-id composite-json-log-viewer--subscription-id)
            (callback composite-json-log-viewer--source-callback))
        (unless (memq source-buffer composite-json-log-viewer--source-buffers)
          (push source-buffer composite-json-log-viewer--source-buffers)
          (with-current-buffer source-buffer
            (json-log-viewer-subscribe subscription-id callback)))))
    source-buffer))

(defun composite-json-log-viewer-remove-source (composite-buffer-or-name source-buffer-or-name)
  "Unsubscribe COMPOSITE-BUFFER-OR-NAME from SOURCE-BUFFER-OR-NAME."
  (let ((composite-buffer (json-log-viewer-get-buffer composite-buffer-or-name))
        (source-buffer (json-log-viewer-get-buffer source-buffer-or-name)))
    (with-current-buffer composite-buffer
      (unless composite-json-log-viewer--is-composite
        (user-error "Not a composite buffer: %s" (buffer-name composite-buffer)))
      (setq-local composite-json-log-viewer--source-buffers
                  (delq source-buffer composite-json-log-viewer--source-buffers))
      (composite-json-log-viewer--unsubscribe-source
       source-buffer
       composite-json-log-viewer--subscription-id))
    source-buffer))

(defun composite-json-log-viewer-create (buffer-name source-buffers)
  "Create composite BUFFER-NAME from SOURCE-BUFFERS.

Existing source data is intentionally ignored in this first iteration."
  (when-let ((existing (get-buffer buffer-name)))
    (with-current-buffer existing
      (when composite-json-log-viewer--is-composite
        (composite-json-log-viewer--cleanup))))
  (let ((buffer (json-log-viewer-make-buffer
                 buffer-name
                 :log-lines nil
                 :streaming t
                 :direction 'oldest-first)))
    (with-current-buffer buffer
      (setq-local composite-json-log-viewer--is-composite t)
      (setq-local composite-json-log-viewer--source-buffers nil)
      (setq-local composite-json-log-viewer--next-signature-id 0)
      (setq-local composite-json-log-viewer--subscription-id
                  (list 'composite-json-log-viewer buffer))
      (setq-local composite-json-log-viewer--source-callback
                  (composite-json-log-viewer--make-source-callback buffer))
      (add-hook 'kill-buffer-hook #'composite-json-log-viewer--cleanup nil t)
      (dolist (source-buffer source-buffers)
        (composite-json-log-viewer-add-source buffer source-buffer)))
    (display-buffer buffer)
    buffer))

(defun composite-json-log-viewer-setup (buffer-name source-buffer-names)
  "Interactively create a composite BUFFER-NAME from SOURCE-BUFFER-NAMES."
  (interactive
   (let* ((candidates (composite-json-log-viewer--source-candidates))
          (candidate-names (mapcar #'buffer-name candidates)))
     (unless candidate-names
       (user-error "No json-log-viewer buffers are available"))
     (list
      (read-string "Composite buffer name: " "*Composite JSON Logs*")
      (completing-read-multiple "Source buffers: " candidate-names nil t))))
  (let ((sources (composite-json-log-viewer--normalize-source-buffers source-buffer-names)))
    (unless sources
      (user-error "Select at least one source json-log-viewer buffer"))
    (composite-json-log-viewer-create buffer-name sources)))

(provide 'composite-json-log-viewer)
;;; composite-json-log-viewer.el ends here
