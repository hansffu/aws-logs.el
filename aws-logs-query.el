;;; aws-logs-query.el --- Logs Insights query editing -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Query editing support for aws-logs.
;; Provides a major-mode for CloudWatch Logs Insights query text and a popup
;; editor buffer that returns the edited query string.
;;
;;; Code:

(require 'subr-x)
(require 'font-lock)

(defgroup aws-logs-query nil
  "CloudWatch Logs Insights query editing."
  :group 'aws-logs)

(defvar aws-logs-insights-query-mode-font-lock-keywords
  (let* ((commands
          '("fields" "filter" "parse" "stats" "sort" "limit" "display" "dedup"
            "pattern" "diff" "unmask" "unnest" "anomaly" "filterIndex" "source"))
         (control
          '("and" "or" "not" "in" "as" "by" "asc" "desc"))
         (functions
          '("count" "sum" "avg" "min" "max" "bin" "ispresent" "count_distinct"
            "pct" "isIpv4InSubnet"))
         (re-commands (concat "\\_<" (regexp-opt commands t) "\\_>"))
         (re-control  (concat "\\_<" (regexp-opt control t) "\\_>"))
         (re-functions (concat "\\_<" (regexp-opt functions t) "\\_>\\s-*(")))
    `(("#.*$" . font-lock-comment-face)
      (,re-commands 1 font-lock-keyword-face)
      (,re-control 1 font-lock-builtin-face)
      (,re-functions 1 font-lock-function-name-face)
      ("@[A-Za-z0-9_]+" . font-lock-variable-name-face)
      ("/[^/\\n]*\\(?:\\.[^/\\n]*\\)*/" . font-lock-constant-face)))
  "Font-lock keywords for `aws-logs-insights-query-mode`.")

(define-derived-mode aws-logs-insights-query-mode prog-mode "LogsInsightsQL"
  "Major mode for AWS CloudWatch Logs Insights query language.

Supports basic syntax highlighting and #-style comments."
  :group 'aws-logs-query
  (setq-local comment-start "#")
  (setq-local comment-end "")
  (setq-local comment-start-skip "#\\s-*")

  (setq-local font-lock-keywords-case-fold-search t)
  (setq-local font-lock-defaults '(aws-logs-insights-query-mode-font-lock-keywords))

  (font-lock-mode 1)
  (font-lock-flush))

(defvar aws-logs--insights-query-edit-buffer "*AWS Logs Insights Query*"
  "Buffer name used for editing Logs Insights queries.")

(defvar aws-logs--insights-query-edit-result nil
  "Internal storage for the result of the query editor.")

(defvar aws-logs--insights-query-edit-canceled nil
  "Non-nil when the query edit was canceled.")

(defun aws-logs-insights-query-edit-finish ()
  "Finish editing the Logs Insights query and close the editor." 
  (interactive)
  (setq aws-logs--insights-query-edit-canceled nil)
  (setq aws-logs--insights-query-edit-result (string-trim-right (buffer-string)))
  (exit-recursive-edit))

(defun aws-logs-insights-query-edit-cancel ()
  "Cancel editing the Logs Insights query and close the editor." 
  (interactive)
  (setq aws-logs--insights-query-edit-canceled t)
  (setq aws-logs--insights-query-edit-result nil)
  (exit-recursive-edit))

(defvar-keymap aws-logs-insights-query-edit-mode-map
  :doc "Keymap for Logs Insights query editor buffers."
  "C-c C-c" #'aws-logs-insights-query-edit-finish
  "C-c C-k" #'aws-logs-insights-query-edit-cancel)

(defun aws-logs--insights-query-edit (initial)
  "Pop up a buffer to edit a Logs Insights query starting with INITIAL.

Returns the edited query string, or nil if canceled."
  (let ((buf (get-buffer-create aws-logs--insights-query-edit-buffer))
        (origin-window (selected-window)))
    (setq aws-logs--insights-query-edit-result nil)
    (setq aws-logs--insights-query-edit-canceled nil)
    (with-current-buffer buf
      (setq buffer-read-only nil)
      ;; Only reset contents if we're not already editing this buffer.
      ;; This lets you reopen the editor and keep/modify existing text.
      (unless (eq (current-buffer) (window-buffer (selected-window)))
        (erase-buffer)
        (when initial (insert initial)))
      (goto-char (point-min))
      ;; Enable query mode + editor keybindings.
      (aws-logs-insights-query-mode)
      (use-local-map (make-composed-keymap aws-logs-insights-query-edit-mode-map (current-local-map)))
      (setq-local header-line-format
                  "Edit Logs Insights query   C-c C-c: finish   C-c C-k: cancel"))

    ;; Display as a popup side window.
    (let* ((popup-window
            (display-buffer-in-side-window
             buf '((side . bottom)
                   (slot . 0)
                   (dedicated . t))))
           (target-height 15))
      (when (window-live-p popup-window)
        (window-resize popup-window
                       (- target-height (window-total-height popup-window))
                       nil
                       t))
      (unwind-protect
          (with-selected-window popup-window
            (recursive-edit))
        ;; Restore focus to where transient lives.
        (when (window-live-p origin-window)
          (select-window origin-window))
        ;; Close only the popup window; keep buffer for reuse.
        (when (window-live-p popup-window)
          (delete-window popup-window))))

    (unless aws-logs--insights-query-edit-canceled
      aws-logs--insights-query-edit-result)))

(provide 'aws-logs-query)
;;; aws-logs-query.el ends here
