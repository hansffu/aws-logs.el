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

(defconst aws-logs-insights-query--commands
  '("anomaly" "dedup" "diff" "display" "fields" "filter" "filterIndex"
    "limit" "parse" "pattern" "sort" "source" "stats" "unmask" "unnest")
  "Logs Insights QL commands from AWS documentation.")

(defconst aws-logs-insights-query--keywords
  '("and" "as" "asc" "by" "desc" "in" "is" "like" "not" "or")
  "Keywords and control words in Logs Insights QL.")

(defconst aws-logs-insights-query--functions
  '("abs" "avg" "bin" "ceil" "coalesce" "concat" "count" "count_distinct"
    "dateceil" "datefloor" "earliest" "floor" "fromMillis" "greatest"
    "isblank" "isempty" "isIpInSubnet" "isIpv4InSubnet" "isIpv6InSubnet"
    "ispresent" "isValidIp" "isValidIpV4" "isValidIpV6" "jsonParse"
    "jsonStringify" "latest" "least" "log" "ltrim" "max" "min" "now"
    "pct" "replace" "rtrim" "sortsFirst" "sortsLast" "sqrt" "stddev"
    "strcontains" "strlen" "substr" "sum" "toMillis" "tolower" "toupper"
    "trim")
  "Logs Insights QL functions from AWS documentation.")

(defvar aws-logs-insights-query-mode-font-lock-keywords
  (let* ((re-command
          (concat "\\(?:^\\|[|]\\)\\s-*\\("
                  (regexp-opt aws-logs-insights-query--commands t)
                  "\\)\\_>"))
         (re-keyword
          (concat "\\_<" (regexp-opt aws-logs-insights-query--keywords t) "\\_>"))
         (re-function
          (concat "\\_<\\(" (regexp-opt aws-logs-insights-query--functions t)
                  "\\)\\_>\\s-*("))
         (re-regex-literal "/\\(?:\\\\.\\|[^/\\\n]\\)+/")
         (re-regex-after-like-or-match
          (concat "\\(?:\\_<\\(?:like\\|not\\s-+like\\)\\_>\\|=~\\)\\s-*\\("
                  re-regex-literal "\\)"))
         (re-regex-in-parse
          (concat "\\_<parse\\_>\\(?:[^#\n|]*?\\)\\(" re-regex-literal "\\)"))
         (re-duration
          "\\_<[0-9]+\\(?:ms\\|msec\\|sec\\|min\\|mon\\|qtr\\|yr\\|s\\|m\\|h\\|d\\|w\\|q\\|y\\|mo\\|hr\\)s?\\_>")
         (re-number
          "\\_<[-+]?[0-9]+\\(?:\\.[0-9]+\\)?\\(?:[eE][-+]?[0-9]+\\)?\\_>")
         (re-operator
          "\\(?:=~\\|!=\\|<=\\|>=\\|=\\|<\\|>\\|[+*/%^|-]\\)"))
    `((,re-command 1 font-lock-keyword-face)
      (,re-function 1 font-lock-function-name-face)
      (,re-keyword . font-lock-builtin-face)
      (,re-regex-after-like-or-match 1 font-lock-constant-face)
      (,re-regex-in-parse 1 font-lock-constant-face)
      ("\\_<\\(?:true\\|false\\|null\\)\\_>" . font-lock-constant-face)
      (,re-duration . font-lock-constant-face)
      (,re-number . font-lock-constant-face)
      (,re-operator . font-lock-builtin-face)
      ("`[^`\n]+`" . font-lock-variable-name-face)
      ("@[A-Za-z0-9_.]+" . font-lock-variable-name-face)))
  "Font-lock keywords for `aws-logs-insights-query-mode`.")

(defvar aws-logs-insights-query-mode-syntax-table
  (let ((st (make-syntax-table)))
    ;; Treat # ... end-of-line as comments so strings inside comments are ignored.
    (modify-syntax-entry ?# "<" st)
    (modify-syntax-entry ?\n ">" st)
    ;; Query language supports single-quoted and double-quoted strings.
    (modify-syntax-entry ?\" "\"" st)
    (modify-syntax-entry ?' "\"" st)
    st)
  "Syntax table for `aws-logs-insights-query-mode`.")

(define-derived-mode aws-logs-insights-query-mode prog-mode "LogsInsightsQL"
  "Major mode for AWS CloudWatch Logs Insights query language.

Supports basic syntax highlighting and #-style comments."
  :syntax-table aws-logs-insights-query-mode-syntax-table
  :group 'aws-logs-query
  (setq-local comment-start "#")
  (setq-local comment-end "")
  (setq-local comment-start-skip "#\\s-*")

  (setq-local font-lock-keywords-case-fold-search t)
  (setq-local font-lock-defaults '(aws-logs-insights-query-mode-font-lock-keywords))

  (font-lock-mode 1)
  (font-lock-flush))

(defun aws-logs-insights-query-fontify-string (query)
  "Return QUERY with Logs Insights syntax highlighting text properties.

Existing text properties in QUERY are stripped before fontification."
  (if (or (null query) (string-empty-p query))
      query
    (with-temp-buffer
      (insert (substring-no-properties query))
      (aws-logs-insights-query-mode)
      (font-lock-ensure (point-min) (point-max))
      (buffer-string))))

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
  (setq aws-logs--insights-query-edit-result
        (string-trim-right
         (buffer-substring-no-properties (point-min) (point-max))))
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
        (when initial (insert (substring-no-properties initial))))
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
