;;; aws-logs.el --- Description -*- lexical-binding: t; -*-
;;
;; Copyright (C) 2021 Hans Fredrik Furholt
;;
;; Author: Hans Fredrik Furholt <https://github.com/hansffu>
;; Created: March 19, 2021
;; Modified: March 19, 2021
;; Version: 0.0.1
;; Keywords: Symbol’s value as variable is void: finder-known-keywords
;; Homepage: https://github.com/hansffu/aws-logs
;; Package-Requires: ((emacs "30.2"))
;;
;; This file is not part of GNU Emacs.
;;
;;; Commentary:
;;
;;  Description
;;
;;; Code:

(require 'subr-x)
(require 'comint)
(require 'transient)

;; CloudWatch Logs Insights query editing
(require 'font-lock)

(defcustom aws-logs-cli "aws"
  "The cli command for aws cli."
  :type 'string
  :group 'aws-logs)
(defcustom aws-logs-endpoint nil
  "Customize endpoint."
  :type 'string
  :group 'aws-logs)
(defcustom aws-logs-region "eu-north-1"
  "Customize region."
  :type 'string
  :group 'aws-logs)
(defcustom aws-logs-format "detailed"
  "Sets the --format option on aws command."
  :type 'string
  :options '("detailed" "short" "json")
  :group 'aws-logs)
(defcustom aws-logs-since "10m"
  "Sets the --since option on aws command."
  :type 'string
  :group 'aws-logs)
(defcustom aws-logs-mode-hook '()
  "Hook for customizing aws-logs-mode."
  :type 'hook
  :group 'aws-logs)

(defcustom aws-logs-default-group nil
  "Default log group to use"
  :type 'string
  :group 'aws-logs)

;; Backing fields for transient options.
;; These are the source of truth for aws-logs option state.
(defvar aws-logs-log-group aws-logs-default-group
  "Selected CloudWatch log group.")

(defvar aws-logs-query nil
  "Selected CloudWatch Logs Insights query string, or nil when disabled.")

(defvar aws-logs-follow nil
  "Non-nil means follow (tail) logs.")

(defvar aws-logs-time-range aws-logs-since
  "Selected time range (e.g. 10m). Used as --since for `aws logs tail`.")

(defun aws-logs--transient-reprompt ()
  "Refresh transient so UI reflects current backing fields." 
  (transient-quit-one)
  (transient-setup 'aws-logs-transient))

(defvar aws-logs-mode-map
  (let ((map (make-keymap)))
    (define-key map "q" #'aws-logs-quit-process-and-window)
    map)
  "Keymap for aws-logs-mode.")

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
    `(
      ("#.*$" . font-lock-comment-face)

      (,re-commands 1 font-lock-keyword-face)
      (,re-control 1 font-lock-builtin-face)
      (,re-functions 1 font-lock-function-name-face)
      ("@[A-Za-z0-9_]+" . font-lock-variable-name-face)
      ("/[^/\\n]*\\(?:\\.[^/\\n]*\\)*/" . font-lock-constant-face)))
  "Font-lock keywords for `aws-logs-insights-query-mode`.")

(define-derived-mode aws-logs-insights-query-mode prog-mode "LogsInsightsQL"
  "Major mode for AWS CloudWatch Logs Insights query language.

Supports basic syntax highlighting and #-style comments."
  :group 'aws-logs
  (setq-local comment-start "#")
  (setq-local comment-end "")
  (setq-local comment-start-skip "#\\s-*")

  (setq-local font-lock-keywords-case-fold-search t)
  (setq-local font-lock-defaults '(aws-logs-insights-query-mode-font-lock-keywords))

  (font-lock-mode 1)
  (font-lock-flush))

(defun aws-logs-quit-process-and-window ()
  "Quit current process and window."
  (interactive)
  (cl-letf (((symbol-function 'process-kill-buffer-query-function) (lambda () (always nil))))))

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
                       ;; nil => resize height (not width)
                       nil
                       ;; t => don't error if limited; resize as much as possible
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


(defun aws-logs--command (&rest args)
  "Build cli command with endpoint, region and ARGS."
  (let ((endpoint (if aws-logs-endpoint (format "--endpoint-url=%s" aws-logs-endpoint) ""))
        (region (format "--region=%s" aws-logs-region))
        (profile "--profile=dev"))
    (string-join (append (list aws-logs-cli endpoint region profile) args) " ")
    )
  )

(defun aws-logs--list-log-groups ()
  "Return log group name for all log groups."
  (let* ((result (with-temp-buffer
                   (list :exit-status
                         (call-process-shell-command (aws-logs--command "logs" "describe-log-groups") nil t)
                         :output
                         (buffer-string))))
         (output (plist-get result :output))
         (log-groups (alist-get 'logGroups (json-parse-string output :object-type 'alist) ))
         )
    (mapcar (lambda (log-group) (alist-get 'logGroupName log-group)) log-groups)
    )
  )

(define-derived-mode aws-logs-mode comint-mode "AWS-LOGS"
  "Displays logs fetched by aws-logs command."
  :interactive nil
  :group 'aws-logs
  (read-only-mode 1)
  )

(defun aws-logs-simple ()
  "Select a stream to tail."
  (interactive)
  (let* ((log-group (completing-read "Log group: " (aws-logs--list-log-groups)))
         (process (start-process-shell-command "aws-cli"
                                               (format "*AWS logs - %s*" log-group)
                                               (aws-logs--command "logs tail" log-group
                                                                  "--follow"
                                                                  "--format" aws-logs-format
                                                                  "--since" aws-logs-since))))
    (with-current-buffer (process-buffer process)
      (display-buffer (current-buffer))
      (aws-logs-mode)
      (set-process-filter process 'comint-output-filter)
      )
    ))

(transient-define-infix aws-logs-infix-log-group ()
  :description "Log group"
  :class 'transient-option
  :allow-empty nil
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-log-group))
  :reader (lambda (prompt initial hist)
            (let ((choices (aws-logs--list-log-groups)))
              (if choices
                  (let ((val (completing-read (concat prompt " ") choices nil t initial hist)))
                    (setq aws-logs-log-group val)
                    val)
                (read-string (concat prompt " (no groups found) ") ""))))
  :argument "--log-group=")

;; Infix for showing current Logs Insights query status. Editing is done via
;; suffix commands (see `aws-logs-query-edit`, `aws-logs-query-clear`, etc.).
(transient-define-infix aws-logs-infix-query ()
  :description "Query (Logs Insights)"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj)
                ;; Ensure the visible value always reflects the backing field
                ;; when the transient is entered/re-entered.
                (transient-infix-set obj aws-logs-query))
  :reader (lambda (_prompt initial _hist)
            ;; Keep this lightweight: just accept raw input if someone insists.
            ;; Most users should use the Edit/Preset/Clear suffixes.
            (let ((val (read-string "Query: " (or initial ""))))
              (setq aws-logs-query (if (string-empty-p val) nil val))
              val))
  :argument "--query=")

(transient-define-suffix aws-logs-query-edit ()
  "Edit the current Logs Insights query in a popup, and store it in the backing field."
  :transient t
  (interactive)
  (let ((edited (aws-logs--insights-query-edit aws-logs-query)))
    (when edited
      (setq aws-logs-query (if (string-empty-p edited) nil edited))
      (aws-logs--transient-reprompt))))

(transient-define-suffix aws-logs-query-clear ()
  "Disable the Logs Insights query (unset `aws-logs-query`)."
  :transient t
  (interactive)
  (setq aws-logs-query nil)
  (aws-logs--transient-reprompt))

(defcustom aws-logs-insights-query-presets nil
  "Alist of named Logs Insights query presets.

Each entry is (NAME . QUERY)."
  :type '(alist :key-type string :value-type string)
  :group 'aws-logs)

(transient-define-suffix aws-logs-query-preset ()
  "Select a preset Logs Insights query and store it in the backing field."
  :transient t
  (interactive)
  (unless aws-logs-insights-query-presets
    (user-error "No presets configured in `aws-logs-insights-query-presets`"))
  (let* ((name (completing-read "Preset: " (mapcar #'car aws-logs-insights-query-presets) nil t))
         (query (cdr (assoc name aws-logs-insights-query-presets))))
    (setq aws-logs-query query)
    (aws-logs--transient-reprompt)))

(transient-define-infix aws-logs-infix-follow ()
  :description "Follow (tail)"
  :class 'transient-switch
  :argument "--follow"
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-follow))
  :reader (lambda (_prompt initial _hist)
            (setq aws-logs-follow (not initial))
            aws-logs-follow))

;; Infix for specifying --since / time-range
(transient-define-infix aws-logs-infix-since ()
  :description "Since / Time range"
  :class 'transient-option
  :reader (lambda (prompt initial hist)
            (let ((val (read-string prompt initial hist)))
              (setq aws-logs-time-range val)
              val))
  :argument "--since=")


(defun aws-logs--getopt (name args)
  "Get value for option prefix NAME (e.g. \"--since=\") from ARGS."
  (when-let ((s (seq-find (lambda (a) (string-prefix-p name a)) args)))
    (substring s (length name))))

(transient-define-suffix aws-logs-print-logs ()
  (interactive)
  (message "aws-logs selection:\n  log-group=%S\n  since=%S\n  query=%S\n  follow=%S"
           aws-logs-log-group aws-logs-time-range aws-logs-query aws-logs-follow)
  (message "%s" (aws-logs--command "logs tail" aws-logs-log-group
                                   (when aws-logs-follow "--follow")
                                   (when aws-logs-time-range (format "--since=%s" aws-logs-time-range)))))

(defvar aws-logs--transient-history nil)


;; Main transient prefix
(transient-define-prefix aws-logs-transient ()
  "AWS Logs transient menu.

This transient currently only collects options; the Run action is a placeholder
and is not implemented yet."
  :remember-value 'exit
  [["Target"
    ("g" aws-logs-infix-log-group)]
   
   ["Time / Tail options"
    ("s" "Since" aws-logs-infix-since)
    ("f" "Follow (tail)" aws-logs-infix-follow)]
   
   [4 :description (lambda ()(format "Query: %s" (aws-logs--query-summary)))
      ("e" "Edit query…" aws-logs-query-edit)
      ("p" "Preset…" aws-logs-query-preset)
      ("x" "Clear query" aws-logs-query-clear)]]
  [["Actions"
    ("t" "Tail" aws-logs-print-logs)]]
  ;; Hint line
  (interactive)
  (transient-setup 'aws-logs-transient))

(defun aws-logs ()
  "Open aws-logs transient menu for selecting log group, query and time options.

This currently only opens the transient UI; executing the chosen options is
not implemented yet."
  (interactive)
  (call-interactively #'aws-logs-transient))

(provide 'aws-logs)
;;; aws-logs.el ends here
