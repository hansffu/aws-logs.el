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

(require 'aws-logs-query)
(require 'aws-logs-insights)

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
(defcustom aws-logs-default-profile "dev"
  "Default AWS CLI profile name to use.

The transient UI updates the session variable `aws-logs-profile` using `setq`.
If you want to change the default for new Emacs sessions, customize this option."
  :type '(choice (const :tag "Default" nil) string)
  :group 'aws-logs)

(defcustom aws-logs-since "10m"
  "Default time range to use as --since for `aws logs tail`."
  :type 'string
  :group 'aws-logs)

(defcustom aws-logs-default-log-group nil
  "Default CloudWatch log group used to initialize `aws-logs-log-group`."
  :type '(choice (const :tag "None" nil) string)
  :group 'aws-logs)

(defcustom aws-logs-default-query nil
  "Default Logs Insights query used to initialize `aws-logs-query`."
  :type '(choice (const :tag "None" nil) string)
  :group 'aws-logs)

(defcustom aws-logs-default-summary-timestamp-field nil
  "Optional default field/path used for Insights summary timestamp."
  :type '(choice (const :tag "Auto (guess)" nil) string)
  :group 'aws-logs)

(defcustom aws-logs-default-summary-level-field nil
  "Optional default field/path used for Insights summary level."
  :type '(choice (const :tag "Auto (guess)" nil) string)
  :group 'aws-logs)

(defcustom aws-logs-default-summary-message-field nil
  "Optional default field/path used for Insights summary message."
  :type '(choice (const :tag "Auto (guess)" nil) string)
  :group 'aws-logs)

(defcustom aws-logs-default-summary-extra-fields nil
  "Optional list of extra field/paths to include in Insights summaries.

Each configured field is rendered as a bracketed segment between level and
message, for example: [service] [class]."
  :type '(choice (const :tag "None" nil) (repeat string))
  :group 'aws-logs)

(defcustom aws-logs-default-follow nil
  "Default value for `aws-logs-follow`."
  :type 'boolean
  :group 'aws-logs)

(defcustom aws-logs-default-since aws-logs-since
  "Default time range used to initialize `aws-logs-time-range`."
  :type 'string
  :group 'aws-logs)
(defcustom aws-logs-mode-hook '()
  "Hook for customizing aws-logs-mode."
  :type 'hook
  :group 'aws-logs)

(defcustom aws-logs-default-group nil
  "Default log group to use (legacy).

Prefer `aws-logs-default-log-group`."
  :type '(choice (const :tag "None" nil) string)
  :group 'aws-logs)

;; Backing fields for transient options.
;; These are the source of truth for aws-logs option state.

(defvar aws-logs-profile aws-logs-default-profile
  "AWS CLI profile name to use for this Emacs session.")

(defvar aws-logs-log-group aws-logs-default-log-group
  "Selected CloudWatch log group for this Emacs session.")

(defvar aws-logs-query aws-logs-default-query
  "Selected CloudWatch Logs Insights query string for this Emacs session, or nil when disabled.")

(defvar aws-logs-summary-timestamp-field aws-logs-default-summary-timestamp-field
  "Optional field/path used for Insights summary timestamp in this session.")

(defvar aws-logs-summary-level-field aws-logs-default-summary-level-field
  "Optional field/path used for Insights summary level in this session.")

(defvar aws-logs-summary-message-field aws-logs-default-summary-message-field
  "Optional field/path used for Insights summary message in this session.")

(defvar aws-logs-summary-extra-fields aws-logs-default-summary-extra-fields
  "Optional list of extra field/paths for Insights summary in this session.")

(defvar aws-logs-follow aws-logs-default-follow
  "Non-nil means follow (tail) logs for this Emacs session.")

(defvar aws-logs-time-range aws-logs-default-since
  "Selected time range (e.g. 10m) for this Emacs session. Used as --since for `aws logs tail`.")

(defvar aws-logs-presets nil
  "Alist of named aws-logs presets.

Each element has the form (NAME . PLIST), where NAME is a string and PLIST is
a property list as accepted by `aws-logs-make-preset`.")

(defun aws-logs--transient-reprompt ()
  "Refresh transient so UI reflects current backing fields." 
  (transient-quit-one)
  (transient-setup 'aws-logs-transient))


(declare-function aws-logs--insights-query-edit "aws-logs-query" (initial))
(declare-function aws-logs-insights-query-mode "aws-logs-query")
(defvar aws-logs-mode-map
  (let ((map (make-keymap)))
    (define-key map "q" #'aws-logs-quit-process-and-window)
    map)
  "Keymap for aws-logs-mode.")

(defun aws-logs--query ()
  "Return a one-line summary of `aws-logs-query` for display in transient."
  (if (and aws-logs-query (not (string-empty-p aws-logs-query)))
      (let ((s (replace-regexp-in-string "[\n\t ]+" " " aws-logs-query)))
        (if (> (length s) 80)
            (concat (substring s 0 77) "…")
          s))
    "— (none)"))

(defun aws-logs-quit-process-and-window ()
  "Quit current process and window."
  (interactive)
  (let ((proc (get-buffer-process (current-buffer))))
    (when (process-live-p proc)
      (delete-process proc))
    (quit-window t)))


(defun aws-logs--command (&rest args)
  "Build cli command with endpoint, region and ARGS."
  (let ((endpoint (if aws-logs-endpoint (format "--endpoint-url=%s" aws-logs-endpoint) ""))
        (region (format "--region=%s" aws-logs-region))
        (profile (when aws-logs-profile (format "--profile=%s" aws-logs-profile))))
    (string-join (delq nil (append (list aws-logs-cli endpoint region profile) args)) " ")))

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

(defcustom aws-logs-insights-saved-queries nil
  "Alist of named saved Logs Insights queries.

Each entry is (NAME . QUERY)."
  :type '(alist :key-type string :value-type string)
  :group 'aws-logs)

(transient-define-suffix aws-logs-query-apply-saved ()
  "Select a saved Logs Insights query and store it in the backing field."
  :transient t
  (interactive)
  (unless aws-logs-insights-saved-queries
    (user-error "No saved queries configured in `aws-logs-insights-saved-queries`"))
  (let* ((name (completing-read "Saved query: " (mapcar #'car aws-logs-insights-saved-queries) nil t))
         (query (cdr (assoc name aws-logs-insights-saved-queries))))
    (setq aws-logs-query query)
    (aws-logs--transient-reprompt)))

(defconst aws-logs--preset-keys
  '(:log-group :since :follow :profile :query
    :summary-timestamp-field :summary-level-field :summary-message-field
    :summary-extra-fields)
  "Allowed keys for aws-logs presets.")

(defun aws-logs--preset-plist-valid-p (plist)
  "Return non-nil if PLIST is valid for `aws-logs-make-preset`.

Signals a `user-error` when an unsupported key is present."
  (let ((cursor plist))
    (while cursor
      (let ((key (car cursor)))
        (unless (keywordp key)
          (user-error "Preset key must be a keyword, got: %S" key))
        (unless (memq key aws-logs--preset-keys)
          (user-error "Unsupported preset key: %S" key)))
      (setq cursor (cddr cursor))))
  t)

(defun aws-logs-make-preset (name &rest options)
  "Create or replace a named aws-logs preset NAME.

NAME is a string (or symbol, which is converted to string). OPTIONS is a plist
with any subset of these keys:

- `:log-group` string or nil
- `:since` string or nil
- `:follow` boolean or nil
- `:profile` string or nil
- `:query` string or nil
- `:summary-timestamp-field` string or nil
- `:summary-level-field` string or nil
- `:summary-message-field` string or nil
- `:summary-extra-fields` list of strings or nil

Preset apply semantics:
- Missing key: leave current session value unchanged
- Key present with nil: unset/clear session value
- Key present with non-nil: set session value

If a preset with NAME already exists, it is replaced."
  (let ((preset-name (if (symbolp name) (symbol-name name) name)))
    (unless (stringp preset-name)
      (user-error "Preset name must be a string or symbol, got: %S" name))
    (unless (zerop (% (length options) 2))
      (user-error "Preset options must be key/value pairs"))
    (aws-logs--preset-plist-valid-p options)
    (setq aws-logs-presets (assoc-delete-all preset-name aws-logs-presets))
    (push (cons preset-name options) aws-logs-presets)
    (car aws-logs-presets)))

(defun aws-logs--apply-preset-plist (plist)
  "Apply preset PLIST to current session backing fields."
  (dolist (entry '((:log-group . aws-logs-log-group)
                   (:since . aws-logs-time-range)
                   (:follow . aws-logs-follow)
                   (:profile . aws-logs-profile)
                   (:query . aws-logs-query)
                   (:summary-timestamp-field . aws-logs-summary-timestamp-field)
                   (:summary-level-field . aws-logs-summary-level-field)
                   (:summary-message-field . aws-logs-summary-message-field)
                   (:summary-extra-fields . aws-logs-summary-extra-fields)))
    (let ((key (car entry))
          (var (cdr entry)))
      (when (plist-member plist key)
        (set var (plist-get plist key))))))

(transient-define-suffix aws-logs-apply-preset ()
  "Select and apply a preset from `aws-logs-presets`."
  :transient t
  (interactive)
  (unless aws-logs-presets
    (user-error "No presets configured; use `aws-logs-make-preset`"))
  (let* ((name (completing-read "Preset: " (mapcar #'car aws-logs-presets) nil t))
         (preset (assoc name aws-logs-presets)))
    (unless preset
      (user-error "Preset not found: %s" name))
    (aws-logs--apply-preset-plist (cdr preset))
    (aws-logs--transient-reprompt)))

(transient-define-infix aws-logs-infix-follow ()
  :description "Follow (tail)"
  :class 'transient-switch
  :argument "--follow"
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-follow))
  :reader (lambda (_prompt initial _hist)
            (setq aws-logs-follow (not initial))
            aws-logs-follow))

(transient-define-infix aws-logs-infix-profile ()
  :description "Profile"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-profile))
  :reader (lambda (prompt initial hist)
            (let ((val (read-string prompt initial hist)))
              (setq aws-logs-profile (if (string-empty-p val) nil val))
              aws-logs-profile))
  :argument "--profile=")

;; Infix for specifying --since / time-range
(transient-define-infix aws-logs-infix-since ()
  :description "Since / Time range"
  :class 'transient-option
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-time-range))
  :reader (lambda (prompt initial hist)
            (let ((val (read-string prompt initial hist)))
              (setq aws-logs-time-range val)
              val))
  :argument "--since=")

(transient-define-infix aws-logs-infix-summary-timestamp-field ()
  :description "Timestamp field"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-summary-timestamp-field))
  :reader (lambda (_prompt initial _hist)
            (let ((input (string-trim (read-string "Timestamp field (empty=auto): "
                                                   (or initial "")))))
              (setq aws-logs-summary-timestamp-field
                    (unless (string-empty-p input) input))
              aws-logs-summary-timestamp-field))
  :argument "--summary-timestamp=")

(transient-define-infix aws-logs-infix-summary-level-field ()
  :description "Level field"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-summary-level-field))
  :reader (lambda (_prompt initial _hist)
            (let ((input (string-trim (read-string "Level field (empty=auto): "
                                                   (or initial "")))))
              (setq aws-logs-summary-level-field
                    (unless (string-empty-p input) input))
              aws-logs-summary-level-field))
  :argument "--summary-level=")

(transient-define-infix aws-logs-infix-summary-message-field ()
  :description "Message field"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-summary-message-field))
  :reader (lambda (_prompt initial _hist)
            (let ((input (string-trim (read-string "Message field (empty=auto): "
                                                   (or initial "")))))
              (setq aws-logs-summary-message-field
                    (unless (string-empty-p input) input))
              aws-logs-summary-message-field))
  :argument "--summary-message=")

(defun aws-logs--formatting-reprompt ()
  "Refresh the formatting transient."
  (transient-quit-one)
  (transient-setup 'aws-logs-formatting-transient))

(defun aws-logs--formatting-extra-summary ()
  "Return one-line summary of currently configured extra fields."
  (if aws-logs-summary-extra-fields
      (string-join aws-logs-summary-extra-fields ", ")
    "none"))

(transient-define-suffix aws-logs-formatting-extra-add ()
  "Add one extra summary field/path."
  :transient t
  (interactive)
  (let ((input (string-trim (read-string "Add extra field: "))))
    (when (string-empty-p input)
      (user-error "Field cannot be empty"))
    (unless (member input aws-logs-summary-extra-fields)
      (setq aws-logs-summary-extra-fields
            (append aws-logs-summary-extra-fields (list input))))
    (aws-logs--formatting-reprompt)))

(transient-define-suffix aws-logs-formatting-extra-delete ()
  "Delete one extra summary field/path."
  :transient t
  (interactive)
  (unless aws-logs-summary-extra-fields
    (user-error "No extra fields to delete"))
  (let ((selection (completing-read "Delete extra field: "
                                    aws-logs-summary-extra-fields nil t)))
    (setq aws-logs-summary-extra-fields
          (delete selection (copy-sequence aws-logs-summary-extra-fields)))
    (aws-logs--formatting-reprompt)))

(transient-define-suffix aws-logs-formatting-done ()
  "Return from formatting transient to the main transient."
  :transient nil
  (interactive)
  (transient-quit-one)
  (transient-setup 'aws-logs-transient))

(transient-define-prefix aws-logs-formatting-transient ()
  "Formatting options for Insights summary rendering."
  :remember-value 'exit
  [["Fields"
    ("t" "Timestamp field" aws-logs-infix-summary-timestamp-field)
    ("l" "Level field" aws-logs-infix-summary-level-field)
    ("m" "Message field" aws-logs-infix-summary-message-field)]

   [4 :description (lambda () (format "Extras: %s" (aws-logs--formatting-extra-summary)))
      ("a" "Add extra field" aws-logs-formatting-extra-add)
      ("d" "Delete extra field" aws-logs-formatting-extra-delete)]]

  [["Done"
    ("RET" "Back to main" aws-logs-formatting-done)]]
  (interactive)
  (transient-setup 'aws-logs-formatting-transient))

(transient-define-suffix aws-logs-open-formatting ()
  "Open formatting transient."
  :transient nil
  (interactive)
  (transient-setup 'aws-logs-formatting-transient))


(defun aws-logs--getopt (name args)
  "Get value for option prefix NAME (e.g. \"--since=\") from ARGS."
  (when-let ((s (seq-find (lambda (a) (string-prefix-p name a)) args)))
    (substring s (length name))))

(defun aws-logs--tail-args ()
  "Build argument list for `aws logs tail` using current backing fields."
  (append
   (list "logs" "tail" aws-logs-log-group
         "--format" aws-logs-format)
   (when aws-logs-follow
     (list "--follow"))
   (when (and aws-logs-time-range (not (string-empty-p aws-logs-time-range)))
     (list "--since" aws-logs-time-range))))

(defun aws-logs--global-args ()
  "Build common AWS CLI arguments from current backing fields."
  (delq nil
        (list
         (when aws-logs-endpoint (format "--endpoint-url=%s" aws-logs-endpoint))
         (format "--region=%s" aws-logs-region)
         (when aws-logs-profile (format "--profile=%s" aws-logs-profile)))))

(transient-define-suffix aws-logs-tail ()
  "Start streaming logs in a dedicated buffer using current transient selections."
  :transient nil
  (interactive)
  (unless (and aws-logs-log-group (not (string-empty-p aws-logs-log-group)))
    (user-error "Select a log group first"))
  (let* ((buffer (get-buffer-create (format "*AWS logs - %s*" aws-logs-log-group)))
         (process-name (format "aws-logs-tail:%s" aws-logs-log-group))
         (args (append (aws-logs--global-args) (aws-logs--tail-args)))
         (process (apply #'start-process process-name buffer aws-logs-cli args)))
    (with-current-buffer buffer
      (let ((inhibit-read-only t))
        (erase-buffer))
      (display-buffer buffer)
      (aws-logs-mode)
      (set-process-filter process #'comint-output-filter))
    (set-process-query-on-exit-flag process nil)
    (message "Started logs tail for %s" aws-logs-log-group)))

(transient-define-suffix aws-logs-insights ()
  "Run Logs Insights query and show results in a dedicated buffer."
  :transient nil
  (interactive)
  (aws-logs-insights-run))

(defun aws-logs--dwim-description ()
  "Return dynamic description for the DWIM action."
  (let ((target (if (and aws-logs-query (not (string-empty-p aws-logs-query)))
                    "Insights"
                  "Tail")))
    (concat "Run " (propertize (format "(%s)" target) 'face 'transient-active-infix))))

(transient-define-suffix aws-logs-dwim ()
  "Run tail or Logs Insights depending on whether a query is set."
  :description #'aws-logs--dwim-description
  :transient nil
  (interactive)
  (if (and aws-logs-query (not (string-empty-p aws-logs-query)))
      (call-interactively #'aws-logs-insights)
    (call-interactively #'aws-logs-tail)))

(defvar aws-logs--transient-history nil)


;; Main transient prefix
(transient-define-prefix aws-logs-transient ()
  "AWS Logs transient menu.

Use the Tail action to stream logs with current selections."
  :remember-value 'exit
  [[("@" "Apply preset…" aws-logs-apply-preset)]]
  [["Config"
    ("-g" aws-logs-infix-log-group)
    ("-s" "Since" aws-logs-infix-since)
    ("-f" "Follow (tail)" aws-logs-infix-follow)
    ("-p" "Profile" aws-logs-infix-profile)]

   [4 :description (lambda () (format "Query: %s" (aws-logs--query)))
      ("-q" "Edit query…" aws-logs-query-edit)
      ("Q" "Apply saved query…" aws-logs-query-apply-saved)
      ("X" "Clear query" aws-logs-query-clear)
      ("f" "Formatting…" aws-logs-open-formatting)]]

  [["Actions"
    ("RET" aws-logs-dwim)
    ("t" "Tail" aws-logs-tail)
    ("l" "Logs Insights" aws-logs-insights)]]
  (interactive)
  (transient-setup 'aws-logs-transient))

(defun aws-logs ()
  "Open aws-logs transient menu for selecting and running log-tail options."
  (interactive)
  (call-interactively #'aws-logs-transient))

(provide 'aws-logs)
;;; aws-logs.el ends here
