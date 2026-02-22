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
(require 'transient)
(require 'json)

(require 'aws-logs-query)
(require 'aws-logs-insights)
(require 'aws-logs-tail)

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

(defcustom aws-logs-insights-live-narrow-max-rows 2000
  "Maximum number of Insights rows that allow live narrowing updates.

When an Insights results buffer has more rows than this value, `C-c C-n`
still prompts for a filter but only applies it once after RET.
Set to nil to always allow live updates."
  :type '(choice (const :tag "No limit" nil) integer)
  :group 'aws-logs)

(defcustom aws-logs-insights-live-narrow-debounce 0.2
  "Idle seconds to wait before applying live Insights narrowing updates."
  :type 'number
  :group 'aws-logs)

(defcustom aws-logs-insights-refresh-overlap-seconds 2
  "Seconds of overlap used for incremental Insights refresh windows.

Using a small overlap helps catch late-ingested events. Duplicate rows are
filtered by signature before rendering."
  :type 'integer
  :group 'aws-logs)

(defcustom aws-logs-default-follow nil
  "Default value for `aws-logs-follow`."
  :type 'boolean
  :group 'aws-logs)

(defcustom aws-logs-default-ecs nil
  "Default value for `aws-logs-ecs`."
  :type 'boolean
  :group 'aws-logs)

(defcustom aws-logs-default-filter nil
  "Default regex filter for tail output.

When non-nil, tail output is piped through grep with this regex."
  :type '(choice (const :tag "No filter" nil) string)
  :group 'aws-logs)

(defcustom aws-logs-tail-ecs-batch-flush-interval 0.08
  "Idle seconds before flushing queued ECS follow lines to the viewer."
  :type 'number
  :group 'aws-logs)

(defcustom aws-logs-tail-ecs-batch-max-lines 200
  "Maximum queued ECS follow lines before forcing an immediate flush."
  :type 'integer
  :group 'aws-logs)

(defcustom aws-logs-tail-ecs-normalize-batch-lines 300
  "Maximum raw ECS lines normalized per idle batch.

Lower values improve UI responsiveness under heavy throughput, while higher
values maximize throughput."
  :type 'integer
  :group 'aws-logs)

(defcustom aws-logs-tail-ecs-chunk-batch-size 16
  "Maximum process output chunks consumed per idle batch in ECS follow mode.

Lower values reduce single-run latency in the main thread, while higher values
improve throughput when output arrives very quickly."
  :type 'integer
  :group 'aws-logs)

(defcustom aws-logs-default-since aws-logs-since
  "Default time range used to initialize `aws-logs-time-range`."
  :type 'string
  :group 'aws-logs)

(defcustom aws-logs-default-custom-time-range nil
  "Default explicit from/to range used to initialize `aws-logs-custom-time-range`.

Value is nil or a cons cell (FROM . TO), where each value is a date-time
string parseable by Emacs `date-to-time`."
  :type '(choice (const :tag "None" nil) (cons :tag "From/To" string string))
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
  "Selected CloudWatch Logs Insights query string for this Emacs session,
or nil when disabled.")

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

(defvar aws-logs-ecs aws-logs-default-ecs
  "Non-nil means render tail logs as ECS JSON in `json-log-viewer`.")

(defvar aws-logs-filter aws-logs-default-filter
  "Regex filter for tail output in this Emacs session, or nil.")

(defvar aws-logs-time-range aws-logs-default-since
  "Selected time range (e.g. 10m) for this Emacs session.
Used as --since for `aws logs tail`.")

(defvar aws-logs-custom-time-range aws-logs-default-custom-time-range
  "Selected explicit from/to range for this Emacs session.

Value is nil or (FROM . TO), where both are date-time strings.")

(defvar aws-logs-presets nil
  "Alist of named aws-logs presets.

Each element has the form (NAME . PLIST), where NAME is a string and PLIST is
a property list as accepted by `aws-logs-make-preset`.")

(defun aws-logs--transient-reprompt ()
  "Refresh transient so UI reflects current backing fields."
  (transient-quit-one)
  (transient-setup 'aws-logs-transient))

(defun aws-logs--query-transient-reprompt ()
  "Refresh query transient so UI reflects current backing fields."
  (transient-quit-one)
  (transient-setup 'aws-logs-query-transient))


(declare-function aws-logs--insights-query-edit "aws-logs-query" (initial))
(declare-function aws-logs-insights-query-mode "aws-logs-query")
(declare-function aws-logs-insights-query-fontify-string "aws-logs-query" (query))
(declare-function aws-logs-tail-run "aws-logs-tail" ())
(declare-function org-read-date "org"
                  (&optional with-time to-time from-string prompt default-time default-input))

(defun aws-logs--query ()
  "Return a one-line summary of `aws-logs-query` for display in transient."
  (if (and aws-logs-query (not (string-empty-p aws-logs-query)))
      (let* ((fontified (aws-logs-insights-query-fontify-string aws-logs-query))
             (s (with-temp-buffer
                  (insert fontified)
                  (goto-char (point-min))
                  (while (re-search-forward "[\n\t ]+" nil t)
                    (replace-match " "))
                  (buffer-string))))
        (if (> (length s) 80)
            (concat (substring s 0 77) "…")
          s))
    "— (none)"))

(defun aws-logs--query-full ()
  "Return full multi-line `aws-logs-query` for query transient display."
  (if (and aws-logs-query (not (string-empty-p aws-logs-query)))
      (aws-logs-insights-query-fontify-string aws-logs-query)
    "— (none)"))

(defun aws-logs--global-args ()
  "Build common AWS CLI global arguments from current backing fields."
  (delq nil
        (list
         (when aws-logs-endpoint (format "--endpoint-url=%s" aws-logs-endpoint))
         (format "--region=%s" aws-logs-region)
         (when aws-logs-profile (format "--profile=%s" aws-logs-profile)))))

(defun aws-logs--list-log-groups ()
  "Return list of CloudWatch log group names."
  (with-temp-buffer
    (let* ((args (append (aws-logs--global-args)
                         (list "logs" "describe-log-groups")))
           (exit-status (apply #'call-process aws-logs-cli nil t nil args))
           (output (string-trim (buffer-string))))
      (unless (zerop exit-status)
        (user-error "AWS CLI failed (%s): %s"
                    exit-status
                    (if (string-empty-p output) "no output" output)))
      (let* ((payload (condition-case _err
                          (json-parse-string output :object-type 'alist)
                        (error
                         (user-error "Failed to parse `describe-log-groups` JSON output: %s"
                                     (if (string-empty-p output) "empty output" output)))))
             (log-groups (or (alist-get 'logGroups payload) nil)))
        (unless (listp log-groups)
          (user-error "Unexpected `describe-log-groups` response: missing `logGroups` list"))
        (delq nil
              (mapcar (lambda (log-group)
                        (let ((name (alist-get 'logGroupName log-group)))
                          (and (stringp name) name)))
                      log-groups))))))

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
;; the query transient (see `aws-logs-query-transient`).
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
      (aws-logs--query-transient-reprompt))))

(transient-define-suffix aws-logs-query-clear ()
  "Disable the Logs Insights query (unset `aws-logs-query`)."
  :transient t
  (interactive)
  (setq aws-logs-query nil)
  (aws-logs--transient-reprompt))

(defcustom aws-logs-insights-saved-queries-directory
  (expand-file-name "aws-logs/insights-queries/" user-emacs-directory)
  "Directory where saved Logs Insights query files are stored.

Each regular file in this directory is treated as a saved query, and the file
name is the key shown in transient prompts. Customize this path if you use a
different layout (for example with `no-littering`)."
  :type 'directory
  :group 'aws-logs)

(defun aws-logs--insights-saved-query-dir ()
  "Return normalized saved-query directory path."
  (file-name-as-directory
   (expand-file-name aws-logs-insights-saved-queries-directory)))

(defun aws-logs--insights-saved-query-key (raw)
  "Validate and normalize saved query key from RAW."
  (let ((key (string-trim (or raw ""))))
    (when (string-empty-p key)
      (user-error "Saved query key cannot be empty"))
    (when (member key '("." ".."))
      (user-error "Invalid saved query key: %s" key))
    (when (string-match-p "[/\\]" key)
      (user-error "Saved query key must be a file name, not a path: %s" key))
    key))

(defun aws-logs--insights-saved-query-path (key)
  "Return absolute saved-query file path for KEY."
  (expand-file-name (aws-logs--insights-saved-query-key key)
                    (aws-logs--insights-saved-query-dir)))

(defun aws-logs--insights-saved-query-files ()
  "Return alist of (KEY . PATH) for saved query files."
  (let ((dir (aws-logs--insights-saved-query-dir)))
    (if (file-directory-p dir)
        (sort
         (delq nil
               (mapcar
                (lambda (name)
                  (let ((path (expand-file-name name dir)))
                    (when (file-regular-p path)
                      (cons name path))))
                (directory-files dir nil directory-files-no-dot-files-regexp)))
         (lambda (a b) (string-lessp (car a) (car b))))
      nil)))

(defun aws-logs--insights-read-saved-query (path)
  "Return saved query text from PATH."
  (with-temp-buffer
    (insert-file-contents path)
    (string-trim-right (buffer-string))))

(defun aws-logs--insights-write-saved-query (path query)
  "Write QUERY to PATH."
  (make-directory (file-name-directory path) t)
  (with-temp-file path
    (insert (string-trim-right query))
    (insert "\n")))

(defun aws-logs--insights-choose-saved-query (prompt)
  "Prompt with PROMPT and return selected (KEY . PATH)."
  (let ((candidates (aws-logs--insights-saved-query-files)))
    (unless candidates
      (user-error "No saved queries found in %s"
                  (aws-logs--insights-saved-query-dir)))
    (let* ((key (completing-read prompt (mapcar #'car candidates) nil t))
           (pair (assoc key candidates)))
      (unless pair
        (user-error "Saved query not found: %s" key))
      pair)))

(transient-define-suffix aws-logs-query-load-saved ()
  "Load a saved Logs Insights query file into the active query."
  :transient t
  (interactive)
  (let* ((pair (aws-logs--insights-choose-saved-query "Saved query: "))
         (path (cdr pair))
         (query (aws-logs--insights-read-saved-query path)))
    (setq aws-logs-query (unless (string-empty-p query) query))
    (aws-logs--query-transient-reprompt)))

(transient-define-suffix aws-logs-query-save ()
  "Save current Logs Insights query to a query file."
  :transient t
  (interactive)
  (unless (and aws-logs-query (not (string-empty-p aws-logs-query)))
    (user-error "Set a Logs Insights query first"))
  (let* ((dir (aws-logs--insights-saved-query-dir))
         (_ (make-directory dir t))
         (key (aws-logs--insights-saved-query-key
               (file-name-nondirectory
                (read-file-name "Save query file: " dir nil nil))))
         (path (aws-logs--insights-saved-query-path key)))
    (when (and (file-exists-p path)
               (not (y-or-n-p (format "Overwrite saved query `%s`? " key))))
      (user-error "Canceled"))
    (aws-logs--insights-write-saved-query path aws-logs-query)
    (message "Saved query `%s`" key)
    (aws-logs--query-transient-reprompt)))

(defconst aws-logs--preset-keys
  '(:log-group :since :follow :ecs :profile :query
               :filter
               :custom-time-range
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
- `:custom-time-range` cons cell (FROM . TO) or nil
- `:follow` boolean or nil
- `:ecs` boolean or nil
- `:profile` string or nil
- `:query` string or nil
- `:filter` string or nil
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
                   (:custom-time-range . aws-logs-custom-time-range)
                   (:follow . aws-logs-follow)
                   (:ecs . aws-logs-ecs)
                   (:filter . aws-logs-filter)
                   (:profile . aws-logs-profile)
                   (:query . aws-logs-query)
                   (:summary-timestamp-field . aws-logs-summary-timestamp-field)
                   (:summary-level-field . aws-logs-summary-level-field)
                   (:summary-message-field . aws-logs-summary-message-field)
                   (:summary-extra-fields . aws-logs-summary-extra-fields)))
    (let ((key (car entry))
          (var (cdr entry)))
      (when (plist-member plist key)
        (set var (plist-get plist key)))))
  ;; Keep relative and explicit ranges mutually exclusive.
  (when (and (plist-member plist :since)
             (plist-get plist :since))
    (setq aws-logs-custom-time-range nil))
  (when (and (plist-member plist :custom-time-range)
             (plist-get plist :custom-time-range))
    (setq aws-logs-time-range nil)))

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

(transient-define-suffix aws-logs-toggle-follow ()
  "Toggle follow mode for tail."
  :description (lambda ()
                 (format "Follow (tail): %s"
                         (if aws-logs-follow "on" "off")))
  :transient t
  (interactive)
  (setq aws-logs-follow (not aws-logs-follow))
  (aws-logs--transient-reprompt))

(transient-define-suffix aws-logs-toggle-ecs ()
  "Toggle ECS JSON viewer mode for tail.

Also persists the toggle by updating `aws-logs-default-ecs`."
  :description (lambda ()
                 (format "ECS JSON viewer: %s"
                         (if aws-logs-ecs "on" "off")))
  :transient t
  (interactive)
  (setq aws-logs-ecs (not aws-logs-ecs))
  (setq aws-logs-default-ecs aws-logs-ecs)
  (condition-case err
      (customize-save-variable 'aws-logs-default-ecs aws-logs-default-ecs)
    (error
     (message "Could not persist `aws-logs-default-ecs`: %s"
              (error-message-string err))))
  (aws-logs--transient-reprompt))

(transient-define-infix aws-logs-infix-profile ()
  :description "Profile"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-profile))
  :reader (lambda (prompt initial hist)
            (let ((val (read-string prompt initial hist)))
              (setq aws-logs-profile (if (string-empty-p val) nil val))
              val))
  :argument "--profile=")

;; Infix for specifying --since / time-range
(transient-define-infix aws-logs-infix-since ()
  :description "Since / Time range"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-time-range))
  :reader (lambda (prompt initial hist)
            (let ((val (string-trim (read-string prompt initial hist))))
              (setq aws-logs-time-range (unless (string-empty-p val) val))
              (setq aws-logs-custom-time-range nil)
              (aws-logs--transient-reprompt)
              val))
  :argument "--since=")

(transient-define-infix aws-logs-infix-filter ()
  :description "Filter regex"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-filter))
  :reader (lambda (_prompt initial _hist)
            (let ((input (string-trim (read-string "Filter regex (empty=none): "
                                                   (or initial "")))))
              (setq aws-logs-filter (unless (string-empty-p input) input))
              input))
  :argument "--filter=")

(defun aws-logs--custom-time-range-to-string ()
  "Return `aws-logs-custom-time-range` as \"FROM - TO\", or nil."
  (when aws-logs-custom-time-range
    (format "%s - %s" (car aws-logs-custom-time-range) (cdr aws-logs-custom-time-range))))

(defun aws-logs--select-custom-time-range ()
  "Interactively select from/to using Org timestamp prompts.

Returns a cons cell (FROM . TO), where both values are ISO-like strings."
  (require 'org)
  (let* ((initial-from (when aws-logs-custom-time-range
                         (ignore-errors (date-to-time (car aws-logs-custom-time-range)))))
         (initial-to (when aws-logs-custom-time-range
                       (ignore-errors (date-to-time (cdr aws-logs-custom-time-range)))))
         (from-time (org-read-date nil t nil "From: " initial-from))
         (to-time (org-read-date nil t nil "To: " (or initial-to from-time))))
    (when (time-less-p to-time from-time)
      (user-error "To must be after From"))
    (cons (format-time-string "%Y-%m-%dT%H:%M:%S%z" from-time)
          (format-time-string "%Y-%m-%dT%H:%M:%S%z" to-time))))

(transient-define-infix aws-logs-infix-custom-time-range ()
  :description "Time Range"
  :class 'transient-option
  :allow-empty t
  :init-value (lambda (obj)
                (transient-infix-set obj (aws-logs--custom-time-range-to-string)))
  :reader (lambda (_prompt _initial _hist)
            (setq aws-logs-custom-time-range (aws-logs--select-custom-time-range))
            (setq aws-logs-time-range nil)
            (aws-logs--transient-reprompt)
            (aws-logs--custom-time-range-to-string))
  :argument "--time-range=")

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
              input))
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
              input))
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
              input))
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

(transient-define-suffix aws-logs-query-done ()
  "Return from query transient to the main transient."
  :transient nil
  (interactive)
  (transient-quit-one)
  (transient-setup 'aws-logs-transient))

(transient-define-prefix aws-logs-query-transient ()
  "Query options for Logs Insights."
  :remember-value 'exit
  [[4 :description (lambda () (format "Active query:\n%s" (aws-logs--query-full)))
      ("e" "Edit active query" aws-logs-query-edit)
      ("s" "Save active query" aws-logs-query-save)
      ("l" "Load saved query" aws-logs-query-load-saved)]]
  [["Done"
    ("RET" "Back to main" aws-logs-query-done)]]
  (interactive)
  (transient-setup 'aws-logs-query-transient))

(transient-define-suffix aws-logs-open-query-transient ()
  "Open query transient."
  :transient nil
  (interactive)
  (transient-setup 'aws-logs-query-transient))


(defun aws-logs--getopt (name args)
  "Get value for option prefix NAME (e.g. \"--since=\") from ARGS."
  (when-let ((s (seq-find (lambda (a) (string-prefix-p name a)) args)))
    (substring s (length name))))

(defun aws-logs--sync-session-from-transient ()
  "Sync backing session vars from active `aws-logs-transient` infix args."
  (when (and (boundp 'transient-current-command)
             (eq transient-current-command 'aws-logs-transient))
    (let* ((args (transient-args 'aws-logs-transient))
           (since (transient-arg-value "--since=" args))
           (time-range (transient-arg-value "--time-range=" args))
           (filter (transient-arg-value "--filter=" args))
           (profile (transient-arg-value "--profile=" args))
           (log-group (transient-arg-value "--log-group=" args)))
      (setq aws-logs-time-range
            (unless (or (null since) (string-empty-p since)) since))
      (when aws-logs-time-range
        (setq aws-logs-custom-time-range nil))
      (when (or (null time-range) (string-empty-p time-range))
        (setq aws-logs-custom-time-range nil))
      (setq aws-logs-filter
            (unless (or (null filter) (string-empty-p filter)) filter))
      (setq aws-logs-profile
            (unless (or (null profile) (string-empty-p profile)) profile))
      (setq aws-logs-log-group
            (unless (or (null log-group) (string-empty-p log-group)) log-group)))))

(transient-define-suffix aws-logs-tail ()
  "Start streaming logs in a dedicated buffer using current transient selections."
  :transient nil
  (interactive)
  (aws-logs--sync-session-from-transient)
  (aws-logs-tail-run))

(transient-define-suffix aws-logs-insights ()
  "Run Logs Insights query and show results in a dedicated buffer."
  :transient nil
  (interactive)
  (aws-logs--sync-session-from-transient)
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
    ("-F" "Filter" aws-logs-infix-filter)
    ("-t" "Time Range" aws-logs-infix-custom-time-range)
    ("-f" aws-logs-toggle-follow)
    ("-e" aws-logs-toggle-ecs)
    ("-p" "Profile" aws-logs-infix-profile)]

   [4 :description (lambda () (format "Query: %s" (aws-logs--query)))
      ("q" "Query…" aws-logs-open-query-transient)
      ("x" "Clear query" aws-logs-query-clear)
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
