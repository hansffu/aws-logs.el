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
;; Package-Requires: ((emacs "27.1"))
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

(defvar aws-logs-mode-map
  (let ((map (make-keymap)))
    (define-key map "q" #'aws-logs-quit-process-and-window)
    map)
  "Keymap for aws-logs-mode.")


(defun aws-logs-quit-process-and-window ()
  "Quit current process and window."
  (interactive)
  (cl-letf (((symbol-function 'process-kill-buffer-query-function) (lambda () (always nil))))
    (kill-buffer-and-window)
    )
  )

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
  :init-value (lambda (obj) (transient-infix-set obj aws-logs-default-group))
  :reader (lambda (prompt initial hist)
            (let ((choices (aws-logs--list-log-groups)))
              (if choices
                  (completing-read (concat prompt " ") choices nil t initial hist)
                (read-string (concat prompt " (no groups found) ") ""))))
  :argument "--log-group=")

;; Infix for entering a Logs Insights query
(transient-define-infix aws-logs-infix-query ()
  :description "Query (Logs Insights)"
  :class 'transient-option
  :reader (lambda (prompt _initial _hist)
            (read-string (concat prompt " ") ""))
  :argument "--query=")

;; Infix for specifying --since / time-range
(transient-define-infix aws-logs-infix-since ()
  :description "Since / Time range"
  :class 'transient-option
  :reader #'read-string
  :argument "--since=")


(defun aws-logs--getopt (name args)
  "Get value for option prefix NAME (e.g. \"--since=\") from ARGS."
  (when-let ((s (seq-find (lambda (a) (string-prefix-p name a)) args)))
    (substring s (length name))))

(transient-define-suffix aws-logs-print-logs (&optional args)
  (interactive (list (transient-args transient-current-command)))
  (message "yo %s" args)
  (let ((log-group (aws-logs--getopt "--log-group=" args))
        (since     (aws-logs--getopt "--since=" args))
        (query     (aws-logs--getopt "--query=" args))
        (follow    (member "--follow" args)))
    (message "aws-logs selection:\n  args=%S\n  log-group=%S\n  since=%S\n  query=%S\n  follow=%S"
             args log-group since query follow)
    (message "%s" (aws-logs--command "logs tail" log-group
                                     (when follow "--follow")
                                     (when since (format "--since=%s" since))
                                     ))
    ;; (aws-logs--command "logs tail" log-group
    ;;                    "--follow"
    ;;                    "--format" aws-logs-format
    ;;                    "--since" aws-logs-since
    ;;                    )
    )
  )

(defvar aws-logs--transient-history nil)
;; Main transient prefix
(transient-define-prefix aws-logs-transient ()
  "AWS Logs transient menu.

This transient currently only collects options; the Run action is a placeholder
and is not implemented yet."
  :remember-value 'exit
  :value (lambda ()
           (list (format "--since=%s" aws-logs-since)
                 (format "--log-group=" aws-logs-default-group)
                 ))
  [["Target"
    ("g" aws-logs-infix-log-group)
    ("q" "Query" aws-logs-infix-query)]
   ["Time / Tail options"
    ("s" "Since" aws-logs-infix-since)
    ("f" "Follow (tail)" "--follow")]
   ["Actions"
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
