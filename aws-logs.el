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
;; Package-Requires: ((emacs "24.4"))
;;
;; This file is not part of GNU Emacs.
;;
;;; Commentary:
;;
;;  Description
;;
;;; Code:

(require 'subr-x)

(defvar aws-logs-cli "aws")
(defvar aws-logs-endpoint nil)
(defvar aws-logs-region "eu-west-1")

(defun aws-logs--command (&rest args)
  "Build cli command with endpoint, region and ARGS."
  (let ((endpoint (if aws-logs-endpoint (format "--endpoint-url=%s" aws-logs-endpoint) ""))
        (region (format "--region=%s" aws-logs-region)))
    (string-join (append (list aws-logs-cli endpoint region) args) " ")
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
         (json (json-parse-string output :object-type 'plist))
         (log-groups (alist-get 'logGroups (json-parse-string output :object-type 'alist) ))
         )
    (mapcar (lambda (log-group) (alist-get 'logGroupName log-group)) log-groups)
    )
  )

(defun aws-logs ()
  "Select a stream to tail."
  (interactive)
  (let* ((log-group (completing-read "Log group: " (aws-logs--list-log-groups)))
         (process (start-process-shell-command "aws-cli"
                                               (format "*AWS logs - %s*" log-group)
                                               (aws-logs--command "logs tail" log-group "--follow"))))
    (with-current-buffer (process-buffer process)
      (display-buffer (current-buffer))
      (require 'shell)
      (shell-mode)
      (set-process-filter process 'comint-output-filter))
    ))

(provide 'aws-logs)
;;; aws-logs.el ends here
