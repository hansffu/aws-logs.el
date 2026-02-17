;;; aws-logs-tail.el --- Tail execution and rendering -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Runs CloudWatch log tail commands and renders either:
;; - comint output (default)
;; - ECS JSON output in json-log-viewer
;;
;;; Code:

(require 'cl-lib)
(require 'comint)
(require 'json)
(require 'json-log-viewer)
(require 'subr-x)

(defvar aws-logs-cli)
(defvar aws-logs-endpoint)
(defvar aws-logs-region)
(defvar aws-logs-profile)
(defvar aws-logs-format)
(defvar aws-logs-log-group)
(defvar aws-logs-follow)
(defvar aws-logs-ecs)
(defvar aws-logs-time-range)
(defvar aws-logs-custom-time-range)
(defvar aws-logs-mode-hook)

(declare-function aws-logs--list-log-groups "aws-logs" ())

(defvar-local aws-logs--tail-process nil
  "Tail process associated with current log buffer.")

(defvar-local aws-logs--tail-pending-fragment ""
  "Incomplete trailing process output fragment for ECS streaming buffers.")

(defvar-local aws-logs--tail-pending-json-lines nil
  "Accumulated multiline JSON payload lines for ECS streaming buffers.")

(defvar aws-logs-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map comint-mode-map)
    (define-key map (kbd "q") #'aws-logs-tail-quit-process-and-window)
    map)
  "Keymap for `aws-logs-mode'.")

(define-derived-mode aws-logs-mode comint-mode "AWS-LOGS"
  "Major mode for displaying tail output from AWS CLI."
  :interactive nil
  :group 'aws-logs
  (read-only-mode 1))

(defun aws-logs-tail-quit-process-and-window ()
  "Stop the current tail process (if any) and close the window."
  (interactive)
  (let ((proc (or aws-logs--tail-process
                  (get-buffer-process (current-buffer)))))
    (when (process-live-p proc)
      (delete-process proc))
    (setq aws-logs--tail-process nil)
    (quit-window t)))

(defun aws-logs--tail-global-args ()
  "Build common AWS CLI arguments from current backing fields."
  (delq nil
        (list
         (when aws-logs-endpoint (format "--endpoint-url=%s" aws-logs-endpoint))
         (format "--region=%s" aws-logs-region)
         (when aws-logs-profile (format "--profile=%s" aws-logs-profile)))))

(defun aws-logs--tail-args ()
  "Build argument list for `aws logs tail` using current backing fields."
  (append
   (list "logs" "tail" aws-logs-log-group
         "--format" aws-logs-format)
   (when aws-logs-follow
     (list "--follow"))
   (cond
    ((and aws-logs-custom-time-range (car aws-logs-custom-time-range))
     ;; `aws logs tail` does not support end-time; use custom FROM as --since.
     (list "--since" (car aws-logs-custom-time-range)))
    ((and aws-logs-time-range (not (string-empty-p aws-logs-time-range)))
     (list "--since" aws-logs-time-range)))))

(defun aws-logs--tail-since-display ()
  "Return active --since value for header display."
  (cond
   ((and aws-logs-custom-time-range (car aws-logs-custom-time-range))
    (car aws-logs-custom-time-range))
   ((and aws-logs-time-range (not (string-empty-p aws-logs-time-range)))
    aws-logs-time-range)
   (t "-")))

(defun aws-logs--tail-header-lines (_state)
  "Return extra viewer header lines for ECS tail buffers."
  (list (cons "Log group" (or aws-logs-log-group "-"))
        (cons "Profile" (or aws-logs-profile "-"))
        (cons "Follow" (if aws-logs-follow "yes" "no"))
        (cons "Since" (aws-logs--tail-since-display))))

(defun aws-logs--tail-viewer-buffer-name ()
  "Return ECS viewer buffer name for current log group."
  (format "*AWS logs (ecs) - %s*" aws-logs-log-group))

(defun aws-logs--tail-process-name ()
  "Return process name for current log group."
  (format "aws-logs-tail:%s" aws-logs-log-group))

(defun aws-logs--tail-install-viewer-keymap ()
  "Install buffer-local keymap tweaks for ECS viewer buffers."
  (let ((map (copy-keymap (current-local-map))))
    (define-key map (kbd "q") #'aws-logs-tail-quit-process-and-window)
    (use-local-map map)))

(defun aws-logs--tail-json-line-p (line)
  "Return non-nil when LINE parses as JSON object/array."
  (let ((s (string-trim-left (or line ""))))
    (and (not (string-empty-p s))
         (memq (aref s 0) '(?{ ?\[))
         (condition-case nil
             (progn
               (ignore (json-parse-string s :object-type 'alist :array-type 'list
                                          :null-object nil :false-object :false))
               t)
           (error nil)))))

(defun aws-logs--tail-parse-json-maybe (value)
  "Parse VALUE as JSON and return parsed object or nil."
  (when (and (stringp value)
             (not (string-empty-p (string-trim value))))
    (condition-case nil
        (json-parse-string value :object-type 'alist :array-type 'list
                           :null-object nil :false-object :false)
      (error nil))))

(defun aws-logs--tail-parse-json-from-mixed (value)
  "Parse VALUE as JSON, even when prefixed text exists before JSON."
  (or (aws-logs--tail-parse-json-maybe value)
      (when (stringp value)
        (when-let ((json-pos (aws-logs--tail-find-json-start value)))
          (aws-logs--tail-parse-json-maybe (substring value json-pos))))))

(defun aws-logs--tail-message-field (parsed)
  "Return message-like field from PARSED JSON object."
  (or (alist-get 'message parsed)
      (alist-get '@message parsed)
      (alist-get 'log parsed)))

(defun aws-logs--tail-strip-aws-prefix (line)
  "Strip AWS tail prefix (timestamp + log-group) from LINE when present."
  (if (and (stringp line)
           (string-match
            "\\`[^[:space:]]+\\s-+[^[:space:]]+\\s-+\\(\\(?:{\\|\\[\\).+\\)\\'"
            line))
      (match-string 1 line)
    line))

(defun aws-logs--tail-maybe-unwrap-ecs-message (line)
  "Return unwrapped ECS payload from LINE when possible."
  (let* ((outer (aws-logs--tail-parse-json-from-mixed line))
         (message-field (and (listp outer)
                             (aws-logs--tail-message-field outer)))
         (message-json
          (cond
           ((stringp message-field)
            (aws-logs--tail-parse-json-from-mixed message-field))
           ((listp message-field) message-field)
           (t nil)))
         (log-field (and (listp outer) (alist-get 'log outer)))
         (log-json (and (stringp log-field)
                        (aws-logs--tail-parse-json-from-mixed log-field)))
         (payload (or message-json log-json outer)))
    (if payload
        (json-serialize payload)
      line)))

(defun aws-logs--tail-find-json-start (line)
  "Return index of first JSON opener in LINE, or nil."
  (cl-position-if (lambda (ch) (or (eq ch ?{) (eq ch ?\[))) line))

(defun aws-logs--tail-flush-pending-json-lines ()
  "Flush pending multiline ECS JSON lines as one raw line."
  (when aws-logs--tail-pending-json-lines
    (prog1
        (string-join aws-logs--tail-pending-json-lines "\n")
      (setq aws-logs--tail-pending-json-lines nil))))

(defun aws-logs--tail-ecs-normalize-lines-with-pending (lines pending)
  "Normalize ECS LINES using PENDING multiline JSON state.

Returns (NORMALIZED-LINES . NEW-PENDING)."
  (let ((normalized nil)
        (pending-lines (append pending nil))
        (max-pending-lines 200))
    (dolist (raw-line lines)
      (let* ((clean (string-trim-right (or raw-line "") "\r"))
             (trimmed (aws-logs--tail-strip-aws-prefix clean)))
        (unless (string-empty-p clean)
          (cond
           (pending-lines
            (setq pending-lines (append pending-lines (list trimmed)))
            (let ((candidate (string-join pending-lines "\n")))
              (cond
               ((aws-logs--tail-json-line-p candidate)
                (push (aws-logs--tail-maybe-unwrap-ecs-message candidate) normalized)
                (setq pending-lines nil))
               ((>= (length pending-lines) max-pending-lines)
                (push candidate normalized)
                (setq pending-lines nil)))))
           ((aws-logs--tail-json-line-p trimmed)
            (push (aws-logs--tail-maybe-unwrap-ecs-message trimmed) normalized))
           (t
            (let ((json-pos (aws-logs--tail-find-json-start trimmed)))
              (if (null json-pos)
                  (push trimmed normalized)
                (let ((candidate (substring trimmed json-pos)))
                  (if (aws-logs--tail-json-line-p candidate)
                      (push (aws-logs--tail-maybe-unwrap-ecs-message candidate) normalized)
                    (setq pending-lines (list candidate)))))))))))
    (cons (nreverse normalized) pending-lines)))

(defun aws-logs--tail-ecs-normalize-lines (lines)
  "Normalize ECS LINES using current buffer pending multiline state."
  (let* ((result (aws-logs--tail-ecs-normalize-lines-with-pending
                  lines aws-logs--tail-pending-json-lines)))
    (setq aws-logs--tail-pending-json-lines (cdr result))
    (car result)))

(defun aws-logs--tail-collect-output-lines (args)
  "Run AWS CLI ARGS synchronously and return output lines."
  (with-temp-buffer
    (let ((exit-code (apply #'call-process aws-logs-cli nil t nil args))
          (output (buffer-string)))
      (unless (zerop exit-code)
        (user-error "AWS CLI failed (%s): %s" exit-code (string-trim output)))
      (split-string output "\n" t))))

(defun aws-logs--tail-kill-buffer-process (buffer)
  "Stop live process associated with BUFFER, if any."
  (when (buffer-live-p buffer)
    (with-current-buffer buffer
      (let ((proc (or aws-logs--tail-process
                      (get-buffer-process buffer))))
        (when (process-live-p proc)
          (delete-process proc))
        (setq aws-logs--tail-process nil)
        (setq aws-logs--tail-pending-fragment "")
        (setq aws-logs--tail-pending-json-lines nil)))))

(defun aws-logs--tail-make-ecs-buffer (initial-lines streaming)
  "Create ECS viewer buffer from INITIAL-LINES.

When STREAMING is non-nil, buffer is configured for incremental pushes."
  (let* ((buffer-name (aws-logs--tail-viewer-buffer-name))
         (existing (get-buffer buffer-name))
         (buffer nil))
    (when existing
      (aws-logs--tail-kill-buffer-process existing))
    (setq buffer
          (json-log-viewer-make-buffer
           buffer-name
           :log-lines (or initial-lines nil)
           :timestamp-path "@timestamp"
           :level-path "log.level"
           :message-path "message"
           :streaming streaming
           :direction 'oldest-first
           :header-lines-function #'aws-logs--tail-header-lines))
    (with-current-buffer buffer
      (setq-local aws-logs--tail-process nil)
      (setq-local aws-logs--tail-pending-fragment "")
      (setq-local aws-logs--tail-pending-json-lines nil)
      (aws-logs--tail-install-viewer-keymap))
    buffer))

(defun aws-logs--tail-consume-chunk-lines (chunk)
  "Consume process CHUNK in current ECS buffer and return complete lines."
  (let* ((combined (concat aws-logs--tail-pending-fragment chunk))
         (has-newline (string-suffix-p "\n" combined))
         (parts (split-string combined "\n"))
         (complete-lines (if has-newline parts (butlast parts)))
         (rest (if has-newline "" (car (last parts)))))
    (setq aws-logs--tail-pending-fragment (or rest ""))
    complete-lines))

(defun aws-logs--tail-ecs-process-filter (process output)
  "Push ECS PROCESS OUTPUT into json-log-viewer incrementally."
  (let ((buffer (process-buffer process)))
    (when (buffer-live-p buffer)
      (with-current-buffer buffer
        (let* ((raw-lines (aws-logs--tail-consume-chunk-lines output))
               (lines (aws-logs--tail-ecs-normalize-lines raw-lines)))
          (when lines
            (json-log-viewer-push buffer lines)))))))

(defun aws-logs--tail-ecs-flush-pending-line ()
  "Flush buffered trailing ECS line in current viewer buffer."
  (when (and aws-logs--tail-pending-fragment
             (not (string-empty-p aws-logs--tail-pending-fragment)))
    (let ((lines (aws-logs--tail-ecs-normalize-lines
                  (list aws-logs--tail-pending-fragment))))
      (setq aws-logs--tail-pending-fragment "")
      (when lines
        (json-log-viewer-push (current-buffer) lines))))
  (when-let ((pending-json (aws-logs--tail-flush-pending-json-lines)))
    (json-log-viewer-push (current-buffer) (list pending-json))))

(defun aws-logs--tail-ecs-process-sentinel (process event)
  "Handle ECS PROCESS lifecycle EVENT updates."
  (let ((buffer (process-buffer process)))
    (when (buffer-live-p buffer)
      (with-current-buffer buffer
        (aws-logs--tail-ecs-flush-pending-line)
        (setq aws-logs--tail-process nil)))
    (when (and (memq (process-status process) '(exit signal))
               (not (zerop (process-exit-status process))))
      (message "AWS logs tail exited (%s): %s"
               (process-exit-status process)
               (string-trim event)))))

(defun aws-logs--tail-run-comint ()
  "Run tail in a comint buffer."
  (let* ((buffer (get-buffer-create (format "*AWS logs - %s*" aws-logs-log-group)))
         (args (append (aws-logs--tail-global-args) (aws-logs--tail-args))))
    (aws-logs--tail-kill-buffer-process buffer)
    (with-current-buffer buffer
      (let ((inhibit-read-only t))
        (erase-buffer))
      (aws-logs-mode))
    (let ((process (apply #'start-process (aws-logs--tail-process-name)
                          buffer aws-logs-cli args)))
      (with-current-buffer buffer
        (setq-local aws-logs--tail-process process)
        (display-buffer buffer)
        (set-process-filter process #'comint-output-filter))
      (set-process-query-on-exit-flag process nil)
      (message "Started logs tail for %s" aws-logs-log-group))))

(defun aws-logs--tail-run-ecs-once ()
  "Run ECS tail once and render in json-log-viewer."
  (let* ((args (append (aws-logs--tail-global-args) (aws-logs--tail-args)))
         (raw-lines (aws-logs--tail-collect-output-lines args))
         (normalized+pending (aws-logs--tail-ecs-normalize-lines-with-pending raw-lines nil))
         (lines (car normalized+pending)))
    (when-let ((pending (cdr normalized+pending))
               (joined (and pending (string-join pending "\n"))))
      (setq lines (append lines (list joined))))
    (let ((buffer (aws-logs--tail-make-ecs-buffer lines nil)))
      (display-buffer buffer)
      (message "Fetched logs tail for %s" aws-logs-log-group))))

(defun aws-logs--tail-run-ecs-stream ()
  "Run ECS tail in follow mode and stream lines into json-log-viewer."
  (let* ((buffer (aws-logs--tail-make-ecs-buffer nil t))
         (args (append (aws-logs--tail-global-args) (aws-logs--tail-args)))
         (process (apply #'start-process (aws-logs--tail-process-name)
                         buffer aws-logs-cli args)))
    (with-current-buffer buffer
      (setq-local aws-logs--tail-process process)
      (setq-local aws-logs--tail-pending-fragment "")
      (setq-local aws-logs--tail-pending-json-lines nil)
      (display-buffer buffer))
    (set-process-filter process #'aws-logs--tail-ecs-process-filter)
    (set-process-sentinel process #'aws-logs--tail-ecs-process-sentinel)
    (set-process-query-on-exit-flag process nil)
    (message "Started ECS logs tail for %s" aws-logs-log-group)))

(defun aws-logs-tail-run ()
  "Run logs tail based on current session selections.

When `aws-logs-ecs` is non-nil, tail output is rendered in `json-log-viewer`.
When `aws-logs-follow` is non-nil in ECS mode, logs stream into the viewer."
  (interactive)
  (unless (and aws-logs-log-group (not (string-empty-p aws-logs-log-group)))
    (user-error "Select a log group first"))
  (if aws-logs-ecs
      (if aws-logs-follow
          (aws-logs--tail-run-ecs-stream)
        (aws-logs--tail-run-ecs-once))
    (aws-logs--tail-run-comint)))

(defun aws-logs-simple ()
  "Select a log group and start following logs in comint mode."
  (interactive)
  (let ((aws-logs-log-group
         (completing-read "Log group: " (aws-logs--list-log-groups)))
        (aws-logs-follow t)
        (aws-logs-ecs nil))
    (aws-logs-tail-run)))

(provide 'aws-logs-tail)
;;; aws-logs-tail.el ends here
