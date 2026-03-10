;;; async-job-queue.el --- Ordered async job queue via subordinate Emacs -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; A tiny FIFO async queue that runs jobs in a dedicated subordinate Emacs.
;; Jobs are processed in push order and callbacks run in the same order.
;;
;; Public API:
;; - `async-job-queue-create'
;; - `async-job-queue-push'
;; - `async-job-queue-stop'
;;
;;; Code:

(require 'cl-lib)
(require 'subr-x)

(defvar async-job-queue--next-id 0
  "Monotonic identifier used for queue instances.")

(cl-defstruct (async-job-queue
               (:constructor async-job-queue--make))
  "Queue state object."
  process
  callback-func
  process-func
  partial-output
  next-job-id
  pending-head
  pending-tail
  ready-results
  stopping-p
  stopped-p)

(defun async-job-queue--emacs-program ()
  "Return Emacs executable path used to spawn worker process."
  (or (and (stringp invocation-name)
           (if (and (stringp invocation-directory)
                    (not (string-empty-p invocation-directory)))
               (expand-file-name invocation-name invocation-directory)
             (executable-find invocation-name)))
      (executable-find "emacs")
      "emacs"))

(defun async-job-queue--serialize-function (func label)
  "Serialize FUNC as readable Lisp source, or raise user-error.
LABEL is used in error messages."
  (unless (functionp func)
    (user-error "%s must be a function, got: %S" label func))
  (let* ((resolved (if (symbolp func)
                       (or (symbol-function func) func)
                     func))
         (text (prin1-to-string resolved)))
    (condition-case err
        (let ((parsed (car (read-from-string text))))
          (unless (functionp parsed)
            (user-error "%s is not serializable as a function: %S" label func))
          text)
      (error
       (user-error "%s is not readable in worker Emacs: %s"
                   label
                   (error-message-string err))))))

(defun async-job-queue--serialize-optional-function (func label)
  "Serialize optional FUNC as readable Lisp source, or nil.
LABEL is used in error messages."
  (when func
    (async-job-queue--serialize-function func label)))

(defun async-job-queue--worker-eval-form (encoded-process-func
                                          encoded-init-func
                                          encoded-teardown-func)
  "Return worker bootstrap `--eval` form string.
ENCODED-PROCESS-FUNC is base64-encoded PROCESS-FUNC text.
ENCODED-INIT-FUNC and ENCODED-TEARDOWN-FUNC are optional base64-encoded
function texts."
  (prin1-to-string
   `(let* ((process-func-text (base64-decode-string ,encoded-process-func))
           (init-func-text ,encoded-init-func)
           (teardown-func-text ,encoded-teardown-func)
           (process-func (car (read-from-string process-func-text)))
           (init-func (when init-func-text
                        (car (read-from-string
                              (base64-decode-string init-func-text)))))
           (teardown-func (when teardown-func-text
                            (car (read-from-string
                                  (base64-decode-string teardown-func-text))))))
      (unless (functionp process-func)
        (error "PROCESS-FUNC is not callable in worker: %S" process-func))
      (when (and init-func (not (functionp init-func)))
        (error "INIT-FUNC is not callable in worker: %S" init-func))
      (when (and teardown-func (not (functionp teardown-func)))
        (error "TEARDOWN-FUNC is not callable in worker: %S" teardown-func))
      (let ((debug-on-error nil)
            (send (lambda (message)
                    (let ((print-escape-newlines t)
                          (print-escape-control-characters t))
                      (princ (prin1-to-string message) 'external-debugging-output))
                    (princ "\n" 'external-debugging-output)))
            (run-teardown-p nil))
        (condition-case init-err
            (when init-func
              (funcall init-func))
          (error
           (funcall send (list :lifecycle-error
                               (format "INIT-FUNC failed: %s"
                                       (error-message-string init-err))))
           (kill-emacs 0)))
        (catch 'done
          (while t
            (condition-case err
                (pcase (read)
                  (`(:process ,id ,element)
                   (condition-case process-err
                       (funcall send (list :ok id (funcall process-func element)))
                     (error
                      (funcall send (list :error id (error-message-string process-err)))))
                   nil)
                  (`(:stop)
                   (setq run-teardown-p t)
                   (throw 'done t))
                  (other
                   (funcall send (list :lifecycle-error
                                       (format "Unknown worker command: %S" other)))))
              (end-of-file
               (throw 'done t))
              (error
               (funcall send (list :lifecycle-error (error-message-string err)))))))
        (when (and run-teardown-p teardown-func)
          (condition-case teardown-err
              (funcall teardown-func)
            (error
             (funcall send (list :lifecycle-error
                                 (format "TEARDOWN-FUNC failed: %s"
                                         (error-message-string teardown-err)))))))
        (kill-emacs 0)))))

;;;###autoload
(cl-defun async-job-queue-create (process-func callback-func
                                               &key init-func teardown-func)
  "Create an async job queue with PROCESS-FUNC and CALLBACK-FUNC.

PROCESS-FUNC runs in a subordinate Emacs for each pushed element.
CALLBACK-FUNC runs in the current Emacs with the processed result.
INIT-FUNC runs once in worker Emacs before processing jobs when non-nil.
TEARDOWN-FUNC runs once in worker Emacs during stop when non-nil.

Returns an `async-job-queue' object."
  (unless (functionp callback-func)
    (user-error "CALLBACK-FUNC must be a function, got: %S" callback-func))
  (let* ((serialized-process-func (async-job-queue--serialize-function
                                   process-func
                                   "PROCESS-FUNC"))
         (serialized-init-func (async-job-queue--serialize-optional-function
                                init-func
                                "INIT-FUNC"))
         (serialized-teardown-func (async-job-queue--serialize-optional-function
                                    teardown-func
                                    "TEARDOWN-FUNC"))
         (encoded-process-func (base64-encode-string serialized-process-func t))
         (encoded-init-func (when serialized-init-func
                              (base64-encode-string serialized-init-func t)))
         (encoded-teardown-func (when serialized-teardown-func
                                  (base64-encode-string serialized-teardown-func t)))
         (queue-id (cl-incf async-job-queue--next-id))
         (buffer (generate-new-buffer
                  (format " *async-job-queue-%d*" queue-id)))
         (eval-form (async-job-queue--worker-eval-form encoded-process-func
                                                       encoded-init-func
                                                       encoded-teardown-func))
         (command (list (async-job-queue--emacs-program)
                        "-Q" "--batch"
                        "--eval" eval-form))
         (queue nil)
         (process (make-process
                   :name (format "async-job-queue-%d" queue-id)
                   :buffer buffer
                   :command command
                   :coding 'utf-8-unix
                   :noquery t
                   :connection-type 'pipe)))
    (set-process-query-on-exit-flag process nil)
    (setq queue (async-job-queue--make
                 :process process
                 :callback-func callback-func
                 :process-func process-func
                 :partial-output ""
                 :next-job-id 0
                 :pending-head nil
                 :pending-tail nil
                 :ready-results (make-hash-table :test 'eql)
                 :stopping-p nil
                 :stopped-p nil))
    (set-process-filter
     process
     (lambda (proc output)
       (async-job-queue--process-filter queue proc output)))
    (set-process-sentinel
     process
     (lambda (proc event)
       (async-job-queue--process-sentinel queue proc event)))
    queue))

(defun async-job-queue--serialize-command (command)
  "Serialize COMMAND to one protocol line."
  (concat (prin1-to-string command) "\n"))

(defun async-job-queue--pending-empty-p (queue)
  "Return non-nil when QUEUE has no pending callback ids."
  (null (async-job-queue-pending-head queue)))

(defun async-job-queue--pending-peek (queue)
  "Return next pending callback id in QUEUE, or nil."
  (car (async-job-queue-pending-head queue)))

(defun async-job-queue--pending-push (queue id)
  "Append callback ID to pending order queue."
  (let ((node (list id)))
    (if (async-job-queue-pending-tail queue)
        (setcdr (async-job-queue-pending-tail queue) node)
      (setf (async-job-queue-pending-head queue) node))
    (setf (async-job-queue-pending-tail queue) node)))

(defun async-job-queue--pending-pop (queue)
  "Pop and return next pending callback id from QUEUE, or nil."
  (when-let ((head (async-job-queue-pending-head queue)))
    (let ((id (car head)))
      (setf (async-job-queue-pending-head queue) (cdr head))
      (when (null (async-job-queue-pending-head queue))
        (setf (async-job-queue-pending-tail queue) nil))
      id)))

(defun async-job-queue--dispatch-ready-results (queue)
  "Dispatch available callback results from QUEUE in push order."
  (let ((ready (async-job-queue-ready-results queue))
        (callback (async-job-queue-callback-func queue)))
    (while (and (not (async-job-queue--pending-empty-p queue))
                (gethash (async-job-queue--pending-peek queue) ready))
      (let* ((id (async-job-queue--pending-pop queue))
             (packet (gethash id ready)))
        (remhash id ready)
        (condition-case err
            (pcase packet
              (`(ok . ,value)
               (funcall callback value))
              (`(error . ,message-text)
               ;; Worker errors are surfaced as plist payloads.
               (funcall callback (list :error message-text)))
              (_
               (funcall callback (list :error (format "Malformed worker packet: %S"
                                                      packet)))))
          (error
           (message "async-job-queue callback error: %s"
                    (error-message-string err))))))))

(defun async-job-queue--handle-worker-line (queue line)
  "Handle one decoded protocol LINE from worker for QUEUE."
  (unless (string-empty-p line)
    (condition-case err
        (let ((msg (car (read-from-string line))))
          (pcase msg
            (`(:ok ,id ,value)
             (puthash id (cons 'ok value) (async-job-queue-ready-results queue)))
            (`(:error ,id ,message-text)
             (when id
               (puthash id (cons 'error message-text) (async-job-queue-ready-results queue))))
            (`(:lifecycle-error ,message-text)
             (condition-case callback-err
                 (funcall (async-job-queue-callback-func queue)
                          (list :error message-text))
               (error
                (message "async-job-queue callback error: %s"
                         (error-message-string callback-err)))))
            (_
             (message "async-job-queue: unknown worker message: %S" msg))))
      (error
       (message "async-job-queue: failed to parse worker message: %s"
                (error-message-string err)))))
  (async-job-queue--dispatch-ready-results queue))

(defun async-job-queue--process-filter (queue _process output)
  "Process worker OUTPUT for QUEUE."
  (unless (async-job-queue-stopped-p queue)
    ;; `read` in batch workers emits "Lisp expression: " prompts on stdout.
    ;; Strip them from protocol traffic before line framing.
    (let* ((clean-output (replace-regexp-in-string "Lisp expression: ?" "" output))
           (combined (concat (or (async-job-queue-partial-output queue) "")
                             clean-output))
           (parts (split-string combined "\n"))
           (rest (car (last parts)))
           (lines (butlast parts)))
      (setf (async-job-queue-partial-output queue) (or rest ""))
      (dolist (line lines)
        (async-job-queue--handle-worker-line queue line)))))

(defun async-job-queue--cleanup-buffer (queue)
  "Kill worker process buffer for QUEUE when still live."
  (when-let* ((proc (async-job-queue-process queue))
              (buf (process-buffer proc)))
    (when (buffer-live-p buf)
      (kill-buffer buf))))

(defun async-job-queue--process-sentinel (queue _process event)
  "Handle worker process EVENT for QUEUE."
  (unless (async-job-queue-stopped-p queue)
    (setf (async-job-queue-stopped-p queue) t)
    (setf (async-job-queue-stopping-p queue) nil)
    (message "async-job-queue worker exited: %s" (string-trim event)))
  (async-job-queue--cleanup-buffer queue))

(defun async-job-queue--queue-live-p (queue)
  "Return non-nil when QUEUE worker process is alive."
  (and (async-job-queue-p queue)
       (not (async-job-queue-stopped-p queue))
       (not (async-job-queue-stopping-p queue))
       (process-live-p (async-job-queue-process queue))))

;;;###autoload
(defun async-job-queue-push (queue element)
  "Push ELEMENT onto QUEUE for async processing."
  (unless (async-job-queue-p queue)
    (user-error "QUEUE must be an async-job-queue object, got: %S" queue))
  (unless (async-job-queue--queue-live-p queue)
    (user-error "QUEUE is not running"))
  (let ((job-id (prog1 (async-job-queue-next-job-id queue)
                  (setf (async-job-queue-next-job-id queue)
                        (1+ (async-job-queue-next-job-id queue))))))
    (async-job-queue--pending-push queue job-id)
    (process-send-string
     (async-job-queue-process queue)
     (async-job-queue--serialize-command
      (list :process job-id element)))
    queue))

;;;###autoload
(defun async-job-queue-stop (queue)
  "Stop QUEUE and terminate its subordinate Emacs process."
  (unless (async-job-queue-p queue)
    (user-error "QUEUE must be an async-job-queue object, got: %S" queue))
  (unless (or (async-job-queue-stopped-p queue)
              (async-job-queue-stopping-p queue))
    (setf (async-job-queue-stopping-p queue) t)
    (setf (async-job-queue-partial-output queue) "")
    (setf (async-job-queue-pending-head queue) nil)
    (setf (async-job-queue-pending-tail queue) nil)
    (clrhash (async-job-queue-ready-results queue))
    (when (process-live-p (async-job-queue-process queue))
      (ignore-errors
        (process-send-string
         (async-job-queue-process queue)
         (async-job-queue--serialize-command '(:stop))))))
  queue)

(provide 'async-job-queue)
;;; async-job-queue.el ends here
