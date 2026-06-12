;;; composite-log-viewer-test.el --- composite log viewer tests -*- lexical-binding: t; -*-

(require 'ert)
(require 'cl-lib)

(require 'composite-log-viewer)

(ert-deftest composite-log-viewer-create-dispatches-sources-test ()
  (let (calls required made-name created-buffer)
    (cl-letf (((symbol-function 'require)
               (lambda (feature &optional _filename _noerror)
                 (push feature required)
                 t))
              ((symbol-function 'composite-log-viewer--make-buffer)
               (lambda (name)
                 (setq made-name name)
                 (setq created-buffer
                       (generate-new-buffer "*composite-create-test*"))
                 (with-current-buffer created-buffer
                   (composite-log-viewer-mode))
                 created-buffer))
              ((symbol-function 'kafka-logs-stream-to-buffer)
               (lambda (buffer source)
                 (push (list :kafka buffer source) calls)))
              ((symbol-function 'kube-logs-stream-to-buffer)
               (lambda (buffer source)
                 (push (list :kube buffer source) calls)))
              ((symbol-function 'display-buffer)
               (lambda (buffer &rest _args) buffer)))
      (unwind-protect
          (let ((buffer
                 (composite-log-viewer-create
                  '(:name "*prod logs*"
                    :sources ((:type kafka :connection "prod" :topic "orders")
                              (:type kube :namespace "payments" :target "api"))))))
            (should (eq buffer created-buffer))
            (should (equal made-name "*prod logs*"))
            (should (equal (mapcar #'car (nreverse calls))
                           '(:kafka :kube)))
            (should (memq 'kafka-logs required))
            (should (memq 'kube-logs required)))
        (when (buffer-live-p created-buffer)
          (kill-buffer created-buffer))))))

(provide 'composite-log-viewer-test)
;;; composite-log-viewer-test.el ends here
