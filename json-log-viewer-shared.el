;;; json-log-viewer-shared.el --- Shared JSON/path helpers -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Shared utility helpers used by json-log-viewer and its async worker.
;;
;;; Code:

(require 'cl-lib)
(require 'json)
(require 'subr-x)

(defun json-log-viewer-shared--value->string (value)
  "Convert VALUE into a display string."
  (cond
   ((stringp value) value)
   ((numberp value) (number-to-string value))
   ((eq value t) "true")
   ((eq value :false) "false")
   ((null value) nil)
   (t (format "%s" value))))

(defun json-log-viewer-shared--alist-like-p (node)
  "Return non-nil when NODE looks like an alist object."
  (and (listp node)
       (or (null node)
           (let ((first (car node)))
             (and (consp first)
                  (or (stringp (car first))
                      (symbolp (car first))))))))

(defun json-log-viewer-shared--parse-json-line (line)
  "Parse LINE as JSON and return parsed structure, or nil."
  (when (stringp line)
    (condition-case nil
        (json-parse-string line :object-type 'alist :array-type 'list
                           :null-object nil :false-object :false)
      (error nil))))

(defun json-log-viewer-shared--parse-json-maybe (value)
  "Parse VALUE as JSON when it looks like a JSON object or array."
  (when (and (stringp value)
             (string-match-p "\\`[[:space:]\n\r\t]*[{\\[]" value))
    (json-log-viewer-shared--parse-json-line value)))

(defun json-log-viewer-shared--normalize-json-value-for-serialize (value)
  "Normalize VALUE into a shape `json-serialize' can encode reliably."
  (cond
   ((hash-table-p value)
    (let ((normalized (make-hash-table :test 'equal)))
      (maphash
       (lambda (key child)
         (when-let ((name (json-log-viewer-shared--value->string key)))
           (puthash name
                    (json-log-viewer-shared--normalize-json-value-for-serialize child)
                    normalized)))
       value)
      normalized))
   ((json-log-viewer-shared--alist-like-p value)
    (let ((normalized (make-hash-table :test 'equal)))
      (dolist (pair value)
        (when (consp pair)
          (when-let ((name (json-log-viewer-shared--value->string (car pair))))
            (puthash name
                     (json-log-viewer-shared--normalize-json-value-for-serialize (cdr pair))
                     normalized))))
      normalized))
   ((vectorp value)
    (vconcat
     (mapcar #'json-log-viewer-shared--normalize-json-value-for-serialize
             (append value nil))))
   ((listp value)
    (vconcat
     (mapcar #'json-log-viewer-shared--normalize-json-value-for-serialize value)))
   (t value)))

(defun json-log-viewer-shared--value->summary-string (value)
  "Convert resolved path VALUE into one-line summary text."
  (cond
   ((or (hash-table-p value)
        (vectorp value)
        (json-log-viewer-shared--alist-like-p value)
        (and (listp value) (not (null value))))
    (let ((json (condition-case nil
                    (json-serialize
                     (json-log-viewer-shared--normalize-json-value-for-serialize value)
                     :null-object nil
                     :false-object :false)
                  (error nil))))
      (or json (json-log-viewer-shared--value->string value))))
   (t
    (json-log-viewer-shared--value->string value))))

(defun json-log-viewer-shared--array-index (key)
  "Return integer array index from string KEY, or nil."
  (when (and (stringp key)
             (string-match-p "\\`[0-9]+\\'" key))
    (string-to-number key)))

(defun json-log-viewer-shared--split-path (path)
  "Split PATH into segments, allowing escaped dots.

Use `\\.' to represent a literal dot within a key segment."
  (let ((idx 0)
        (len (length path))
        (current "")
        parts)
    (while (< idx len)
      (let ((ch (aref path idx)))
        (cond
         ((= ch ?\\)
          (setq idx (1+ idx))
          (setq current
                (concat current
                        (if (< idx len)
                            (string (aref path idx))
                          "\\"))))
         ((= ch ?.)
          (push current parts)
          (setq current ""))
         (t
          (setq current (concat current (string ch))))))
      (setq idx (1+ idx)))
    (push current parts)
    (nreverse parts)))

(defun json-log-viewer-shared--json-get-child (node key)
  "Return child value KEY from NODE."
  (cond
   ((hash-table-p node)
    (or (gethash key node)
        (gethash (intern-soft key) node)))
   ((json-log-viewer-shared--alist-like-p node)
    (or (alist-get key node nil nil #'equal)
        (when-let ((sym (intern-soft key)))
          (alist-get sym node))))
   ((listp node)
    (when-let ((idx (json-log-viewer-shared--array-index key)))
      (nth idx node)))
   (t nil)))

(defun json-log-viewer-shared--json-get-path (node path)
  "Return value at dot-separated PATH in NODE.

Use `\\.' in PATH for literal dots in JSON keys."
  (let ((parts (json-log-viewer-shared--split-path path))
        (current node)
        (failed nil))
    (while (and parts (not failed))
      (when-let ((parsed (json-log-viewer-shared--parse-json-maybe current)))
        (setq current parsed))
      (setq current (json-log-viewer-shared--json-get-child current (car parts)))
      (unless current
        (setq failed t))
      (setq parts (cdr parts)))
    (unless failed
      current)))

(defun json-log-viewer-shared--join-path (prefix part)
  "Join PREFIX and PART using details-view dot notation."
  (if (and prefix (not (string-empty-p prefix)))
      (concat prefix "." part)
    part))

(defun json-log-viewer-shared--flatten-path-values (parsed)
  "Return flattened (PATH . VALUE) pairs for PARSED."
  (cl-labels
      ((flatten (node &optional prefix)
         (cond
          ((hash-table-p node)
           (let (fields keys)
             (maphash (lambda (key _value)
                        (when-let ((k (json-log-viewer-shared--value->string key)))
                          (push k keys)))
                      node)
             (setq keys (sort keys #'string-lessp))
             (if (null keys)
                 (when prefix (list (cons prefix "")))
               (dolist (key keys)
                 (setq fields
                       (append fields
                               (flatten (or (gethash key node)
                                            (when-let ((sym (intern-soft key)))
                                              (gethash sym node)))
                                        (json-log-viewer-shared--join-path prefix key)))))
               fields)))
          ((json-log-viewer-shared--alist-like-p node)
           (if (null node)
               (when prefix (list (cons prefix "")))
             (let (fields)
               (dolist (pair node)
                 (when (consp pair)
                   (when-let ((k (json-log-viewer-shared--value->string (car pair))))
                     (setq fields
                           (append fields
                                   (flatten (cdr pair)
                                            (json-log-viewer-shared--join-path prefix k)))))))
               fields)))
          ((listp node)
           (let ((base (or prefix "value")))
             (if (null node)
                 (list (cons base "[]"))
               (let ((idx 0)
                     fields)
                 (dolist (item node)
                   (setq fields
                         (append fields
                                 (flatten item (format "%s[%d]" base idx))))
                   (setq idx (1+ idx)))
                 fields))))
          (t
           (list (cons (or prefix "value")
                       (or (json-log-viewer-shared--value->summary-string node) "")))))))
    (and parsed (flatten parsed nil))))

(defun json-log-viewer-shared--resolve-path (parsed path &optional flattened-fields)
  "Resolve PATH from PARSED JSON object and return one-line string value.

Lookup first checks FLATTENED-FIELDS (or a fresh flattening of PARSED), so
paths match detail keys like \"payload.log.level\" without escaping dots."
  (when (and parsed
             (stringp path)
             (not (string-empty-p path)))
    (let ((flattened (or flattened-fields
                         (json-log-viewer-shared--flatten-path-values parsed))))
      (or (cdr (assoc path flattened))
          (json-log-viewer-shared--value->summary-string
           (json-log-viewer-shared--json-get-path parsed path))))))

(provide 'json-log-viewer-shared)
;;; json-log-viewer-shared.el ends here
