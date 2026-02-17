;;; json-log-viewer.el --- Generic foldable JSON log viewer -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Displays foldable JSON log entries with optional refresh callbacks.
;;
;;; Code:

(require 'cl-lib)
(require 'json)
(require 'subr-x)

(defgroup json-log-viewer nil
  "Foldable JSON log viewer buffers."
  :group 'tools)

(defface json-log-viewer-key-face
  '((t :inherit font-lock-keyword-face))
  "Face for keys in expanded log entry details."
  :group 'json-log-viewer)

(defface json-log-viewer-header-key-face
  '((t :inherit font-lock-keyword-face :weight bold))
  "Face for header keys in log viewer buffers."
  :group 'json-log-viewer)

(defface json-log-viewer-header-value-face
  '((t :inherit default))
  "Face for header values in log viewer buffers."
  :group 'json-log-viewer)

(defface json-log-viewer-keybinding-face
  '((t :inherit font-lock-constant-face :weight bold))
  "Face for keybinding tokens in log viewer headers."
  :group 'json-log-viewer)

(defface json-log-viewer-timestamp-face
  '((t :inherit shadow))
  "Face for timestamp segments in summary lines."
  :group 'json-log-viewer)

(defface json-log-viewer-level-face
  '((t :inherit font-lock-constant-face))
  "Face for level segments in summary lines."
  :group 'json-log-viewer)

(defface json-log-viewer-message-face
  '((t :inherit default))
  "Face for message segments in summary lines."
  :group 'json-log-viewer)

(defface json-log-viewer-extra-face
  '((t :inherit font-lock-variable-name-face))
  "Face for bracketed extra segments in summary lines."
  :group 'json-log-viewer)

(defvar-local json-log-viewer--fold-overlays nil
  "Fold overlays in the current viewer buffer.")

(defvar-local json-log-viewer--entry-overlays nil
  "Entry overlays in the current viewer buffer.")

(defvar-local json-log-viewer--current-line-overlay nil
  "Overlay used to highlight current entry.")

(defvar-local json-log-viewer--seen-signatures nil
  "Hash table of entry signatures already rendered in this buffer.")

(defvar-local json-log-viewer--filter-string nil
  "Current substring filter for rendered entries, or nil.")

(defvar-local json-log-viewer--context nil
  "Opaque refresh context owned by the caller.")

(defvar-local json-log-viewer--metadata nil
  "Opaque header metadata owned by the caller.")

(defvar-local json-log-viewer--entry-fields-function nil
  "Callback: (ENTRY) -> alist of field/value pairs.")

(defvar-local json-log-viewer--summary-function nil
  "Callback: (ENTRY FIELDS) -> summary string.")

(defvar-local json-log-viewer--refresh-function nil
  "Callback: (STATE) -> plist refresh payload.")

(defvar-local json-log-viewer--header-function nil
  "Callback: (STATE) -> alist of (KEY . VALUE) header lines.")

(defvar-local json-log-viewer--signature-function nil
  "Callback: (ENTRY) -> stable entry signature string.")

(defvar-local json-log-viewer--sort-key-function nil
  "Callback: (ENTRY) -> sortable key for ordering.")

(defvar-local json-log-viewer--streaming nil
  "Non-nil means streaming mode for this buffer.")

(defvar-local json-log-viewer--direction 'newest-first
  "Non-streaming direction: `newest-first' or `oldest-first'.")

(defvar-local json-log-viewer--live-narrow-max-rows 2000
  "Maximum row count that allows live narrowing updates.")

(defvar-local json-log-viewer--live-narrow-debounce 0.2
  "Idle seconds before applying live narrowing updates.")

(defvar-local json-log-viewer--raw-log-lines nil
  "Current raw JSON log lines shown in this buffer.")

(defvar-local json-log-viewer--line-to-entry-function nil
  "Buffer-local callback that converts one raw line into one entry object.")

(defvar-local json-log-viewer--next-entry-id 0
  "Next synthetic entry id for JSON-line based buffers.")

(defvar-local json-log-viewer--timestamp-path nil
  "JSON path used for timestamp summary rendering.")

(defvar-local json-log-viewer--level-path nil
  "JSON path used for level summary rendering.")

(defvar-local json-log-viewer--message-path nil
  "JSON path used for message summary rendering.")

(defvar-local json-log-viewer--extra-paths nil
  "List of JSON paths used for extra summary segments.")

(defvar-local json-log-viewer--json-refresh-log-lines-function nil
  "Callback: (OLD-LOG-LINES) -> NEW-LOG-LINES for JSON-line buffers.")

(defvar-local json-log-viewer--json-header-lines-function nil
  "Optional callback: (STATE) -> additional header lines for JSON-line buffers.")

(defvar-local json-log-viewer--auto-follow nil
  "Non-nil means keep point at newest entry while streaming.")

(defvar-local json-log-viewer--auto-follow-point-before-command nil
  "Point value captured in `pre-command-hook' for follow disabling logic.")

(defvar-local json-log-viewer--auto-follow-internal-move nil
  "Non-nil while viewer code moves point for auto-follow housekeeping.")

(defun json-log-viewer-get-buffer (buffer-name)
  "Return validated json-log-viewer buffer from BUFFER-NAME.

BUFFER-NAME can be a live buffer object or a buffer name string."
  (let ((buffer (cond
                 ((bufferp buffer-name) buffer-name)
                 ((stringp buffer-name) (get-buffer buffer-name))
                 (t nil))))
    (unless (buffer-live-p buffer)
      (user-error "Buffer not found: %S" buffer-name))
    (with-current-buffer buffer
      (unless (derived-mode-p 'json-log-viewer-mode)
        (user-error "Not a json-log-viewer buffer: %s" (buffer-name buffer))))
    buffer))

(defun json-log-viewer--value->string (value)
  "Convert VALUE into a display string."
  (cond
   ((stringp value) value)
   ((numberp value) (number-to-string value))
   ((eq value t) "true")
   ((eq value :false) "false")
   ((null value) nil)
   (t (format "%s" value))))

(defun json-log-viewer--normalize-fields (fields)
  "Normalize FIELDS into an alist of (string . string)."
  (let (normalized)
    (dolist (pair fields)
      (when (consp pair)
        (let ((key (json-log-viewer--value->string (car pair))))
          (when key
            (push (cons key
                        (or (json-log-viewer--value->string (cdr pair)) ""))
                  normalized)))))
    (nreverse normalized)))

(defun json-log-viewer--alist-like-p (node)
  "Return non-nil when NODE looks like an alist object."
  (and (listp node)
       (or (null node)
           (let ((first (car node)))
             (and (consp first)
                  (or (stringp (car first))
                      (symbolp (car first))))))))

(defun json-log-viewer--json-parse-line (line)
  "Parse LINE as JSON and return parsed structure, or nil."
  (when (stringp line)
    (condition-case nil
        (json-parse-string line :object-type 'alist :array-type 'list
                           :null-object nil :false-object :false)
      (error nil))))

(defun json-log-viewer--json-parse-maybe (value)
  "Parse VALUE as JSON when it looks like a JSON object or array."
  (when (and (stringp value)
             (string-match-p "\\`[[:space:]\n\r\t]*[{\\[]" value))
    (json-log-viewer--json-parse-line value)))

(defun json-log-viewer--array-index (key)
  "Return integer array index from string KEY, or nil."
  (when (and (stringp key)
             (string-match-p "\\`[0-9]+\\'" key))
    (string-to-number key)))

(defun json-log-viewer--json-get-child (node key)
  "Return child value KEY from NODE."
  (cond
   ((hash-table-p node)
    (or (gethash key node)
        (gethash (intern-soft key) node)))
   ((json-log-viewer--alist-like-p node)
    (or (alist-get key node nil nil #'equal)
        (when-let ((sym (intern-soft key)))
          (alist-get sym node))))
   ((listp node)
    (when-let ((idx (json-log-viewer--array-index key)))
      (nth idx node)))
   (t nil)))

(defun json-log-viewer--json-get-path (node path)
  "Return value at dot-separated PATH in NODE."
  (let ((parts (split-string path "\\."))
        (current node)
        (failed nil))
    (while (and parts (not failed))
      (when-let ((parsed (json-log-viewer--json-parse-maybe current)))
        (setq current parsed))
      (setq current (json-log-viewer--json-get-child current (car parts)))
      (unless current
        (setq failed t))
      (setq parts (cdr parts)))
    (unless failed
      current)))

(defun json-log-viewer--resolve-path (parsed path)
  "Resolve string PATH from PARSED JSON object and return string value."
  (when (and parsed
             (stringp path)
             (not (string-empty-p path)))
    (json-log-viewer--value->string
     (json-log-viewer--json-get-path parsed path))))

(defun json-log-viewer--json-object-fields (parsed raw-line)
  "Build detail fields alist from PARSED JSON and RAW-LINE."
  (cl-labels
      ((join-path (prefix part)
         (if (and prefix (not (string-empty-p prefix)))
             (concat prefix "." part)
           part))
       (flatten (node &optional prefix)
         (cond
          ((hash-table-p node)
           (let (fields keys)
             (maphash (lambda (key _value)
                        (let ((k (json-log-viewer--value->string key)))
                          (when k
                            (push k keys))))
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
                                        (join-path prefix key)))))
               fields)))
          ((json-log-viewer--alist-like-p node)
           (if (null node)
               (when prefix (list (cons prefix "")))
             (let (fields)
               (dolist (pair node)
                 (when (consp pair)
                   (let ((k (json-log-viewer--value->string (car pair))))
                     (when k
                       (setq fields
                             (append fields
                                     (flatten (cdr pair) (join-path prefix k))))))))
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
                       (or (json-log-viewer--value->string node) "")))))))
    (let ((fields (and parsed (flatten parsed nil))))
      (if fields
          fields
        (list (cons "raw" (or raw-line "")))))))

(defun json-log-viewer--parse-time (value)
  "Return epoch seconds parsed from VALUE, or nil."
  (when (and (stringp value) (not (string-empty-p value)))
    (let ((parsed (ignore-errors (date-to-time value))))
      (when parsed
        (float-time parsed)))))

(defun json-log-viewer--level-face (level)
  "Return face symbol suitable for LEVEL."
  (let ((normalized (downcase (or level ""))))
    (cond
     ((string-match-p "\\`\\(error\\|fatal\\|crit\\|panic\\)" normalized) 'error)
     ((string-match-p "\\`\\(warn\\|warning\\)" normalized) 'warning)
     ((string-match-p "\\`\\(debug\\|trace\\)" normalized) 'font-lock-doc-face)
     (t 'json-log-viewer-level-face))))

(defun json-log-viewer--truncate (value limit)
  "Truncate VALUE to LIMIT characters."
  (if (> (length value) limit)
      (concat (substring value 0 (max 0 (- limit 3))) "...")
    value))

(defun json-log-viewer--normalize-direction (direction)
  "Normalize DIRECTION symbol to `newest-first' or `oldest-first'."
  (pcase direction
    ((or 'newest-first 'desc 'descending) 'newest-first)
    ((or 'oldest-first 'asc 'ascending) 'oldest-first)
    (_
     (user-error "Invalid direction: %S (expected newest-first/oldest-first or asc/desc)"
                 direction))))

(defun json-log-viewer--ensure-log-lines (log-lines source)
  "Validate LOG-LINES from SOURCE and return a copied list."
  (unless (or (null log-lines)
              (and (listp log-lines)
                   (cl-every #'stringp log-lines)))
    (user-error "%s must be a list of JSON strings, got: %S" source log-lines))
  (append log-lines nil))

(defun json-log-viewer--json-line->entry (line)
  "Convert one JSON log LINE into a viewer entry plist."
  (let* ((entry-id json-log-viewer--next-entry-id)
         (parsed (json-log-viewer--json-parse-line line))
         (timestamp (json-log-viewer--resolve-path parsed json-log-viewer--timestamp-path))
         (timestamp-epoch (json-log-viewer--parse-time timestamp))
         (sort-key (or timestamp-epoch (+ 1000000000000.0 entry-id))))
    (setq json-log-viewer--next-entry-id (1+ json-log-viewer--next-entry-id))
    (list :id entry-id
          :raw line
          :parsed parsed
          :fields (json-log-viewer--json-object-fields parsed line)
          :sort-key sort-key)))

(defun json-log-viewer--json-line->entry-with-config (line entry-id timestamp-path)
  "Convert LINE into an entry plist using ENTRY-ID and TIMESTAMP-PATH."
  (let* ((parsed (json-log-viewer--json-parse-line line))
         (timestamp (json-log-viewer--resolve-path parsed timestamp-path))
         (timestamp-epoch (json-log-viewer--parse-time timestamp))
         (sort-key (or timestamp-epoch (+ 1000000000000.0 entry-id))))
    (list :id entry-id
          :raw line
          :parsed parsed
          :fields (json-log-viewer--json-object-fields parsed line)
          :sort-key sort-key)))

(defun json-log-viewer--json-lines->entries (lines timestamp-path start-id)
  "Convert LINES into entries using TIMESTAMP-PATH, starting at START-ID.

Returns cons cell (ENTRIES . NEXT-ID)."
  (let ((next-id start-id)
        entries)
    (dolist (line lines)
      (push (json-log-viewer--json-line->entry-with-config
             line next-id timestamp-path)
            entries)
      (setq next-id (1+ next-id)))
    (cons (nreverse entries) next-id)))

(defun json-log-viewer--json-entry-fields (entry)
  "Return detail fields from JSON-line ENTRY."
  (plist-get entry :fields))

(defun json-log-viewer--json-entry-signature (entry)
  "Return stable signature string for JSON-line ENTRY."
  (number-to-string (or (plist-get entry :id) 0)))

(defun json-log-viewer--json-entry-sort-key (entry)
  "Return sort key for JSON-line ENTRY."
  (plist-get entry :sort-key))

(defun json-log-viewer--json-summary (entry _fields)
  "Return formatted summary line for JSON-line ENTRY."
  (let* ((parsed (plist-get entry :parsed))
         (raw (or (plist-get entry :raw) ""))
         (timestamp (or (json-log-viewer--resolve-path parsed json-log-viewer--timestamp-path) "-"))
         (level (or (json-log-viewer--resolve-path parsed json-log-viewer--level-path) "-"))
         (message (or (json-log-viewer--resolve-path parsed json-log-viewer--message-path)
                      raw
                      "-"))
         (extras nil))
    (dolist (path json-log-viewer--extra-paths)
      (when-let ((value (json-log-viewer--resolve-path parsed path)))
        (push value extras)))
    (setq extras (nreverse extras))
    (concat
     (propertize timestamp 'face 'json-log-viewer-timestamp-face)
     " "
     (propertize (upcase level) 'face (json-log-viewer--level-face level))
     (if extras
         (concat " "
                 (mapconcat (lambda (value)
                              (propertize
                               (format "[%s]" (json-log-viewer--truncate value 80))
                               'face 'json-log-viewer-extra-face))
                            extras
                            " "))
       "")
     " "
     (propertize (json-log-viewer--truncate message 240)
                 'face 'json-log-viewer-message-face))))

(defun json-log-viewer--json-header-lines (state)
  "Return header lines for current JSON-line buffer and STATE."
  (append
   (list (cons "Mode" (if (plist-get state :streaming) "streaming" "non-streaming"))
         (cons "Direction" (symbol-name (plist-get state :direction)))
         (cons "Auto follow" (if (plist-get state :auto-follow) "on" "off")))
   (when (functionp json-log-viewer--json-header-lines-function)
     (or (funcall json-log-viewer--json-header-lines-function state) nil))))

(defun json-log-viewer--json-refresh (_state)
  "Refresh callback used for JSON-line buffers."
  (unless (functionp json-log-viewer--json-refresh-log-lines-function)
    (user-error "No JSON-line refresh function configured"))
  (let* ((old-lines (append json-log-viewer--raw-log-lines nil))
         (refresh-value (funcall json-log-viewer--json-refresh-log-lines-function old-lines)))
    (if (eq refresh-value :async)
        ;; Async refresh function will mutate the buffer later.
        (list :entries nil :replace nil)
      (let* ((new-lines (json-log-viewer--ensure-log-lines
                         refresh-value
                         "json-log-viewer refresh-function return value"))
             (entries (mapcar json-log-viewer--line-to-entry-function new-lines)))
        (setq json-log-viewer--raw-log-lines new-lines)
        (list :entries entries :replace t)))))

(defun json-log-viewer--entry-signature (entry)
  "Return stable signature for ENTRY."
  (if json-log-viewer--signature-function
      (or (funcall json-log-viewer--signature-function entry)
          (prin1-to-string entry))
    (prin1-to-string entry)))

(defun json-log-viewer--sort-key< (a b)
  "Return non-nil when sortable key A is strictly before B."
  (cond
   ((and (numberp a) (numberp b)) (< a b))
   ((and (stringp a) (stringp b)) (string-lessp a b))
   (t (string-lessp (format "%s" a) (format "%s" b)))))

(defun json-log-viewer--sort-entries (entries)
  "Return ENTRIES in display order for current buffer mode."
  (let ((ordered (append entries nil)))
    (if (not json-log-viewer--sort-key-function)
        ordered
      (let ((ascending (if json-log-viewer--streaming
                           t
                         (eq json-log-viewer--direction 'oldest-first))))
        (cl-stable-sort
         ordered
         (lambda (a b)
           (let ((ka (funcall json-log-viewer--sort-key-function a))
                 (kb (funcall json-log-viewer--sort-key-function b)))
             (cond
              ((and (null ka) (null kb)) nil)
              ((null ka) nil)
              ((null kb) t)
              (ascending (json-log-viewer--sort-key< ka kb))
              (t (json-log-viewer--sort-key< kb ka))))))))))

(defun json-log-viewer--state ()
  "Return current viewer state plist for callbacks."
  (list :context json-log-viewer--context
        :metadata json-log-viewer--metadata
        :streaming json-log-viewer--streaming
        :direction json-log-viewer--direction
        :auto-follow json-log-viewer--auto-follow
        :filter json-log-viewer--filter-string
        :row-count (length json-log-viewer--entry-overlays)
        :visible-row-count (json-log-viewer--visible-entry-count)))

(defun json-log-viewer--set-point-to-latest-entry ()
  "Move point and all visible windows for current buffer to latest entry."
  (let ((target (point-max)))
    (setq json-log-viewer--auto-follow-internal-move t)
    (unwind-protect
        (progn
          (goto-char target)
          (dolist (window (get-buffer-window-list (current-buffer) nil t))
            (set-window-point window target)))
      (setq json-log-viewer--auto-follow-internal-move nil))))

(defun json-log-viewer--remember-point-before-command ()
  "Record current point for auto-follow cursor-move detection."
  (setq json-log-viewer--auto-follow-point-before-command (point)))

(defun json-log-viewer--maybe-disable-auto-follow-after-command ()
  "Disable auto-follow when cursor moved by user command."
  (when (and json-log-viewer--auto-follow
             (not json-log-viewer--auto-follow-internal-move)
             (integer-or-marker-p json-log-viewer--auto-follow-point-before-command)
             (/= (point) json-log-viewer--auto-follow-point-before-command)
             (not (eq this-command 'json-log-viewer-toggle-auto-follow)))
    (setq json-log-viewer--auto-follow nil)
    (json-log-viewer--refresh-header)
    (message "Auto-follow disabled (cursor moved)")))

(defun json-log-viewer--clear-overlays ()
  "Remove all fold and entry overlays in the current buffer."
  (mapc #'delete-overlay json-log-viewer--fold-overlays)
  (mapc #'delete-overlay json-log-viewer--entry-overlays)
  (when json-log-viewer--current-line-overlay
    (delete-overlay json-log-viewer--current-line-overlay))
  (setq json-log-viewer--fold-overlays nil)
  (setq json-log-viewer--entry-overlays nil)
  (setq json-log-viewer--current-line-overlay nil))

(defun json-log-viewer--fold-overlay-at-point ()
  "Return fold overlay at point, or nil."
  (or (get-text-property (point) 'json-log-viewer-details-overlay)
      (get-text-property (line-beginning-position) 'json-log-viewer-details-overlay)
      (catch 'found
        (dolist (ov (overlays-at (point)))
          (when (overlay-get ov 'json-log-viewer-fold)
            (throw 'found ov)))
        nil)))

(defun json-log-viewer-toggle-entry ()
  "Toggle fold state for current log entry."
  (interactive)
  (when-let ((ov (json-log-viewer--fold-overlay-at-point)))
    (overlay-put ov 'invisible (not (overlay-get ov 'invisible)))))

(defun json-log-viewer-toggle-all ()
  "Toggle fold state for all log entries."
  (interactive)
  (let ((show-any nil))
    (dolist (ov json-log-viewer--fold-overlays)
      (when (overlay-get ov 'invisible)
        (setq show-any t)))
    (dolist (ov json-log-viewer--fold-overlays)
      (overlay-put ov 'invisible (not show-any)))))

(defun json-log-viewer--entry-overlay-at-point (&optional pos)
  "Return entry overlay at POS, or nil."
  (catch 'found
    (dolist (ov (overlays-at (or pos (point))))
      (when (overlay-get ov 'json-log-viewer-entry)
        (throw 'found ov)))
    nil))

(defun json-log-viewer--highlight-current-line ()
  "Update current entry highlight overlay in viewer buffers."
  (when (derived-mode-p 'json-log-viewer-mode)
    (let* ((entries-start (json-log-viewer--header-end-position))
           (pos (point))
           (visible-pos
            (if (invisible-p pos)
                (or (next-single-char-property-change pos 'invisible nil (point-max))
                    (previous-single-char-property-change pos 'invisible nil (point-min))
                    pos)
              pos))
           (_ (when (< visible-pos entries-start)
                (setq visible-pos pos)))
           (entry-ov (or (json-log-viewer--entry-overlay-at-point visible-pos)
                         (json-log-viewer--entry-overlay-at-point pos))))
      (if (or (not entry-ov)
              (< pos entries-start))
          (when json-log-viewer--current-line-overlay
            (delete-overlay json-log-viewer--current-line-overlay)
            (setq json-log-viewer--current-line-overlay nil))
        (let* ((entry-start (max entries-start (overlay-start entry-ov)))
               (entry-end (overlay-end entry-ov))
               (fold-ov (overlay-get entry-ov 'json-log-viewer-fold-overlay))
               (collapsed (and fold-ov (overlay-get fold-ov 'invisible)))
               (highlight-end (if (and collapsed fold-ov)
                                  (max entries-start (overlay-start fold-ov))
                                entry-end)))
          (if (<= highlight-end entry-start)
              (when json-log-viewer--current-line-overlay
                (delete-overlay json-log-viewer--current-line-overlay)
                (setq json-log-viewer--current-line-overlay nil))
            (unless json-log-viewer--current-line-overlay
              (setq json-log-viewer--current-line-overlay
                    (make-overlay entry-start highlight-end nil t t))
              (overlay-put json-log-viewer--current-line-overlay 'face 'hl-line)
              (overlay-put json-log-viewer--current-line-overlay 'priority 1000))
            (move-overlay json-log-viewer--current-line-overlay
                          entry-start highlight-end)))))))

(defun json-log-viewer--entry-filter-text (fields)
  "Build searchable text blob from FIELDS."
  (downcase
   (mapconcat (lambda (pair)
                (format "%s %s" (car pair) (cdr pair)))
              fields
              "\n")))

(defun json-log-viewer--filter-match-p (entry-overlay needle)
  "Return non-nil when ENTRY-OVERLAY matches NEEDLE."
  (string-match-p
   (regexp-quote needle)
   (or (overlay-get entry-overlay 'json-log-viewer-filter-text) "")))

(defun json-log-viewer--apply-filter ()
  "Apply active filter to entry overlays in current buffer."
  (let* ((needle (and json-log-viewer--filter-string
                      (string-trim json-log-viewer--filter-string)))
         (normalized (and needle (downcase needle))))
    (dolist (entry-overlay json-log-viewer--entry-overlays)
      (overlay-put entry-overlay 'invisible
                   (if (and normalized
                            (not (string-empty-p normalized))
                            (not (json-log-viewer--filter-match-p entry-overlay normalized)))
                       'json-log-viewer-filter
                     nil)))))

(defun json-log-viewer--set-filter (needle)
  "Set viewer filter to NEEDLE and apply it."
  (let ((normalized (string-trim (or needle ""))))
    (setq json-log-viewer--filter-string
          (unless (string-empty-p normalized) normalized))
    (json-log-viewer--apply-filter)
    (json-log-viewer--highlight-current-line)))

(defun json-log-viewer--live-preview-filter (buffer needle)
  "Apply preview NEEDLE in viewer BUFFER."
  (when (buffer-live-p buffer)
    (with-current-buffer buffer
      (json-log-viewer--set-filter needle))))

(defun json-log-viewer--visible-entry-count ()
  "Return number of visible rendered entries in current buffer."
  (let ((visible 0))
    (dolist (entry-overlay json-log-viewer--entry-overlays)
      (unless (overlay-get entry-overlay 'invisible)
        (setq visible (1+ visible))))
    visible))

(defun json-log-viewer--filter-summary ()
  "Return display string for active filter."
  (if (and json-log-viewer--filter-string
           (not (string-empty-p json-log-viewer--filter-string)))
      (format "\"%s\"" json-log-viewer--filter-string)
    "(none)"))

(defun json-log-viewer--header-line (key value)
  "Return formatted header line from KEY and VALUE."
  (concat
   (propertize (format "%-12s" (concat key ":"))
               'face 'json-log-viewer-header-key-face)
   " "
   (propertize value 'face 'json-log-viewer-header-value-face)))

(defun json-log-viewer--keybinding-fragment (key action)
  "Return formatted keybinding text for KEY and ACTION."
  (concat
   (propertize key 'face 'json-log-viewer-keybinding-face)
   (propertize (format " %s" action) 'face 'json-log-viewer-header-value-face)))

(defun json-log-viewer--keybindings ()
  "Return list of keybinding fragments to show in header."
  (append
   '(("TAB" . "toggle entry")
     ("S-TAB" . "toggle all")
     ("C-c C-n" . "narrow")
     ("C-c C-w" . "widen")
     ("C-c C-f" . "toggle follow")
     ("q" . "quit"))
   (when json-log-viewer--refresh-function
     '(("C-c C-r" . "refresh")))))

(defun json-log-viewer--insert-header (&optional row-count)
  "Insert formatted buffer header using ROW-COUNT."
  (let ((state (json-log-viewer--state)))
    (dolist (line (or (and json-log-viewer--header-function
                           (funcall json-log-viewer--header-function state))
                      nil))
      (let ((key (json-log-viewer--value->string (car line)))
            (value (json-log-viewer--value->string (cdr line))))
        (when key
          (insert (json-log-viewer--header-line key (or value "")) "\n"))))
    (insert (json-log-viewer--header-line
             "Messages"
             (number-to-string (or row-count
                                   (length json-log-viewer--entry-overlays))))
            "\n")
    (insert (json-log-viewer--header-line
             "Narrow filter"
             (json-log-viewer--filter-summary))
            "\n")
    (insert (propertize "Keys:        " 'face 'json-log-viewer-header-key-face))
    (insert
     (string-join
      (mapcar (lambda (binding)
                (json-log-viewer--keybinding-fragment (car binding) (cdr binding)))
              (json-log-viewer--keybindings))
      (propertize "  |  " 'face 'json-log-viewer-header-value-face)))
    (insert "\n\n")))

(defun json-log-viewer-narrow ()
  "Hide entries whose fields do not contain a minibuffer substring.

When result size allows it, updates are previewed live while typing."
  (interactive)
  (let* ((results-buffer (current-buffer))
         (original-filter json-log-viewer--filter-string)
         (row-count (length json-log-viewer--entry-overlays))
         (live-enabled (or (null json-log-viewer--live-narrow-max-rows)
                           (<= row-count json-log-viewer--live-narrow-max-rows)))
         (debounce (max 0 (or json-log-viewer--live-narrow-debounce 0)))
         (committed nil)
         (timer nil)
         (prompt (if live-enabled
                     "Narrow to string: "
                   (format "Narrow to string (live disabled above %d rows): "
                           json-log-viewer--live-narrow-max-rows))))
    (unwind-protect
        (let ((needle
               (minibuffer-with-setup-hook
                   (lambda ()
                     (when live-enabled
                       (add-hook
                        'post-command-hook
                        (lambda ()
                          (let ((input (string-trim (minibuffer-contents-no-properties))))
                            (if (<= debounce 0)
                                (json-log-viewer--live-preview-filter results-buffer input)
                              (when timer
                                (cancel-timer timer))
                              (setq timer
                                    (run-with-idle-timer
                                     debounce nil
                                     #'json-log-viewer--live-preview-filter
                                     results-buffer input)))))
                        nil t)))
                 (read-string prompt (or original-filter "")))))
          (setq needle (string-trim needle))
          (when (string-empty-p needle)
            (json-log-viewer--set-filter original-filter)
            (user-error "Narrow string cannot be empty"))
          (json-log-viewer--set-filter needle)
          (json-log-viewer--refresh-header)
          (setq committed t)
          (message "Narrowed to \"%s\" (%d visible row(s))"
                   needle
                   (json-log-viewer--visible-entry-count)))
      (when timer
        (cancel-timer timer))
      (unless committed
        (json-log-viewer--set-filter original-filter)))))

(defun json-log-viewer-widen ()
  "Clear filter and show all entries."
  (interactive)
  (setq json-log-viewer--filter-string nil)
  (json-log-viewer--apply-filter)
  (json-log-viewer--refresh-header)
  (message "Narrowing cleared"))

(defun json-log-viewer-toggle-auto-follow ()
  "Toggle automatic scrolling to newest entries."
  (interactive)
  (setq json-log-viewer--auto-follow (not json-log-viewer--auto-follow))
  (when json-log-viewer--auto-follow
    (json-log-viewer--set-point-to-latest-entry))
  (json-log-viewer--refresh-header)
  (message "Auto-follow %s" (if json-log-viewer--auto-follow "enabled" "disabled")))

(defun json-log-viewer--header-end-position ()
  "Return position where entries start (after header block)."
  (save-excursion
    (goto-char (point-min))
    (if (search-forward "\n\n" nil t)
        (point)
      (point-min))))

(defun json-log-viewer--refresh-header ()
  "Re-render header in current viewer buffer."
  (let ((inhibit-read-only t))
    (save-excursion
      (let* ((header-end (json-log-viewer--header-end-position))
             (row-count (length json-log-viewer--entry-overlays))
             (snapshots
              (mapcar (lambda (ov)
                        (list ov (overlay-start ov) (overlay-end ov)))
                      (delq nil
                            (append json-log-viewer--fold-overlays
                                    json-log-viewer--entry-overlays
                                    (list json-log-viewer--current-line-overlay)))))
             (delta 0))
        (goto-char (point-min))
        (delete-region (point-min) header-end)
        (json-log-viewer--insert-header row-count)
        (setq delta (- (json-log-viewer--header-end-position) header-end))
        (dolist (snapshot snapshots)
          (pcase-let ((`(,ov ,start ,end) snapshot))
            (when (overlay-buffer ov)
              (move-overlay ov
                            (if (>= start header-end) (+ start delta) start)
                            (if (>= end header-end) (+ end delta) end)))))))))

(defun json-log-viewer--insert-entry (entry)
  "Insert one foldable ENTRY."
  (let* ((raw-fields (funcall json-log-viewer--entry-fields-function entry))
         (fields (json-log-viewer--normalize-fields raw-fields))
         (summary (funcall json-log-viewer--summary-function entry fields))
         (filter-text (json-log-viewer--entry-filter-text fields))
         (summary-start (point))
         details-start details-end fold-ov entry-ov)
    (insert (or (json-log-viewer--value->string summary) "-") "\n")
    (setq details-start (point))
    (dolist (pair fields)
      (insert "  ")
      (insert (propertize (car pair) 'face 'json-log-viewer-key-face))
      (insert ": " (cdr pair) "\n"))
    (insert "\n")
    (setq details-end (point))
    (setq fold-ov (make-overlay details-start details-end))
    (overlay-put fold-ov 'json-log-viewer-fold t)
    (overlay-put fold-ov 'invisible t)
    (push fold-ov json-log-viewer--fold-overlays)
    (setq entry-ov (make-overlay summary-start details-end))
    (overlay-put entry-ov 'json-log-viewer-entry t)
    (overlay-put entry-ov 'json-log-viewer-fold-overlay fold-ov)
    (overlay-put entry-ov 'json-log-viewer-filter-text filter-text)
    (push entry-ov json-log-viewer--entry-overlays)
    (put-text-property summary-start (1- details-start)
                       'json-log-viewer-details-overlay fold-ov)))

(defun json-log-viewer--mark-seen-entries (entries)
  "Mark ENTRIES as seen in current buffer."
  (dolist (entry entries)
    (puthash (json-log-viewer--entry-signature entry) t json-log-viewer--seen-signatures)))

(defun json-log-viewer--unseen-entries (entries)
  "Return subset of ENTRIES not previously seen in current buffer."
  (cl-remove-if
   (lambda (entry)
     (gethash (json-log-viewer--entry-signature entry) json-log-viewer--seen-signatures))
   entries))

(defun json-log-viewer--delete-no-results-placeholder ()
  "Delete a `No results.` placeholder line when present."
  (save-excursion
    (goto-char (json-log-viewer--header-end-position))
    (when (looking-at "No results\\.\n")
      (delete-region (match-beginning 0) (match-end 0)))))

(defun json-log-viewer-replace-entries (entries &optional preserve-filter)
  "Replace rendered entries with ENTRIES.

When PRESERVE-FILTER is non-nil, keep the current active filter."
  (let ((active-filter (and preserve-filter json-log-viewer--filter-string))
        (inhibit-read-only t)
        (ordered (json-log-viewer--sort-entries entries)))
    (setq json-log-viewer--filter-string active-filter)
    (json-log-viewer--clear-overlays)
    (setq json-log-viewer--seen-signatures (make-hash-table :test 'equal))
    (erase-buffer)
    (json-log-viewer--insert-header (length ordered))
    (if (null ordered)
        (insert "No results.\n")
      (mapc #'json-log-viewer--insert-entry ordered))
    (json-log-viewer--mark-seen-entries ordered)
    (json-log-viewer--apply-filter)
    (if json-log-viewer--auto-follow
        (json-log-viewer--set-point-to-latest-entry)
      (goto-char (point-min)))
    (json-log-viewer--highlight-current-line)))

(defun json-log-viewer-append-entries (entries)
  "Append ENTRIES into current viewer buffer.

In streaming mode new entries are always appended to the bottom.
In non-streaming mode the append position follows configured direction."
  (let* ((candidate-entries (json-log-viewer--unseen-entries entries))
         (ordered (json-log-viewer--sort-entries candidate-entries))
         (append-at-bottom (or json-log-viewer--streaming
                               (eq json-log-viewer--direction 'oldest-first)))
         (inhibit-read-only t))
    (when ordered
      (save-excursion
        (json-log-viewer--delete-no-results-placeholder)
        (if append-at-bottom
            (goto-char (point-max))
          (goto-char (json-log-viewer--header-end-position)))
        (mapc #'json-log-viewer--insert-entry ordered))
      (json-log-viewer--mark-seen-entries ordered)
      (json-log-viewer--apply-filter)
      (json-log-viewer--refresh-header)
      (when (and json-log-viewer--auto-follow append-at-bottom)
        (json-log-viewer--set-point-to-latest-entry))
      (json-log-viewer--highlight-current-line))
    ordered))

(defun json-log-viewer-refresh ()
  "Refresh current viewer buffer via configured refresh callback."
  (interactive)
  (unless json-log-viewer--refresh-function
    (user-error "No refresh callback configured for this buffer"))
  (let* ((result (funcall json-log-viewer--refresh-function (json-log-viewer--state)))
         (entries (or (plist-get result :entries) nil))
         (replace (if (plist-member result :replace)
                      (plist-get result :replace)
                    (not json-log-viewer--streaming)))
         (message-text (plist-get result :message)))
    (when (plist-member result :context)
      (setq json-log-viewer--context (plist-get result :context)))
    (when (plist-member result :metadata)
      (setq json-log-viewer--metadata (plist-get result :metadata)))
    (if replace
        (json-log-viewer-replace-entries entries t)
      (json-log-viewer-append-entries entries))
    (json-log-viewer--refresh-header)
    (when message-text
      (message "%s" message-text))))

(defvar-keymap json-log-viewer-mode-map
  :doc "Keymap for `json-log-viewer-mode'."
  "TAB" #'json-log-viewer-toggle-entry
  "<tab>" #'json-log-viewer-toggle-entry
  "<backtab>" #'json-log-viewer-toggle-all
  "C-c C-r" #'json-log-viewer-refresh
  "C-c C-n" #'json-log-viewer-narrow
  "C-c C-f" #'json-log-viewer-toggle-auto-follow
  "C-c C-w" #'json-log-viewer-widen)

(define-derived-mode json-log-viewer-mode special-mode "JsonLogs"
  "Major mode for foldable JSON log entries."
  :group 'json-log-viewer
  (buffer-disable-undo)
  (setq-local truncate-lines t)
  (setq-local line-move-ignore-invisible t)
  (setq-local buffer-invisibility-spec '(t))
  (add-to-invisibility-spec 'json-log-viewer-filter)
  (add-hook 'pre-command-hook #'json-log-viewer--remember-point-before-command nil t)
  (add-hook 'post-command-hook #'json-log-viewer--maybe-disable-auto-follow-after-command nil t)
  (add-hook 'post-command-hook #'json-log-viewer--highlight-current-line t t))

(cl-defun json-log-viewer-make-buffer (buffer-name
                                       &key
                                       log-lines
                                       timestamp-path
                                       level-path
                                       message-path
                                       extra-paths
                                       refresh-function
                                       streaming
                                       (direction 'newest-first)
                                       header-lines-function
                                       (live-narrow-max-rows 2000)
                                       (live-narrow-debounce 0.2))
  "Create BUFFER-NAME for JSON LOG-LINES.

Summary rendering is configured with explicit JSON paths.

LOG-LINES is a list of JSON strings.

TIMESTAMP-PATH, LEVEL-PATH, MESSAGE-PATH are dot-separated JSON paths used for
summary rendering. EXTRA-PATHS is a list of additional paths rendered as
bracketed segments.

REFRESH-FUNCTION, when non-nil, is called with the current old list of raw log
lines and must return the new full list of raw log lines.

STREAMING controls append semantics:
- non-nil: new data is appended to the bottom (use `json-log-viewer-push`)
- nil: ordering follows DIRECTION
  (`newest-first`/`oldest-first` or `desc`/`asc`)

Returns the created buffer."
  (unless (stringp buffer-name)
    (user-error "json-log-viewer-make-buffer requires BUFFER-NAME to be a string"))
  (let* ((normalized-lines (json-log-viewer--ensure-log-lines
                            log-lines "json-log-viewer-make-buffer :log-lines"))
         (entries+next (json-log-viewer--json-lines->entries
                        normalized-lines timestamp-path 0))
         (initial-entries (car entries+next))
         (next-id (cdr entries+next))
         (normalized-direction (json-log-viewer--normalize-direction direction))
         (target (get-buffer-create buffer-name)))
    (with-current-buffer target
      (json-log-viewer-mode)
      (setq-local json-log-viewer--entry-fields-function #'json-log-viewer--json-entry-fields)
      (setq-local json-log-viewer--summary-function #'json-log-viewer--json-summary)
      (setq-local json-log-viewer--refresh-function
                  (when (functionp refresh-function)
                    #'json-log-viewer--json-refresh))
      (setq-local json-log-viewer--header-function #'json-log-viewer--json-header-lines)
      (setq-local json-log-viewer--signature-function #'json-log-viewer--json-entry-signature)
      (setq-local json-log-viewer--sort-key-function #'json-log-viewer--json-entry-sort-key)
      (setq-local json-log-viewer--streaming streaming)
      (setq-local json-log-viewer--direction normalized-direction)
      (setq-local json-log-viewer--context nil)
      (setq-local json-log-viewer--metadata nil)
      (setq-local json-log-viewer--live-narrow-max-rows live-narrow-max-rows)
      (setq-local json-log-viewer--live-narrow-debounce live-narrow-debounce)
      (setq-local json-log-viewer--raw-log-lines normalized-lines)
      (setq-local json-log-viewer--line-to-entry-function #'json-log-viewer--json-line->entry)
      (setq-local json-log-viewer--next-entry-id next-id)
      (setq-local json-log-viewer--timestamp-path timestamp-path)
      (setq-local json-log-viewer--level-path level-path)
      (setq-local json-log-viewer--message-path message-path)
      (setq-local json-log-viewer--extra-paths (append extra-paths nil))
      (setq-local json-log-viewer--json-refresh-log-lines-function refresh-function)
      (setq-local json-log-viewer--json-header-lines-function header-lines-function)
      (setq-local json-log-viewer--seen-signatures (make-hash-table :test 'equal))
      (json-log-viewer-replace-entries initial-entries nil))
    target))

(defun json-log-viewer-push (buffer-or-name log-lines)
  "Push LOG-LINES into BUFFER-OR-NAME for streaming updates.

BUFFER-OR-NAME must identify a live `json-log-viewer-mode` buffer created by
`json-log-viewer-make-buffer` with `:streaming` non-nil."
  (let ((target (json-log-viewer-get-buffer buffer-or-name)))
    (with-current-buffer target
      (unless json-log-viewer--streaming
        (user-error "json-log-viewer-push requires a buffer with streaming mode enabled"))
      (unless (functionp json-log-viewer--line-to-entry-function)
        (user-error "Push is only supported for buffers created by json-log-viewer-make-buffer"))
      (let* ((normalized-lines (json-log-viewer--ensure-log-lines
                                log-lines "json-log-viewer-push"))
             (entries (mapcar json-log-viewer--line-to-entry-function normalized-lines)))
        (setq json-log-viewer--raw-log-lines
              (append json-log-viewer--raw-log-lines normalized-lines))
        (json-log-viewer-append-entries entries)))))

(defun json-log-viewer-current-log-lines (buffer-or-name)
  "Return a copy of current raw log lines from BUFFER-OR-NAME."
  (let ((target (json-log-viewer-get-buffer buffer-or-name)))
    (with-current-buffer target
      (append json-log-viewer--raw-log-lines nil))))

(defun json-log-viewer-replace-log-lines (buffer-or-name log-lines &optional preserve-filter)
  "Replace raw LOG-LINES in BUFFER-OR-NAME.

When PRESERVE-FILTER is non-nil, keep the current active filter."
  (let ((target (json-log-viewer-get-buffer buffer-or-name)))
    (with-current-buffer target
      (unless (functionp json-log-viewer--line-to-entry-function)
        (user-error "Replace is only supported for buffers created by json-log-viewer-make-buffer"))
      (let* ((normalized-lines (json-log-viewer--ensure-log-lines
                                log-lines "json-log-viewer-replace-log-lines"))
             (entries+next
              (if (eq json-log-viewer--line-to-entry-function #'json-log-viewer--json-line->entry)
                  (json-log-viewer--json-lines->entries
                   normalized-lines json-log-viewer--timestamp-path 0)
                (let (entries)
                  (dolist (line normalized-lines)
                    (push (funcall json-log-viewer--line-to-entry-function line) entries))
                  (cons (nreverse entries) json-log-viewer--next-entry-id))))
             (entries (car entries+next))
             (next-id (cdr entries+next)))
        (setq json-log-viewer--raw-log-lines normalized-lines)
        (setq json-log-viewer--next-entry-id next-id)
        (json-log-viewer-replace-entries entries preserve-filter)))))

(provide 'json-log-viewer)
;;; json-log-viewer.el ends here
