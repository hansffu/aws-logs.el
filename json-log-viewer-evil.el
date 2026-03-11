;;; json-log-viewer-evil.el --- Optional Evil bindings for json-log-viewer -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Provides optional Evil keybindings for `json-log-viewer-mode`.
;;
;;; Code:

(defvar json-log-viewer--keybindings-function)


(defun json-log-viewer--evil-popup-keybindings ()
  "Return Evil keybindings for `json-log-viewer-show-info` popup."
  '(("TAB" . "toggle entry")
    ("S-TAB" . "toggle all")
    ("zn" . "narrow")
    ("zw" . "widen")
    ("?" . "show info")
    ("zf" . "toggle follow")))

(defun json-log-viewer-setup-evil ()
  "Set up Evil keybindings for `json-log-viewer-mode`.

Safe to call multiple times."
  (interactive)
  (unless (fboundp 'evil-define-key)
    (user-error "Evil is not available"))
  ;; `evil-define-key` is a macro. Evaluate this form at runtime so
  ;; byte-compilation does not hard-wire a direct macro call.
  (eval
   '(evil-define-key '(motion normal visual) json-log-viewer-mode-map
      (kbd "?") #'json-log-viewer-show-info
      (kbd "zn") #'json-log-viewer-narrow
      (kbd "zw") #'json-log-viewer-widen
      (kbd "zf") #'json-log-viewer-toggle-auto-follow))
  (setq json-log-viewer--keybindings-function #'json-log-viewer--evil-popup-keybindings)
  )

(provide 'json-log-viewer-evil)
;;; json-log-viewer-evil.el ends here
