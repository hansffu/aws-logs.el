;;; json-log-viewer-evil.el --- Optional Evil bindings for json-log-viewer -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:
;;
;; Provides optional Evil keybindings for `json-log-viewer-mode`.
;;
;;; Code:

(require 'json-log-viewer)

(declare-function evil-define-key "evil-core" (state keymap &rest bindings))


(defun json-log-viewer--evil-popup-keybindings ()
  "Return Evil keybindings for `json-log-viewer-show-info` popup."
  (append
   '(("TAB" . "toggle entry")
     ("S-TAB" . "toggle all")
     ("zn" . "narrow")
     ("zw" . "widen")
     ("?" . "show info")
     ("zf" . "toggle follow"))
   (when json-log-viewer--refresh-function
     '(("gr" . "refresh")))))

(defun json-log-viewer-setup-evil ()
  "Set up Evil keybindings for `json-log-viewer-mode`.

Safe to call multiple times."
  (interactive)
  (unless (fboundp 'evil-define-key)
    (user-error "Evil is not available"))
  (evil-define-key '(motion normal visual) json-log-viewer-mode-map
    (kbd "?") #'json-log-viewer-show-info
    (kbd "zn") #'json-log-viewer-narrow
    (kbd "zw") #'json-log-viewer-widen
    (kbd "gr") #'json-log-viewer-refresh
    (kbd "zf") #'json-log-viewer-toggle-auto-follow)
  (setq json-log-viewer--keybindings-function #'json-log-viewer--evil-popup-keybindings)
  )

(provide 'json-log-viewer-evil)
;;; json-log-viewer-evil.el ends here
