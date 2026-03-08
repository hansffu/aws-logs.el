dir := justfile_directory()

test:
    emacs -Q --batch -L . -L test \
        -l test/aws-logs-core-test.el \
        -l test/composite-json-log-viewer-test.el \
        -l test/json-log-viewer-subscribe-test.el \
        -l test/kafka-logs-test.el \
        -l test/kube-logs-test.el \
        -f ert-run-tests-batch-and-exit

test-file file:
    emacs -Q --batch -L . -L test -l {{file}} -f ert-run-tests-batch-and-exit

test-name file name:
    emacs -Q --batch -L . -L test -l {{file}} --eval '(ert-run-tests-batch-and-exit "{{name}}")'

compile:
    emacs -Q --batch -L . -f batch-byte-compile \
        aws-logs.el aws-logs-query.el aws-logs-insights.el aws-logs-tail.el \
        json-log-viewer.el json-log-viewer-shared.el json-log-viewer-repository.el \
        json-log-viewer-async-worker.el async-job-queue.el \
        composite-json-log-viewer.el kube-logs.el kafka-logs.el

clean:
    rm -f {{dir}}/*.elc

run-Q:
    emacs -Q -L {{dir}} \
        -l aws-logs.el -l kube-logs.el -l kafka-logs.el &

run:
    emacs -L {{dir}} &
