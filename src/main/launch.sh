#!/usr/bin/env bash

function init() {
    rm -f mr-out* middle* output*
    rm -f wc.so
}

function server() {
    go run mrcoordinator.go pg-*.txt
}

function worker() {
    go build -buildmode=plugin ../mrapps/wc.go
    go run mrworker.go wc.so
}

function main() {
    init

    cmd="$1"
    case "$cmd" in
        "server")
            server
            ;;
        "worker")
            worker
            ;;
        "clean")
            echo "Clean tmp files"
            ;;
        *)
            echo "Unknown command: $cmd"
            ;;
    esac
}

main "$@"