#!/usr/bin/env bash
rm -rf mr-tmp-*
rm -rf mr-out*
go build -buildmode=plugin ../mrapps/crash.go
for i in {0,1,2,3,4,5,6,7}; do
    go run mrworker.go crash.so &
done

