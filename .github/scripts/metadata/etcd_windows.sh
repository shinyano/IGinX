#!/bin/sh

set -e

powershell -command "Invoke-WebRequest -Uri https://go.dev/dl/go1.21.5.windows-amd64.zip -OutFile go1.21.5.windows-amd64.zip"

powershell -command "Expand-Archive ./go1.21.5.windows-amd64.zip -DestinationPath './'"

sh -c "ls ./"

sh -c "ls go"

export GOROOT=$PWD

export GOPATH=$GOROOT/go

export GOBIN=$GOPATH/bin

export PATH=$PATH:$GOBIN

echo $GOBIN

powershell -command "Invoke-WebRequest -Uri https://github.com/etcd-io/etcd/releases/download/v3.4.28/etcd-v3.4.28-windows-amd64.zip -OutFile etcd-v3.4.28-windows-amd64.zip"

powershell -command "Expand-Archive ./etcd-v3.4.28-windows-amd64.zip -DestinationPath './'"

sh -c "mv etcd-v3.4.28-windows-amd64/etcd* $GOBIN"

powershell -command "Start-Process etcd -NoNewWindow -RedirectStandardOutput 'etcd-run.log' -RedirectStandardError 'etcd-error.log'"
