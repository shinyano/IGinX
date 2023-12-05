#!/bin/sh

set -e

powershell -command "Invoke-WebRequest -Uri https://dlcdn.apache.org/zookeeper/zookeeper-3.7.2/apache-zookeeper-3.7.2-bin.tar.gz -OutFile apache-zookeeper-3.7.2-bin.tar.gz"

sh -c "tar -zxf apache-zookeeper-3.7.2-bin.tar.gz"

sh -c "mv apache-zookeeper-3.7.2-bin zookeeper"

sh -c "cp ./.github/actions/zookeeperRunner/zooWin.cfg zookeeper/conf/zoo.cfg"

powershell -Command "Start-Process -FilePath 'zookeeper/bin/zkServer.cmd' -NoNewWindow -RedirectStandardOutput 'zookeeper/zookeeper.log' -RedirectStandardError 'zookeeper/zookeeper-error.log'"

sh -c "sleep 10"

sh -c "netstat -ano | findstr 2181"
