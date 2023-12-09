#!/bin/sh

set -e

powershell -command "Invoke-WebRequest -Uri https://dlcdn.apache.org/zookeeper/zookeeper-3.7.2/apache-zookeeper-3.7.2-bin.tar.gz -OutFile apache-zookeeper-3.7.2-bin.tar.gz"

sh -c "tar -zxf apache-zookeeper-3.7.2-bin.tar.gz"

sh -c "mv apache-zookeeper-3.7.2-bin zookeeper"

sh -c "cp ./.github/actions/zookeeperRunner/zooWin.cfg zookeeper/conf/zoo.cfg"

sh -c "mkdir zookeeper/logs"

logDir=$(realpath "zookeeper/logs")

echo $logDir

powershell -Command "Start-Process -FilePath 'zookeeper/bin/zkServer.cmd' -NoNewWindow -RedirectStandardOutput '$logDir/zookeeper.log' -RedirectStandardError '$logDir/zookeeper-error.log'"

sleep 3

echo "ls zookeeper/data"
sh -c "ls zookeeper/data"
echo "ls zookeeper/logs"
sh -c "ls zookeeper/logs"
