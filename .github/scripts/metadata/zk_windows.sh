#!/bin/sh

set -e

powershell -command "Invoke-WebRequest -Uri https://dlcdn.apache.org/zookeeper/zookeeper-3.7.2/apache-zookeeper-3.7.2-bin.tar.gz -OutFile apache-zookeeper-3.7.2-bin.tar.gz"

sh -c "tar -zxf apache-zookeeper-3.7.2-bin.tar.gz"

sh -c "mv apache-zookeeper-3.7.2-bin zookeeper"

sh -c "cp ./.github/actions/zookeeperRunner/zooWin.cfg zookeeper/conf/zoo.cfg"

cd zookeeper

sh -c "mkdir logs"

sh -c "mkdir data"

dataDir=$(cygpath -w "${PWD}/data" | sed 's/\\/\\\\/g')

logDir=$(cygpath -w "${PWD}/logs" | sed 's/\\/\\\\/g')

echo $logDir

sed -i "s#^dataDir=.*#dataDir=$dataDir#" conf/zoo.cfg

sed -i "s#^dataLogDir=.*#dataLogDir=$logDir#" conf/zoo.cfg

powershell -Command "Start-Process -FilePath 'bin/zkServer.cmd' -NoNewWindow -RedirectStandardOutput 'logs/zookeeper.log' -RedirectStandardError 'logs/zookeeper-error.log'"

sleep 3

echo "ls data"
sh -c "ls data"
echo "ls logs"
sh -c "ls logs"

sleep 3

sh -c "cat logs/zookeeper.log"
