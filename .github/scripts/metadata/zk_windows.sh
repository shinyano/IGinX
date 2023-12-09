#!/bin/sh

set -e

powershell -command "Invoke-WebRequest -Uri https://dlcdn.apache.org/zookeeper/zookeeper-3.7.2/apache-zookeeper-3.7.2-bin.tar.gz -OutFile apache-zookeeper-3.7.2-bin.tar.gz"

sh -c "tar -zxf apache-zookeeper-3.7.2-bin.tar.gz"

sh -c "mv apache-zookeeper-3.7.2-bin zookeeper"

sh -c "cp ./.github/actions/zookeeperRunner/zooWin.cfg zookeeper/conf/zoo.cfg"

cd zookeeper

sh -c "mkdir logs"

sh -c "mkdir data"

dataDir=$(realpath "data")

logDir=$(realpath "logs")

echo $logDir

sed -i "s/^dataDir=.*/dataDir=$dataDir/" conf/zoo.cfg

sed -i "s/^dataLogDir=.*/dataLogDir=$logDir/" conf/zoo.cfg

powershell -Command "Start-Process -FilePath 'zookeeper/bin/zkServer.cmd' -NoNewWindow -RedirectStandardOutput '$logDir/zookeeper.log' -RedirectStandardError '$logDir/zookeeper-error.log'"

sleep 3

echo "ls data"
sh -c "ls data"
echo "ls logs"
sh -c "ls logs"

sleep 3

sh -c "cat $logDir/zookeeper.log"
