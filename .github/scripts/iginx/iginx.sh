#!/bin/sh

set -e

sed -i "s/port=[0-9]\+/port=$1/g" core/target/iginx-core-*/conf/config.properties

sed -i "s/#iginx_port=[0-9]\+#/#iginx_port=$1#/g" core/target/iginx-core-*/conf/config.properties

sed -i "s/restPort=[0-9]\+/restPort=$2/g" core/target/iginx-core-*/conf/config.properties

if [ -n "$MSYSTEM" ]; then
    echo "starting iginx on windows..."
    echo $(netstat -ano | grep 2181)
    batPath="$(find core/target -name 'start_iginx.bat' | grep 'iginx-core-.*\/sbin' | head -n 1)"
    echo "$(realpath ${batPath})"
    powershell -Command "Start-Process -FilePath '$batPath' -NoNewWindow -RedirectStandardOutput 'iginx-$1.log' -RedirectStandardError 'iginx-$1-error.log'"
    echo "finished"

    sleep 3
    sh -c "cat iginx-$1-error.log"

    echo "================================"

    sh -c "cat iginx-$1.log"

    echo "================================"

    sh -c "cat zookeeper/logs/zookeeper.log"

    echo "================================"

    sh -c "cat zookeeper/logs/zookeeper-error.log"
else
    sh -c "chmod +x core/target/iginx-core-*/sbin/start_iginx.sh"

    sh -c "nohup core/target/iginx-core-*/sbin/start_iginx.sh > iginx-$1.log 2>&1 &"
fi
