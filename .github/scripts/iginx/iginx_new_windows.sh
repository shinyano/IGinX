#!/bin/sh

set -e

sed -i "s/port=[0-9]\+/port=$1/g" core/target/iginx-core-*/conf/config.properties

sed -i "s/#iginx_port=[0-9]\+#/#iginx_port=$1#/g" core/target/iginx-core-*/conf/config.properties

sed -i "s/restPort=[0-9]\+/restPort=$2/g" core/target/iginx-core-*/conf/config.properties

echo "starting iginx on windows..."

batPath="$(find core/target -name 'start_iginx.bat' | grep 'iginx-core-.*\/sbin' | head -n 1)"

echo "$(realpath ${batPath})"

powershell -Command "Start-Process -FilePath '$batPath' -NoNewWindow -RedirectStandardOutput 'iginx-$1.log' -RedirectStandardError 'iginx-$1-error.log'"

sh -c "sleep 20"

echo "==================================================="

sh -c "cat iginx-$1.log"

echo "==================================================="

sh -c "cat iginx-$1-error.log"

echo "finished"
