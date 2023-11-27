#!/bin/sh

set -e

if [ "$1" = "wget" ]; then
    DOWNLOAD_COMMAND="wget -nv"
    SUDO_COMMAND="sudo "
else
    DOWNLOAD_COMMAND="curl -LJO"
    SUDO_COMMAND=""
fi

shift

sh -c "$DOWNLOAD_COMMAND https://github.com/thulab/IginX-benchmarks/raw/main/resources/apache-iotdb-0.12.6-server-bin.zip"

sh -c "unzip -qq apache-iotdb-0.12.6-server-bin.zip"

sh -c "sleep 10"

sh -c "ls ./"

sh -c "echo ========================="

sh -c "ls apache-iotdb-0.12.6-server-bin"

for port in "$@"
do
  sh -c "${SUDO_COMMAND}cp -r apache-iotdb-0.12.6-server-bin/ apache-iotdb-0.12.6-server-bin-$port"

  sh -c "${SUDO_COMMAND}sed -i 's/# wal_buffer_size=16777216/wal_buffer_size=167772160/g' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sh -c "${SUDO_COMMAND}sed -i 's/6667/$port/g' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sh -c "${SUDO_COMMAND}cd apache-iotdb-0.12.6-server-bin-$port/; ${SUDO_COMMAND}nohup sbin/start-server.sh &"
done
