#!/bin/sh

set -e

powershell -command "Invoke-WebRequest -Uri https://github.com/thulab/IginX-benchmarks/raw/main/resources/apache-iotdb-0.12.6-server-bin.zip -OutFile apache-iotdb-0.12.6-server-bin.zip"

powershell -command "Expand-Archive ./apache-iotdb-0.12.6-server-bin.zip -DestinationPath './'"

sh -c "sleep 10"

sh -c "ls ./"

sh -c "echo ========================="

sh -c "ls apache-iotdb-0.12.6-server-bin"

for port in "$@"
do
  sh -c "cp -r apache-iotdb-0.12.6-server-bin/ apache-iotdb-0.12.6-server-bin-$port"

  sh -c "sed -i 's/# wal_buffer_size=16777216/wal_buffer_size=167772160/g' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sh -c "sed -i 's/6667/$port/g' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sh -c "mkdir -p apache-iotdb-0.12.6-server-bin-$port/logs"

  powershell -Command "Start-Process -FilePath 'apache-iotdb-0.12.6-server-bin-$port/sbin/start-server.bat' -NoNewWindow -RedirectStandardOutput 'apache-iotdb-0.12.6-server-bin-$port/logs/db.log' -RedirectStandardError 'apache-iotdb-0.12.6-server-bin-$port/logs/db-error.log'"
done