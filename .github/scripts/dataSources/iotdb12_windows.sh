#!/bin/sh

set -e

sh -c "curl -LJO https://github.com/thulab/IginX-benchmarks/raw/main/resources/apache-iotdb-0.12.6-server-bin.zip -o apache-iotdb-0.12.6-server-bin.zip"

sh -c "unzip -qq apache-iotdb-0.12.6-server-bin.zip"

sh -c "sleep 10"

sh -c "ls ./"

sh -c "echo ========================="

sh -c "ls apache-iotdb-0.12.6-server-bin"

for port in "$@"
do
  sh -c "cp -r apache-iotdb-0.12.6-server-bin/ apache-iotdb-0.12.6-server-bin-$port"

  sh -c "sed -i 's/# wal_buffer_size=16777216/wal_buffer_size=167772160/g' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sh -c "sed -i 's/6667/$port/g' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sh -c "sed -i 's/^@REM set MAX_HEAP_SIZE=.*$/set MAX_HEAP_SIZE=1G/g' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-env.bat"

  sh -c "sed -i 's/^@REM set HEAP_NEWSIZE=.*$/set HEAP_NEWSIZE=1G/g' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-env.bat"

  sh -c "sed -i 's/^# compaction_strategy=.*$/compaction_strategy=NO_COMPACTION/g' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sh -c "grep '^set MAX_HEAP_SIZE=' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-env.bat"

  sh -c "grep '^set HEAP_NEWSIZE=' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-env.bat"

  sh -c "grep '^compaction_strategy=' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sh -c "mkdir -p apache-iotdb-0.12.6-server-bin-$port/logs"

  powershell -Command "Start-Process -FilePath 'apache-iotdb-0.12.6-server-bin-$port/sbin/start-server.bat' -NoNewWindow -RedirectStandardOutput 'apache-iotdb-0.12.6-server-bin-$port/logs/db.log' -RedirectStandardError 'apache-iotdb-0.12.6-server-bin-$port/logs/db-error.log'"
done