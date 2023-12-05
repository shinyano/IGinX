#!/bin/sh

set -e

echo "Downloading files..."

powershell -command "Invoke-WebRequest -Uri https://github.com/redis-windows/redis-windows/releases/download/7.0.14/Redis-7.0.14-Windows-x64.tar.gz -OutFile Redis-7.0.14-Windows-x64.tar.gz"

sh -c "tar -xzvf Redis-7.0.14-Windows-x64.tar.gz"

sh -c "ls Redis-7.0.14-Windows-x64"

sed -i "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#6379/storageEngineList=127.0.0.1#6379/g" conf/config.properties

for port in "$@"
do
  filePrefix="Redis-7.0.14-Windows-x64-$port"

  sh -c "cp -r Redis-7.0.14-Windows-x64 $filePrefix"

  sh -c "mkdir -p $filePrefix/logs"

  redirect="-RedirectStandardOutput '$filePrefix/logs/redis-$port.log' -RedirectStandardError '$filePrefix/logs/redis-$port-error.log'"

  powershell -command "Start-Process -FilePath '$filePrefix/redis-server' -ArgumentList '--port', '$port' -NoNewWindow $redirect"
  # sh -c "nohup redis-server --port $port &"
done