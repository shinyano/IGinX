#!/bin/sh

set -e

powershell -command "Invoke-WebRequest -Uri https://dl.influxdata.com/influxdb/releases/influxdb2-2.7.4-windows.zip -OutFile influxdb2-2.7.4-windows.zip"

powershell -command "Expand-Archive ./influxdb2-2.7.4-windows.zip -DestinationPath './influxdb2-2.7.4-windows/'"

powershell -command "Invoke-WebRequest -Uri https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.3-windows-amd64.zip -OutFile influxdb2-client-2.7.3-windows-amd64.zip"

powershell -command "Expand-Archive ./influxdb2-client-2.7.3-windows-amd64.zip -DestinationPath './influxdb2-client-2.7.3-windows-amd64/'"

sh -c "ls influxdb2-2.7.4-windows"

sh -c "mkdir influxdb2-2.7.4-windows/.influxdbv2"

sh -c "mkdir influxdb2-2.7.4-windows/logs"

arguments="-ArgumentList 'run', '--bolt-path=influxdb2-2.7.4-windows/.influxdbv2/influxd.bolt', '--engine-path=influxdb2-2.7.4-windows/.influxdbv2/engine', '--http-bind-address=:8086', '--query-memory-bytes=300971520'"

redirect="-RedirectStandardOutput 'influxdb2-2.7.4-windows/logs/influx.log' -RedirectStandardError 'influxdb2-2.7.4-windows/logs/influx-error.log'"

powershell -command "Start-Process -FilePath 'influxdb2-2.7.4-windows/influxd' $arguments -NoNewWindow $redirect"

sh -c "sleep 3"

boltAb=$(realpath ./influxdb2-2.7.4-windows/.influxdbv2/influxd.bolt)

engAb=$(realpath ./influxdb2-2.7.4-windows/.influxdbv2/engine)

echo $boltAb

echo $engAb

sh -c "sleep 3"

sh -c "./influxdb2-client-2.7.3-windows-amd64/influx setup --host http://localhost:8086 --org testOrg --bucket testBucket --username user --password 12345678 --token testToken --force"

sed -i "s/your-token/testToken/g" conf/config.properties

sed -i "s/your-organization/testOrg/g" conf/config.properties

sed -i "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#8086/storageEngineList=127.0.0.1#8086/g" conf/config.properties

for port in "$@"
do
  sh -c "cp -r influxdb2-2.7.4-windows/ influxdb2-2.7.4-windows-$port/"

  arguments="-ArgumentList 'run', '--bolt-path=$boltAb', '--engine-path=$engAb', '--http-bind-address=:$port', '--query-memory-bytes=20971520'"

  redirect="-RedirectStandardOutput 'influxdb2-2.7.4-windows-$port/logs/influx.log' -RedirectStandardError 'influxdb2-2.7.4-windows-$port/logs/influx-error.log'"

  powershell -command "Start-Process -FilePath 'influxdb2-2.7.4-windows-$port/influxd' $arguments -NoNewWindow $redirect"

  sh -c "sleep 10"

  sh -c "cat influxdb2-2.7.4-windows-$port/logs/influx.log"

  echo "==========================================="

  sh -c "cat influxdb2-2.7.4-windows-$port/logs/influx-error.log"

  sh -c "./influxdb2-client-2.7.3-windows-amd64/influx setup --host http://localhost:$port --org testOrg --bucket testBucket --username user --password 12345678 --token testToken --force --name testName$port"

  sh -c "sleep 10"
done
