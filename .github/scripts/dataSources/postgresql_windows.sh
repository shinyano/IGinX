#!/bin/sh

set -e

echo "Downloading files..."

powershell -command "Invoke-WebRequest -Uri https://get.enterprisedb.com/postgresql/postgresql-15.5-1-windows-x64-binaries.zip -OutFile postgresql-15.5-1-windows-x64-binaries.zip"

powershell -command "Expand-Archive ./postgresql-15.5-1-windows-x64-binaries.zip -DestinationPath './'"

sh -c "ls pgsql/bin"

sed -i "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#5432#postgresql/storageEngineList=127.0.0.1#5432#postgresql/g" conf/config.properties

for port in "$@"
do

  sh -c "cp -R pgsql pgsql-$port"

  filePrefix="pgsql-$port"

  sh -c "mkdir -p $filePrefix/data"

  sh -c "mkdir -p $filePrefix/logs"

  arguments="-ArgumentList '-D', '$filePrefix/data', '--username=postgres', '--auth', 'trust', '--no-instructions'"

  redirect="-RedirectStandardOutput '$filePrefix/logs/inidb.log' -RedirectStandardError '$filePrefix/logs/initdb-error.log'"

  powershell -command "Start-Process -FilePath '$filePrefix/bin/initdb' -NoNewWindow $arguments $redirect"

  ctlarguments="-ArgumentList '-D', '$filePrefix/data', '-o', '\"-F -p $port\"', 'start'"

  ctlredirect="-RedirectStandardOutput '$filePrefix/logs/pg_ctl.log' -RedirectStandardError '$filePrefix/logs/pg_ctl-error.log'"

  sh -c "sleep 6"

  powershell -command "Start-Process -FilePath '$filePrefix/bin/pg_ctl' -NoNewWindow $ctlarguments $ctlredirect"

  sql='"ALTER USER postgres WITH PASSWORD '\'\''postgres'\'\'';"'

  sqlarguments="-ArgumentList '-c', $sql"

  sqlredirect="-RedirectStandardOutput '$filePrefix/logs/psql.log' -RedirectStandardError '$filePrefix/logs/psql-error.log'"

  powershell -command "Start-Process -FilePath '$filePrefix/bin/psql' -NoNewWindow $sqlarguments $sqlredirect"
done
