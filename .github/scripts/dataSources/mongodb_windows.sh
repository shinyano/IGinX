#!/bin/sh

set -e

echo "Downloading zip archive. This may take a few minutes..."

powershell -command "Invoke-WebRequest -Uri https://fastdl.mongodb.org/windows/mongodb-windows-x86_64-6.0.12.zip -OutFile mongodb-windows-x86_64-6.0.12.zip"

powershell -command "Expand-Archive ./mongodb-windows-x86_64-6.0.12.zip -DestinationPath './'"

sh -c "ls mongodb-win32-x86_64-windows-6.0.12/bin"

sed -i "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#27017/storageEngineList=127.0.0.1#27017/g" conf/config.properties

for port in "$@"
do
  sh -c "cp -r mongodb-win32-x86_64-windows-6.0.12/ mongodb-win32-x86_64-windows-6.0.12-$port/"

  sh -c "cd mongodb-win32-x86_64-windows-6.0.12-$port/; mkdir -p data/db; mkdir -p data/log; "

  filePrefix="mongodb-win32-x86_64-windows-6.0.12-$port"

  arguments="-ArgumentList '--port', '$port', '--dbpath', '$filePrefix/data/db', '--logpath', '$filePrefix/data/log/mongo.log'"

  powershell -command "Start-Process -FilePath '$filePrefix/bin/mongod' $arguments"

  echo $pwd
done
