#!/bin/sh

set -e

sh -c "chmod +x core/target/iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.sh"

cd core/target/iginx-core-0.6.0-SNAPSHOT/

export IGINX_HOME=$PWD

# execute start-up script in different directory, to test whether udf-file path detection will be effected
cd ..

if [ -n "$MSYSTEM" ]; then
    cmd.exe /c "iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.bat > ../../iginx.log 2>&1 &"
else
    sh -c "chmod +x core/target/iginx-core-*/sbin/start_iginx.sh"

    sh -c "nohup iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.sh > ../../iginx.log 2>&1 &"
fi
