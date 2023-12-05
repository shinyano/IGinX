#!/bin/sh

set -e

#sh -c "chmod +x core/target/iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.sh"

cd core/target/iginx-core-0.6.0-SNAPSHOT/

#export IGINX_HOME=$PWD

iginx_home_path=$PWD

# execute start-up script in different directory, to test whether udf-file path detection will be effected
cd ..

#sh -c "nohup iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.sh > ../../iginx.log 2>&1 &"

if [ -n "$MSYSTEM" ]; then
    echo "$iginx_home_path"
    windows_path=$(cygpath -w "$iginx_home_path")
    echo "$windows_path"
    export IGINX_HOME=$windows_path
    echo $IGINX_HOME
#    sh -c "start /min iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.bat > ../../iginx-udf.log 2>&1"
#    cmd.exe /c "iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.bat > ../../iginx.log 2>&1 &"
#    sh -c "./iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.bat > ../../iginx-udf.log 2>&1"
#    batPath="$(find core/target -name 'start_iginx.bat' | grep 'iginx-core-.*\/sbin' | head -n 1)"
    powershell -Command "Start-Process -FilePath 'iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.bat' -NoNewWindow -RedirectStandardOutput '../../iginx-udf.log' -RedirectStandardError '../../iginx-udf-error.log'"
#    start /B cmd.exe /C "CALL iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.bat > ../../iginx-udf.log 2>&1"


#    cat "../../iginx-udf.log"
else
    export IGINX_HOME=$iginx_home_path

    sh -c "chmod +x iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.sh"

    sh -c "nohup iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.sh > ../../iginx.log 2>&1 &"
fi
