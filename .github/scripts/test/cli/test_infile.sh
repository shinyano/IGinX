#!/bin/bash

set -e

WINDOWS_COMMAND="cmd.exe /c 'client\\target\\iginx-client-0.6.0-SNAPSHOT\\sbin\\start_cli.bat -e {}'"
LINUX_COMMAND="bash client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e '{}'"

if [ -n "$MSYSTEM" ]; then
    echo "Testing on Windows..."
    SCRIPT_COMMAND=WINDOWS_COMMAND
else
    echo "Testing on Unix..."
    SCRIPT_COMMAND=LINUX_COMMAND
    bash -c "chmod +x client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh"
fi

bash -c "echo 'LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" AS CSV INTO t(key, a, b, c, d);' | xargs -0 -t -i ${SCRIPT_COMMAND}"
