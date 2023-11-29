#!/bin/bash

set -e


#WINDOWS_COMMAND="cmd.exe /c 'client\\target\\iginx-client-0.6.0-SNAPSHOT\\sbin\\start_cli.bat -e {}'"
#LINUX_COMMAND="bash client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e '{}'"
#
#if [ -n "$MSYSTEM" ]; then
#    echo "Testing on Windows..."
#    SCRIPT_COMMAND=WINDOWS_COMMAND
#else
#    echo "Testing on Unix..."
#    SCRIPT_COMMAND=LINUX_COMMAND
#
#    bash -c "chmod +x client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh"
#fi

bash -c "chmod +x client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh"

SCRIPT_COMMAND="bash client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e '{}'"

bash -c "sleep 10"

bash -c "echo 'clear data;' | xargs -t -i ${SCRIPT_COMMAND}"

bash -c "echo 'insert into test(key, s1) values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4);' | xargs -0 -t -i ${SCRIPT_COMMAND}"

bash -c "echo 'insert into test(key, s2) values (0, 0.5), (1, 1.5), (2, 2.5), (3, 3.5), (4, 4.5);' | xargs -0 -t -i ${SCRIPT_COMMAND}"

bash -c "echo 'insert into test(key, s3) values (0, true), (1, false), (2, true), (3, false), (4, true);' | xargs -0 -t -i ${SCRIPT_COMMAND}"

bash -c "echo 'insert into test(key, s4) values (0, "'"aaa"'"), (1, "'"bbb"'"), (2, "'"ccc"'"), (3, "'"ddd"'"), (4, "'"eee"'");' | xargs -0 -t -i ${SCRIPT_COMMAND}"

bash -c "echo 'select * from test into outfile "'"test/src/test/resources/fileReadAndWrite/byteStream"'" as stream;' | xargs -0 -t -i ${SCRIPT_COMMAND}"

bash -c "echo 'select * from test into outfile "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" as csv;' | xargs -0 -t -i ${SCRIPT_COMMAND}"
