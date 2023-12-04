@echo off

set PARAM=LOAD DATA FROM INFILE 'test/src/test/resources/fileReadAndWrite/csv/test.csv' AS CSV INTO t(key, a, b, c, d);

call client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.bat -e "%PARAM%"

exit 0