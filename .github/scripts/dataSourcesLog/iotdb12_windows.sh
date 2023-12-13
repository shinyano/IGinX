#!/bin/bash

# 查找所有名为 iot-* 的文件夹
for dir in apache-iotdb-0.12.6-server-bin-*; do
    # 确保它是一个目录
    if [ -d "$dir" ]; then
        echo "Entering: $dir"

        # 检查db.log文件是否存在
        if [ -f "$dir/logs/db.log" ]; then
            echo "cat $dir/logs/db.log :"
            cat "$dir/logs/db.log"
        else
            echo "$dir/logs/db.log not found."
        fi

        # 检查db-error.log文件是否存在
        if [ -f "$dir/logs/db-error.log" ]; then
            echo "cat $dir/logs/db-error.log :"
            cat "$dir/logs/db-error.log"
        else
            echo "$dir/logs/db-error.log not found."
        fi
    fi
done
