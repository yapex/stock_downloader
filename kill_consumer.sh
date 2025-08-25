#!/bin/bash
# kill_consumer.sh

echo "正在查找后台运行的consumer进程..."

PIDS=$(pgrep -f "neo dp")

if [ -z "$PIDS" ]; then
    echo "没有找到正在运行的consumer进程"
else
    echo "找到以下consumer进程:"
    ps -p $PIDS -o pid,ppid,cmd
    echo "正在终止这些进程..."
    kill $PIDS
    sleep 2
    # 检查是否还有残留进程
    PIDS=$(pgrep -f "neo dp")
    if [ ! -z "$PIDS" ]; then
        echo "强制终止残留进程..."
        kill -9 $PIDS
    fi
    echo "进程终止完成"
fi