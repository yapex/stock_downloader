#!/bin/bash

# 脚本功能：同时运行快速和慢速两个消费者进程，并启动监控器。

# 检查参数决定是否启用 debug 模式
DEBUG_MODE=false
if [[ "$1" == "--debug" ]]; then
    DEBUG_MODE=true
fi

echo "启动快速和慢速消费者..."
if [ "$DEBUG_MODE" = true ]; then
    echo "Debug 模式已启用"
fi

# 启动快速队列消费者
if [ "$DEBUG_MODE" = true ]; then
    { uv run neo dp fast --debug > logs/consumer_fast.log 2>&1; } &
else
    { uv run neo dp fast > logs/consumer_fast.log 2>&1; } &
fi
FAST_DP_PID=$!
echo "快速消费者已启动 (PID: $FAST_DP_PID)"

# 启动慢速队列消费者
if [ "$DEBUG_MODE" = true ]; then
    { uv run neo dp slow --debug > logs/consumer_slow.log 2>&1; } &
else
    { uv run neo dp slow > logs/consumer_slow.log 2>&1; } &
fi
SLOW_DP_PID=$!
echo "慢速消费者已启动 (PID: $SLOW_DP_PID)"


# 等待一段时间确保消费者已经启动
sleep 3

# 运行监控器
uv run scripts/huey_monitor.py

# 当监控器结束时，终止两个消费者进程
echo "正在停止消费者进程..."
kill $FAST_DP_PID 2>/dev/null
kill $SLOW_DP_PID 2>/dev/null

wait $FAST_DP_PID 2>/dev/null
wait $SLOW_DP_PID 2>/dev/null

echo "所有进程已结束"
