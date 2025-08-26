#!/bin/bash

# 脚本功能：同时运行快速、慢速和维护三个消费者进程，并启动监控器。

# 检查参数决定是否启用 debug 模式
DEBUG_MODE=false
if [[ "$1" == "--debug" ]]; then
    DEBUG_MODE=true
fi

echo "启动所有消费者 (fast, slow, maint)..."
if [ "$DEBUG_MODE" = true ]; then
    echo "Debug 模式已启用"
    DEBUG_FLAG="--debug"
else
    DEBUG_FLAG=""
fi

# 启动快速队列消费者
{ uv run python -m neo.main dp fast $DEBUG_FLAG > logs/consumer_fast.log 2>&1; } &
FAST_DP_PID=$!
echo "快速消费者已启动 (PID: $FAST_DP_PID)"

# 启动慢速队列消费者
{ uv run python -m neo.main dp slow $DEBUG_FLAG > logs/consumer_slow.log 2>&1; } &
SLOW_DP_PID=$!
echo "慢速消费者已启动 (PID: $SLOW_DP_PID)"

# 启动维护队列消费者 (用于自动同步元数据)
{ uv run python -m neo.main dp maint $DEBUG_FLAG > logs/consumer_maint.log 2>&1; } &
MAINT_DP_PID=$!
echo "维护消费者已启动 (PID: $MAINT_DP_PID)"


# 等待一段时间确保消费者已经启动
sleep 3

# 运行监控器
uv run python -m scripts.huey_monitor

# 当监控器结束时，终止所有消费者进程
echo "正在停止消费者进程..."
kill $FAST_DP_PID 2>/dev/null
kill $SLOW_DP_PID 2>/dev/null
kill $MAINT_DP_PID 2>/dev/null

wait $FAST_DP_PID 2>/dev/null
wait $SLOW_DP_PID 2>/dev/null
wait $MAINT_DP_PID 2>/dev/null

echo "所有进程已结束"
