#!/bin/bash

# 脚本功能：同时运行 dp --debug 和 huey_monitor.py

# 检查参数决定是否启用 debug 模式
DEBUG_MODE=false
if [[ "$1" == "--debug" ]]; then
    DEBUG_MODE=true
fi

echo "启动 dp 和监控器..."
if [ "$DEBUG_MODE" = true ]; then
    echo "Debug 模式已启用"
fi

# 使用后台运行方式启动 dp
if [ "$DEBUG_MODE" = true ]; then
    { uv run neo dp --debug > /dev/null 2>&1; } &
else
    { uv run neo dp > /dev/null 2>&1; } &
fi

DP_PID=$!

# 等待一段时间确保 dp 已经启动
sleep 3

# 运行监控器
uv run scripts/huey_monitor.py

# 当监控器结束时，终止 dp 进程
kill $DP_PID 2>/dev/null

echo "所有进程已结束"