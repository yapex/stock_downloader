#!/bin/bash

# 清理环境脚本 - 依次运行数据库初始化、数据摘要、系统任务和数据摘要

echo "🚀 开始执行清理环境脚本..."

echo "📋 步骤 0: 杀掉相关进程"

# 查找并杀死所有与neo.main dp相关的Python进程
# 注意：这里假设uv run python -m neo.main dp fast等命令会在进程名中包含'neo.main dp'
# 根据你的实际进程名，可能需要调整pgrep的模式
PIDS=$(pgrep -f "uv run python -m neo.main dp")
if [ -n "$PIDS" ]; then
    echo "找到以下与neo.main dp相关的进程: $PIDS"
    kill -9 $PIDS
    echo "已杀死这些进程。"
else
    echo "未找到与neo.main dp相关的进程。"
fi

# 查找并杀死所有与scripts.huey_monitor相关的Python进程
PIDS_MONITOR=$(pgrep -f "uv run python -m scripts.huey_monitor")
if [ -n "$PIDS_MONITOR" ]; then
    echo "找到以下与scripts.huey_monitor相关的进程: $PIDS_MONITOR"
    kill -9 $PIDS_MONITOR
    echo "已杀死这些进程。"
else
    echo "未找到与scripts.huey_monitor相关的进程。"
fi

# 确保所有相关进程在清理前确实停止
# 给一些时间让进程终止，如果进程是僵尸进程，kill -9会立即终止
sleep 2

echo "📋 步骤 1: 清空日志文件..."
truncate -s 0 logs/*.log

echo "📋 步骤 2: 清空任务数据库..."
rm -f data/tasks_*

echo "✅ 清理环境脚本执行完成！"