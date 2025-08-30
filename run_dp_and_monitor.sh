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

# 定义一个清理函数，用于终止后台运行的消费者进程
cleanup() {
    echo "捕获到退出信号，正在停止所有消费者进程..."
    # 使用 kill 终止进程，2>/dev/null 会抑制错误（例如进程已不存在）
    if [ -n "$FAST_DP_PID" ]; then kill $FAST_DP_PID 2>/dev/null; fi
    if [ -n "$SLOW_DP_PID" ]; then kill $SLOW_DP_PID 2>/dev/null; fi
    if [ -n "$MAINT_DP_PID" ]; then kill $MAINT_DP_PID 2>/dev/null; fi
    
    # 等待所有后台进程结束
    wait $FAST_DP_PID 2>/dev/null
    wait $SLOW_DP_PID 2>/dev/null
    wait $MAINT_DP_PID 2>/dev/null
    
    echo "所有后台进程已清理完毕。"
}

# 设置 trap，在脚本退出时（EXIT）、被中断时（INT）或被终止时（TERM）调用 cleanup 函数
# 这能确保即使用户通过 Ctrl+C 退出，后台进程也能被正确关闭
trap cleanup EXIT INT TERM


# --- 新增步骤：预初始化Huey数据库 ---
echo "预初始化Huey数据库..."
uv run python -m scripts.init_huey_db
if [ $? -ne 0 ]; then
    echo "数据库初始化失败，退出。"
    exit 1
fi
echo "Huey数据库预初始化完成。"
# ------------------------------------

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

# 运行监控器，脚本会在这里阻塞，直到监控器进程结束或被中断
uv run python -m scripts.huey_monitor

# 当脚本退出时，之前设置的 trap 会自动执行 cleanup 函数
echo "监控器已退出。"