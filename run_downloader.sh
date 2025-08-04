#!/bin/bash

source ~/.zshrc

# ==========================================================
# Stock Downloader 启动脚本 for cron
# ==========================================================

# 设置脚本的根目录为脚本文件所在的目录
# 这确保了无论从哪里调用此脚本，路径都是正确的
cd "$(dirname "$0")"

# 定义你的虚拟环境的Python解释器路径
# 【关键】请务必修改为你的实际路径
VENV_PYTHON="./.venv/bin/python"

# 定义你的主程序脚本路径
MAIN_SCRIPT="./main.py"

# 获取当前日期和时间，用于日志文件名
LOG_DATE=$(date +"%Y-%m-%d")
CRON_LOG_FILE="./cron_run_${LOG_DATE}.log"

echo "=============================================" >> ${CRON_LOG_FILE}
echo "Cron job started at: $(date)" >> ${CRON_LOG_FILE}

# 检查虚拟环境是否存在
if [ ! -f "$VENV_PYTHON" ]; then
    echo "错误：找不到虚拟环境的Python解释器: $VENV_PYTHON" >> ${CRON_LOG_FILE}
    exit 1
fi

# 执行Python主程序，并将所有输出（标准输出和标准错误）追加到日志文件中
# 我们在这里可以添加 --force 参数，如果希望定时任务总是强制刷新
# 或者不加，让程序自己判断是否需要更新
$VENV_PYTHON $MAIN_SCRIPT >> ${CRON_LOG_FILE} 2>&1

echo "Cron job finished at: $(date)" >> ${CRON_LOG_FILE}
echo "=============================================" >> ${CRON_LOG_FILE}