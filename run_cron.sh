#!/bin/bash
# ==========================================================
# Stock Downloader Cron Job Script
# ==========================================================

# 切换到脚本所在的目录，以确保路径正确
cd "$(dirname "$0")"

# 定义日志文件
LOG_FILE="cron_run.log"

# --- 执行命令 ---
# 使用 uv 在虚拟环境中执行快捷命令 'dl'
# 'all': 指定下载全市场股票
# '--force': 强制执行，忽略冷却期，确保定时任务总是运行
# '>> ${LOG_FILE} 2>&1': 将所有输出（标准和错误）追加到日志文件
echo "Cron job started at: $(date)" >> ${LOG_FILE}
uv run dl all >> ${LOG_FILE} 2>&1
echo "Cron job finished at: $(date)" >> ${LOG_FILE}
echo "=============================================" >> ${LOG_FILE}
