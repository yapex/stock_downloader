#!/bin/bash
# Stock Downloader 启动脚本
# 确保从项目根目录执行
cd "$(dirname "$0")"

# 使用 uv 在虚拟环境中执行快捷命令
# "$@" 会将所有传递给此脚本的参数原封不动地交给 stock-downloader
uv run dl "$@"
