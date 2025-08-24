#!/bin/bash

# 清理环境脚本 - 依次运行数据库初始化、数据摘要、系统任务和数据摘要

echo "🚀 开始执行清理环境脚本..."

echo "📋 步骤 0: 删除日志..."
rm -rf logs/*

echo "📋 步骤 1: 创建数据库表..."
uv run scripts/create_tables.py
if [ $? -ne 0 ]; then
    echo "❌ 创建数据库表失败"
    exit 1
fi

echo "📊 步骤 2: 显示数据摘要 (初始状态)..."
uv run scripts/show_data_summary.py
if [ $? -ne 0 ]; then
    echo "❌ 显示数据摘要失败"
    exit 1
fi

echo "⚙️ 步骤 3: 运行系统任务..."
uv run neo -g sys
if [ $? -ne 0 ]; then
    echo "❌ 运行系统任务失败"
    exit 1
fi

echo "📊 步骤 4: 显示数据摘要 (最终状态)..."
uv run scripts/show_data_summary.py
if [ $? -ne 0 ]; then
    echo "❌ 显示数据摘要失败"
    exit 1
fi

echo "✅ 清理环境脚本执行完成！"