#!/bin/bash

# 测试100个随机股票代码的 income 数据下载
# 使用之前生成的100个测试股票代码

echo "🚀 开始提交100个股票的 income 数据下载任务..."
echo "📊 任务组: income (财务报表)"
echo "🔧 模式: 全量替换，按 ts_code 分区"

uv run python -c "
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
import duckdb
import random

# 从stock_basic表随机选择100个股票 (使用固定种子确保可重复)
with duckdb.connect('data/metadata.db') as conn:
    result = conn.execute('''
        SELECT ts_code 
        FROM stock_basic 
        WHERE ts_code IS NOT NULL 
        AND ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ'
        ORDER BY ts_code
    ''').fetchall()
    
    all_stocks = [row[0] for row in result]
    random.seed(42)  # 固定种子
    test_stocks = random.sample(all_stocks, min(100, len(all_stocks)))
    
    # 输出为shell参数格式
    stock_args = ' '.join([f'-s {stock}' for stock in test_stocks])
    print(stock_args)
" > temp_stock_args.txt

# 读取生成的股票参数
STOCK_ARGS=$(cat temp_stock_args.txt)

echo "📋 股票代码参数已准备完成"
echo "⚡ 提交任务到队列..."

# 执行下载命令
uv run python -m neo.main dl -g financial $STOCK_ARGS

# 清理临时文件
rm temp_stock_args.txt

echo ""
echo "✅ 任务提交完成！"
echo "📝 请等待消费者处理任务"
echo "📁 完成后检查 data/parquet/income/ 目录下的分区结构"
echo "📊 期望的目录结构: data/parquet/income/year=YYYY/*.parquet"
