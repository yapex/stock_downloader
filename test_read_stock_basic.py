#!/usr/bin/env python3
"""测试读取实际的 stock_basic 文件"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

def test_read_stock_basic():
    """测试读取实际的 stock_basic 文件"""
    
    file_path = Path("/Users/yapex/workspace/stock_downloader/data/parquet/stock_basic")
    
    print(f"文件路径: {file_path}")
    print(f"文件存在: {file_path.exists()}")
    print(f"是否是文件: {file_path.is_file()}")
    print(f"是否是目录: {file_path.is_dir()}")
    print(f"文件大小: {file_path.stat().st_size} bytes")
    
    try:
        # 尝试作为 parquet 文件读取
        print("\n=== 尝试使用 pandas 读取 ===")
        df = pd.read_parquet(file_path)
        print(f"成功读取 {len(df)} 行 x {len(df.columns)} 列数据")
        print("前5行数据:")
        print(df.head())
        print("\n列信息:")
        print(df.dtypes)
        
    except Exception as e:
        print(f"pandas 读取失败: {e}")
        
    try:
        # 尝试使用 pyarrow 读取
        print("\n=== 尝试使用 pyarrow 读取 ===")
        table = pq.read_table(file_path)
        print(f"成功读取 {len(table)} 行数据")
        print("Schema:")
        print(table.schema)
        
    except Exception as e:
        print(f"pyarrow 读取失败: {e}")

if __name__ == "__main__":
    test_read_stock_basic()
