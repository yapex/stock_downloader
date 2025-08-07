#!/usr/bin/env python3
"""
测试缓冲池功能的简单脚本
"""

import sys
import os
import pandas as pd
import time
from datetime import datetime

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from downloader.storage import DuckDBStorage
from downloader.buffer_pool import DataBufferPool

def test_buffer_pool():
    """测试缓冲池基本功能"""
    print("开始测试缓冲池功能...")
    
    # 创建临时数据库
    test_db_path = "/tmp/test_buffer_pool.db"
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
    
    storage = DuckDBStorage(test_db_path)
    
    # 创建缓冲池（较小的配置用于测试）
    buffer_pool = DataBufferPool(
        storage=storage,
        max_buffer_size=5,  # 小缓冲区，容易触发刷新
        flush_interval_seconds=3,   # 3秒自动刷新
        max_buffer_memory_mb=10
    )
    
    print(f"缓冲池创建完成，配置: max_buffer_size=5, flush_interval_seconds=3s")
    
    # 创建测试数据
    test_data = [
        {
            'ts_code': '600519.SH',
            'data': pd.DataFrame({
                'ts_code': ['600519.SH'] * 3,
                'trade_date': ['20240101', '20240102', '20240103'],
                'close': [100.0, 101.0, 102.0],
                'volume': [1000, 1100, 1200]
            }),
            'data_type': 'daily_qfq',
            'date_col': 'trade_date',
            'task_name': '日K线-前复权'
        },
        {
            'ts_code': '000001.SZ',
            'data': pd.DataFrame({
                'ts_code': ['000001.SZ'] * 2,
                'trade_date': ['20240101', '20240102'],
                'close': [10.0, 10.5],
                'volume': [2000, 2100]
            }),
            'data_type': 'daily_qfq',
            'date_col': 'trade_date',
            'task_name': '日K线-前复权'
        }
    ]
    
    # 测试添加数据
    print("\n添加测试数据到缓冲池...")
    for i, item in enumerate(test_data):
        print(f"添加数据 {i+1}: {item['ts_code']}")
        buffer_pool.add_data(
            item['data'], 
            item['data_type'], 
            item['ts_code'], 
            item['date_col'], 
            item['task_name']
        )
        
        # 显示当前统计
        stats = buffer_pool.get_stats()
        print(f"  当前缓冲区大小: {stats['current_buffer_size']}")
        print(f"  内存使用: {stats['current_buffer_memory_mb']:.2f} MB")
        
        time.sleep(1)
    
    # 等待自动刷新
    print("\n等待自动刷新...")
    time.sleep(5)
    
    # 添加更多数据测试手动刷新
    print("\n添加更多数据...")
    for i in range(3):
        test_df = pd.DataFrame({
            'ts_code': [f'00000{i}.SZ'] * 2,
            'trade_date': ['20240104', '20240105'],
            'close': [20.0 + i, 21.0 + i],
            'volume': [3000 + i*100, 3100 + i*100]
        })
        
        buffer_pool.add_data(
            test_df, 
            'daily_qfq', 
            f'00000{i}.SZ', 
            'trade_date', 
            '日K线-前复权'
        )
    
    # 手动刷新
    print("\n执行手动刷新...")
    buffer_pool.flush_all()
    
    # 最终统计
    final_stats = buffer_pool.get_stats()
    print("\n最终统计信息:")
    print(f"  总缓冲: {final_stats['total_buffered']}")
    print(f"  总刷新: {final_stats['total_flushed']}")
    print(f"  当前缓冲: {final_stats['current_buffer_size']}")
    print(f"  刷新次数: {final_stats['flush_count']}")
    print(f"  最后刷新时间: {final_stats['last_flush_time']}")
    print(f"  内存使用: {final_stats['current_buffer_memory_mb']:.2f} MB")
    
    # 停止缓冲池
    buffer_pool.shutdown()
    
    # 验证数据是否正确写入数据库
    print("\n验证数据库中的数据...")
    try:
        # 查询数据 - 使用正确的方法签名
        result = storage.query("SELECT COUNT(*) as count FROM daily_qfq_600519_SH", "600519.SH")
        if not result.empty:
            print(f"600519.SH 数据条数: {result.iloc[0]['count']}")
        
        result = storage.query("SELECT COUNT(*) as count FROM daily_qfq_000001_SZ", "000001.SZ")
        if not result.empty:
            print(f"000001.SZ 数据条数: {result.iloc[0]['count']}")
            
    except Exception as e:
        print(f"查询数据时出错: {e}")
        # 尝试直接查询表是否存在
        try:
            conn = storage._get_connection()
            tables = conn.execute("SHOW TABLES").fetchall()
            print(f"数据库中的表: {[table[0] for table in tables]}")
        except Exception as e2:
            print(f"查询表列表时出错: {e2}")
    
    print("\n缓冲池测试完成!")
    
    # 清理测试文件
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
        print("测试数据库文件已清理")

if __name__ == "__main__":
    test_buffer_pool()