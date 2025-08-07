#!/usr/bin/env python3
"""
批量写入性能测试

比较普通插入和批量插入的性能差异。
"""

import sys
import time
import pandas as pd
import tempfile
from pathlib import Path

# 添加src到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from downloader.storage import DuckDBStorage


def generate_test_data(num_stocks: int = 10, days_per_stock: int = 1000) -> list[tuple[pd.DataFrame, str, str, str]]:
    """生成测试数据。"""
    data_batches = []
    
    for i in range(num_stocks):
        stock_code = f'00000{i:02d}.SZ'
        
        # 生成该股票的历史数据
        dates = pd.date_range(start='2020-01-01', periods=days_per_stock, freq='D')
        df = pd.DataFrame({
            'trade_date': dates.strftime('%Y%m%d'),
            'ts_code': [stock_code] * days_per_stock,
            'close': 10.0 + i + pd.Series(range(days_per_stock)) * 0.01,
            'volume': 1000 + i * 100 + pd.Series(range(days_per_stock)) * 10,
            'high': 10.5 + i + pd.Series(range(days_per_stock)) * 0.01,
            'low': 9.5 + i + pd.Series(range(days_per_stock)) * 0.01,
            'open': 10.0 + i + pd.Series(range(days_per_stock)) * 0.01,
        })
        
        data_batches.append((df, 'daily', stock_code, 'trade_date'))
    
    return data_batches


def test_individual_inserts(storage: DuckDBStorage, data_batches: list):
    """测试逐个插入性能。"""
    print("测试逐个插入...")
    start_time = time.time()
    
    for df, data_type, entity_id, date_col in data_batches:
        storage.save_incremental(df, data_type, entity_id, date_col)
    
    end_time = time.time()
    duration = end_time - start_time
    
    total_records = sum(len(df) for df, _, _, _ in data_batches)
    print(f"逐个插入: {duration:.2f}秒, {total_records}条记录, {total_records/duration:.0f}条/秒")
    
    return duration


def test_bulk_inserts(storage: DuckDBStorage, data_batches: list):
    """测试单个批量插入性能。"""
    print("测试单个批量插入...")
    start_time = time.time()
    
    for df, data_type, entity_id, date_col in data_batches:
        storage.bulk_insert(df, data_type, entity_id, date_col)
    
    end_time = time.time()
    duration = end_time - start_time
    
    total_records = sum(len(df) for df, _, _, _ in data_batches)
    print(f"单个批量插入: {duration:.2f}秒, {total_records}条记录, {total_records/duration:.0f}条/秒")
    
    return duration


def test_multiple_bulk_inserts(storage: DuckDBStorage, data_batches: list):
    """测试多批次批量插入性能。"""
    print("测试多批次批量插入...")
    start_time = time.time()
    
    storage.bulk_insert_multiple(data_batches)
    
    end_time = time.time()
    duration = end_time - start_time
    
    total_records = sum(len(df) for df, _, _, _ in data_batches)
    print(f"多批次批量插入: {duration:.2f}秒, {total_records}条记录, {total_records/duration:.0f}条/秒")
    
    return duration


def test_thread_safety():
    """测试线程安全性。"""
    import threading
    from concurrent.futures import ThreadPoolExecutor
    
    print("\n测试线程安全性...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "thread_test.db"
        storage = DuckDBStorage(db_path)
        
        def worker_task(worker_id: int):
            """工作线程任务。"""
            stock_code = f'{600000 + worker_id}.SH'  # 使用正确的6位股票代码格式
            df = pd.DataFrame({
                'trade_date': [f'2023-01-{i:02d}' for i in range(1, 101)],
                'ts_code': [stock_code] * 100,
                'close': [100 + worker_id + i * 0.01 for i in range(100)],
                'volume': [1000 + worker_id * 100 + i * 10 for i in range(100)]
            })
            
            storage.bulk_insert(df, 'daily', stock_code, 'trade_date')
            return len(df)
        
        # 并发执行
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(worker_task, i) for i in range(10)]
            results = [future.result() for future in futures]
        
        duration = time.time() - start_time
        total_records = sum(results)
        
        print(f"并发批量插入: {duration:.2f}秒, {total_records}条记录, {total_records/duration:.0f}条/秒")
        
        # 验证数据完整性
        print("验证线程安全性...")
        for i in range(10):
            stock_code = f'{600000 + i}.SH'
            df = storage.query('daily', stock_code)
            assert len(df) == 100, f"股票 {stock_code} 应该有1100条记录，实际有{len(df)}条"
        
        print("线程安全性验证通过！")


def main():
    print("批量写入性能测试")
    print("=" * 50)
    
    # 生成测试数据
    print("生成测试数据...")
    data_batches = generate_test_data(num_stocks=10, days_per_stock=500)
    total_records = sum(len(df) for df, _, _, _ in data_batches)
    print(f"生成了 {len(data_batches)} 个批次，共 {total_records} 条记录")
    
    # 测试1：逐个插入
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path1 = Path(temp_dir) / "individual.db"
        storage1 = DuckDBStorage(db_path1)
        duration1 = test_individual_inserts(storage1, data_batches)
    
    # 测试2：单个批量插入
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path2 = Path(temp_dir) / "bulk.db"
        storage2 = DuckDBStorage(db_path2)
        duration2 = test_bulk_inserts(storage2, data_batches)
    
    # 测试3：多批次批量插入
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path3 = Path(temp_dir) / "multiple_bulk.db"
        storage3 = DuckDBStorage(db_path3)
        duration3 = test_multiple_bulk_inserts(storage3, data_batches)
    
    # 性能比较
    print("\n性能比较:")
    print("-" * 30)
    print(f"逐个插入:         {duration1:.2f}秒 (基准)")
    print(f"单个批量插入:     {duration2:.2f}秒 (提升 {duration1/duration2:.1f}x)")
    print(f"多批次批量插入:   {duration3:.2f}秒 (提升 {duration1/duration3:.1f}x)")
    
    # 测试线程安全性
    test_thread_safety()
    
    print("\n测试完成！")


if __name__ == "__main__":
    main()
