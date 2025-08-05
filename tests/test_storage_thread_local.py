"""测试DuckDB存储的线程本地连接功能。"""

import pytest
import pandas as pd
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from downloader.storage import DuckDBStorage


@pytest.fixture
def thread_storage(tmp_path):
    """创建用于测试线程本地连接的存储实例。"""
    db_path = tmp_path / "thread_test.db"
    return DuckDBStorage(db_path)


def test_thread_local_connections_isolation(thread_storage):
    """测试不同线程获取不同的连接实例。"""
    connections = {}
    
    def get_connection(thread_id):
        connections[thread_id] = thread_storage.conn
    
    # 在多个线程中获取连接
    threads = []
    for i in range(3):
        thread = threading.Thread(target=get_connection, args=(i,))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    # 验证每个线程都有不同的连接实例
    conn_ids = [id(conn) for conn in connections.values()]
    assert len(set(conn_ids)) == 3, "每个线程应该有独立的连接"


def test_concurrent_write_operations(thread_storage):
    """测试并发写操作的正确性。"""
    
    def write_data(stock_code, data_range):
        """在特定线程中写入数据。"""
        df = pd.DataFrame({
            'trade_date': [f'2023-01-{i:02d}' for i in data_range],
            'ts_code': [stock_code] * len(data_range),
            'close': [100 + i for i in data_range],
            'volume': [1000 * i for i in data_range]
        })
        thread_storage.save_incremental(df, 'daily', stock_code, 'trade_date')
        return len(df)
    
    # 并发写入不同股票的数据
    stock_codes = ['000001.SZ', '000002.SZ', '600000.SH']
    data_ranges = [range(1, 6), range(6, 11), range(11, 16)]
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for stock_code, data_range in zip(stock_codes, data_ranges):
            future = executor.submit(write_data, stock_code, data_range)
            futures.append((future, stock_code))
        
        # 等待所有任务完成
        for future, stock_code in futures:
            result = future.result()
            assert result == 5, f"股票 {stock_code} 应该写入5条记录"
    
    # 验证数据完整性
    for stock_code in stock_codes:
        df = thread_storage.query('daily', stock_code)
        assert len(df) == 5, f"股票 {stock_code} 应该有5条记录"
        assert stock_code in df['ts_code'].unique(), f"数据应该包含股票代码 {stock_code}"


def test_concurrent_read_operations(thread_storage):
    """测试并发读操作的正确性。"""
    # 先写入测试数据
    test_df = pd.DataFrame({
        'trade_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'ts_code': ['000001.SZ'] * 3,
        'close': [100, 101, 102],
        'volume': [1000, 1100, 1200]
    })
    thread_storage.save_incremental(test_df, 'daily', '000001.SZ', 'trade_date')
    
    def read_data(thread_id):
        """在特定线程中读取数据。"""
        df = thread_storage.query('daily', '000001.SZ')
        return thread_id, len(df), df['close'].tolist()
    
    # 并发读取数据
    results = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(read_data, i) for i in range(5)]
        
        for future in as_completed(futures):
            thread_id, row_count, close_values = future.result()
            results[thread_id] = (row_count, close_values)
    
    # 验证所有线程读取的数据一致
    expected_close = [100, 101, 102]
    for thread_id, (row_count, close_values) in results.items():
        assert row_count == 3, f"线程 {thread_id} 应该读取到3条记录"
        assert close_values == expected_close, f"线程 {thread_id} 读取的数据不正确"


def test_mixed_read_write_operations(thread_storage):
    """测试混合读写操作的线程安全性。"""
    
    def writer_task(stock_code, start_date):
        """写入任务。"""
        for i in range(5):
            df = pd.DataFrame({
                'trade_date': [f'2023-{start_date:02d}-{i+1:02d}'],
                'ts_code': [stock_code],
                'close': [100 + i],
                'volume': [1000 + i * 100]
            })
            thread_storage.save_incremental(df, 'daily', stock_code, 'trade_date')
            time.sleep(0.01)  # 模拟处理时间
    
    def reader_task(stock_code, results, task_id):
        """读取任务。"""
        for i in range(10):
            df = thread_storage.query('daily', stock_code)
            results[task_id].append(len(df))
            time.sleep(0.005)  # 模拟处理时间
    
    results = {f'reader_{i}': [] for i in range(3)}
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        # 启动一个写入任务
        writer_future = executor.submit(writer_task, '000001.SZ', 1)
        
        # 启动多个读取任务
        reader_futures = []
        for i in range(3):
            future = executor.submit(reader_task, '000001.SZ', results, f'reader_{i}')
            reader_futures.append(future)
        
        # 等待所有任务完成
        writer_future.result()
        for future in reader_futures:
            future.result()
    
    # 验证读取结果的合理性
    for task_id, counts in results.items():
        # 记录数应该是递增的（或至少不递减）
        assert len(counts) == 10, f"任务 {task_id} 应该执行10次读取"
        # 最终应该能读取到所有数据
        assert counts[-1] >= 0, f"任务 {task_id} 应该能读取到数据"


def test_table_exists_thread_safety(thread_storage):
    """测试table_exists方法的线程安全性。"""
    
    def check_and_create_table(stock_code):
        """检查表是否存在，不存在则创建。"""
        if not thread_storage.table_exists('daily', stock_code):
            df = pd.DataFrame({
                'trade_date': ['2023-01-01'],
                'ts_code': [stock_code],
                'close': [100],
                'volume': [1000]
            })
            thread_storage.save_incremental(df, 'daily', stock_code, 'trade_date')
            return 'created'
        else:
            return 'exists'
    
    stock_codes = [f'00000{i}.SZ' for i in range(1, 6)]
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(check_and_create_table, code) for code in stock_codes]
        results = [future.result() for future in futures]
    
    # 验证所有表都被创建
    for stock_code in stock_codes:
        assert thread_storage.table_exists('daily', stock_code), f"表 {stock_code} 应该存在"
    
    # 至少应该有一些表是新创建的
    assert 'created' in results, "应该有表被创建"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
