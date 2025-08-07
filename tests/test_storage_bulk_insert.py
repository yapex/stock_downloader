"""测试DuckDB存储的批量插入和executemany优化功能。"""

import pytest
import pandas as pd
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from downloader.storage import DuckDBStorage


@pytest.fixture
def bulk_storage(tmp_path):
    """创建用于测试批量插入的存储实例。"""
    db_path = tmp_path / "bulk_test.db"
    return DuckDBStorage(db_path)


def test_bulk_insert_incremental(bulk_storage):
    """测试批量增量插入功能。"""
    # 准备测试数据
    df1 = pd.DataFrame({
        'trade_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'ts_code': ['000001.SZ'] * 3,
        'close': [100, 101, 102],
        'volume': [1000, 1100, 1200]
    })
    
    # 第一次批量插入
    bulk_storage.bulk_insert(df1, 'daily', '000001.SZ', 'trade_date')
    
    # 验证数据
    result = bulk_storage.query('daily', '000001.SZ')
    assert len(result) == 3
    assert result['close'].tolist() == [100, 101, 102]
    
    # 准备重叠数据（会覆盖前两条）
    df2 = pd.DataFrame({
        'trade_date': ['2023-01-02', '2023-01-03', '2023-01-04'],
        'ts_code': ['000001.SZ'] * 3,
        'close': [201, 202, 203],
        'volume': [2000, 2200, 2400]
    })
    
    # 第二次批量插入
    bulk_storage.bulk_insert(df2, 'daily', '000001.SZ', 'trade_date')
    
    # 验证数据更新
    result = bulk_storage.query('daily', '000001.SZ')
    assert len(result) == 4  # 应该有4条记录
    
    # 按日期排序验证
    result = result.sort_values('trade_date').reset_index(drop=True)
    expected_close = [100, 201, 202, 203]  # 01-01保持，01-02和01-03更新，01-04新增
    assert result['close'].tolist() == expected_close


def test_bulk_insert_full(bulk_storage):
    """测试批量全量插入功能。"""
    # 准备测试数据
    df1 = pd.DataFrame({
        'name': ['股票A', '股票B', '股票C'],
        'industry': ['金融', '科技', '制造'],
        'market': ['主板', '创业板', '中小板']
    })
    
    # 全量插入
    bulk_storage.bulk_insert(df1, 'system', 'stock_info')
    
    # 验证数据
    result = bulk_storage.query('system', 'stock_info')
    assert len(result) == 3
    assert result['name'].tolist() == ['股票A', '股票B', '股票C']
    
    # 准备新的全量数据
    df2 = pd.DataFrame({
        'name': ['股票D', '股票E'],
        'industry': ['医药', '消费'],
        'market': ['主板', '主板']
    })
    
    # 第二次全量插入
    bulk_storage.bulk_insert(df2, 'system', 'stock_info')
    
    # 验证数据完全替换
    result = bulk_storage.query('system', 'stock_info')
    assert len(result) == 2
    assert result['name'].tolist() == ['股票D', '股票E']


def test_bulk_insert_empty_data(bulk_storage):
    """测试空数据的批量插入。"""
    # 空DataFrame
    empty_df = pd.DataFrame()
    
    # 不应该报错，只是跳过
    bulk_storage.bulk_insert(empty_df, 'daily', '000001.SZ', 'trade_date')
    
    # 确保没有创建表
    assert not bulk_storage.table_exists('daily', '000001.SZ')


def test_bulk_insert_transaction_rollback(bulk_storage):
    """测试事务回滚机制。"""
    # 准备正常数据
    df_good = pd.DataFrame({
        'trade_date': ['2023-01-01', '2023-01-02'],
        'ts_code': ['000001.SZ'] * 2,
        'close': [100, 101],
        'volume': [1000, 1100]
    })
    
    # 先插入正常数据
    bulk_storage.bulk_insert(df_good, 'daily', '000001.SZ', 'trade_date')
    
    # 验证数据存在
    result = bulk_storage.query('daily', '000001.SZ')
    assert len(result) == 2
    
    # 准备有问题的数据（故意制造错误）
    df_bad = pd.DataFrame({
        'trade_date': ['2023-01-03', '2023-01-04'],
        'ts_code': ['000001.SZ'] * 2,
        'close': [102, 103],
        'volume': [1200, 1300],
        'invalid_col': [None, None]  # 这可能导致某些问题，但在DuckDB中通常不会
    })
    
    # 模拟事务失败的情况，我们通过暴露内部方法来测试
    try:
        # 手动制造一个失败场景
        bulk_storage._bulk_incremental_insert(df_bad, 'daily', '000001.SZ', 'non_existent_date_col')
    except Exception:
        # 预期会失败
        pass
    
    # 验证原始数据没有被破坏
    result = bulk_storage.query('daily', '000001.SZ')
    assert len(result) == 2  # 原始数据应该保持不变


def test_bulk_insert_multiple_batches(bulk_storage):
    """测试多批次批量插入功能。"""
    # 准备多个批次的数据
    data_batches = []
    
    # 批次1: 股票A的日线数据
    df1 = pd.DataFrame({
        'trade_date': ['2023-01-01', '2023-01-02'],
        'ts_code': ['000001.SZ'] * 2,
        'close': [100, 101],
        'volume': [1000, 1100]
    })
    data_batches.append((df1, 'daily', '000001.SZ', 'trade_date'))
    
    # 批次2: 股票B的日线数据
    df2 = pd.DataFrame({
        'trade_date': ['2023-01-01', '2023-01-02'],
        'ts_code': ['000002.SZ'] * 2,
        'close': [200, 201],
        'volume': [2000, 2100]
    })
    data_batches.append((df2, 'daily', '000002.SZ', 'trade_date'))
    
    # 批次3: 系统数据
    df3 = pd.DataFrame({
        'name': ['股票A', '股票B'],
        'industry': ['金融', '科技']
    })
    data_batches.append((df3, 'system', 'stock_info', None))
    
    # 执行多批次插入
    bulk_storage.bulk_insert_multiple(data_batches)
    
    # 验证每个批次的数据
    result1 = bulk_storage.query('daily', '000001.SZ')
    assert len(result1) == 2
    assert result1['close'].tolist() == [100, 101]
    
    result2 = bulk_storage.query('daily', '000002.SZ')
    assert len(result2) == 2
    assert result2['close'].tolist() == [200, 201]
    
    result3 = bulk_storage.query('system', 'stock_info')
    assert len(result3) == 2
    assert result3['name'].tolist() == ['股票A', '股票B']


def test_bulk_insert_concurrent_safety(bulk_storage):
    """测试并发批量插入的安全性。"""
    
    def concurrent_insert_task(stock_code, data_range, task_id):
        """并发插入任务。"""
        df = pd.DataFrame({
            'trade_date': [f'2023-{task_id:02d}-{i:02d}' for i in data_range],
            'ts_code': [stock_code] * len(data_range),
            'close': [100 + task_id * 10 + i for i in data_range],
            'volume': [1000 + task_id * 100 + i * 10 for i in data_range]
        })
        
        bulk_storage.bulk_insert(df, 'daily', stock_code, 'trade_date')
        return len(df)
    
    # 准备并发任务
    tasks = []
    stock_codes = ['000001.SZ', '000002.SZ', '000003.SZ']
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        
        for i, stock_code in enumerate(stock_codes):
            future = executor.submit(concurrent_insert_task, stock_code, range(1, 6), i + 1)
            futures.append((future, stock_code))
        
        # 等待所有任务完成
        for future, stock_code in futures:
            result = future.result()
            assert result == 5, f"股票 {stock_code} 应该插入5条记录"
    
    # 验证所有数据都正确插入
    for stock_code in stock_codes:
        result = bulk_storage.query('daily', stock_code)
        assert len(result) == 5, f"股票 {stock_code} 应该有5条记录"
        assert stock_code in result['ts_code'].unique()


def test_metadata_update_after_bulk_insert(bulk_storage):
    """测试批量插入后元数据的更新。"""
    # 准备测试数据
    df = pd.DataFrame({
        'trade_date': ['2023-01-01', '2023-01-02'],
        'ts_code': ['000001.SZ'] * 2,
        'close': [100, 101],
        'volume': [1000, 1100]
    })
    
    # 执行批量插入
    bulk_storage.bulk_insert(df, 'daily', '000001.SZ', 'trade_date')
    
    # 验证元数据已更新
    last_updated = bulk_storage.get_table_last_updated('daily', '000001.SZ')
    assert last_updated is not None, "元数据应该已更新"
    
    # 验证元数据的时间是最近的
    import datetime
    time_diff = datetime.datetime.now() - last_updated
    assert time_diff.total_seconds() < 10, "更新时间应该是最近的"


def test_bulk_insert_large_dataset(bulk_storage):
    """测试大数据集的批量插入性能。"""
    import time
    
    # 创建较大的数据集（10000条记录）
    size = 10000
    df = pd.DataFrame({
        'trade_date': [f'2023-{i%12+1:02d}-{i%28+1:02d}' for i in range(size)],
        'ts_code': ['000001.SZ'] * size,
        'close': [100 + i * 0.01 for i in range(size)],
        'volume': [1000 + i * 10 for i in range(size)],
        'high': [101 + i * 0.01 for i in range(size)],
        'low': [99 + i * 0.01 for i in range(size)],
        'open': [100 + i * 0.01 for i in range(size)]
    })
    
    # 测量批量插入时间
    start_time = time.time()
    bulk_storage.bulk_insert(df, 'daily', '000001.SZ', 'trade_date')
    insert_time = time.time() - start_time
    
    # 验证数据完整性
    result = bulk_storage.query('daily', '000001.SZ')
    assert len(result) > 0, "应该有数据被插入"
    
    # 批量插入应该相对较快（这里假设10秒内完成是合理的）
    assert insert_time < 10, f"批量插入 {size} 条记录花费时间过长: {insert_time:.2f}秒"
    
    print(f"批量插入 {size} 条记录耗时: {insert_time:.2f}秒")


def test_thread_local_connections_in_bulk_insert(bulk_storage):
    """测试批量插入中线程本地连接的正确性。"""
    connection_ids = {}
    
    def get_connection_id_during_insert(thread_id):
        """在批量插入过程中获取连接ID。"""
        df = pd.DataFrame({
            'trade_date': [f'2023-01-{thread_id:02d}'],
            'ts_code': [f'00000{thread_id}.SZ'],
            'close': [100 + thread_id],
            'volume': [1000 + thread_id * 100]
        })
        
        # 执行插入并记录连接ID
        bulk_storage.bulk_insert(df, 'daily', f'00000{thread_id}.SZ', 'trade_date')
        connection_ids[thread_id] = id(bulk_storage.conn)
    
    # 在多个线程中执行批量插入
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(get_connection_id_during_insert, i) for i in range(1, 4)]
        
        for future in futures:
            future.result()
    
    # 验证每个线程都使用了不同的连接
    unique_conn_ids = set(connection_ids.values())
    assert len(unique_conn_ids) == 3, "每个线程应该使用独立的连接"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
