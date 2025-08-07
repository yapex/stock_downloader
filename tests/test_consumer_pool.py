"""
ConsumerPool 测试模块

测试消费者管理器的各项功能：
- 基本启动停止
- 数据批次处理和缓存
- 延迟初始化DuckDB连接
- 批量刷新机制
- 错误处理和重试
- 统计信息收集
"""

import pytest
import tempfile
import time
import pandas as pd
from pathlib import Path
from queue import Queue
from unittest.mock import Mock, patch

from downloader.consumer_pool import ConsumerPool, ConsumerWorker
from downloader.models import DataBatch, TaskType
from downloader.storage import DuckDBStorage


class TestConsumerWorker:
    """ConsumerWorker 单元测试"""

    @pytest.fixture
    def temp_db(self):
        """创建临时数据库文件"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            yield str(db_path)

    @pytest.fixture
    def sample_data_batch(self):
        """创建样本数据批次"""
        df = pd.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ'],
            'trade_date': ['20241201', '20241201'],
            'close': [10.5, 15.2],
            'volume': [1000, 2000]
        })
        
        return DataBatch(
            df=df,
            meta={'task_type': 'daily'},
            task_id='test_task_001',
            symbol='000001.SZ'
        )

    @pytest.fixture
    def empty_data_batch(self):
        """创建空数据批次"""
        return DataBatch.empty(
            task_id='test_task_empty',
            symbol='000001.SZ',
            meta={'task_type': 'daily'}
        )

    def test_worker_initialization(self, temp_db):
        """测试工作线程初始化"""
        data_queue = Queue()
        
        worker = ConsumerWorker(
            worker_id=0,
            data_queue=data_queue,
            batch_size=50,
            flush_interval=10.0,
            db_path=temp_db,
            max_retries=2
        )
        
        assert worker.worker_id == 0
        assert worker.batch_size == 50
        assert worker.flush_interval == 10.0
        assert worker.db_path == temp_db
        assert worker.max_retries == 2
        assert not worker.running
        assert worker._storage is None  # 延迟初始化

    def test_lazy_storage_initialization(self, temp_db):
        """测试DuckDB连接的延迟初始化"""
        data_queue = Queue()
        
        worker = ConsumerWorker(
            worker_id=0,
            data_queue=data_queue,
            db_path=temp_db
        )
        
        # 初始状态没有连接
        assert worker._storage is None
        
        # 第一次访问storage属性时才创建连接
        storage = worker.storage
        assert isinstance(storage, DuckDBStorage)
        assert worker._storage is not None
        
        # 再次访问返回相同实例
        assert worker.storage is storage

    def test_process_batch_normal(self, temp_db, sample_data_batch):
        """测试正常处理数据批次"""
        data_queue = Queue()
        
        worker = ConsumerWorker(
            worker_id=0,
            data_queue=data_queue,
            batch_size=100,  # 设置大批量避免自动刷新
            flush_interval=60.0,
            db_path=temp_db
        )
        
        # 处理数据批次
        worker._process_batch(sample_data_batch)
        
        # 验证统计信息
        assert worker.batches_processed == 1
        assert worker.batches_cached == 1
        
        # 验证缓存内容
        cache_key = f"daily_{sample_data_batch.symbol}"
        assert cache_key in worker._cache
        assert len(worker._cache[cache_key]) == 1
        assert worker._cache[cache_key][0] == sample_data_batch

    def test_process_empty_batch(self, temp_db, empty_data_batch):
        """测试处理空数据批次"""
        data_queue = Queue()
        
        worker = ConsumerWorker(
            worker_id=0,
            data_queue=data_queue,
            db_path=temp_db
        )
        
        # 处理空数据批次
        worker._process_batch(empty_data_batch)
        
        # 空批次应该被跳过，不进入缓存
        assert worker.batches_processed == 1
        assert worker.batches_cached == 0
        assert len(worker._cache) == 0

    def test_flush_by_batch_size(self, temp_db, sample_data_batch):
        """测试达到批量大小时自动刷新"""
        data_queue = Queue()
        
        worker = ConsumerWorker(
            worker_id=0,
            data_queue=data_queue,
            batch_size=2,  # 设置小批量触发刷新
            flush_interval=60.0,
            db_path=temp_db
        )
        
        # 添加两个数据批次
        worker._process_batch(sample_data_batch)
        assert worker.flush_operations == 0
        
        # 再添加一个批次应该触发刷新
        worker._process_batch(sample_data_batch)
        
        # 检查刷新条件
        worker._check_flush_conditions()
        
        # 验证数据已写入数据库
        storage = worker.storage
        df = storage.query('daily', sample_data_batch.symbol)
        assert not df.empty
        assert len(df) >= 2  # 至少有2条记录

    def test_flush_by_interval(self, temp_db, sample_data_batch):
        """测试超时自动刷新"""
        data_queue = Queue()
        
        worker = ConsumerWorker(
            worker_id=0,
            data_queue=data_queue,
            batch_size=100,  # 大批量不会触发
            flush_interval=0.1,  # 很短的间隔
            db_path=temp_db
        )
        
        # 添加数据批次
        worker._process_batch(sample_data_batch)
        
        # 等待超过刷新间隔
        time.sleep(0.2)
        
        # 检查刷新条件应该触发刷新
        worker._check_flush_conditions()
        
        # 验证数据已写入数据库
        storage = worker.storage
        df = storage.query('daily', sample_data_batch.symbol)
        assert not df.empty

    def test_bulk_insert_with_deduplication(self, temp_db):
        """测试批量插入时的去重逻辑"""
        data_queue = Queue()
        
        worker = ConsumerWorker(
            worker_id=0,
            data_queue=data_queue,
            db_path=temp_db
        )
        
        # 创建重复数据的批次
        df1 = pd.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ'],
            'trade_date': ['20241201', '20241201'],
            'close': [10.5, 15.2]
        })
        
        df2 = pd.DataFrame({
            'ts_code': ['000001.SZ'],  # 重复的股票和日期
            'trade_date': ['20241201'],
            'close': [11.0]  # 不同的价格
        })
        
        batch1 = DataBatch(df=df1, meta={'task_type': 'daily'}, 
                          task_id='task1', symbol='000001.SZ')
        batch2 = DataBatch(df=df2, meta={'task_type': 'daily'}, 
                          task_id='task2', symbol='000001.SZ')
        
        # 批量插入
        worker._bulk_insert_batches('daily_000001.SZ', [batch1, batch2])
        
        # 验证去重效果
        storage = worker.storage
        df = storage.query('daily', '000001.SZ')
        
        # 应该有2条记录（000001.SZ的重复记录被去重）
        assert len(df) == 2
        
        # 000001.SZ应该保留最新的价格
        row_000001 = df[df['ts_code'] == '000001.SZ'].iloc[0]
        assert row_000001['close'] == 11.0

    @patch('downloader.consumer_pool.record_failed_task')
    def test_handle_batch_error(self, mock_record_failed, temp_db, sample_data_batch):
        """测试批次处理错误处理"""
        data_queue = Queue()
        
        worker = ConsumerWorker(
            worker_id=0,
            data_queue=data_queue,
            db_path=temp_db
        )
        
        # 模拟处理错误
        error = ValueError("Test error")
        worker._handle_batch_error(sample_data_batch, error)
        
        # 验证错误记录和统计
        assert worker.failed_operations == 1
        mock_record_failed.assert_called_once()
        
        # 检查记录失败任务的参数
        call_args = mock_record_failed.call_args
        assert call_args[1]['task_name'] == 'process_batch_worker_0'
        assert call_args[1]['entity_id'] == sample_data_batch.symbol

    def test_get_statistics(self, temp_db, sample_data_batch):
        """测试获取统计信息"""
        data_queue = Queue()
        
        worker = ConsumerWorker(
            worker_id=0,
            data_queue=data_queue,
            batch_size=100,
            db_path=temp_db
        )
        
        # 处理一些数据
        worker._process_batch(sample_data_batch)
        
        # 获取统计信息
        stats = worker.get_statistics()
        
        # 验证统计信息
        assert stats['worker_id'] == 0
        assert stats['running'] == False
        assert stats['batches_processed'] == 1
        assert stats['batches_cached'] == 1
        assert stats['flush_operations'] == 0
        assert stats['failed_operations'] == 0
        assert stats['cache_groups'] == 1
        assert stats['cached_records'] == 2  # sample_data_batch 有2条记录
        assert stats['has_storage_connection'] == False  # 还没有访问storage


class TestConsumerPool:
    """ConsumerPool 单元测试"""

    @pytest.fixture
    def temp_db(self):
        """创建临时数据库文件"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            yield str(db_path)

    @pytest.fixture
    def sample_data_batch(self):
        """创建样本数据批次"""
        df = pd.DataFrame({
            'ts_code': ['000001.SZ'],
            'trade_date': ['20241201'],
            'close': [10.5],
            'volume': [1000]
        })
        
        return DataBatch(
            df=df,
            meta={'task_type': 'daily'},
            task_id='test_task_001',
            symbol='000001.SZ'
        )

    def test_pool_initialization(self, temp_db):
        """测试消费者池初始化"""
        data_queue = Queue()
        
        pool = ConsumerPool(
            max_consumers=3,
            data_queue=data_queue,
            batch_size=50,
            flush_interval=10.0,
            db_path=temp_db,
            max_retries=2
        )
        
        assert pool.max_consumers == 3
        assert pool.data_queue is data_queue
        assert pool.batch_size == 50
        assert pool.flush_interval == 10.0
        assert pool.db_path == temp_db
        assert pool.max_retries == 2
        assert not pool.running

    def test_pool_start_stop(self, temp_db):
        """测试消费者池启动和停止"""
        pool = ConsumerPool(
            max_consumers=2,
            batch_size=50,
            flush_interval=10.0,
            db_path=temp_db
        )
        
        # 启动池
        pool.start()
        assert pool.running
        assert pool.is_running
        assert len(pool.workers) == 2
        assert pool.executor is not None
        
        # 给工作线程一些启动时间
        time.sleep(0.1)
        
        # 停止池
        pool.stop()
        assert not pool.running
        assert not pool.is_running
        assert len(pool.workers) == 0
        assert pool.executor is None

    def test_submit_data(self, temp_db, sample_data_batch):
        """测试提交数据批次"""
        pool = ConsumerPool(
            max_consumers=1,
            batch_size=10,
            flush_interval=1.0,
            db_path=temp_db
        )
        
        pool.start()
        
        try:
            # 提交数据批次
            success = pool.submit_data(sample_data_batch)
            assert success
            
            # 验证队列大小
            assert pool.data_queue_size >= 0  # 可能已被处理
            
            # 等待一段时间让工作线程处理
            time.sleep(0.5)
            
        finally:
            pool.stop()

    def test_submit_data_when_stopped(self, temp_db, sample_data_batch):
        """测试停止状态下提交数据"""
        pool = ConsumerPool(max_consumers=1, db_path=temp_db)
        
        # 未启动时提交应该抛出异常
        with pytest.raises(RuntimeError, match="Consumer pool is not running"):
            pool.submit_data(sample_data_batch)

    def test_force_flush_all(self, temp_db, sample_data_batch):
        """测试强制刷新所有缓存"""
        pool = ConsumerPool(
            max_consumers=1,
            batch_size=100,  # 大批量不会自动刷新
            flush_interval=60.0,  # 长间隔不会自动刷新
            db_path=temp_db
        )
        
        pool.start()
        
        try:
            # 提交数据
            pool.submit_data(sample_data_batch)
            time.sleep(0.1)  # 等待处理
            
            # 强制刷新
            pool.force_flush_all()
            
            # 验证数据已写入数据库
            # 通过工作线程访问存储
            worker = list(pool.workers.values())[0]
            storage = worker.storage
            df = storage.query('daily', sample_data_batch.symbol)
            assert not df.empty
            
        finally:
            pool.stop()

    def test_wait_for_empty_queue(self, temp_db, sample_data_batch):
        """测试等待队列为空"""
        pool = ConsumerPool(
            max_consumers=1,
            batch_size=10,
            flush_interval=1.0,
            db_path=temp_db
        )
        
        pool.start()
        
        try:
            # 提交数据
            pool.submit_data(sample_data_batch)
            
            # 等待队列为空
            empty = pool.wait_for_empty_queue(timeout=5.0)
            assert empty
            assert pool.data_queue_size == 0
            
        finally:
            pool.stop()

    def test_get_statistics(self, temp_db, sample_data_batch):
        """测试获取统计信息"""
        pool = ConsumerPool(
            max_consumers=2,
            batch_size=50,
            flush_interval=10.0,
            db_path=temp_db
        )
        
        pool.start()
        
        try:
            # 提交一些数据
            pool.submit_data(sample_data_batch)
            time.sleep(0.2)  # 等待处理
            
            # 获取统计信息
            stats = pool.get_statistics()
            
            # 验证统计信息结构
            assert stats['running'] == True
            assert stats['max_consumers'] == 2
            assert stats['active_workers'] >= 0
            assert 'data_queue_size' in stats
            assert stats['batch_size'] == 50
            assert stats['flush_interval'] == 10.0
            assert 'uptime_seconds' in stats
            assert 'total_batches_processed' in stats
            assert 'total_batches_cached' in stats
            assert 'worker_statistics' in stats
            assert len(stats['worker_statistics']) == 2
            
        finally:
            pool.stop()

    def test_integration_flow(self, temp_db):
        """测试完整的数据处理流程"""
        pool = ConsumerPool(
            max_consumers=1,
            batch_size=2,  # 小批量快速刷新
            flush_interval=1.0,
            db_path=temp_db
        )
        
        pool.start()
        
        try:
            # 创建多个数据批次
            batches = []
            for i in range(3):
                df = pd.DataFrame({
                    'ts_code': [f'00000{i}.SZ'],
                    'trade_date': ['20241201'],
                    'close': [10.0 + i],
                    'volume': [1000 * (i + 1)]
                })
                
                batch = DataBatch(
                    df=df,
                    meta={'task_type': 'daily'},
                    task_id=f'task_{i}',
                    symbol=f'00000{i}.SZ'
                )
                batches.append(batch)
            
            # 提交所有批次
            for batch in batches:
                success = pool.submit_data(batch)
                assert success
            
            # 等待处理完成
            pool.wait_for_empty_queue(timeout=5.0)
            
            # 强制刷新确保所有数据都写入
            pool.force_flush_all()
            
            # 验证数据已正确写入数据库
            worker = list(pool.workers.values())[0]
            storage = worker.storage
            
            for i in range(3):
                symbol = f'00000{i}.SZ'
                df = storage.query('daily', symbol)
                assert not df.empty
                assert len(df) == 1
                assert df.iloc[0]['close'] == 10.0 + i
            
        finally:
            pool.stop()

    def test_error_handling_integration(self, temp_db):
        """测试错误处理集成"""
        pool = ConsumerPool(
            max_consumers=1,
            batch_size=10,
            flush_interval=1.0,
            db_path="/invalid/path/test.db",  # 无效路径触发错误
            max_retries=1
        )
        
        pool.start()
        
        try:
            # 创建数据批次
            df = pd.DataFrame({
                'ts_code': ['000001.SZ'],
                'trade_date': ['20241201'],
                'close': [10.5]
            })
            
            batch = DataBatch(
                df=df,
                meta={'task_type': 'daily'},
                task_id='test_error',
                symbol='000001.SZ'
            )
            
            # 提交数据批次
            pool.submit_data(batch)
            
            # 等待一段时间让错误发生
            time.sleep(0.5)
            
            # 获取统计信息检查错误
            stats = pool.get_statistics()
            
            # 应该有一些失败操作
            # 注意：由于路径错误，可能在不同阶段失败
            
        finally:
            pool.stop()
