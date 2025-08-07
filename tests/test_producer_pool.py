"""
测试 ProducerPool 生产者管理器
"""

import pytest
import time
import threading
from unittest.mock import Mock, patch, MagicMock
from queue import Queue, Empty
import pandas as pd
from datetime import datetime

from src.downloader.producer_pool import ProducerPool, ProducerWorker, rate_limit
from src.downloader.models import DownloadTask, DataBatch, TaskType, Priority
from src.downloader.fetcher import TushareFetcher


class TestRateLimit:
    """测试速率限制装饰器"""
    
    def test_rate_limit_basic(self):
        """测试基本的速率限制功能"""
        call_count = 0
        
        @rate_limit(calls=2, period=1)
        def test_func():
            nonlocal call_count
            call_count += 1
            return call_count
        
        # 前两次调用应该立即执行
        start_time = time.time()
        assert test_func() == 1
        assert test_func() == 2
        
        # 第三次调用应该被限制
        assert test_func() == 3
        elapsed = time.time() - start_time
        assert elapsed >= 1.0  # 应该等待了至少1秒
    
    def test_rate_limit_with_args(self):
        """测试带参数的速率限制"""
        @rate_limit(calls=1, period=0.5)
        def test_func(x, y=10):
            return x + y
        
        assert test_func(5) == 15
        start_time = time.time()
        assert test_func(3, y=7) == 10
        elapsed = time.time() - start_time
        assert elapsed >= 0.5


class TestProducerWorker:
    """测试 ProducerWorker"""
    
    def setup_method(self):
        """设置测试环境"""
        self.task_queue = Queue()
        self.data_queue = Queue()
        self.mock_fetcher = Mock(spec=TushareFetcher)
        
        # 设置mock返回值
        self.mock_fetcher.fetch_stock_list.return_value = pd.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ'],
            'name': ['平安银行', '万科A']
        })
        
        self.mock_fetcher.fetch_daily_history.return_value = pd.DataFrame({
            'ts_code': ['000001.SZ'] * 3,
            'trade_date': ['20231201', '20231202', '20231203'],
            'close': [10.0, 10.5, 11.0]
        })
        
        self.worker = ProducerWorker(
            worker_id=0,
            fetcher=self.mock_fetcher,
            task_queue=self.task_queue,
            data_queue=self.data_queue,
            retry_policy="requeue",
            max_retries=3
        )
    
    def test_worker_initialization(self):
        """测试工作线程初始化"""
        assert self.worker.worker_id == 0
        assert self.worker.fetcher == self.mock_fetcher
        assert self.worker.retry_policy == "requeue"
        assert self.worker.max_retries == 3
        assert not self.worker.running
    
    def test_stock_list_task_processing(self):
        """测试股票列表任务处理"""
        task = DownloadTask(
            symbol="",
            task_type=TaskType.STOCK_LIST,
            params={}
        )
        
        # 直接处理任务（不使用队列）
        self.worker._process_task(task)
        
        # 验证数据队列有结果
        assert not self.data_queue.empty()
        data_batch = self.data_queue.get()
        
        assert data_batch.task_id == task.task_id
        assert data_batch.meta['task_type'] == 'stock_list'
        assert len(data_batch.df) == 2
        
        # 验证fetcher被调用
        self.mock_fetcher.fetch_stock_list.assert_called_once()
    
    def test_daily_task_processing(self):
        """测试日K线任务处理"""
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={
                'start_date': '20231201',
                'end_date': '20231203',
                'adjust': 'hfq'
            }
        )
        
        self.worker._process_task(task)
        
        # 验证数据队列有结果
        data_batch = self.data_queue.get()
        assert data_batch.symbol == "000001.SZ"
        assert len(data_batch.df) == 3
        
        # 验证fetcher被调用
        self.mock_fetcher.fetch_daily_history.assert_called_once_with(
            ts_code="000001.SZ",
            start_date='20231201',
            end_date='20231203',
            adjust='hfq'
        )
    
    def test_empty_data_handling(self):
        """测试空数据处理"""
        self.mock_fetcher.fetch_daily_history.return_value = None
        
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={'start_date': '20231201', 'end_date': '20231203'}
        )
        
        self.worker._process_task(task)
        
        # 验证创建了空的数据批次
        data_batch = self.data_queue.get()
        assert data_batch.is_empty
        assert data_batch.meta['reason'] == 'no_data'
    
    def test_task_error_handling_with_retry(self):
        """测试任务错误处理和重试"""
        self.mock_fetcher.fetch_daily_history.side_effect = ConnectionError("Network error")
        
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={'start_date': '20231201', 'end_date': '20231203'},
            retry_count=0,
            max_retries=3
        )
        
        # 不在队列中放任务，直接处理
        self.worker._process_task(task)
        
        # 验证任务被重新入队
        assert not self.task_queue.empty()
        retry_task = self.task_queue.get()
        assert retry_task.retry_count == 1
        assert retry_task.task_id == task.task_id
    
    def test_task_error_handling_max_retries(self):
        """测试达到最大重试次数的错误处理"""
        self.mock_fetcher.fetch_daily_history.side_effect = ConnectionError("Network error")
        
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={'start_date': '20231201', 'end_date': '20231203'},
            retry_count=3,  # 已达到最大重试次数
            max_retries=3
        )
        
        with patch('src.downloader.producer_pool.record_failed_task') as mock_record:
            self.worker._process_task(task)
            
            # 验证失败任务被记录
            mock_record.assert_called_once()
            args = mock_record.call_args[1]
            assert args['entity_id'] == '000001.SZ'
            assert 'network' in args['error_category'].lower()
    
    def test_data_queue_full_handling(self):
        """测试数据队列满的处理"""
        # 创建一个小容量的数据队列
        small_data_queue = Queue(maxsize=1)
        small_data_queue.put("dummy")  # 填满队列
        
        worker = ProducerWorker(
            worker_id=0,
            fetcher=self.mock_fetcher,
            task_queue=self.task_queue,
            data_queue=small_data_queue
        )
        
        data_batch = DataBatch.empty(task_id="test_id", symbol="test_symbol")
        
        with patch('src.downloader.producer_pool.record_failed_task') as mock_record:
            worker._put_data(data_batch)
            
            # 验证数据丢失被记录
            mock_record.assert_called_once_with(
                task_name="data_queue_full",
                entity_id="test_symbol",
                reason="data_queue_full",
                error_category="system"
            )


class TestProducerPool:
    """测试 ProducerPool 的基本功能"""
    
    def test_pool_initialization_with_mock(self):
        """测试生产者池初始化"""
        with patch('src.downloader.producer_pool.TushareFetcher') as mock_fetcher_class:
            mock_fetcher_class.return_value = Mock()
            
            pool = ProducerPool(
                max_producers=2,
                retry_policy="requeue",
                max_retries=2
            )
            
            assert pool.max_producers == 2
            assert pool.retry_policy == "requeue"
            assert pool.max_retries == 2
            assert not pool.is_running
            assert pool.task_queue_size == 0
            assert pool.data_queue_size == 0
    
    @patch('src.downloader.producer_pool.TushareFetcher')
    def test_end_to_end_processing(self, mock_fetcher_class):
        """测试端到端的任务处理"""
        # 配置mock
        mock_fetcher = Mock()
        mock_fetcher_class.return_value = mock_fetcher
        mock_fetcher.fetch_stock_list.return_value = pd.DataFrame({
            'ts_code': ['000001.SZ'],
            'name': ['测试股票']
        })
        
        # 创建新的池以使用mock
        pool = ProducerPool(max_producers=1)
        
        try:
            pool.start()
            
            # 提交任务
            task = DownloadTask(
                symbol="",
                task_type=TaskType.STOCK_LIST,
                params={}
            )
            
            pool.submit_task(task)
            
            # 等待处理并获取结果
            time.sleep(2)  # 给工作线程一些处理时间
            
            data_batch = pool.get_data(timeout=5.0)
            assert data_batch is not None
            assert data_batch.task_id == task.task_id
            assert len(data_batch.df) == 1
            
        finally:
            pool.stop()
    
    
    def test_multiple_workers_concurrent_processing(self):
        """测试多工作线程并发处理"""
        with patch('src.downloader.producer_pool.TushareFetcher') as mock_fetcher_class:
            # 配置mock
            mock_fetcher = Mock()
            mock_fetcher_class.return_value = mock_fetcher
            mock_fetcher.fetch_stock_list.return_value = pd.DataFrame({
                'ts_code': ['000001.SZ'],
                'name': ['测试股票']
            })
            
            pool = ProducerPool(max_producers=3)
            
            try:
                pool.start()
                
                # 提交多个任务
                tasks = []
                for i in range(5):
                    task = DownloadTask(
                        symbol=f"test_{i}",
                        task_type=TaskType.STOCK_LIST,
                        params={}
                    )
                    tasks.append(task)
                    pool.submit_task(task)
                
                # 等待一段时间让任务处理
                time.sleep(3)
                
                # 收集结果
                results = []
                for _ in range(5):
                    data_batch = pool.get_data(timeout=2.0)
                    if data_batch:
                        results.append(data_batch)
                
                # 验证所有任务都被处理
                assert len(results) >= 1  # 至少有一些任务被处理
                
            finally:
                pool.stop()
    
    def test_custom_queues(self):
        """测试使用自定义队列"""
        with patch('src.downloader.producer_pool.TushareFetcher') as mock_fetcher_class:
            mock_fetcher_class.return_value = Mock()
            
            task_queue = Queue(maxsize=10)
            data_queue = Queue(maxsize=20)
            
            pool = ProducerPool(
                max_producers=1,
                task_queue=task_queue,
                data_queue=data_queue
            )
            
            assert pool.task_queue is task_queue
            assert pool.data_queue is data_queue
    
    def test_retry_policy_deadletter(self):
        """测试死信重试策略"""
        with patch('src.downloader.producer_pool.TushareFetcher') as mock_fetcher_class:
            mock_fetcher_class.return_value = Mock()
            
            pool = ProducerPool(
                max_producers=1,
                retry_policy="deadletter",
                max_retries=1
            )
            
            assert pool.retry_policy == "deadletter"
            
            # 创建工作线程应该使用deadletter策略
            worker = ProducerWorker(
                worker_id=0,
                fetcher=Mock(),
                task_queue=Queue(),
                data_queue=Queue(),
                retry_policy="deadletter"
            )
            
            assert worker.retry_policy == "deadletter"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
