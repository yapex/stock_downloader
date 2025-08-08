#!/usr/bin/env python3
"""
测试单线程模式的ProducerPool
验证单线程模式能够正常处理任务并避免并发API调用
"""

import pytest
import time
import threading
from queue import Queue, Empty
from unittest.mock import Mock, patch

from src.downloader.producer_pool import ProducerPool
from src.downloader.models import DownloadTask, TaskType
from src.downloader.fetcher import TushareFetcher


class TestSingleThreadMode:
    """测试单线程模式"""
    
    def setup_method(self):
        """设置测试环境"""
        self.task_queue = Queue(maxsize=10)
        self.data_queue = Queue(maxsize=10)
        self.fetcher = Mock()  # 不使用spec限制
        
        # 设置Mock方法
        import pandas as pd
        mock_data = pd.DataFrame({'test': [1, 2, 3]})
        self.fetcher.fetch_daily_history.return_value = mock_data
        self.fetcher.fetch_stock_list.return_value = mock_data
        self.fetcher.fetch_financials.return_value = mock_data
        
    def test_producer_pool_single_thread_initialization(self):
        """测试ProducerPool单线程初始化"""
        pool = ProducerPool(fetcher=self.fetcher)
        
        # 验证初始状态
        stats = pool.get_statistics()
        assert stats['running'] == False
        assert stats['active_workers'] == 0
        assert stats['tasks_processed'] == 0
    
    def test_producer_pool_start_stop(self):
        """测试ProducerPool启动和停止"""
        pool = ProducerPool(fetcher=self.fetcher)
        
        # 启动
        pool.start()
        stats = pool.get_statistics()
        assert stats['running'] == True
        assert stats['active_workers'] == 1
        
        # 停止
        pool.stop()
        stats = pool.get_statistics()
        assert stats['running'] == False
        assert stats['active_workers'] == 0
    
    def test_single_thread_task_processing(self):
        """测试单线程任务处理"""
        pool = ProducerPool(fetcher=self.fetcher)
        
        # 创建测试任务
        tasks = [
            DownloadTask(symbol="000001.SZ", task_type=TaskType.DAILY, params={}),
            DownloadTask(symbol="000002.SZ", task_type=TaskType.DAILY, params={}),
            DownloadTask(symbol="000003.SZ", task_type=TaskType.DAILY, params={})
        ]
        
        # 模拟API调用
        import pandas as pd
        mock_data = pd.DataFrame({'test': [1, 2, 3]})
        
        # 记录API调用时间，验证是否串行执行
        call_times = []
        
        def mock_fetch_daily(*args, **kwargs):
            call_times.append(time.time())
            time.sleep(0.1)  # 模拟API调用耗时
            return mock_data
        
        self.fetcher.fetch_daily_history.side_effect = mock_fetch_daily
        
        # 启动池
        pool.start()
        
        try:
            # 提交任务
            for task in tasks:
                success = pool.submit_task(task)
                assert success
            
            # 获取处理结果
            processed_count = 0
            for _ in range(len(tasks)):
                data_batch = pool.get_data(timeout=2.0)
                if data_batch is not None:
                    processed_count += 1
            
            assert processed_count == len(tasks)
            
            # 验证API调用是串行的（时间间隔应该大于0.1秒）
            if len(call_times) > 1:
                for i in range(1, len(call_times)):
                    time_diff = call_times[i] - call_times[i-1]
                    assert time_diff >= 0.09, f"API调用间隔太短: {time_diff}秒，可能存在并发调用"
            
        finally:
            pool.stop()
    
    def test_no_concurrent_api_calls(self):
        """测试确保没有并发API调用"""
        pool = ProducerPool(fetcher=self.fetcher)
        
        # 创建多个任务
        tasks = [
            DownloadTask(symbol=f"00000{i}.SZ", task_type=TaskType.DAILY, params={})
            for i in range(1, 6)
        ]
        
        # 记录并发调用
        active_calls = 0
        max_concurrent_calls = 0
        call_lock = threading.Lock()
        
        def mock_fetch_daily(*args, **kwargs):
            nonlocal active_calls, max_concurrent_calls
            
            with call_lock:
                active_calls += 1
                max_concurrent_calls = max(max_concurrent_calls, active_calls)
            
            try:
                time.sleep(0.1)  # 模拟API调用耗时
                import pandas as pd
                return pd.DataFrame({'test': [1, 2, 3]})
            finally:
                with call_lock:
                    active_calls -= 1
        
        self.fetcher.fetch_daily_history.side_effect = mock_fetch_daily
        
        # 启动池
        pool.start()
        
        try:
            # 快速提交所有任务
            for task in tasks:
                pool.submit_task(task)
            
            # 获取所有结果以确保任务完成
            for _ in range(len(tasks)):
                pool.get_data(timeout=2.0)
            
            # 验证最大并发调用数为1
            assert max_concurrent_calls == 1, f"检测到并发API调用，最大并发数: {max_concurrent_calls}"
            
        finally:
            pool.stop()
    
    def test_rate_limiter_integration(self):
        """测试与Rate Limiter的集成"""
        # 使用Mock的TushareFetcher来模拟Rate Limiter行为
        pool = ProducerPool(fetcher=self.fetcher)
        
        # 创建多个任务
        tasks = [
            DownloadTask(symbol=f"00000{i}.SZ", task_type=TaskType.DAILY, params={
                'start_date': '20240101',
                'end_date': '20240102'
            })
            for i in range(1, 4)
        ]
        
        # 模拟Rate Limiter延迟
        def mock_fetch_with_delay(*args, **kwargs):
            time.sleep(0.2)  # 模拟Rate Limiter造成的延迟
            import pandas as pd
            return pd.DataFrame({'test': [1, 2, 3]})
        
        self.fetcher.fetch_daily_history.side_effect = mock_fetch_with_delay
        
        # 记录处理时间
        start_time = time.time()
        
        pool.start()
        
        try:
            # 提交任务
            for task in tasks:
                pool.submit_task(task)
            
            # 获取所有结果以确保任务完成
            for _ in range(len(tasks)):
                pool.get_data(timeout=2.0)
            
            end_time = time.time()
            total_time = end_time - start_time
            
            # 验证串行处理（处理时间应该累积）
            # 3个任务，每个0.2秒，应该至少需要0.6秒
            assert total_time >= 0.5, f"处理时间太短: {total_time}秒，可能存在并发处理"
            
        finally:
            pool.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])