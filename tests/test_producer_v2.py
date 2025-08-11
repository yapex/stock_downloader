"""ProducerV2 测试

测试 ProducerV2 的核心功能，包括任务处理、事件通知和生命周期管理。
"""

import pytest
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from unittest.mock import Mock, MagicMock
import pandas as pd

from src.downloader.producer_v2 import ProducerV2
from src.downloader.interfaces.producer import ProducerEvents
from src.downloader.models import DownloadTask, DataBatch, TaskType, Priority


class TestProducerV2:
    """ProducerV2 测试类"""
    
    @pytest.fixture
    def mock_fetcher(self):
        """模拟 fetcher"""
        fetcher = Mock()
        # 默认返回有效数据
        default_data = pd.DataFrame({
            'date': ['2023-01-01'],
            'close': [100.0]
        })
        fetcher.fetch_daily_history.return_value = default_data
        fetcher.fetch_daily_basic.return_value = default_data
        fetcher.fetch_stock_list.return_value = default_data
        fetcher.fetch_income.return_value = default_data
        fetcher.fetch_balancesheet.return_value = default_data
        fetcher.fetch_cashflow.return_value = default_data
        return fetcher
    
    @pytest.fixture
    def mock_event_bus(self):
        """模拟事件总线"""
        return Mock()
    
    @pytest.fixture
    def executor(self):
        """线程池执行器"""
        executor = ThreadPoolExecutor(max_workers=2)
        yield executor
        executor.shutdown(wait=True)
    
    @pytest.fixture
    def sample_task(self):
        """示例任务"""
        return DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={
                'start_date': '2023-01-01',
                'end_date': '2023-01-31',
                'adjust': 'qfq'
            },
            priority=Priority.NORMAL
        )
    
    @pytest.fixture
    def producer(self, mock_fetcher, executor, mock_event_bus):
        """ProducerV2 实例"""
        return ProducerV2(
            fetcher=mock_fetcher,
            executor=executor,
            event_bus=mock_event_bus,
            queue_size=10
        )
    
    def test_init(self, mock_fetcher, executor, mock_event_bus):
        """测试初始化"""
        producer = ProducerV2(
            fetcher=mock_fetcher,
            executor=executor,
            event_bus=mock_event_bus,
            queue_size=5
        )
        
        assert producer._fetcher is mock_fetcher
        assert producer._executor is executor
        assert producer._event_bus is mock_event_bus
        assert producer._task_queue.maxsize == 5
        assert not producer.is_running
    
    def test_start_stop(self, producer):
        """测试启动和停止"""
        # 初始状态
        assert not producer.is_running
        
        # 启动
        producer.start()
        assert producer.is_running
        
        # 重复启动应该无效
        producer.start()
        assert producer.is_running
        
        # 停止
        producer.stop()
        assert not producer.is_running
    
    def test_submit_task_when_not_running(self, producer, sample_task):
        """测试未运行时提交任务"""
        with pytest.raises(RuntimeError, match="Producer is not running"):
            producer.submit_task(sample_task)
    
    def test_submit_task_success(self, producer, sample_task):
        """测试成功提交任务"""
        producer.start()
        try:
            result = producer.submit_task(sample_task)
            assert result is True
        finally:
            producer.stop()
    
    def test_submit_task_queue_full(self, mock_fetcher, executor, mock_event_bus, sample_task):
        """测试队列满时提交任务"""
        # 创建小队列，但不启动生产者（这样任务不会被处理）
        producer = ProducerV2(
            fetcher=mock_fetcher,
            executor=executor,
            event_bus=mock_event_bus,
            queue_size=1
        )
        
        # 不启动生产者，直接测试队列满的情况
        # 手动设置 running 状态以允许提交任务
        producer._running = True
        
        try:
            # 第一个任务应该成功
            assert producer.submit_task(sample_task) is True
            
            # 第二个任务应该失败（队列满）
            task2 = DownloadTask(
                symbol="000002.SZ",
                task_type=TaskType.DAILY,
                params={
                    'start_date': '2023-01-01',
                    'end_date': '2023-01-31'
                }
            )
            assert producer.submit_task(task2, timeout=0.1) is False
        finally:
            producer._running = False
    
    def test_process_task_success(self, producer, mock_event_bus, sample_task):
        """测试成功处理任务"""
        producer.start()
        try:
            # 提交任务
            producer.submit_task(sample_task)
            
            # 等待任务处理完成
            time.sleep(0.2)
            
            # 验证事件被触发
            assert mock_event_bus.publish.call_count >= 2
            
            # 检查 DATA_READY 事件
            data_ready_calls = [
                call for call in mock_event_bus.publish.call_args_list
                if call[0][0] == ProducerEvents.DATA_READY
            ]
            assert len(data_ready_calls) == 1
            
            # 检查 TASK_COMPLETED 事件
            task_completed_calls = [
                call for call in mock_event_bus.publish.call_args_list
                if call[0][0] == ProducerEvents.TASK_COMPLETED
            ]
            assert len(task_completed_calls) == 1
            
        finally:
            producer.stop()
    
    def test_process_task_no_data(self, producer, mock_fetcher, mock_event_bus, sample_task):
        """测试处理任务时无数据"""
        # 设置 fetcher 返回空数据
        mock_fetcher.fetch_daily_history.return_value = pd.DataFrame()
        
        producer.start()
        try:
            producer.submit_task(sample_task)
            time.sleep(0.2)
            
            # 验证 TASK_FAILED 事件被触发
            task_failed_calls = [
                call for call in mock_event_bus.publish.call_args_list
                if call[0][0] == ProducerEvents.TASK_FAILED
            ]
            assert len(task_failed_calls) == 1
            
        finally:
            producer.stop()
    
    def test_process_task_fetcher_exception(self, producer, mock_fetcher, mock_event_bus, sample_task):
        """测试 fetcher 抛出异常"""
        # 设置 fetcher 抛出异常
        mock_fetcher.fetch_daily_history.side_effect = Exception("Fetch error")
        
        producer.start()
        try:
            producer.submit_task(sample_task)
            time.sleep(0.2)
            
            # 验证 TASK_FAILED 事件被触发
            task_failed_calls = [
                call for call in mock_event_bus.publish.call_args_list
                if call[0][0] == ProducerEvents.TASK_FAILED
            ]
            assert len(task_failed_calls) == 1
            
        finally:
            producer.stop()
    
    def test_multiple_tasks(self, producer, mock_event_bus, sample_task):
        """测试处理多个任务"""
        producer.start()
        try:
            # 提交多个任务
            tasks = []
            for i in range(3):
                task = DownloadTask(
                    symbol=f"00000{i}.SZ",
                    task_type=TaskType.DAILY,
                    params={
                        'start_date': '2023-01-01',
                        'end_date': '2023-01-31'
                    }
                )
                tasks.append(task)
                producer.submit_task(task)
            
            # 等待所有任务处理完成
            time.sleep(0.5)
            
            # 验证所有任务都被处理
            data_ready_calls = [
                call for call in mock_event_bus.publish.call_args_list
                if call[0][0] == ProducerEvents.DATA_READY
            ]
            assert len(data_ready_calls) == 3
            
        finally:
            producer.stop()
    
    def test_stop_timeout(self, producer, mock_fetcher):
        """测试停止超时"""
        # 设置 fetcher 执行很慢
        def slow_fetch(*args, **kwargs):
            time.sleep(2)
            return pd.DataFrame({'date': ['2023-01-01'], 'close': [100.0]})
        
        mock_fetcher.fetch_daily_history.side_effect = slow_fetch
        
        producer.start()
        
        # 提交任务
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={
                'start_date': '2023-01-01',
                'end_date': '2023-01-31'
            }
        )
        producer.submit_task(task)
        
        # 快速停止（应该超时但不会崩溃）
        start_time = time.time()
        producer.stop(timeout=0.1)
        stop_time = time.time()
        
        # 验证停止时间接近超时时间
        assert stop_time - start_time < 1.0  # 应该很快返回
        assert not producer.is_running