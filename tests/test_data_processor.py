import pytest
import pandas as pd
from unittest.mock import Mock

from downloader.consumer.data_processor import DataProcessor
from downloader.consumer.interfaces import IBatchSaver
from downloader.producer.fetcher_builder import TaskType
from downloader.task.types import DownloadTaskConfig, TaskResult, TaskPriority


class MockBatchSaver:
    """模拟批量保存器"""
    
    def __init__(self, success: bool = True):
        self.success = success
        self.saved_batches = []
    
    def save_batch(self, task_type: TaskType, data: pd.DataFrame) -> bool:
        """模拟批量保存数据"""
        self.saved_batches.append((task_type, data.copy()))
        return self.success


class TestDataProcessor:
    """测试DataProcessor类"""
    
    def test_init(self):
        """测试初始化"""
        batch_saver = MockBatchSaver()
        processor = DataProcessor(batch_saver, batch_size=50)
        
        assert processor.batch_saver == batch_saver
        assert processor.batch_size == 50
        assert processor._processed_count == 0
        assert processor._failed_count == 0
    
    def test_process_successful_task_result(self):
        """测试处理成功的任务结果"""
        batch_saver = MockBatchSaver()
        processor = DataProcessor(batch_saver, batch_size=100)
        
        # 创建测试数据
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        data = pd.DataFrame({"ts_code": ["600519.SH"], "close": [100.0]})
        task_result = TaskResult(config=config, success=True, data=data)
        
        # 处理任务结果
        processor.process_task_result(task_result)
        
        # 验证统计信息
        stats = processor.get_stats()
        assert stats['processed_count'] == 1
        assert stats['failed_count'] == 0
        assert stats['pending_count'] == 1
        
        # 验证数据未被保存（因为未达到批量大小）
        assert len(batch_saver.saved_batches) == 0
    
    def test_process_failed_task_result(self):
        """测试处理失败的任务结果"""
        batch_saver = MockBatchSaver()
        processor = DataProcessor(batch_saver, batch_size=100)
        
        # 创建失败的任务结果
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        task_result = TaskResult(
            config=config, 
            success=False, 
            error=Exception("Network error")
        )
        
        # 处理任务结果
        processor.process_task_result(task_result)
        
        # 验证统计信息
        stats = processor.get_stats()
        assert stats['processed_count'] == 0
        assert stats['failed_count'] == 1
        assert stats['pending_count'] == 0
        
        # 验证数据未被保存
        assert len(batch_saver.saved_batches) == 0
    
    def test_process_empty_data_task_result(self):
        """测试处理空数据的任务结果"""
        batch_saver = MockBatchSaver()
        processor = DataProcessor(batch_saver, batch_size=100)
        
        # 创建空数据的任务结果
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        empty_data = pd.DataFrame()
        task_result = TaskResult(config=config, success=True, data=empty_data)
        
        # 处理任务结果
        processor.process_task_result(task_result)
        
        # 验证统计信息
        stats = processor.get_stats()
        assert stats['processed_count'] == 0
        assert stats['failed_count'] == 0
        assert stats['pending_count'] == 0
        
        # 验证数据未被保存
        assert len(batch_saver.saved_batches) == 0
    
    def test_batch_save_when_size_reached(self):
        """测试达到批量大小时自动保存"""
        batch_saver = MockBatchSaver()
        processor = DataProcessor(batch_saver, batch_size=2)  # 小批量大小便于测试
        
        # 创建多个任务结果
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        
        for i in range(3):
            data = pd.DataFrame({"ts_code": [f"60051{i}.SH"], "close": [100.0 + i]})
            task_result = TaskResult(config=config, success=True, data=data)
            processor.process_task_result(task_result)
        
        # 验证统计信息
        stats = processor.get_stats()
        assert stats['processed_count'] == 3
        assert stats['failed_count'] == 0
        assert stats['pending_count'] == 1  # 还有一条数据待处理
        
        # 验证批量保存被调用
        assert len(batch_saver.saved_batches) == 1
        saved_task_type, saved_data = batch_saver.saved_batches[0]
        assert saved_task_type == TaskType.STOCK_DAILY
        assert len(saved_data) == 2
    
    def test_flush_pending_data(self):
        """测试刷新待处理数据"""
        batch_saver = MockBatchSaver()
        processor = DataProcessor(batch_saver, batch_size=100)
        
        # 添加一些数据
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        data = pd.DataFrame({"ts_code": ["600519.SH"], "close": [100.0]})
        task_result = TaskResult(config=config, success=True, data=data)
        processor.process_task_result(task_result)
        
        # 验证数据未被保存
        assert len(batch_saver.saved_batches) == 0
        
        # 刷新待处理数据
        processor.flush_pending_data()
        
        # 验证数据被保存
        assert len(batch_saver.saved_batches) == 1
        saved_task_type, saved_data = batch_saver.saved_batches[0]
        assert saved_task_type == TaskType.STOCK_DAILY
        assert len(saved_data) == 1
        
        # 验证统计信息
        stats = processor.get_stats()
        assert stats['pending_count'] == 0
    
    def test_multiple_task_types(self):
        """测试处理多种任务类型"""
        batch_saver = MockBatchSaver()
        processor = DataProcessor(batch_saver, batch_size=2)
        
        # 添加不同类型的任务结果
        daily_config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        daily_data = pd.DataFrame({"ts_code": ["600519.SH"], "close": [100.0]})
        daily_result = TaskResult(config=daily_config, success=True, data=daily_data)
        
        basic_config = DownloadTaskConfig("", TaskType.STOCK_BASIC)
        basic_data = pd.DataFrame({"ts_code": ["600519.SH"], "name": ["贵州茅台"]})
        basic_result = TaskResult(config=basic_config, success=True, data=basic_data)
        
        # 处理任务结果
        processor.process_task_result(daily_result)
        processor.process_task_result(basic_result)
        
        # 验证统计信息
        stats = processor.get_stats()
        assert stats['processed_count'] == 2
        assert stats['pending_count'] == 2  # 每种类型各有一条待处理
        
        # 刷新所有数据
        processor.flush_pending_data()
        
        # 验证两种类型的数据都被保存
        assert len(batch_saver.saved_batches) == 2
        saved_types = [batch[0] for batch in batch_saver.saved_batches]
        assert TaskType.STOCK_DAILY in saved_types
        assert TaskType.STOCK_BASIC in saved_types
    
    def test_batch_saver_failure(self):
        """测试批量保存失败的情况"""
        batch_saver = MockBatchSaver(success=False)  # 模拟保存失败
        processor = DataProcessor(batch_saver, batch_size=1)
        
        # 添加数据
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        data = pd.DataFrame({"ts_code": ["600519.SH"], "close": [100.0]})
        task_result = TaskResult(config=config, success=True, data=data)
        
        # 处理任务结果（会触发批量保存）
        processor.process_task_result(task_result)
        
        # 验证保存被调用但失败
        assert len(batch_saver.saved_batches) == 1
        
        # 验证缓存被清空（避免重复处理）
        stats = processor.get_stats()
        assert stats['pending_count'] == 0
    
    def test_thread_safety(self):
        """测试线程安全性"""
        import threading
        import time
        
        batch_saver = MockBatchSaver()
        processor = DataProcessor(batch_saver, batch_size=10)
        
        def worker():
            """工作线程函数"""
            for i in range(5):
                config = DownloadTaskConfig(f"60051{i}", TaskType.STOCK_DAILY)
                data = pd.DataFrame({"ts_code": [f"60051{i}.SH"], "close": [100.0 + i]})
                task_result = TaskResult(config=config, success=True, data=data)
                processor.process_task_result(task_result)
                time.sleep(0.001)  # 模拟处理时间
        
        # 创建多个线程
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=worker)
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 验证统计信息
        stats = processor.get_stats()
        assert stats['processed_count'] == 15  # 3个线程 * 5个任务
        assert stats['failed_count'] == 0