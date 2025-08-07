"""
增强队列管理器测试
"""

import time
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.downloader.models import DownloadTask, DataBatch, TaskType, Priority
from src.downloader.enhanced_queue_manager import EnhancedQueueManager


class TestDownloadTask:
    """测试DownloadTask数据类"""
    
    def test_create_download_task(self):
        """测试创建下载任务"""
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={"start_date": "2024-01-01", "end_date": "2024-01-31"}
        )
        
        assert task.symbol == "000001.SZ"
        assert task.task_type == TaskType.DAILY
        assert task.params["start_date"] == "2024-01-01"
        assert task.priority == Priority.NORMAL
        assert task.retry_count == 0
        assert task.max_retries == 3
        assert task.task_id is not None
        assert task.created_at is not None
    
    def test_task_retry_logic(self):
        """测试任务重试逻辑"""
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={}
        )
        
        # 初始状态可以重试
        assert task.can_retry()
        
        # 增加重试次数
        retry_task = task.increment_retry()
        assert retry_task.retry_count == 1
        assert retry_task.can_retry()
        
        # 达到最大重试次数
        task_max_retry = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={},
            retry_count=3
        )
        assert not task_max_retry.can_retry()
    
    def test_task_serialization(self):
        """测试任务序列化和反序列化"""
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={"key": "value"},
            priority=Priority.HIGH
        )
        
        # 序列化
        task_dict = task.to_dict()
        assert task_dict["symbol"] == "000001.SZ"
        assert task_dict["task_type"] == "daily"
        assert task_dict["priority"] == 10
        
        # 反序列化
        restored_task = DownloadTask.from_dict(task_dict)
        assert restored_task.symbol == task.symbol
        assert restored_task.task_type == task.task_type
        assert restored_task.priority == task.priority
        assert restored_task.params == task.params


class TestDataBatch:
    """测试DataBatch数据类"""
    
    def test_create_data_batch(self):
        """测试创建数据批次"""
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "price": [10.0, 11.0]
        })
        
        batch = DataBatch(
            df=df,
            meta={"source": "test", "count": 2},
            task_id="task-123",
            symbol="000001.SZ"
        )
        
        assert batch.size == 2
        assert not batch.is_empty
        assert batch.columns == ["date", "price"]
        assert batch.symbol == "000001.SZ"
        assert batch.meta["source"] == "test"
    
    def test_empty_data_batch(self):
        """测试空数据批次"""
        batch = DataBatch.empty("task-123", "000001.SZ", {"test": True})
        
        assert batch.size == 0
        assert batch.is_empty
        assert batch.columns == []
        assert batch.meta["test"] is True
    
    def test_data_batch_records(self):
        """测试数据批次记录转换"""
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "price": [10.0, 11.0]
        })
        
        batch = DataBatch(
            df=df,
            meta={},
            task_id="task-123"
        )
        
        # 转换为记录
        records = batch.to_records()
        assert len(records) == 2
        assert records[0]["date"] == "2024-01-01"
        assert records[0]["price"] == 10.0
        
        # 从记录创建
        new_batch = DataBatch.from_records(
            records, {"restored": True}, "task-456", "000002.SZ"
        )
        assert new_batch.size == 2
        assert new_batch.symbol == "000002.SZ"
        assert new_batch.meta["restored"] is True
    
    def test_data_batch_serialization(self):
        """测试数据批次序列化"""
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        batch = DataBatch(
            df=df,
            meta={"test": True},
            task_id="task-123",
            symbol="000001.SZ"
        )
        
        batch_dict = batch.to_dict()
        assert batch_dict["size"] == 2
        assert batch_dict["symbol"] == "000001.SZ"
        assert batch_dict["columns"] == ["col1", "col2"]
        assert not batch_dict["is_empty"]


class TestEnhancedQueueManager:
    """测试增强队列管理器"""
    
    @pytest.fixture
    def queue_manager(self):
        """创建队列管理器实例"""
        manager = EnhancedQueueManager(
            task_queue_size=10,
            data_queue_size=10,
            enable_smart_retry=True
        )
        return manager
    
    def test_queue_manager_lifecycle(self, queue_manager):
        """测试队列管理器生命周期"""
        assert not queue_manager.is_running
        
        # 启动
        queue_manager.start()
        assert queue_manager.is_running
        assert queue_manager.task_queue_size == 0
        assert queue_manager.data_queue_size == 0
        
        # 停止
        queue_manager.stop()
        assert not queue_manager.is_running
    
    def test_task_queue_operations(self, queue_manager):
        """测试任务队列操作"""
        queue_manager.start()
        
        # 创建任务
        task1 = DownloadTask("000001.SZ", TaskType.DAILY, {}, Priority.HIGH)
        task2 = DownloadTask("000002.SZ", TaskType.DAILY, {}, Priority.NORMAL)
        task3 = DownloadTask("000003.SZ", TaskType.DAILY, {}, Priority.LOW)
        
        # 添加任务
        assert queue_manager.put_task(task1)
        assert queue_manager.put_task(task2) 
        assert queue_manager.put_task(task3)
        assert queue_manager.task_queue_size == 3
        
        # 获取任务（应按优先级顺序）
        retrieved_task1 = queue_manager.get_task(timeout=1.0)
        assert retrieved_task1.symbol == "000001.SZ"  # 高优先级
        queue_manager.task_done()
        
        retrieved_task2 = queue_manager.get_task(timeout=1.0) 
        assert retrieved_task2.symbol == "000002.SZ"  # 普通优先级
        queue_manager.task_done()
        
        retrieved_task3 = queue_manager.get_task(timeout=1.0)
        assert retrieved_task3.symbol == "000003.SZ"  # 低优先级
        queue_manager.task_done()
        
        # 队列应为空
        assert queue_manager.task_queue_size == 0
        
        queue_manager.stop()
    
    def test_data_queue_operations(self, queue_manager):
        """测试数据队列操作"""
        queue_manager.start()
        
        # 创建数据批次
        df = pd.DataFrame({"col1": [1, 2, 3]})
        batch = DataBatch(df, {"test": True}, "task-123", "000001.SZ")
        
        # 添加数据
        assert queue_manager.put_data(batch)
        assert queue_manager.data_queue_size == 1
        
        # 获取数据
        retrieved_batch = queue_manager.get_data(timeout=1.0)
        assert retrieved_batch.size == 3
        assert retrieved_batch.symbol == "000001.SZ"
        queue_manager.data_done()
        
        assert queue_manager.data_queue_size == 0
        
        queue_manager.stop()
    
    def test_queue_full_handling(self, queue_manager):
        """测试队列满载处理"""
        queue_manager.start()
        
        # 填满任务队列
        for i in range(10):
            task = DownloadTask(f"00000{i}.SZ", TaskType.DAILY, {})
            assert queue_manager.put_task(task, timeout=0.1)
        
        # 队列已满，应该失败
        overflow_task = DownloadTask("overflow.SZ", TaskType.DAILY, {})
        assert not queue_manager.put_task(overflow_task, timeout=0.1)
        
        queue_manager.stop()
    
    def test_retry_mechanism(self, queue_manager):
        """测试重试机制"""
        queue_manager.start()
        
        # 创建任务
        task = DownloadTask("000001.SZ", TaskType.DAILY, {})
        
        # 调度重试
        assert queue_manager.schedule_retry(task, "Network error")
        
        # 等待重试处理
        time.sleep(0.1)
        
        # 检查指标
        metrics = queue_manager.metrics
        assert metrics['tasks_retried'] >= 1
        
        queue_manager.stop()
    
    def test_retry_limit(self, queue_manager):
        """测试重试限制"""
        queue_manager.start()
        
        # 创建已达到最大重试次数的任务
        task = DownloadTask("000001.SZ", TaskType.DAILY, {}, retry_count=3)
        
        # 不应该能够重试
        assert not queue_manager.schedule_retry(task, "Network error")
        
        # 应标记为失败
        metrics = queue_manager.metrics
        assert metrics['tasks_failed'] >= 1
        
        queue_manager.stop()
    
    def test_metrics_collection(self, queue_manager):
        """测试指标收集"""
        queue_manager.start()
        
        # 添加一些活动
        task = DownloadTask("000001.SZ", TaskType.DAILY, {})
        queue_manager.put_task(task)
        queue_manager.report_task_completed(task)
        
        df = pd.DataFrame({"col1": [1, 2]})
        batch = DataBatch(df, {}, "task-123")
        queue_manager.put_data(batch)
        
        # 检查指标
        metrics = queue_manager.metrics
        assert metrics['tasks_submitted'] >= 1
        assert metrics['tasks_completed'] >= 1
        assert metrics['data_batches_processed'] >= 1
        assert metrics['success_rate'] >= 0.0
        assert metrics['uptime_seconds'] >= 0.0
        
        queue_manager.stop()
    
    def test_priority_retry_delays(self, queue_manager):
        """测试优先级相关的重试延迟"""
        queue_manager.start()
        
        # 高优先级任务重试延迟较短
        high_delay = queue_manager._get_retry_delay(Priority.HIGH, 0)
        normal_delay = queue_manager._get_retry_delay(Priority.NORMAL, 0)
        low_delay = queue_manager._get_retry_delay(Priority.LOW, 0)
        
        assert high_delay < normal_delay < low_delay
        
        queue_manager.stop()
    
    def test_queue_timeout_handling(self, queue_manager):
        """测试队列超时处理"""
        queue_manager.start()
        
        # 从空队列获取任务应该超时返回None
        task = queue_manager.get_task(timeout=0.1)
        assert task is None
        
        data = queue_manager.get_data(timeout=0.1)
        assert data is None
        
        queue_manager.stop()
    
    def test_error_logging(self, queue_manager):
        """测试错误日志记录"""
        queue_manager.start()
        
        # 创建一个不能重试的任务（超过重试次数）
        task = DownloadTask("000001.SZ", TaskType.DAILY, {}, retry_count=3)
        
        # 测试任务失败报告（不能重试，直接失败）
        queue_manager.report_task_failed(task, "Test error")
        
        # 验证指标更新
        metrics = queue_manager.metrics
        assert metrics['tasks_failed'] >= 1
        
        queue_manager.stop()


# 边界测试和异常测试
class TestEdgeCases:
    """边界情况和异常测试"""
    
    def test_invalid_task_type(self):
        """测试无效任务类型"""
        with pytest.raises(ValueError):
            DownloadTask("000001.SZ", "invalid_type", {})
    
    def test_invalid_priority(self):
        """测试无效优先级"""
        with pytest.raises(Exception):
            DownloadTask("000001.SZ", TaskType.DAILY, {}, priority=100)
    
    def test_none_dataframe_handling(self):
        """测试None DataFrame处理"""
        batch = DataBatch(None, {}, "task-123")
        assert batch.is_empty
        assert batch.size == 0
        assert batch.columns == []
    
    def test_queue_manager_operations_when_stopped(self):
        """测试停止状态下的队列操作"""
        manager = EnhancedQueueManager()
        
        task = DownloadTask("000001.SZ", TaskType.DAILY, {})
        
        # 未启动状态下操作应该抛出异常或返回None
        with pytest.raises(RuntimeError):
            manager.put_task(task)
        
        assert manager.get_task() is None
    
    def test_large_data_batch_handling(self):
        """测试大数据批次处理"""
        # 创建大型DataFrame
        large_df = pd.DataFrame({
            "col1": range(10000),
            "col2": range(10000, 20000)
        })
        
        batch = DataBatch(large_df, {"size": "large"}, "task-123")
        assert batch.size == 10000
        assert not batch.is_empty
        
        # 序列化不应包含DataFrame数据
        batch_dict = batch.to_dict()
        assert "df" not in batch_dict
        assert batch_dict["size"] == 10000
        
    def test_concurrent_queue_access(self, monkeypatch):
        """测试并发队列访问（简单模拟）"""
        manager = EnhancedQueueManager()
        manager.start()
        
        # 模拟多线程访问
        tasks = [
            DownloadTask(f"00000{i}.SZ", TaskType.DAILY, {})
            for i in range(5)
        ]
        
        # 添加任务
        for task in tasks:
            assert manager.put_task(task)
        
        # 并发获取
        retrieved_tasks = []
        while manager.task_queue_size > 0:
            task = manager.get_task(timeout=1.0)
            if task:
                retrieved_tasks.append(task)
                manager.task_done()
        
        assert len(retrieved_tasks) == 5
        
        manager.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
