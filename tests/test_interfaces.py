"""
测试接口契约和消息类型的实现
验证所有抽象接口的具体实现
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock

from downloader.interfaces import (
    TaskMessage, DataMessage, TaskType, Priority,
    ExponentialBackoffRetryPolicy, PerformanceMetrics
)


class TestTaskMessage:
    """测试任务消息类"""
    
    def test_create_task_message(self):
        """测试创建任务消息"""
        params = {"symbol": "000001", "start_date": "2023-01-01"}
        task = TaskMessage.create(TaskType.DAILY, params, Priority.HIGH)
        
        assert task.task_id is not None
        assert len(task.task_id) > 0
        assert task.task_type == TaskType.DAILY
        assert task.params == params
        assert task.priority == Priority.HIGH
        assert task.retry_count == 0
        assert task.created_at is not None
    
    def test_task_message_serialization(self):
        """测试任务消息序列化和反序列化"""
        params = {"symbol": "000001", "start_date": "2023-01-01"}
        original_task = TaskMessage.create(TaskType.DAILY, params)
        
        # 序列化
        task_dict = original_task.to_dict()
        assert task_dict['task_id'] == original_task.task_id
        assert task_dict['task_type'] == 'daily'
        assert task_dict['params'] == params
        
        # 反序列化
        restored_task = TaskMessage.from_dict(task_dict)
        assert restored_task.task_id == original_task.task_id
        assert restored_task.task_type == original_task.task_type
        assert restored_task.params == original_task.params
        assert restored_task.priority == original_task.priority
    
    def test_task_message_enum_conversion(self):
        """测试枚举类型转换"""
        task_dict = {
            'task_id': 'test-123',
            'task_type': 'daily',
            'params': {},
            'priority': 5,
            'retry_count': 0
        }
        task = TaskMessage.from_dict(task_dict)
        
        assert task.task_type == TaskType.DAILY
        assert task.priority == Priority.NORMAL


class TestDataMessage:
    """测试数据消息类"""
    
    def test_create_data_message(self):
        """测试创建数据消息"""
        task_id = "test-task-123"
        data_type = "stock_daily"
        data = [{"symbol": "000001", "close": 10.5}]
        metadata = {"source": "tushare", "count": 1}
        
        message = DataMessage.create(task_id, data_type, data, metadata)
        
        assert message.message_id is not None
        assert len(message.message_id) > 0
        assert message.task_id == task_id
        assert message.data_type == data_type
        assert message.data == data
        assert message.metadata == metadata
        assert message.size == 1
        assert message.created_at is not None
    
    def test_data_message_serialization(self):
        """测试数据消息序列化和反序列化"""
        task_id = "test-task-123"
        data_type = "stock_daily"
        data = [{"symbol": "000001", "close": 10.5}]
        
        original_message = DataMessage.create(task_id, data_type, data)
        
        # 序列化
        message_dict = original_message.to_dict()
        assert message_dict['message_id'] == original_message.message_id
        assert message_dict['task_id'] == task_id
        assert message_dict['data'] == data
        
        # 反序列化
        restored_message = DataMessage.from_dict(message_dict)
        assert restored_message.message_id == original_message.message_id
        assert restored_message.task_id == original_message.task_id
        assert restored_message.data == original_message.data
        assert restored_message.size == original_message.size
    
    def test_data_message_size_property(self):
        """测试数据消息大小属性"""
        data = [{"id": i} for i in range(10)]
        message = DataMessage.create("task-1", "test", data)
        
        assert message.size == 10


class TestExponentialBackoffRetryPolicy:
    """测试指数退避重试策略"""
    
    def test_retry_policy_initialization(self):
        """测试重试策略初始化"""
        policy = ExponentialBackoffRetryPolicy(
            max_attempts=5,
            base_delay=2.0,
            max_delay=120.0,
            backoff_factor=3.0
        )
        
        assert policy.get_max_attempts() == 5
        assert policy.base_delay == 2.0
        assert policy.max_delay == 120.0
        assert policy.backoff_factor == 3.0
    
    def test_should_retry_with_connection_error(self):
        """测试连接错误的重试判断"""
        policy = ExponentialBackoffRetryPolicy(max_attempts=3)
        
        error = ConnectionError("Network error")
        assert policy.should_retry(error, 0) is True
        assert policy.should_retry(error, 1) is True
        assert policy.should_retry(error, 2) is True
        assert policy.should_retry(error, 3) is False  # 超过最大次数
    
    def test_should_retry_with_timeout_error(self):
        """测试超时错误的重试判断"""
        policy = ExponentialBackoffRetryPolicy(max_attempts=2)
        
        error = TimeoutError("Request timeout")
        assert policy.should_retry(error, 0) is True
        assert policy.should_retry(error, 1) is True
        assert policy.should_retry(error, 2) is False  # 超过最大次数
    
    def test_should_retry_with_http_errors(self):
        """测试HTTP错误的重试判断"""
        policy = ExponentialBackoffRetryPolicy(max_attempts=3)
        
        # 模拟HTTP 500错误（服务器错误，可重试）
        error_500 = MagicMock()
        error_500.response.status_code = 500
        assert policy.should_retry(error_500, 0) is True
        
        # 模拟HTTP 404错误（客户端错误，不重试）
        error_404 = MagicMock()
        error_404.response.status_code = 404
        assert policy.should_retry(error_404, 0) is False
        
        # 模拟HTTP 503错误（服务不可用，可重试）
        error_503 = MagicMock()
        error_503.response.status_code = 503
        assert policy.should_retry(error_503, 0) is True
    
    def test_get_delay_exponential_backoff(self):
        """测试指数退避延迟计算"""
        policy = ExponentialBackoffRetryPolicy(
            base_delay=1.0,
            backoff_factor=2.0,
            max_delay=10.0
        )
        
        # 第0次重试：1.0 * (2^0) = 1.0
        assert policy.get_delay(0) == 1.0
        
        # 第1次重试：1.0 * (2^1) = 2.0
        assert policy.get_delay(1) == 2.0
        
        # 第2次重试：1.0 * (2^2) = 4.0
        assert policy.get_delay(2) == 4.0
        
        # 第3次重试：1.0 * (2^3) = 8.0
        assert policy.get_delay(3) == 8.0
        
        # 第4次重试：1.0 * (2^4) = 16.0，但受max_delay限制为10.0
        assert policy.get_delay(4) == 10.0
    
    def test_should_retry_unknown_error(self):
        """测试未知错误类型的重试判断"""
        policy = ExponentialBackoffRetryPolicy()
        
        # 未知错误类型默认不重试
        unknown_error = ValueError("Some unknown error")
        assert policy.should_retry(unknown_error, 0) is False


class TestPerformanceMetrics:
    """测试性能监控指标"""
    
    def test_metrics_initialization(self):
        """测试指标初始化"""
        metrics = PerformanceMetrics()
        
        assert metrics.tasks_total == 0
        assert metrics.tasks_completed == 0
        assert metrics.tasks_failed == 0
        assert metrics.data_messages_total == 0
        assert metrics.data_messages_processed == 0
        assert isinstance(metrics.queue_sizes, dict)
        assert isinstance(metrics.processing_rates, dict)
    
    def test_success_rate_calculation(self):
        """测试成功率计算"""
        metrics = PerformanceMetrics()
        
        # 无任务时成功率为0
        assert metrics.success_rate == 0.0
        
        # 有任务时计算成功率
        metrics.tasks_total = 10
        metrics.tasks_completed = 8
        assert metrics.success_rate == 0.8
    
    def test_failure_rate_calculation(self):
        """测试失败率计算"""
        metrics = PerformanceMetrics()
        
        # 无任务时失败率为0
        assert metrics.failure_rate == 0.0
        
        # 有任务时计算失败率
        metrics.tasks_total = 10
        metrics.tasks_failed = 2
        assert metrics.failure_rate == 0.2
    
    def test_metrics_reset(self):
        """测试指标重置"""
        metrics = PerformanceMetrics()
        
        # 设置一些值
        metrics.tasks_total = 100
        metrics.tasks_completed = 90
        metrics.tasks_failed = 10
        metrics.queue_sizes["task_queue"] = 5
        metrics.processing_rates["producer"] = 10.5
        
        # 重置
        metrics.reset()
        
        assert metrics.tasks_total == 0
        assert metrics.tasks_completed == 0
        assert metrics.tasks_failed == 0
        assert metrics.data_messages_total == 0
        assert metrics.data_messages_processed == 0
        assert len(metrics.queue_sizes) == 0
        assert len(metrics.processing_rates) == 0


class TestEnumTypes:
    """测试枚举类型"""
    
    def test_task_type_enum(self):
        """测试任务类型枚举"""
        assert TaskType.DAILY.value == "daily"
        assert TaskType.DAILY_BASIC.value == "daily_basic"
        assert TaskType.FINANCIALS.value == "financials"
        assert TaskType.STOCK_LIST.value == "stock_list"
    
    def test_priority_enum(self):
        """测试优先级枚举"""
        assert Priority.LOW.value == 1
        assert Priority.NORMAL.value == 5
        assert Priority.HIGH.value == 10
        
        # 测试优先级比较
        assert Priority.HIGH.value > Priority.NORMAL.value > Priority.LOW.value


# 边界测试和异常测试
class TestEdgeCases:
    """测试边界情况和异常处理"""
    
    def test_task_message_empty_params(self):
        """测试空参数的任务消息"""
        task = TaskMessage.create(TaskType.DAILY, {})
        assert task.params == {}
    
    def test_data_message_empty_data(self):
        """测试空数据的数据消息"""
        message = DataMessage.create("task-1", "test", [])
        assert message.size == 0
        assert message.data == []
    
    def test_data_message_large_data(self):
        """测试大数据量的数据消息"""
        large_data = [{"id": i, "value": f"data_{i}"} for i in range(10000)]
        message = DataMessage.create("task-1", "test", large_data)
        
        assert message.size == 10000
        assert len(message.data) == 10000
    
    def test_retry_policy_zero_max_attempts(self):
        """测试零重试次数的策略"""
        policy = ExponentialBackoffRetryPolicy(max_attempts=0)
        
        error = ConnectionError("Test error")
        assert policy.should_retry(error, 0) is False
        assert policy.get_max_attempts() == 0
    
    def test_retry_policy_very_large_backoff(self):
        """测试非常大的退避因子"""
        policy = ExponentialBackoffRetryPolicy(
            base_delay=1.0,
            backoff_factor=10.0,
            max_delay=60.0
        )
        
        # 应该受max_delay限制
        assert policy.get_delay(10) == 60.0
