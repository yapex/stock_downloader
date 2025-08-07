"""
测试智能重试策略和死信处理机制
"""

import json
import pytest
import tempfile
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.downloader.retry_policy import (
    RetryPolicy, BackoffStrategy, DeadLetterRecord, DeadLetterLogger,
    DEFAULT_RETRY_POLICY, NETWORK_RETRY_POLICY, API_LIMIT_RETRY_POLICY
)
from src.downloader.models import DownloadTask, TaskType, Priority


class TestRetryPolicy:
    """测试重试策略"""
    
    def test_default_policy_creation(self):
        """测试默认重试策略创建"""
        policy = RetryPolicy()
        assert policy.max_attempts == 3
        assert policy.backoff == BackoffStrategy.EXPONENTIAL
        assert policy.base_delay == 1.0
        assert policy.max_delay == 60.0
    
    def test_should_retry_network_error(self):
        """测试网络错误的重试判断"""
        policy = NETWORK_RETRY_POLICY
        
        # 网络错误应该重试
        network_error = Exception("Connection timeout")
        assert policy.should_retry(network_error, 1) == True
        assert policy.should_retry(network_error, 3) == True
        assert policy.should_retry(network_error, 6) == False  # 超过max_attempts
    
    def test_should_retry_non_retryable_error(self):
        """测试不可重试错误"""
        policy = DEFAULT_RETRY_POLICY
        
        # 400错误不应该重试
        client_error = Exception("400 Bad Request")
        assert policy.should_retry(client_error, 1) == False
        
        # 参数错误不应该重试
        param_error = Exception("Invalid parameter: symbol")
        assert policy.should_retry(param_error, 1) == False
    
    def test_delay_calculation(self):
        """测试延迟计算"""
        # 指数退避
        exponential_policy = RetryPolicy(
            backoff=BackoffStrategy.EXPONENTIAL,
            base_delay=1.0,
            backoff_factor=2.0,
            max_delay=10.0
        )
        
        assert exponential_policy.get_delay(1) == 1.0  # 1.0 * 2^0
        assert exponential_policy.get_delay(2) == 2.0  # 1.0 * 2^1
        assert exponential_policy.get_delay(3) == 4.0  # 1.0 * 2^2
        assert exponential_policy.get_delay(5) == 10.0  # 受max_delay限制
        
        # 线性退避
        linear_policy = RetryPolicy(
            backoff=BackoffStrategy.LINEAR,
            base_delay=2.0,
            backoff_factor=1.5
        )
        
        assert linear_policy.get_delay(1) == 3.0   # 2.0 * (1 * 1.5)
        assert linear_policy.get_delay(2) == 6.0   # 2.0 * (2 * 1.5)
        assert linear_policy.get_delay(3) == 9.0   # 2.0 * (3 * 1.5)
        
        # 固定延迟
        fixed_policy = RetryPolicy(
            backoff=BackoffStrategy.FIXED,
            base_delay=5.0
        )
        
        assert fixed_policy.get_delay(1) == 5.0
        assert fixed_policy.get_delay(2) == 5.0
        assert fixed_policy.get_delay(10) == 5.0
    
    def test_serialization(self):
        """测试序列化和反序列化"""
        original = RetryPolicy(
            max_attempts=5,
            backoff=BackoffStrategy.LINEAR,
            base_delay=2.0,
            max_delay=30.0,
            backoff_factor=1.5
        )
        
        # 序列化
        data = original.to_dict()
        assert data['max_attempts'] == 5
        assert data['backoff'] == 'linear'
        assert data['base_delay'] == 2.0
        
        # 反序列化
        restored = RetryPolicy.from_dict(data)
        assert restored.max_attempts == original.max_attempts
        assert restored.backoff == original.backoff
        assert restored.base_delay == original.base_delay
        assert restored.backoff_factor == original.backoff_factor


class TestDeadLetterRecord:
    """测试死信记录"""
    
    def create_sample_task(self):
        """创建示例任务"""
        return DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={"start_date": "20230101", "end_date": "20231231"},
            priority=Priority.NORMAL,
            retry_count=2,
            max_retries=3
        )
    
    def test_from_task(self):
        """测试从任务创建死信记录"""
        task = self.create_sample_task()
        error = Exception("Network connection failed")
        
        record = DeadLetterRecord.from_task(task, error)
        
        assert record.task_id == task.task_id
        assert record.symbol == task.symbol
        assert record.task_type == task.task_type.value
        assert record.params == task.params
        assert record.priority == task.priority.value
        assert record.retry_count == task.retry_count
        assert record.error_message == str(error)
        assert record.error_type == "Exception"
    
    def test_to_task(self):
        """测试转换回任务对象"""
        task = self.create_sample_task()
        error = Exception("Network connection failed")
        
        record = DeadLetterRecord.from_task(task, error)
        restored_task = record.to_task()
        
        assert restored_task.symbol == task.symbol
        assert restored_task.task_type == task.task_type
        assert restored_task.params == task.params
        assert restored_task.priority == task.priority
        assert restored_task.retry_count == 0  # 重置为0
        assert restored_task.task_id == task.task_id
    
    def test_serialization(self):
        """测试序列化和反序列化"""
        task = self.create_sample_task()
        error = Exception("Test error")
        
        original = DeadLetterRecord.from_task(task, error)
        
        # 序列化
        data = original.to_dict()
        assert data['symbol'] == task.symbol
        assert data['task_type'] == task.task_type.value
        assert data['error_message'] == "Test error"
        
        # 反序列化
        restored = DeadLetterRecord.from_dict(data)
        assert restored.symbol == original.symbol
        assert restored.task_type == original.task_type
        assert restored.error_message == original.error_message
        assert restored.failed_at == original.failed_at


class TestDeadLetterLogger:
    """测试死信日志管理器"""
    
    def create_sample_task(self):
        """创建示例任务"""
        return DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={"start_date": "20230101", "end_date": "20231231"}
        )
    
    @pytest.fixture
    def temp_log_file(self):
        """临时日志文件"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.jsonl') as f:
            yield f.name
        # 清理
        Path(f.name).unlink(missing_ok=True)
    
    def test_write_and_read(self, temp_log_file):
        """测试写入和读取死信记录"""
        logger = DeadLetterLogger(temp_log_file)
        
        # 创建测试数据
        task1 = self.create_sample_task()
        task2 = DownloadTask(
            symbol="000002.SZ",
            task_type=TaskType.DAILY_BASIC,
            params={"start_date": "20230101"}
        )
        
        error1 = Exception("Network error")
        error2 = Exception("API limit exceeded")
        
        # 写入记录
        logger.write_dead_letter(task1, error1)
        logger.write_dead_letter(task2, error2)
        
        # 读取记录
        records = logger.read_dead_letters()
        
        assert len(records) == 2
        assert records[0].symbol == task1.symbol
        assert records[1].symbol == task2.symbol
        assert records[0].error_message == "Network error"
        assert records[1].error_message == "API limit exceeded"
    
    def test_read_with_filters(self, temp_log_file):
        """测试使用过滤器读取"""
        logger = DeadLetterLogger(temp_log_file)
        
        # 创建不同类型的任务
        daily_task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={}
        )
        
        basic_task = DownloadTask(
            symbol="000002.SZ",
            task_type=TaskType.DAILY_BASIC,
            params={}
        )
        
        logger.write_dead_letter(daily_task, Exception("Error 1"))
        logger.write_dead_letter(basic_task, Exception("Error 2"))
        
        # 按任务类型过滤
        daily_records = logger.read_dead_letters(task_type="daily")
        assert len(daily_records) == 1
        assert daily_records[0].task_type == "daily"
        
        # 按股票代码过滤
        symbol_records = logger.read_dead_letters(symbol_pattern="000001")
        assert len(symbol_records) == 1
        assert symbol_records[0].symbol == "000001.SZ"
        
        # 限制数量
        limited_records = logger.read_dead_letters(limit=1)
        assert len(limited_records) == 1
    
    def test_convert_to_tasks(self, temp_log_file):
        """测试转换死信记录为任务列表"""
        logger = DeadLetterLogger(temp_log_file)
        
        # 创建并写入测试任务
        original_task = self.create_sample_task()
        logger.write_dead_letter(original_task, Exception("Test error"))
        
        # 读取并转换
        records = logger.read_dead_letters()
        tasks = logger.convert_to_tasks(records)
        
        assert len(tasks) == 1
        
        restored_task = tasks[0]
        assert restored_task.symbol == original_task.symbol
        assert restored_task.task_type == original_task.task_type
        assert restored_task.params == original_task.params
        assert restored_task.retry_count == 0  # 重置
    
    def test_get_statistics(self, temp_log_file):
        """测试统计信息"""
        logger = DeadLetterLogger(temp_log_file)
        
        # 空文件统计
        stats = logger.get_statistics()
        assert stats['total_count'] == 0
        
        # 添加一些记录
        task1 = DownloadTask(symbol="000001.SZ", task_type=TaskType.DAILY, params={})
        task2 = DownloadTask(symbol="000002.SZ", task_type=TaskType.DAILY, params={})
        task3 = DownloadTask(symbol="000003.SZ", task_type=TaskType.DAILY_BASIC, params={})
        
        logger.write_dead_letter(task1, ConnectionError("Network error"))
        logger.write_dead_letter(task2, TimeoutError("Request timeout"))
        logger.write_dead_letter(task3, ValueError("Invalid parameter"))
        
        stats = logger.get_statistics()
        
        assert stats['total_count'] == 3
        assert stats['by_task_type']['daily'] == 2
        assert stats['by_task_type']['daily_basic'] == 1
        assert stats['by_error_type']['ConnectionError'] == 1
        assert stats['by_error_type']['TimeoutError'] == 1
        assert stats['by_error_type']['ValueError'] == 1
        assert len(stats['recent_failures']) <= 10
    
    def test_archive_processed(self, temp_log_file):
        """测试归档已处理的记录"""
        logger = DeadLetterLogger(temp_log_file)
        
        # 创建测试任务
        task1 = self.create_sample_task()
        task2 = DownloadTask(symbol="000002.SZ", task_type=TaskType.DAILY, params={})
        
        logger.write_dead_letter(task1, Exception("Error 1"))
        logger.write_dead_letter(task2, Exception("Error 2"))
        
        # 读取记录
        records = logger.read_dead_letters()
        assert len(records) == 2
        
        # 归档第一个任务
        processed_ids = [records[0].task_id]
        logger.archive_processed(processed_ids)
        
        # 验证只剩下一个记录
        remaining_records = logger.read_dead_letters()
        assert len(remaining_records) == 1
        assert remaining_records[0].task_id == records[1].task_id


class TestPredefinedPolicies:
    """测试预定义的重试策略"""
    
    def test_default_policy(self):
        """测试默认策略"""
        policy = DEFAULT_RETRY_POLICY
        
        assert policy.max_attempts == 3
        assert policy.backoff == BackoffStrategy.EXPONENTIAL
        
        # 应该重试网络错误
        network_error = Exception("Connection failed")
        assert policy.should_retry(network_error, 1) == True
        
        # 不应该重试参数错误
        param_error = Exception("Invalid parameter")
        assert policy.should_retry(param_error, 1) == False
    
    def test_network_policy(self):
        """测试网络重试策略"""
        policy = NETWORK_RETRY_POLICY
        
        assert policy.max_attempts == 5
        assert policy.backoff == BackoffStrategy.EXPONENTIAL
        
        # 网络相关错误应该重试
        network_errors = [
            Exception("Connection timeout"),
            Exception("HTTPConnectionPool error"),
            Exception("SSLError occurred")
        ]
        
        for error in network_errors:
            assert policy.should_retry(error, 1) == True
    
    def test_api_limit_policy(self):
        """测试API限制重试策略"""
        policy = API_LIMIT_RETRY_POLICY
        
        assert policy.max_attempts == 3
        assert policy.backoff == BackoffStrategy.LINEAR
        
        # API限制错误应该重试
        api_errors = [
            Exception("rate limit exceeded"),
            Exception("quota exceeded"),
            Exception("too many requests")
        ]
        
        for error in api_errors:
            assert policy.should_retry(error, 1) == True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
