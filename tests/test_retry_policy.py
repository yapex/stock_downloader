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
    RetryPolicy, BackoffStrategy, RetryLogger, retry_logger,
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


class TestRetryLogger:
    """测试重试日志记录器"""
    
    @pytest.fixture
    def temp_log_file(self):
        """创建临时日志文件"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            temp_path = f.name
        yield temp_path
        # 清理
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    
    def test_log_and_read_symbols(self, temp_log_file):
        """测试记录和读取股票代码"""
        logger = RetryLogger(temp_log_file)
        
        # 记录失败的股票代码
        logger.log_failed_symbol("600519")
        logger.log_failed_symbol("000001")
        
        # 读取股票代码
        symbols = logger.read_symbols()
        assert "600519" in symbols
        assert "000001" in symbols
        assert len(symbols) == 2
    
    def test_log_missing_symbols(self, temp_log_file):
        """测试批量记录缺失的股票代码"""
        logger = RetryLogger(temp_log_file)
        
        missing_symbols = ["600519", "000001", "000002"]
        logger.log_missing_symbols(missing_symbols)
        
        # 读取股票代码
        symbols = logger.read_symbols()
        for symbol in missing_symbols:
            assert symbol in symbols
        assert len(symbols) == 3
    
    def test_clear_symbols(self, temp_log_file):
        """测试清空股票代码"""
        logger = RetryLogger(temp_log_file)
        
        # 先记录一些股票代码
        logger.log_failed_symbol("600519")
        logger.log_failed_symbol("000001")
        
        # 清空
        logger.clear_symbols()
        
        # 验证已清空
        symbols = logger.read_symbols()
        assert len(symbols) == 0
    
    def test_remove_symbols(self, temp_log_file):
        """测试移除指定的股票代码"""
        logger = RetryLogger(temp_log_file)
        
        # 记录股票代码
        symbols_to_log = ["600519", "000001", "000002", "600036"]
        logger.log_missing_symbols(symbols_to_log)
        
        # 移除部分股票代码
        symbols_to_remove = ["600519", "000002"]
        logger.remove_symbols(symbols_to_remove)
        
        # 验证剩余的股票代码
        remaining_symbols = logger.read_symbols()
        assert "000001" in remaining_symbols
        assert "600036" in remaining_symbols
        assert "600519" not in remaining_symbols
        assert "000002" not in remaining_symbols
        assert len(remaining_symbols) == 2


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
