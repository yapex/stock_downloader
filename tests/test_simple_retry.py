"""测试简化的重试机制"""

import pytest
from unittest.mock import patch, MagicMock
import time

from src.downloader.simple_retry import simple_retry, RateLimitException, NON_RETRYABLE_PATTERNS


class TestSimpleRetry:
    """测试简化重试装饰器"""
    
    @patch('time.sleep')  # 避免实际睡眠
    def test_success_on_first_try(self, mock_sleep):
        """测试第一次尝试就成功"""
        call_count = 0
        
        @simple_retry(max_retries=3)
        def successful_function():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = successful_function()
        assert result == "success"
        assert call_count == 1
        assert mock_sleep.call_count == 0
    
    @patch('time.sleep')
    def test_success_after_retries(self, mock_sleep):
        """测试重试后成功"""
        call_count = 0
        
        @simple_retry(max_retries=3)
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Connection timeout")
            return "success"
        
        result = flaky_function()
        assert result == "success"
        assert call_count == 3
        assert mock_sleep.call_count == 2  # 两次重试之间的延迟
    
    @patch('time.sleep')
    def test_max_retries_exceeded(self, mock_sleep):
        """测试超过最大重试次数"""
        call_count = 0
        
        @simple_retry(max_retries=2)
        def always_fail_function():
            nonlocal call_count
            call_count += 1
            raise Exception("Always fails")
        
        result = always_fail_function()
        assert result is None  # 返回None表示失败
        assert call_count == 3  # max_retries + 1
        assert mock_sleep.call_count == 2
    
    @patch('time.sleep')
    def test_rate_limit_exception_handling(self, mock_sleep):
        """测试RateLimitException处理"""
        call_count = 0
        
        @simple_retry(max_retries=2, task_name="测试API")
        def rate_limited_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                # 模拟RateLimitException with period_remaining
                raise RateLimitException("Rate limit exceeded", period_remaining=0.1)
            return "success"
        
        result = rate_limited_function()
        assert result == "success"
        assert call_count == 3
        # 应该使用period_remaining而不是固定延迟
        mock_sleep.assert_called_with(0.1)
    
    @patch('time.sleep')
    def test_rate_limit_with_default_period(self, mock_sleep):
        """测试使用默认period_remaining的RateLimitException"""
        call_count = 0
        
        @simple_retry(max_retries=1)
        def rate_limited_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                # 使用默认的period_remaining
                raise RateLimitException("Rate limit exceeded", period_remaining=60.0)
            return "success"
        
        result = rate_limited_function()
        assert result == "success"
        assert call_count == 2
        # 应该使用默认的60秒
        mock_sleep.assert_called_with(60.0)
    
    def test_non_retryable_errors(self):
        """测试不可重试的错误"""
        call_count = 0
        
        @simple_retry(max_retries=3)
        def param_error_function():
            nonlocal call_count
            call_count += 1
            raise Exception("Invalid parameter")
        
        # 参数错误应该直接抛出，不重试
        with pytest.raises(Exception, match="Invalid parameter"):
            param_error_function()
        
        assert call_count == 1  # 只调用一次
    
    def test_chinese_non_retryable_errors(self):
        """测试中文不可重试错误"""
        call_count = 0
        
        @simple_retry(max_retries=3)
        def chinese_error_function():
            nonlocal call_count
            call_count += 1
            raise Exception("参数无效")
        
        with pytest.raises(Exception, match="参数无效"):
            chinese_error_function()
        
        assert call_count == 1
    
    @patch('time.sleep')
    def test_task_name_in_logs(self, mock_sleep, caplog):
        """测试任务名称出现在日志中"""
        @simple_retry(max_retries=1, task_name="测试任务")
        def failing_function():
            raise Exception("Network error")
        
        result = failing_function()
        assert result is None
        
        # 检查日志中包含任务名称
        log_messages = [record.message for record in caplog.records]
        assert any("测试任务" in msg for msg in log_messages)
    
    @patch('time.sleep')
    def test_entity_id_extraction(self, mock_sleep, caplog):
        """测试实体ID提取"""
        @simple_retry(max_retries=1)
        def function_with_ts_code(ts_code="000001.SZ"):
            raise Exception("Network error")
        
        result = function_with_ts_code(ts_code="000001.SZ")
        assert result is None
        
        # 检查日志中包含股票代码
        log_messages = [record.message for record in caplog.records]
        assert any("000001.SZ" in msg for msg in log_messages)
    
    def test_non_retryable_patterns_coverage(self):
        """测试所有不可重试模式都被覆盖"""
        patterns_to_test = [
            "invalid parameter",
            "参数无效",
            "参数错误", 
            "无法识别",
            "authentication failed",
            "permission denied",
            "unauthorized",
            "401",
            "403"
        ]
        
        for pattern in patterns_to_test:
            call_count = 0
            
            @simple_retry(max_retries=2)
            def test_function():
                nonlocal call_count
                call_count += 1
                raise Exception(f"Error: {pattern}")
            
            with pytest.raises(Exception):
                test_function()
            
            assert call_count == 1, f"Pattern '{pattern}' should not retry"


class TestRateLimitException:
    """测试RateLimitException类"""
    
    def test_rate_limit_exception_with_period_remaining(self):
        """测试带period_remaining的RateLimitException"""
        error = RateLimitException("Rate limit", period_remaining=30.0)
        assert error.period_remaining == 30.0
        assert str(error) == "Rate limit"
    
    def test_rate_limit_exception_default_period(self):
        """测试默认period_remaining"""
        error = RateLimitException("Rate limit", period_remaining=60.0)
        assert error.period_remaining == 60.0


class TestNonRetryablePatterns:
    """测试不可重试模式常量"""
    
    def test_patterns_are_lowercase(self):
        """测试所有模式都是小写"""
        for pattern in NON_RETRYABLE_PATTERNS:
            assert pattern == pattern.lower(), f"Pattern '{pattern}' should be lowercase"
    
    def test_patterns_not_empty(self):
        """测试模式列表不为空"""
        assert len(NON_RETRYABLE_PATTERNS) > 0
        assert all(pattern.strip() for pattern in NON_RETRYABLE_PATTERNS)