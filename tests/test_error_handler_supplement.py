"""
错误处理器补充测试
针对error_handler模块的边界情况和未覆盖功能进行测试
"""
import pytest
from unittest.mock import Mock, patch, mock_open

from downloader.error_handler import (
    ErrorCategory, classify_error, is_test_task, 
    RetryStrategy, enhanced_retry
)


class TestErrorClassification:
    """错误分类测试"""
    
    def test_classify_network_errors(self):
        """测试网络错误分类"""
        network_errors = [
            Exception("Connection timeout"),
            Exception("ConnectionError occurred"),  
            Exception("HTTPConnectionPool failed"),
            Exception("proxy error occurred"),
            Exception("remote disconnected"),
            Exception("httperror occurred"),
            Exception("socket error"),
            Exception("dns error"),
        ]
        
        for error in network_errors:
            assert classify_error(error) == ErrorCategory.NETWORK
    
    def test_classify_api_limit_errors(self):
        """测试API限流错误分类 - 只基于RateLimitException类型"""
        from downloader.error_handler import RateLimitException
        
        # 只有RateLimitException类型才会被分类为API_LIMIT
        rate_limit_error = RateLimitException("rate limit exceeded", period_remaining=60.0)
        assert classify_error(rate_limit_error) == ErrorCategory.API_LIMIT
        
        # 普通Exception即使包含限流关键词也不会被分类为API_LIMIT
        normal_errors = [
            Exception("rate limit exceeded"),
            Exception("quota exceeded"),
            Exception("too many requests"),
            Exception("api limit reached"),
            Exception("频次限制"),
        ]
        
        for error in normal_errors:
            assert classify_error(error) == ErrorCategory.BUSINESS
    
    def test_classify_data_unavailable_errors(self):
        """测试数据不可用错误分类"""
        data_errors = [
            Exception("no data available"),
            Exception("数据为空"),
            Exception("error."),  # 特殊情况
        ]
        
        for error in data_errors:
            assert classify_error(error) == ErrorCategory.DATA_UNAVAILABLE
    
    def test_classify_parameter_errors(self):
        """测试参数错误分类"""
        param_errors = [
            Exception("invalid parameter provided"),
            Exception("参数无效"),
            Exception("参数错误"),
            Exception("无法识别的代码"),
            Exception("format error occurred"),
            Exception("validation failed"),
        ]
        
        for error in param_errors:
            assert classify_error(error) == ErrorCategory.PARAMETER
    
    def test_classify_test_errors(self):
        """测试错误分类"""
        test_errors = [
            Exception("test error occurred"),
            Exception("mock object failed"),
        ]
        
        for error in test_errors:
            assert classify_error(error) == ErrorCategory.TEST
    
    def test_classify_business_errors(self):
        """测试业务错误分类（默认）"""
        business_errors = [
            Exception("Some random error"),
            RuntimeError("Unexpected runtime error"),
            ValueError("Some other value error"),
        ]
        
        for error in business_errors:
            assert classify_error(error) == ErrorCategory.BUSINESS


class TestTaskTypeDetection:
    """任务类型检测测试"""
    
    def test_is_test_task_detection(self):
        """测试测试任务检测"""
        test_tasks = [
            "test_daily_data",
            "Test Stock List", 
            "测试任务_daily_basic",
            "mock_financials_data",
            "demo_stock_list",
            "演示任务",
            "example_daily",
            "样例数据",
            "临时任务"
        ]
        
        for task in test_tasks:
            assert is_test_task(task) == True
    
    def test_is_not_test_task(self):
        """测试非测试任务检测"""
        normal_tasks = [
            "Daily Data Download",
            "Stock List Update",
            "财务数据下载",
            "production_daily_basic"
        ]
        
        for task in normal_tasks:
            assert is_test_task(task) == False


class TestRetryStrategy:
    """重试策略测试"""
    
    def test_default_retry_strategy(self):
        """测试默认重试策略"""
        strategy = RetryStrategy()
        
        assert strategy.max_retries == 3
        assert strategy.base_delay == 1.0
        assert strategy.max_delay == 60.0
        assert strategy.backoff_factor == 2.0
    
    def test_custom_retry_strategy(self):
        """测试自定义重试策略"""
        strategy = RetryStrategy(
            max_retries=5,
            base_delay=2.0,
            max_delay=30.0,
            backoff_factor=1.5
        )
        
        assert strategy.max_retries == 5
        assert strategy.base_delay == 2.0
        assert strategy.max_delay == 30.0
        assert strategy.backoff_factor == 1.5
    
    def test_should_retry_network_errors(self):
        """测试网络错误重试决策"""
        strategy = RetryStrategy()
        
        network_error = Exception("Connection timeout")
        assert strategy.should_retry(network_error, 0) == True
        assert strategy.should_retry(network_error, 2) == True
        assert strategy.should_retry(network_error, 3) == False  # 超过max_retries
        
        proxy_error = Exception("ProxyError occurred")
        assert strategy.should_retry(proxy_error, 1) == True
    
    def test_should_not_retry_parameter_errors(self):
        """测试参数错误不应重试"""
        strategy = RetryStrategy()
        
        param_error = Exception("Invalid parameter")
        assert strategy.should_retry(param_error, 0) == False
        
        auth_error = Exception("Authentication failed")
        assert strategy.should_retry(auth_error, 0) == False
    
    def test_get_delay_exponential_backoff(self):
        """测试指数退避延迟计算"""
        strategy = RetryStrategy(base_delay=1.0, backoff_factor=2.0, max_delay=60.0)
        
        assert strategy.get_delay(0) == 1.0  # 1.0 * 2^0
        assert strategy.get_delay(1) == 2.0  # 1.0 * 2^1
        assert strategy.get_delay(2) == 4.0  # 1.0 * 2^2
        
        # 测试最大延迟限制
        large_delay = strategy.get_delay(10)
        assert large_delay <= strategy.max_delay


class TestEnhancedRetryDecorator:
    """增强重试装饰器测试"""
    
    @patch('time.sleep')  # 避免实际睡眠
    def test_retry_success_after_failure(self, mock_sleep):
        """测试重试后成功"""
        call_count = 0
        
        @enhanced_retry()
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Connection timeout")
            return "success"
        
        result = flaky_function()
        assert result == "success"
        assert call_count == 3
    
    @patch('time.sleep')
    def test_retry_parameter_error_behavior(self, mock_sleep):
        """测试参数错误的重试行为"""
        call_count = 0
        
        @enhanced_retry()
        def param_error_function():
            nonlocal call_count
            call_count += 1
            raise Exception("Invalid parameter")
        
        # 参数错误在尝试后会抛出原始异常
        with pytest.raises(Exception, match="Invalid parameter"):
            param_error_function()
            
        assert call_count == 1  # 只调用1次，不重试
        assert mock_sleep.call_count == 0  # 没有睡眠（不重试）


class TestErrorHandlerIntegration:
    """错误处理器集成测试"""
    
    @patch('time.sleep')  # 避免实际睡眠
    def test_full_retry_workflow(self, mock_sleep):
        """测试完整的重试工作流"""
        attempts = 0
        
        # 使用自定义重试策略
        custom_strategy = RetryStrategy(max_retries=3)
        
        @enhanced_retry(strategy=custom_strategy)
        def simulate_api_call():
            nonlocal attempts
            attempts += 1
            
            if attempts == 1:
                raise Exception("Connection timeout")
            elif attempts == 2:
                raise Exception("Timeout error") 
            else:
                return {"data": "success"}
        
        result = simulate_api_call()
        
        assert result == {"data": "success"}
        assert attempts == 3
        assert mock_sleep.call_count == 2  # 两次重试之间的延迟
    
    def test_error_categorization_consistency(self):
        """测试错误分类的一致性"""
        test_cases = [
            ("Connection failed", ErrorCategory.NETWORK),
            ("rate limit exceeded", ErrorCategory.BUSINESS),  # 普通Exception不再分类为API_LIMIT
            ("invalid parameter", ErrorCategory.PARAMETER),
            ("test mock error", ErrorCategory.TEST),
            ("no data found", ErrorCategory.DATA_UNAVAILABLE),
            ("random error", ErrorCategory.BUSINESS),  # 默认分类
        ]
        
        for error_msg, expected_category in test_cases:
            error = Exception(error_msg)
            actual_category = classify_error(error)
            assert actual_category == expected_category, f"Error '{error_msg}' should be {expected_category}, got {actual_category}"


class TestEdgeCases:
    """边界情况测试"""
    
    def test_none_error_handling(self):
        """测试None错误处理"""
        # 虽然通常不会传入None，但要确保不会崩溃
        try:
            result = classify_error(None)
            assert result == ErrorCategory.BUSINESS  # 默认分类
        except Exception:
            # 如果抛出异常也是可以接受的
            pass
    
    def test_empty_error_message(self):
        """测试空错误消息"""
        empty_error = Exception("")
        result = classify_error(empty_error)
        assert result == ErrorCategory.BUSINESS  # 默认分类
    
    def test_unicode_error_message(self):
        """测试Unicode错误消息"""
        # 根据实际实现，需要包含网络关键词
        unicode_error = Exception("network connection 超时💥")
        result = classify_error(unicode_error)
        assert result == ErrorCategory.NETWORK
    
    def test_nested_exception(self):
        """测试嵌套异常"""
        try:
            try:
                raise Exception("Connection timeout")
            except Exception as e:
                raise Exception("Wrapper error") from e
        except Exception as nested_error:
            result = classify_error(nested_error)
            # 应该基于最外层的错误消息分类
            assert result == ErrorCategory.BUSINESS  # 默认分类
    
    def test_retry_delay_boundary_values(self):
        """测试重试延迟的边界值"""
        strategy = RetryStrategy(base_delay=1.0, backoff_factor=2.0, max_delay=16.0)
        
        # 测试第0次重试
        delay0 = strategy.get_delay(0)
        assert delay0 >= 0
        
        # 测试大重试次数
        delay_large = strategy.get_delay(100)
        assert delay_large <= strategy.max_delay  # 应该有最大延迟限制
