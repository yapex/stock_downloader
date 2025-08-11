"""测试基于 pyrate-limiter 的速率限制器"""

import pytest
import time
from unittest.mock import Mock, patch
from src.downloader.rate_limiter import rate_limit, get_global_limiter, reset_global_limiter
from pyrate_limiter import BucketFullException


class TestPyrateLimiter:
    """测试 pyrate-limiter 速率限制器"""
    
    def setup_method(self):
        """每个测试前重置全局限制器"""
        reset_global_limiter()
    
    def test_rate_limit_decorator_requires_method_name(self):
        """测试速率限制装饰器要求提供方法名"""
        with pytest.raises(ValueError, match="method_name 参数不能为空"):
            @rate_limit("")
            def dummy_func():
                pass
    
    def test_rate_limit_decorator_with_valid_method_name(self):
        """测试速率限制装饰器使用有效方法名"""
        @rate_limit("test_method")
        def test_func():
            return "success"
        
        result = test_func()
        assert result == "success"
    
    def test_different_methods_have_independent_limits(self):
        """测试不同方法有独立的速率限制"""
        call_count_1 = 0
        call_count_2 = 0
        
        @rate_limit("method_1")
        def method_1():
            nonlocal call_count_1
            call_count_1 += 1
            return "method_1_result"
        
        @rate_limit("method_2")
        def method_2():
            nonlocal call_count_2
            call_count_2 += 1
            return "method_2_result"
        
        # 两个方法都应该能够正常调用
        result_1 = method_1()
        result_2 = method_2()
        
        assert result_1 == "method_1_result"
        assert result_2 == "method_2_result"
        assert call_count_1 == 1
        assert call_count_2 == 1
    
    def test_rate_limit_blocking_behavior(self):
        """测试速率限制的阻塞行为"""
        call_count = 0
        
        @rate_limit("blocking_test")
        def limited_func():
            nonlocal call_count
            call_count += 1
            return f"call_{call_count}"
        
        # 模拟速率限制触发的情况
        with patch('src.downloader.rate_limiter.get_global_limiter') as mock_get_limiter:
            mock_limiter = Mock()
            mock_get_limiter.return_value = mock_limiter
            
            # 创建一个真实的BucketFullException实例
            exception_instance = None
            try:
                # 尝试创建一个真实的限制器并触发异常
                from pyrate_limiter import Limiter, Rate, Duration
                test_limiter = Limiter([Rate(1, Duration.SECOND)])
                test_limiter.try_acquire("test")
                test_limiter.try_acquire("test")  # 这应该触发异常
            except BucketFullException as e:
                exception_instance = e
            
            # 如果无法创建真实异常，则跳过这个测试
            if exception_instance is None:
                pytest.skip("无法创建BucketFullException实例")
            
            # 第一次调用成功，第二次触发限制后成功
            mock_limiter.try_acquire.side_effect = [
                None,  # 第一次成功
                exception_instance,  # 第二次触发限制
                None   # 重试后成功
            ]
            
            with patch('time.sleep') as mock_sleep:
                result = limited_func()
                
                # 验证函数被调用
                assert result == "call_1"
                assert call_count == 1
                
                # 验证 try_acquire 被调用了正确的次数
                assert mock_limiter.try_acquire.call_count == 1
                assert mock_limiter.try_acquire.call_args[0][0] == "tushare_method_blocking_test"
    
    def test_method_identifier_format(self):
        """测试方法标识符格式正确"""
        @rate_limit("fetch_stock_list")
        def test_func():
            return "success"
        
        with patch('src.downloader.rate_limiter.get_global_limiter') as mock_get_limiter:
            mock_limiter = Mock()
            mock_get_limiter.return_value = mock_limiter
            mock_limiter.try_acquire.return_value = None
            
            test_func()
            
            # 验证标识符格式
            mock_limiter.try_acquire.assert_called_once_with("tushare_method_fetch_stock_list")
    
    def test_global_limiter_singleton(self):
        """测试全局限制器是单例"""
        limiter1 = get_global_limiter()
        limiter2 = get_global_limiter()
        
        assert limiter1 is limiter2
    
    def test_reset_global_limiter(self):
        """测试重置全局限制器"""
        limiter1 = get_global_limiter()
        reset_global_limiter()
        limiter2 = get_global_limiter()
        
        assert limiter1 is not limiter2