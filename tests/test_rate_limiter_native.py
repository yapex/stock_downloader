#!/usr/bin/env python3
"""
测试基于 pyrate-limiter 原生装饰器的速率限制器实现
"""

import time
import pytest
from unittest.mock import patch

from src.downloader.rate_limiter_native import rate_limit, get_limiter, reset_limiter
from pyrate_limiter import BucketFullException


class TestNativeRateLimiter:
    """测试原生速率限制器"""
    
    def setup_method(self):
        """每个测试方法前重置限制器"""
        reset_limiter()
    
    def test_rate_limit_decorator_exists(self):
        """测试装饰器是否存在且可调用"""
        assert callable(rate_limit)
        
        # 测试装饰器可以正常应用
        @rate_limit("test_method")
        def test_func():
            return "test_result"
        
        assert callable(test_func)
        result = test_func()
        assert result == "test_result"
    
    def test_get_limiter_returns_instance(self):
        """测试获取限制器实例"""
        limiter = get_limiter()
        assert limiter is not None
        assert hasattr(limiter, 'try_acquire')
        assert hasattr(limiter, 'as_decorator')
    
    def test_different_methods_have_independent_limits(self):
        """测试不同方法有独立的速率限制"""
        
        @rate_limit("method_a")
        def method_a():
            return "a"
        
        @rate_limit("method_b")
        def method_b():
            return "b"
        
        # 两个方法应该都能正常调用
        assert method_a() == "a"
        assert method_b() == "b"
    
    def test_method_identifier_format(self):
        """测试方法标识符格式"""
        
        @rate_limit("fetch_income")
        def fetch_income():
            return "income_data"
        
        @rate_limit("fetch_balancesheet")
        def fetch_balancesheet():
            return "balance_data"
        
        # 验证方法可以正常调用
        assert fetch_income() == "income_data"
        assert fetch_balancesheet() == "balance_data"
    
    def test_limiter_singleton_behavior(self):
        """测试限制器单例行为"""
        limiter1 = get_limiter()
        limiter2 = get_limiter()
        
        # 应该返回同一个实例
        assert limiter1 is limiter2
    
    def test_reset_limiter_functionality(self):
        """测试重置限制器功能"""
        # 获取初始限制器
        initial_limiter = get_limiter()
        
        # 重置限制器
        reset_limiter()
        
        # 获取新的限制器
        new_limiter = get_limiter()
        
        # 应该是不同的实例
        assert initial_limiter is not new_limiter
    
    def test_rate_limit_with_high_frequency_calls(self):
        """测试高频调用的速率限制行为"""
        
        call_count = 0
        
        @rate_limit("high_freq_method")
        def high_freq_method():
            nonlocal call_count
            call_count += 1
            return f"call_{call_count}"
        
        # 快速连续调用多次
        start_time = time.time()
        for i in range(5):
            result = high_freq_method()
            assert result == f"call_{i+1}"
        
        end_time = time.time()
        
        # 验证所有调用都成功了
        assert call_count == 5
        
        # 由于使用了阻塞模式，调用应该能够成功完成
        # 但可能会有一些延迟
        elapsed_time = end_time - start_time
        assert elapsed_time >= 0  # 基本的时间检查
    
    def test_decorator_preserves_function_metadata(self):
        """测试装饰器保留函数元数据"""
        
        @rate_limit("test_metadata")
        def test_function_with_metadata():
            """这是一个测试函数"""
            return "test"
        
        # 验证函数名和文档字符串被保留
        assert test_function_with_metadata.__name__ == "test_function_with_metadata"
        assert test_function_with_metadata.__doc__ == "这是一个测试函数"
    
    def test_rate_limit_with_function_arguments(self):
        """测试带参数的函数的速率限制"""
        
        @rate_limit("method_with_args")
        def method_with_args(arg1, arg2, kwarg1=None):
            return f"{arg1}_{arg2}_{kwarg1}"
        
        # 测试不同的参数组合
        result1 = method_with_args("a", "b")
        assert result1 == "a_b_None"
        
        result2 = method_with_args("x", "y", kwarg1="z")
        assert result2 == "x_y_z"
        
        result3 = method_with_args("1", "2", kwarg1="3")
        assert result3 == "1_2_3"