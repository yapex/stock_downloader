#!/usr/bin/env python3
"""
可配置速率限制器的测试用例
"""

import time
import pytest
from pyrate_limiter import Rate, Duration

from src.downloader.rate_limiter_native import rate_limit, get_limiter, reset_limiter


class TestConfigurableRateLimiter:
    """可配置速率限制器测试类"""
    
    def setup_method(self):
        """每个测试方法前重置限制器"""
        reset_limiter()
    
    def test_default_rate_limit(self):
        """测试默认速率限制"""
        @rate_limit("test_default")
        def test_func():
            return "result"
        
        # 默认限制很宽松，应该能快速执行多次
        start_time = time.time()
        for _ in range(3):
            result = test_func()
            assert result == "result"
        end_time = time.time()
        
        # 默认限制下，3次调用应该很快完成（小于1秒）
        assert end_time - start_time < 1.0
    
    def test_custom_rate_limit(self):
        """测试自定义速率限制"""
        # 设置严格的速率限制：2次/3秒
        custom_rate = Rate(2, Duration.SECOND * 3)
        
        @rate_limit("test_custom", custom_rate)
        def test_func():
            return "custom_result"
        
        # 前两次调用应该很快
        start_time = time.time()
        result1 = test_func()
        result2 = test_func()
        quick_time = time.time() - start_time
        
        assert result1 == "custom_result"
        assert result2 == "custom_result"
        assert quick_time < 1.0  # 前两次应该很快
        
        # 第三次调用应该被阻塞
        start_time = time.time()
        result3 = test_func()
        blocked_time = time.time() - start_time
        
        assert result3 == "custom_result"
        assert blocked_time >= 2.5  # 应该被阻塞至少2.5秒
    
    def test_different_custom_rates(self):
        """测试不同的自定义速率限制"""
        rate1 = Rate(1, Duration.SECOND * 2)  # 1次/2秒
        rate2 = Rate(2, Duration.SECOND * 2)  # 2次/2秒
        
        @rate_limit("method1", rate1)
        def method1():
            return "method1_result"
        
        @rate_limit("method2", rate2)
        def method2():
            return "method2_result"
        
        # method1 只能调用1次，第二次会被阻塞
        start_time = time.time()
        result1 = method1()
        quick_time1 = time.time() - start_time
        assert result1 == "method1_result"
        assert quick_time1 < 0.5
        
        # method2 可以快速调用2次
        start_time = time.time()
        result2a = method2()
        result2b = method2()
        quick_time2 = time.time() - start_time
        assert result2a == "method2_result"
        assert result2b == "method2_result"
        assert quick_time2 < 0.5
    
    def test_get_limiter_with_custom_rate(self):
        """测试获取自定义速率的限制器"""
        custom_rate = Rate(5, Duration.SECOND * 10)
        
        # 获取默认限制器
        default_limiter = get_limiter()
        assert default_limiter is not None
        
        # 获取自定义速率的限制器
        custom_limiter = get_limiter(custom_rate)
        assert custom_limiter is not None
        
        # 两个限制器应该是不同的实例
        assert default_limiter is not custom_limiter
        
        # 再次获取相同速率的限制器应该返回同一个实例
        same_custom_limiter = get_limiter(custom_rate)
        assert custom_limiter is same_custom_limiter
    
    def test_rate_key_generation(self):
        """测试速率键的生成"""
        rate1 = Rate(10, Duration.SECOND * 5)
        rate2 = Rate(10, Duration.SECOND * 5)  # 相同的速率
        rate3 = Rate(20, Duration.SECOND * 5)  # 不同的速率
        
        limiter1 = get_limiter(rate1)
        limiter2 = get_limiter(rate2)
        limiter3 = get_limiter(rate3)
        
        # 相同速率应该返回同一个限制器
        assert limiter1 is limiter2
        
        # 不同速率应该返回不同的限制器
        assert limiter1 is not limiter3
    
    def test_reset_clears_custom_limiters(self):
        """测试重置功能清除自定义限制器"""
        custom_rate = Rate(3, Duration.SECOND * 5)
        
        # 创建自定义限制器
        limiter1 = get_limiter(custom_rate)
        assert limiter1 is not None
        
        # 重置
        reset_limiter()
        
        # 重新获取应该是新的实例
        limiter2 = get_limiter(custom_rate)
        assert limiter2 is not None
        assert limiter1 is not limiter2
    
    def test_backward_compatibility(self):
        """测试向后兼容性"""
        # 不提供速率参数应该使用默认行为
        @rate_limit("backward_compat")
        def old_style_func():
            return "old_style_result"
        
        result = old_style_func()
        assert result == "old_style_result"
        
        # 应该使用默认限制器
        default_limiter = get_limiter()
        assert default_limiter is not None
    
    def test_function_metadata_preservation(self):
        """测试函数元数据保持"""
        custom_rate = Rate(5, Duration.SECOND * 10)
        
        @rate_limit("test_metadata", custom_rate)
        def documented_func():
            """这是一个有文档的函数"""
            return "documented_result"
        
        # 检查函数名和文档字符串是否保持
        assert documented_func.__name__ == "documented_func"
        assert documented_func.__doc__ == "这是一个有文档的函数"
        
        # 检查函数功能
        result = documented_func()
        assert result == "documented_result"
    
    def test_multiple_decorators_same_rate(self):
        """测试多个装饰器使用相同速率"""
        shared_rate = Rate(4, Duration.SECOND * 5)  # 5秒内允许4次调用
        
        @rate_limit("func_a", shared_rate)
        def func_a():
            return "a"
        
        @rate_limit("func_b", shared_rate)
        def func_b():
            return "b"
        
        # 两个函数应该共享同一个限制器实例
        limiter_a = get_limiter(shared_rate)
        limiter_b = get_limiter(shared_rate)
        assert limiter_a is limiter_b
        
        # 测试每个函数都有独立的配额
        # 每个函数在5秒内可以调用4次
        start_time = time.time()
        results_a = [func_a() for _ in range(2)]  # func_a 调用2次
        results_b = [func_b() for _ in range(2)]  # func_b 调用2次
        total_time = time.time() - start_time
        
        assert all(r == "a" for r in results_a)
        assert all(r == "b" for r in results_b)
        assert total_time < 2.0  # 4次调用应该都能快速完成