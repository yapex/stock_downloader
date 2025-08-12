#!/usr/bin/env python3
"""
测试速率限制装饰器和重试装饰器的组合使用

验证两个装饰器能够：
1. 不互相干扰
2. 各自正常工作
3. 正确处理各种异常情况
"""

import pytest
import time
from unittest.mock import patch, MagicMock
from src.downloader.rate_limiter_native import rate_limit, reset_limiter
from src.downloader.simple_retry_simplified import simple_retry


class TestCombinedDecorators:
    """测试装饰器组合"""
    
    def setup_method(self):
        """每个测试方法前重置限制器"""
        reset_limiter()
    
    def test_both_decorators_success_case(self):
        """测试两个装饰器在成功情况下的协作"""
        call_count = 0
        
        @simple_retry(max_retries=2, task_name="成功测试")
        @rate_limit("success_method")
        def successful_method(ts_code: str):
            nonlocal call_count
            call_count += 1
            return f"success_{ts_code}_{call_count}"
        
        result = successful_method("000001.SZ")
        assert result == "success_000001.SZ_1"
        assert call_count == 1
    
    def test_retry_decorator_handles_network_errors(self):
        """测试重试装饰器处理网络错误，速率限制正常工作"""
        call_count = 0
        
        @simple_retry(max_retries=2, task_name="网络错误测试")
        @rate_limit("network_error_method")
        def network_error_method(ts_code: str):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError(f"网络错误第{call_count}次")
            return f"success_after_retry_{ts_code}"
        
        result = network_error_method("000002.SZ")
        assert result == "success_after_retry_000002.SZ"
        assert call_count == 3  # 初始调用 + 2次重试
    
    def test_retry_decorator_respects_non_retryable_errors(self):
        """测试重试装饰器正确处理不可重试错误"""
        call_count = 0
        
        @simple_retry(max_retries=3, task_name="参数错误测试")
        @rate_limit("param_error_method")
        def param_error_method(ts_code: str):
            nonlocal call_count
            call_count += 1
            raise ValueError("invalid parameter: 无效的股票代码")
        
        with pytest.raises(ValueError):
            param_error_method("INVALID")
        
        assert call_count == 1  # 不应该重试
    
    def test_rate_limit_works_with_retry_max_attempts(self):
        """测试速率限制在重试达到最大次数时仍正常工作"""
        call_count = 0
        
        @simple_retry(max_retries=2, task_name="最大重试测试")
        @rate_limit("max_retry_method")
        def always_fail_method(ts_code: str):
            nonlocal call_count
            call_count += 1
            raise RuntimeError(f"总是失败第{call_count}次")
        
        result = always_fail_method("000003.SZ")
        assert result is None  # 重试装饰器返回 None
        assert call_count == 3  # 初始调用 + 2次重试
    
    def test_different_methods_independent_rate_limits(self):
        """测试不同方法的独立速率限制在重试场景下正常工作"""
        method_a_calls = 0
        method_b_calls = 0
        
        @simple_retry(max_retries=1, task_name="方法A")
        @rate_limit("method_a")
        def method_a(ts_code: str):
            nonlocal method_a_calls
            method_a_calls += 1
            if method_a_calls == 1:
                raise ConnectionError("方法A网络错误")
            return f"method_a_success_{ts_code}"
        
        @simple_retry(max_retries=1, task_name="方法B")
        @rate_limit("method_b")
        def method_b(ts_code: str):
            nonlocal method_b_calls
            method_b_calls += 1
            return f"method_b_success_{ts_code}"
        
        # 方法A需要重试
        result_a = method_a("000001.SZ")
        assert result_a == "method_a_success_000001.SZ"
        assert method_a_calls == 2
        
        # 方法B直接成功
        result_b = method_b("000002.SZ")
        assert result_b == "method_b_success_000002.SZ"
        assert method_b_calls == 1
    
    def test_decorator_order_consistency(self):
        """测试装饰器顺序的一致性"""
        # 测试 retry -> rate_limit 顺序
        retry_first_calls = 0
        
        @simple_retry(max_retries=1, task_name="重试在前")
        @rate_limit("retry_first_method")
        def retry_first_method():
            nonlocal retry_first_calls
            retry_first_calls += 1
            return "retry_first_success"
        
        # 测试 rate_limit -> retry 顺序
        rate_first_calls = 0
        
        @rate_limit("rate_first_method")
        @simple_retry(max_retries=1, task_name="速率在前")
        def rate_first_method():
            nonlocal rate_first_calls
            rate_first_calls += 1
            return "rate_first_success"
        
        # 两种顺序都应该正常工作
        result1 = retry_first_method()
        assert result1 == "retry_first_success"
        assert retry_first_calls == 1
        
        result2 = rate_first_method()
        assert result2 == "rate_first_success"
        assert rate_first_calls == 1
    
    def test_high_frequency_calls_with_both_decorators(self):
        """测试高频调用时两个装饰器的协作"""
        call_count = 0
        
        @simple_retry(max_retries=1, task_name="高频测试")
        @rate_limit("high_freq_method")
        def high_freq_method(call_id: int):
            nonlocal call_count
            call_count += 1
            return f"call_{call_id}_count_{call_count}"
        
        # 快速连续调用
        results = []
        for i in range(5):
            result = high_freq_method(i)
            results.append(result)
        
        # 验证所有调用都成功
        assert len(results) == 5
        assert call_count == 5
        
        # 验证结果格式正确
        for i, result in enumerate(results):
            assert f"call_{i}_count_{i+1}" == result
    
    def test_function_metadata_preservation_with_both_decorators(self):
        """测试两个装饰器都保留函数元数据"""
        
        @simple_retry(max_retries=1, task_name="元数据测试")
        @rate_limit("metadata_method")
        def test_function_with_metadata(param1: str, param2: int = 10):
            """这是一个带元数据的测试函数
            
            Args:
                param1: 字符串参数
                param2: 整数参数，默认值为10
            
            Returns:
                str: 格式化的结果字符串
            """
            return f"{param1}_{param2}"
        
        # 验证函数名和文档字符串被保留
        assert test_function_with_metadata.__name__ == "test_function_with_metadata"
        assert "这是一个带元数据的测试函数" in test_function_with_metadata.__doc__
        
        # 验证函数正常工作
        result = test_function_with_metadata("test", 20)
        assert result == "test_20"
    
    @patch('src.downloader.simple_retry_simplified.logger')
    def test_logging_behavior_with_both_decorators(self, mock_logger):
        """测试两个装饰器的日志记录行为"""
        
        @simple_retry(max_retries=1, task_name="日志测试")
        @rate_limit("logging_method")
        def failing_method():
            raise RuntimeError("测试失败")
        
        result = failing_method()
        
        # 验证重试装饰器的日志调用
        assert mock_logger.warning.called
        assert mock_logger.error.called
        assert result is None
    
    def test_complex_scenario_with_mixed_errors(self):
        """测试复杂场景：混合不同类型的错误"""
        call_sequence = []
        
        @simple_retry(max_retries=3, task_name="复杂场景")
        @rate_limit("complex_method")
        def complex_method(scenario: str):
            call_sequence.append(f"call_{scenario}_{len(call_sequence)+1}")
            
            if scenario == "network_then_success":
                if len(call_sequence) == 1:
                    raise ConnectionError("网络错误")
                return "success_after_network_error"
            
            elif scenario == "param_error":
                raise ValueError("invalid parameter: 参数错误")
            
            elif scenario == "always_fail":
                raise RuntimeError("持续失败")
            
            else:
                return f"success_{scenario}"
        
        # 场景1：网络错误后成功
        call_sequence.clear()
        result1 = complex_method("network_then_success")
        assert result1 == "success_after_network_error"
        assert len(call_sequence) == 2  # 失败1次，成功1次
        
        # 场景2：参数错误（不重试）
        call_sequence.clear()
        with pytest.raises(ValueError):
            complex_method("param_error")
        assert len(call_sequence) == 1  # 只调用1次
        
        # 场景3：持续失败（达到最大重试）
        call_sequence.clear()
        result3 = complex_method("always_fail")
        assert result3 is None
        assert len(call_sequence) == 4  # 初始调用 + 3次重试
        
        # 场景4：直接成功
        call_sequence.clear()
        result4 = complex_method("direct_success")
        assert result4 == "success_direct_success"
        assert len(call_sequence) == 1  # 只调用1次