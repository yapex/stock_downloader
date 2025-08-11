#!/usr/bin/env python3
"""
测试简化版重试机制
"""

import pytest
import time
from unittest.mock import patch, MagicMock
from src.downloader.simple_retry_simplified import simple_retry, NON_RETRYABLE_PATTERNS


class TestSimplifiedRetry:
    """测试简化版重试装饰器"""
    
    def test_successful_call_no_retry(self):
        """测试成功调用，无需重试"""
        call_count = 0
        
        @simple_retry(max_retries=3, task_name="测试任务")
        def successful_func():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = successful_func()
        assert result == "success"
        assert call_count == 1
    
    def test_retry_on_network_error(self):
        """测试网络错误重试"""
        call_count = 0
        
        @simple_retry(max_retries=2, task_name="网络测试")
        def network_error_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("网络连接失败")
            return "success after retry"
        
        result = network_error_func()
        assert result == "success after retry"
        assert call_count == 3
    
    def test_max_retries_exceeded(self):
        """测试超过最大重试次数"""
        call_count = 0
        
        @simple_retry(max_retries=2, task_name="失败测试")
        def always_fail_func():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("总是失败")
        
        result = always_fail_func()
        assert result is None  # 返回 None 表示失败
        assert call_count == 3  # 初始调用 + 2次重试
    
    def test_non_retryable_error_patterns(self):
        """测试不可重试的错误模式"""
        for pattern in ['invalid parameter', '参数无效', 'authentication failed', '401']:
            call_count = 0
            
            @simple_retry(max_retries=3, task_name="参数错误测试")
            def param_error_func():
                nonlocal call_count
                call_count += 1
                raise ValueError(f"错误包含: {pattern}")
            
            with pytest.raises(ValueError):
                param_error_func()
            
            assert call_count == 1  # 不应该重试
    
    def test_entity_id_extraction_from_kwargs(self):
        """测试从kwargs提取实体ID"""
        
        @simple_retry(max_retries=1, task_name="实体测试")
        def func_with_ts_code(ts_code: str):
            raise RuntimeError("测试错误")
        
        result = func_with_ts_code(ts_code="000001.SZ")
        assert result is None
    
    def test_entity_id_extraction_from_args(self):
        """测试从args提取实体ID"""
        
        @simple_retry(max_retries=1, task_name="参数测试")
        def func_with_args(code, other_param):
            raise RuntimeError("测试错误")
        
        result = func_with_args("600000.SH", "other")
        assert result is None
    
    def test_retry_delay_timing(self):
        """测试重试延迟时间"""
        call_times = []
        
        @simple_retry(max_retries=2, task_name="延迟测试")
        def delayed_func():
            call_times.append(time.time())
            if len(call_times) < 3:
                raise RuntimeError("需要重试")
            return "success"
        
        start_time = time.time()
        result = delayed_func()
        end_time = time.time()
        
        assert result == "success"
        assert len(call_times) == 3
        
        # 验证延迟时间（大约2秒，允许一些误差）
        total_time = end_time - start_time
        assert 1.8 <= total_time <= 2.5  # 2次重试，每次1秒延迟
    
    def test_function_metadata_preservation(self):
        """测试函数元数据保留"""
        
        @simple_retry(max_retries=1, task_name="元数据测试")
        def test_function_with_metadata():
            """这是一个测试函数"""
            return "test"
        
        assert test_function_with_metadata.__name__ == "test_function_with_metadata"
        assert test_function_with_metadata.__doc__ == "这是一个测试函数"
    
    def test_different_error_types(self):
        """测试不同类型的错误处理"""
        
        # 网络相关错误 - 应该重试
        @simple_retry(max_retries=1, task_name="网络错误")
        def network_error():
            raise ConnectionError("网络错误")
        
        result = network_error()
        assert result is None
        
        # API 临时错误 - 应该重试
        @simple_retry(max_retries=1, task_name="API错误")
        def api_error():
            raise RuntimeError("服务器内部错误")
        
        result = api_error()
        assert result is None
        
        # 参数错误 - 不应该重试
        @simple_retry(max_retries=3, task_name="参数错误")
        def param_error():
            raise ValueError("invalid parameter: 无效的股票代码")
        
        with pytest.raises(ValueError):
            param_error()
    
    @patch('src.downloader.simple_retry_simplified.logger')
    def test_logging_behavior(self, mock_logger):
        """测试日志记录行为"""
        
        @simple_retry(max_retries=1, task_name="日志测试")
        def failing_func():
            raise RuntimeError("测试错误")
        
        result = failing_func()
        
        # 验证日志调用
        assert mock_logger.warning.called
        assert mock_logger.error.called
        assert result is None
    
    def test_non_retryable_patterns_coverage(self):
        """测试所有不可重试模式的覆盖"""
        patterns_to_test = [
            'invalid parameter',
            '参数无效',
            '参数错误', 
            '无法识别',
            'authentication failed',
            'permission denied',
            'unauthorized',
            '401',
            '403'
        ]
        
        for pattern in patterns_to_test:
            call_count = 0
            
            @simple_retry(max_retries=2, task_name=f"模式测试-{pattern}")
            def pattern_test_func():
                nonlocal call_count
                call_count += 1
                raise ValueError(f"包含模式: {pattern}")
            
            with pytest.raises(ValueError):
                pattern_test_func()
            
            assert call_count == 1, f"模式 '{pattern}' 应该不重试，但调用了 {call_count} 次"