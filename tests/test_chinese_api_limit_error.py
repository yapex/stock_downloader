#!/usr/bin/env python3
"""
测试中文API限流错误的识别和重试机制
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.downloader.error_handler import (
    classify_error, 
    ErrorCategory, 
    API_LIMIT_RETRY_STRATEGY,
    enhanced_retry
)


class TestChineseApiLimitError:
    """测试中文API限流错误处理"""
    
    def test_chinese_rate_limit_error_classification(self):
        """测试中文限流错误是否被正确分类为API_LIMIT"""
        # 实际的tushare中文限流错误信息
        error_msg = "抱歉，您每分钟最多访问该接口200次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。"
        error = Exception(error_msg)
        
        category = classify_error(error)
        assert category == ErrorCategory.API_LIMIT
    
    def test_partial_chinese_rate_limit_error_classification(self):
        """测试部分中文限流错误信息的分类"""
        error_msg = "每分钟最多访问该接口200次"
        error = Exception(error_msg)
        
        category = classify_error(error)
        assert category == ErrorCategory.API_LIMIT
    
    def test_api_limit_retry_strategy_recognizes_chinese_error(self):
        """测试API限流重试策略能识别中文错误"""
        error_msg = "抱歉，您每分钟最多访问该接口200次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。"
        error = Exception(error_msg)
        
        # 测试第一次重试
        should_retry = API_LIMIT_RETRY_STRATEGY.should_retry(error, 0)
        assert should_retry == True
        
        # 测试第二次重试
        should_retry = API_LIMIT_RETRY_STRATEGY.should_retry(error, 1)
        assert should_retry == True
        
        # 测试第三次重试
        should_retry = API_LIMIT_RETRY_STRATEGY.should_retry(error, 2)
        assert should_retry == True
        
        # 测试超过最大重试次数
        should_retry = API_LIMIT_RETRY_STRATEGY.should_retry(error, 3)
        assert should_retry == False
    
    def test_api_limit_retry_delay(self):
        """测试API限流重试延迟计算"""
        # 测试延迟计算
        delay_0 = API_LIMIT_RETRY_STRATEGY.get_delay(0)
        delay_1 = API_LIMIT_RETRY_STRATEGY.get_delay(1)
        delay_2 = API_LIMIT_RETRY_STRATEGY.get_delay(2)
        
        # API_LIMIT_RETRY_STRATEGY 使用 base_delay=10.0, backoff_factor=2.0
        assert delay_0 == 10.0
        assert delay_1 == 20.0
        assert delay_2 == 40.0
    
    def test_mixed_language_error_patterns(self):
        """测试混合语言的错误模式"""
        test_cases = [
            ("抱歉，您每分钟最多访问该接口200次", ErrorCategory.API_LIMIT),
            ("每分钟最多访问该接口", ErrorCategory.API_LIMIT),
            ("rate limit exceeded", ErrorCategory.API_LIMIT),
            ("too many requests", ErrorCategory.API_LIMIT),
            ("quota exceeded", ErrorCategory.API_LIMIT),
            ("频次限制", ErrorCategory.API_LIMIT),
            ("Connection failed", ErrorCategory.NETWORK),
            ("Invalid parameter", ErrorCategory.PARAMETER),
        ]
        
        for error_msg, expected_category in test_cases:
            error = Exception(error_msg)
            category = classify_error(error)
            assert category == expected_category, f"错误信息 '{error_msg}' 应该被分类为 {expected_category}，实际为 {category}"
    
    def test_enhanced_retry_with_chinese_api_limit_error(self):
        """测试enhanced_retry装饰器处理中文API限流错误"""
        call_count = 0
        
        @enhanced_retry(strategy=API_LIMIT_RETRY_STRATEGY, task_name="测试任务")
        def mock_api_call():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise Exception("抱歉，您每分钟最多访问该接口200次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。")
            return "success"
        
        # 应该重试并最终成功
        result = mock_api_call()
        assert result == "success"
        assert call_count == 3  # 第一次失败，第二次失败，第三次成功


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])