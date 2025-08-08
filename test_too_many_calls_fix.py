#!/usr/bin/env python3
"""
测试"too many calls"错误的修复效果
"""

import time
from unittest.mock import patch, MagicMock
from src.downloader.error_handler import (
    classify_error, 
    ErrorCategory, 
    enhanced_retry, 
    API_LIMIT_RETRY_STRATEGY
)
from src.downloader.fetcher import TushareFetcher


def test_too_many_calls_error_classification():
    """测试"too many calls"错误是否被正确分类为API_LIMIT"""
    error = Exception("too many calls")
    category = classify_error(error)
    print(f"错误分类结果: {category}")
    assert category == ErrorCategory.API_LIMIT, f"期望API_LIMIT，实际得到{category}"
    print("✓ 错误分类测试通过")


def test_api_limit_retry_strategy_includes_too_many_calls():
    """测试API_LIMIT_RETRY_STRATEGY是否包含"too many calls"模式"""
    strategy = API_LIMIT_RETRY_STRATEGY
    error = Exception("too many calls")
    
    # 测试第一次重试
    should_retry_first = strategy.should_retry(error, 0)
    print(f"第一次重试判断: {should_retry_first}")
    assert should_retry_first, "应该允许第一次重试"
    
    # 测试第二次重试
    should_retry_second = strategy.should_retry(error, 1)
    print(f"第二次重试判断: {should_retry_second}")
    assert should_retry_second, "应该允许第二次重试"
    
    # 测试第三次重试
    should_retry_third = strategy.should_retry(error, 2)
    print(f"第三次重试判断: {should_retry_third}")
    assert should_retry_third, "应该允许第三次重试"
    
    # 测试超过最大重试次数
    should_retry_exceed = strategy.should_retry(error, 3)
    print(f"超过最大重试次数判断: {should_retry_exceed}")
    assert not should_retry_exceed, "超过最大重试次数应该不允许重试"
    
    print("✓ API限制重试策略测试通过")


@patch('time.sleep')  # 避免实际睡眠
def test_enhanced_retry_with_too_many_calls(mock_sleep):
    """测试enhanced_retry装饰器处理"too many calls"错误的完整流程"""
    call_count = 0
    
    @enhanced_retry(strategy=API_LIMIT_RETRY_STRATEGY, task_name="测试财务报表")
    def mock_api_call():
        nonlocal call_count
        call_count += 1
        
        if call_count <= 2:
            raise Exception("too many calls")
        else:
            return {"data": "success"}
    
    # 执行测试
    result = mock_api_call()
    
    print(f"总调用次数: {call_count}")
    print(f"睡眠调用次数: {mock_sleep.call_count}")
    print(f"返回结果: {result}")
    
    # 验证结果
    assert call_count == 3, f"期望调用3次，实际调用{call_count}次"
    assert mock_sleep.call_count == 2, f"期望睡眠2次，实际睡眠{mock_sleep.call_count}次"
    assert result == {"data": "success"}, "应该返回成功结果"
    
    print("✓ enhanced_retry装饰器测试通过")


@patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
@patch('src.downloader.fetcher.ts')
@patch('time.sleep')  # 避免实际睡眠
def test_fetcher_income_with_too_many_calls_retry(mock_sleep, mock_ts):
    """测试TushareFetcher的fetch_income方法处理"too many calls"错误"""
    # 模拟tushare初始化
    mock_pro = MagicMock()
    mock_ts.pro_api.return_value = mock_pro
    mock_ts.set_token.return_value = None
    mock_pro.trade_cal.return_value = MagicMock()
    
    # 模拟API调用：前两次失败，第三次成功
    call_count = 0
    def mock_income_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise Exception("too many calls")
        else:
            import pandas as pd
            return pd.DataFrame({'ts_code': ['000001.SZ'], 'ann_date': ['20240331']})
    
    mock_pro.income.side_effect = mock_income_call
    
    # 创建fetcher并调用
    fetcher = TushareFetcher()
    result = fetcher.fetch_income('000001.SZ', '20240101', '20241231')
    
    print(f"API调用次数: {call_count}")
    print(f"睡眠调用次数: {mock_sleep.call_count}")
    print(f"返回结果类型: {type(result)}")
    
    # 验证结果
    assert call_count == 3, f"期望API调用3次，实际调用{call_count}次"
    assert mock_sleep.call_count == 2, f"期望睡眠2次，实际睡眠{mock_sleep.call_count}次"
    assert result is not None, "应该返回数据"
    assert len(result) == 1, "应该返回1条记录"
    
    print("✓ TushareFetcher重试机制测试通过")


if __name__ == "__main__":
    print("开始测试'too many calls'错误修复...\n")
    
    try:
        test_too_many_calls_error_classification()
        print()
        
        test_api_limit_retry_strategy_includes_too_many_calls()
        print()
        
        test_enhanced_retry_with_too_many_calls()
        print()
        
        test_fetcher_income_with_too_many_calls_retry()
        print()
        
        print("🎉 所有测试通过！'too many calls'错误修复成功！")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()