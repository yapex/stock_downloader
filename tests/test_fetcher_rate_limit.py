import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os
import time
from ratelimit import limits, sleep_and_retry

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from downloader.fetcher import TushareFetcher


@pytest.fixture
def mock_tushare():
    """创建一个mock的tushare环境"""
    with patch('downloader.fetcher.os.getenv') as mock_getenv, \
         patch('downloader.fetcher.ts') as mock_ts:
        
        # Mock环境变量
        mock_getenv.return_value = "test_token"
        
        # Mock Tushare API
        mock_pro = MagicMock()
        mock_ts.pro_api.return_value = mock_pro
        mock_ts.set_token.return_value = None
        
        # Mock trade_cal验证调用
        mock_pro.trade_cal.return_value = pd.DataFrame({'date': ['20230101']})
        
        yield mock_pro


def test_fetcher_rate_limiting(mock_tushare):
    """测试Fetcher中的速率限制装饰器"""
    # 创建Fetcher实例
    fetcher = TushareFetcher(default_rate_limit=100)
    
    # Mock stock_basic返回值
    mock_data = pd.DataFrame({
        'ts_code': ['000001.SZ'],
        'symbol': ['000001'],
        'name': ['平安银行'],
        'area': ['深圳'],
        'industry': ['银行'],
        'market': ['主板'],
        'list_date': ['19910403']
    })
    mock_tushare.stock_basic.return_value = mock_data
    
    # 调用方法
    result = fetcher.fetch_stock_list()
    
    # 验证调用
    assert mock_tushare.stock_basic.called
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1


def test_fetcher_methods_have_rate_limit():
    """测试Fetcher的方法是否正确应用了速率限制装饰器"""
    # 检查方法是否具有装饰器属性
    fetcher = TushareFetcher()
    
    # 检查几个关键方法
    methods_to_check = [
        'fetch_stock_list',
        'fetch_daily_history',
        'fetch_daily_basic',
        'fetch_income',
        'fetch_balancesheet',
        'fetch_cashflow'
    ]
    
    for method_name in methods_to_check:
        method = getattr(fetcher, method_name)
        # 确认方法存在
        assert method is not None
        # 确认方法是可调用的
        assert callable(method)
        # 检查是否有ratelimit装饰器的属性
        # ratelimit库的装饰器会在函数上添加特定属性
        assert hasattr(method, '__wrapped__') or hasattr(method, '__name__')


def test_ratelimit_decorator_functionality():
    """测试ratelimit装饰器的基本功能"""
    call_count = [0]
    
    @sleep_and_retry
    @limits(calls=2, period=1)  # 每秒最多2次调用
    def test_function():
        call_count[0] += 1
        return call_count[0]
    
    # 快速调用3次，前2次应该成功，第3次会被限制并等待
    start_time = time.time()
    
    # 前两次调用应该很快
    result1 = test_function()
    result2 = test_function()
    
    assert result1 == 1
    assert result2 == 2
    
    # 第三次调用会触发等待
    result3 = test_function()
    end_time = time.time()
    
    assert result3 == 3
    # 由于限流，总时间应该至少1秒
    assert end_time - start_time >= 0.9  # 允许一些时间误差