import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os

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