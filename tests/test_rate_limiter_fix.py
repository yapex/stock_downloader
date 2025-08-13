"""测试ratelimit库的有效性"""

from unittest.mock import patch, MagicMock
from src.downloader.fetcher import TushareFetcher


class TestRateLimitFix:
    """测试ratelimit库修复"""
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('src.downloader.fetcher.ts')
    def test_fetcher_initialization(self, mock_ts):
        """测试TushareFetcher初始化正确"""
        # 模拟tushare初始化
        mock_pro = MagicMock()
        mock_ts.pro_api.return_value = mock_pro
        mock_ts.set_token.return_value = None
        mock_pro.trade_cal.return_value = MagicMock()
        
        fetcher = TushareFetcher("test_token")
        
        # 验证tushare API初始化
        mock_ts.set_token.assert_called_once_with("test_token")
        mock_ts.pro_api.assert_called_once()
        
        # 验证fetcher实例创建成功
        assert fetcher is not None
        assert hasattr(fetcher, 'pro')
        assert fetcher.pro == mock_pro
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('src.downloader.fetcher.ts')
    def test_rate_limit_decorators_exist(self, mock_ts):
        """测试所有API方法都有ratelimit装饰器"""
        # 模拟tushare初始化
        mock_pro = MagicMock()
        mock_ts.pro_api.return_value = mock_pro
        mock_ts.set_token.return_value = None
        mock_pro.trade_cal.return_value = MagicMock()
        
        fetcher = TushareFetcher("test_token")
        
        # 检查关键方法是否存在ratelimit装饰器
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
            # 检查方法是否被装饰器包装
            assert hasattr(method, '__wrapped__'), f"{method_name} 应该有ratelimit装饰器"
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('src.downloader.fetcher.ts')
    def test_api_methods_work_with_rate_limiting(self, mock_ts):
        """测试API方法在ratelimit装饰器下正常工作"""
        # 模拟tushare初始化
        mock_pro = MagicMock()
        mock_ts.pro_api.return_value = mock_pro
        mock_ts.set_token.return_value = None
        mock_pro.trade_cal.return_value = MagicMock()
        
        # 模拟API返回数据
        import pandas as pd
        stock_data = pd.DataFrame({'ts_code': ['000001.SZ'], 'name': ['测试股票']})
        financial_data = pd.DataFrame({'ts_code': ['000001.SZ'], 'ann_date': ['20240331']})
        mock_pro.stock_basic.return_value = stock_data
        mock_pro.income.return_value = financial_data
        mock_pro.balancesheet.return_value = financial_data
        mock_pro.cashflow.return_value = financial_data
        
        fetcher = TushareFetcher("test_token")
        
        # 测试股票列表获取
        result = fetcher.fetch_stock_list()
        assert result is not None
        mock_pro.stock_basic.assert_called_once()
        
        # 测试财务数据获取
        result = fetcher.fetch_income('000001.SZ', '20240101', '20241231')
        assert result is not None
        mock_pro.income.assert_called_once()