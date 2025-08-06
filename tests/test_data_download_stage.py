import pytest
from unittest.mock import MagicMock, patch, Mock
import pandas as pd
from downloader.tasks.daily import DailyTaskHandler
from downloader.tasks.daily_basic import DailyBasicTaskHandler


class TestDataDownloadStage:
    """测试数据下载阶段的核心功能"""

    @pytest.fixture
    def mock_fetcher(self):
        """创建mock的fetcher"""
        fetcher = MagicMock()
        return fetcher
    
    @pytest.fixture  
    def mock_storage(self):
        """创建mock的storage"""
        storage = MagicMock()
        storage.get_latest_date.return_value = None  # 默认无历史数据
        return storage

    def test_rate_limit_wrapper_applied_when_configured(self, mock_fetcher, mock_storage):
        """测试当task_config配置了rate_limit时，_fetch_data被正确封装"""
        task_config = {
            "name": "Test Daily",
            "type": "daily", 
            "adjust": "qfq",
            "rate_limit": {
                "calls_per_minute": 100
            }
        }
        
        # 模拟fetcher返回数据
        mock_data = pd.DataFrame({
            "trade_date": ["20230101", "20230102"],
            "open": [10.0, 10.5]
        })
        mock_fetcher.fetch_daily_history.return_value = mock_data
        
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        
        # 使用patch监控rate_limit装饰器的应用
        with patch('downloader.tasks.base.rate_limit') as mock_rate_limit:
            # 让rate_limit装饰器返回原函数
            mock_rate_limit.return_value = lambda func: func
            
            # 执行单个股票处理
            success, is_network_error = handler._process_single_symbol("000001.SZ", is_retry=False)
            
            # 验证rate_limit装饰器被调用，参数正确
            mock_rate_limit.assert_called_once_with(
                calls_per_minute=100, 
                task_key="Test Daily_000001.SZ"
            )
            
        # 验证fetcher被调用
        assert mock_fetcher.fetch_daily_history.called
        assert success is True
        assert is_network_error is False

    def test_rate_limit_wrapper_not_applied_when_not_configured(self, mock_fetcher, mock_storage):
        """测试当task_config未配置rate_limit时，直接调用fetcher方法"""
        task_config = {
            "name": "Test Daily",
            "type": "daily",
            "adjust": "qfq"
            # 没有rate_limit配置
        }
        
        mock_data = pd.DataFrame({
            "trade_date": ["20230101"],
            "open": [10.0]
        })
        mock_fetcher.fetch_daily_history.return_value = mock_data
        
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        
        with patch('downloader.tasks.base.rate_limit') as mock_rate_limit:
            # 执行单个股票处理
            success, is_network_error = handler._process_single_symbol("000001.SZ", is_retry=False)
            
            # 验证rate_limit装饰器未被调用
            mock_rate_limit.assert_not_called()
            
        # 验证fetcher被直接调用
        assert mock_fetcher.fetch_daily_history.called
        assert success is True

    def test_fetch_data_method_calls_correct_fetcher(self, mock_fetcher, mock_storage):
        """测试fetch_data方法调用对应的fetcher方法"""
        
        # 测试DailyTaskHandler
        daily_config = {"name": "Daily", "type": "daily", "adjust": "qfq"}
        daily_handler = DailyTaskHandler(daily_config, mock_fetcher, mock_storage)
        
        daily_handler.fetch_data("000001.SZ", "20230101", "20230131")
        mock_fetcher.fetch_daily_history.assert_called_with(
            "000001.SZ", "20230101", "20230131", "qfq"
        )
        
        # 测试DailyBasicTaskHandler  
        basic_config = {"name": "DailyBasic", "type": "daily_basic"}
        basic_handler = DailyBasicTaskHandler(basic_config, mock_fetcher, mock_storage)
        
        basic_handler.fetch_data("000001.SZ", "20230101", "20230131")
        mock_fetcher.fetch_daily_basic.assert_called_with(
            "000001.SZ", "20230101", "20230131"
        )

    def test_network_error_detection_and_retry_staging(self, mock_fetcher, mock_storage):
        """测试网络错误检测和暂存待重试机制"""
        task_config = {"name": "Test", "type": "daily", "adjust": "none"}
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        
        # 模拟网络错误
        network_errors = [
            ConnectionError("Network connection failed"),
            TimeoutError("Request timeout"), 
            Exception("SSL certificate error"),
            Exception("Name or service not known")
        ]
        
        for error in network_errors:
            mock_fetcher.fetch_daily_history.side_effect = error
            
            success, is_network_error = handler._process_single_symbol("000001.SZ", is_retry=False)
            
            assert success is False
            assert is_network_error is True
            
            # 重置mock
            mock_fetcher.reset_mock()

    def test_non_network_error_handling(self, mock_fetcher, mock_storage):
        """测试非网络错误处理"""
        task_config = {"name": "Test", "type": "daily", "adjust": "none"}
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        
        # 模拟非网络错误
        mock_fetcher.fetch_daily_history.side_effect = ValueError("Invalid parameter")
        
        success, is_network_error = handler._process_single_symbol("000001.SZ", is_retry=False)
        
        assert success is False
        assert is_network_error is False

    def test_empty_data_handling(self, mock_fetcher, mock_storage):
        """测试空数据返回处理"""
        task_config = {"name": "Test", "type": "daily", "adjust": "none"}
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        
        # 测试返回空DataFrame
        mock_fetcher.fetch_daily_history.return_value = pd.DataFrame()
        success, is_network_error = handler._process_single_symbol("000001.SZ", is_retry=False)
        
        assert success is True
        assert is_network_error is False
        # 空数据不应该保存
        mock_storage.save.assert_not_called()
        
        # 测试返回None
        mock_fetcher.fetch_daily_history.return_value = None  
        success, is_network_error = handler._process_single_symbol("000001.SZ", is_retry=False)
        
        assert success is True
        assert is_network_error is False
        mock_storage.save.assert_not_called()

    def test_retry_task_key_differentiation(self, mock_fetcher, mock_storage):
        """测试重试时task_key的区分"""
        task_config = {
            "name": "Test Daily",
            "type": "daily",
            "adjust": "qfq", 
            "rate_limit": {"calls_per_minute": 100}
        }
        
        mock_data = pd.DataFrame({"trade_date": ["20230101"]})
        mock_fetcher.fetch_daily_history.return_value = mock_data
        
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        
        with patch('downloader.tasks.base.rate_limit') as mock_rate_limit:
            mock_rate_limit.return_value = lambda func: func
            
            # 测试非重试调用
            handler._process_single_symbol("000001.SZ", is_retry=False)
            calls = mock_rate_limit.call_args_list
            assert calls[-1][1]['task_key'] == "Test Daily_000001.SZ"
            
            # 测试重试调用
            handler._process_single_symbol("000001.SZ", is_retry=True)  
            calls = mock_rate_limit.call_args_list
            assert calls[-1][1]['task_key'] == "Test Daily_000001.SZ_retry"
