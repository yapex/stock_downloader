import pytest
from unittest.mock import MagicMock
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

    def test_rate_limit_with_ratelimit_library(self, mock_fetcher, mock_storage):
        """测试使用ratelimit库的限流功能"""
        task_config = {
            "name": "Test Daily",
            "type": "daily",
            "adjust": "qfq",
            "rate_limit": {
                "calls_per_minute": 100
            }
        }
        
        mock_data = pd.DataFrame({
            "trade_date": ["20230101"],
            "open": [10.0]
        })
        mock_fetcher.fetch_daily_history.return_value = mock_data
        
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        
        # 执行单个股票处理
        success = handler._process_single_symbol("000001.SZ")
        
        # 验证fetcher被调用（ratelimit库会自动处理限流）
        assert mock_fetcher.fetch_daily_history.called
        assert success is True

    def test_no_rate_limit_configuration(self, mock_fetcher, mock_storage):
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
        
        # 执行单个股票处理
        success = handler._process_single_symbol("000001.SZ")
        
        # 验证fetcher被直接调用（ratelimit库会自动处理限流）
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
            
            success = handler._process_single_symbol("000001.SZ")
            
            assert success is False
            
            # 重置mock
            mock_fetcher.reset_mock()

    def test_non_network_error_handling(self, mock_fetcher, mock_storage):
        """测试非网络错误处理"""
        task_config = {"name": "Test", "type": "daily", "adjust": "none"}
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        
        # 模拟非网络错误
        mock_fetcher.fetch_daily_history.side_effect = ValueError("Invalid parameter")
        
        success = handler._process_single_symbol("000001.SZ")
        
        assert success is False

    def test_empty_data_handling(self, mock_fetcher, mock_storage):
        """测试空数据返回处理"""
        task_config = {"name": "Test", "type": "daily", "adjust": "none"}
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        
        # 测试返回空DataFrame
        mock_fetcher.fetch_daily_history.return_value = pd.DataFrame()
        success = handler._process_single_symbol("000001.SZ")
        
        assert success is True
        # 空数据不应该保存
        mock_storage.save.assert_not_called()
        
        # 测试返回None
        mock_fetcher.fetch_daily_history.return_value = None  
        success = handler._process_single_symbol("000001.SZ")
        
        assert success is False
        mock_storage.save.assert_not_called()

    def test_task_execution_with_different_symbols(self, mock_fetcher, mock_storage):
        """测试不同股票代码的任务执行"""
        task_config = {
            "name": "Test Daily",
            "type": "daily",
            "adjust": "qfq",
            "rate_limit": {"calls_per_minute": 100}
        }
        
        mock_data = pd.DataFrame({
            "trade_date": ["20230101"],
            "open": [10.0]
        })
        mock_fetcher.fetch_daily_history.return_value = mock_data
        
        handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
        
        # 测试不同股票代码的调用
        success1 = handler._process_single_symbol("000001.SZ")
        success2 = handler._process_single_symbol("000002.SZ")
        
        # 验证两次调用都成功
        assert success1 is True
        assert success2 is True
        assert mock_fetcher.fetch_daily_history.call_count == 2
