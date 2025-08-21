"""SimpleDownloader 测试用例

测试简单下载器的功能，包括速率限制、数据获取等。
"""

import os
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from pyrate_limiter import Limiter, Duration, InMemoryBucket, Rate

from src.neo.downloader.simple_downloader import SimpleDownloader
from src.neo.task_bus.types import TaskType, DownloadTaskConfig, TaskResult


class TestSimpleDownloader:
    """SimpleDownloader 测试类"""
    
    def test_init_with_default_rate_limiters(self):
        """测试使用默认速率限制器初始化"""
        downloader = SimpleDownloader()
        
        assert downloader.rate_limiters == {}
        assert downloader.fetcher_builder is not None
    
    def test_init_with_custom_rate_limiters(self):
        """测试使用自定义速率限制器初始化"""
        custom_limiters = {
            "stock_basic": Limiter(InMemoryBucket([Rate(100, Duration.MINUTE)]))
        }
        
        downloader = SimpleDownloader(rate_limiters=custom_limiters)
        
        assert downloader.rate_limiters == custom_limiters
    
    def test_get_rate_limiter_creates_new_limiter(self):
        """测试为新任务类型创建速率限制器"""
        downloader = SimpleDownloader()
        
        # 第一次获取应该创建新的限制器
        limiter1 = downloader._get_rate_limiter(TaskType.STOCK_BASIC)
        
        assert limiter1 is not None
        assert str(TaskType.STOCK_BASIC.value) in downloader.rate_limiters
        
        # 第二次获取应该返回相同的限制器
        limiter2 = downloader._get_rate_limiter(TaskType.STOCK_BASIC)
        assert limiter1 is limiter2
    
    def test_get_rate_limiter_different_task_types(self):
        """测试不同任务类型有独立的速率限制器"""
        downloader = SimpleDownloader()
        
        limiter_basic = downloader._get_rate_limiter(TaskType.STOCK_BASIC)
        limiter_daily = downloader._get_rate_limiter(TaskType.STOCK_DAILY)
        
        # 应该是不同的限制器实例
        assert limiter_basic is not limiter_daily
        assert id(limiter_basic) != id(limiter_daily)
        
        # 应该在字典中有两个键
        assert len(downloader.rate_limiters) == 2
        assert str(TaskType.STOCK_BASIC.value) in downloader.rate_limiters
        assert str(TaskType.STOCK_DAILY.value) in downloader.rate_limiters
    
    @patch('src.neo.downloader.simple_downloader.SimpleDownloader._fetch_data')
    @patch('src.neo.downloader.simple_downloader.SimpleDownloader._apply_rate_limiting')
    def test_download_success(self, mock_rate_limiting, mock_fetch_data):
        """测试成功下载"""
        # 准备测试数据
        test_data = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        mock_fetch_data.return_value = test_data
        
        downloader = SimpleDownloader()
        config = DownloadTaskConfig(task_type=TaskType.STOCK_BASIC, symbol='000001.SZ')
        
        # 执行下载
        result = downloader.download(config)
        
        # 验证结果
        assert result.success is True
        assert result.config == config
        assert result.data is test_data
        assert result.error is None
        
        # 验证调用
        mock_rate_limiting.assert_called_once_with(TaskType.STOCK_BASIC)
        mock_fetch_data.assert_called_once_with(config)
    
    @patch('src.neo.downloader.simple_downloader.SimpleDownloader._fetch_data')
    @patch('src.neo.downloader.simple_downloader.SimpleDownloader._apply_rate_limiting')
    def test_download_failure(self, mock_rate_limiting, mock_fetch_data):
        """测试下载失败"""
        # 模拟异常
        test_error = Exception("Test error")
        mock_fetch_data.side_effect = test_error
        
        downloader = SimpleDownloader()
        config = DownloadTaskConfig(task_type=TaskType.STOCK_BASIC, symbol='000001.SZ')
        
        # 执行下载
        result = downloader.download(config)
        
        # 验证结果
        assert result.success is False
        assert result.config == config
        assert result.data is None
        assert result.error == test_error
        
        # 验证调用
        mock_rate_limiting.assert_called_once_with(TaskType.STOCK_BASIC)
        mock_fetch_data.assert_called_once_with(config)
    
    def test_apply_rate_limiting(self):
        """测试速率限制应用"""
        # 创建模拟的限制器
        mock_limiter = Mock()
        
        downloader = SimpleDownloader()
        downloader.rate_limiters[str(TaskType.STOCK_BASIC.value)] = mock_limiter
        
        # 应用速率限制
        downloader._apply_rate_limiting(TaskType.STOCK_BASIC)
        
        # 验证限制器被调用
        mock_limiter.try_acquire.assert_called_once_with(TaskType.STOCK_BASIC.value, 1)
    
    @patch('src.neo.downloader.simple_downloader.FetcherBuilder')
    def test_fetch_data_success(self, mock_fetcher_builder_class):
        """测试数据获取成功"""
        # 准备测试数据
        test_data = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        
        # 模拟 FetcherBuilder
        mock_fetcher = Mock(return_value=test_data)
        mock_builder_instance = Mock()
        mock_builder_instance.build_by_task.return_value = mock_fetcher
        mock_fetcher_builder_class.return_value = mock_builder_instance
        
        downloader = SimpleDownloader()
        config = DownloadTaskConfig(task_type=TaskType.STOCK_BASIC, symbol='000001.SZ')
        
        # 执行数据获取
        result = downloader._fetch_data(config)
        
        # 验证结果
        assert result is test_data
        
        # 验证调用
        mock_builder_instance.build_by_task.assert_called_once_with(
            task_type=TaskType.STOCK_BASIC,
            symbol='000001.SZ'
        )
        mock_fetcher.assert_called_once()
    
    @patch('src.neo.downloader.simple_downloader.FetcherBuilder')
    def test_fetch_data_failure(self, mock_fetcher_builder_class):
        """测试数据获取失败"""
        # 模拟异常
        test_error = Exception("Fetch error")
        mock_fetcher = Mock(side_effect=test_error)
        mock_builder_instance = Mock()
        mock_builder_instance.build_by_task.return_value = mock_fetcher
        mock_fetcher_builder_class.return_value = mock_builder_instance
        
        downloader = SimpleDownloader()
        config = DownloadTaskConfig(task_type=TaskType.STOCK_BASIC, symbol='000001.SZ')
        
        # 执行数据获取应该抛出异常
        with pytest.raises(Exception) as exc_info:
            downloader._fetch_data(config)
        
        assert exc_info.value == test_error
    
    def test_rate_limiter_isolation(self):
        """测试速率限制器隔离性"""
        downloader = SimpleDownloader()
        
        # 获取不同任务类型的限制器
        limiter1 = downloader._get_rate_limiter(TaskType.STOCK_BASIC)
        limiter2 = downloader._get_rate_limiter(TaskType.STOCK_DAILY)
        limiter3 = downloader._get_rate_limiter(TaskType.DAILY_BASIC)
        
        # 验证每个任务类型都有独立的限制器
        assert limiter1 is not limiter2
        assert limiter2 is not limiter3
        assert limiter1 is not limiter3
        
        # 验证字典中有正确的键
        expected_keys = {
            str(TaskType.STOCK_BASIC.value),
            str(TaskType.STOCK_DAILY.value),
            str(TaskType.DAILY_BASIC.value)
        }
        actual_keys = set(downloader.rate_limiters.keys())
        assert actual_keys == expected_keys
    
    @patch('src.neo.downloader.simple_downloader.SimpleDownloader._fetch_data')
    def test_download_with_empty_data(self, mock_fetch_data):
        """测试下载空数据"""
        # 返回空的DataFrame
        mock_fetch_data.return_value = pd.DataFrame()
        
        downloader = SimpleDownloader()
        config = DownloadTaskConfig(task_type=TaskType.STOCK_BASIC, symbol='000001.SZ')
        
        # 执行下载
        result = downloader.download(config)
        
        # 验证结果
        assert result.success is True
        assert result.config == config
        assert isinstance(result.data, pd.DataFrame)
        assert len(result.data) == 0
        assert result.error is None
    
    @patch('src.neo.downloader.simple_downloader.SimpleDownloader._fetch_data')
    def test_download_with_none_data(self, mock_fetch_data):
        """测试下载返回None数据"""
        # 返回None
        mock_fetch_data.return_value = None
        
        downloader = SimpleDownloader()
        config = DownloadTaskConfig(task_type=TaskType.STOCK_BASIC, symbol='000001.SZ')
        
        # 执行下载
        result = downloader.download(config)
        
        # 验证结果
        assert result.success is True
        assert result.config == config
        assert result.data is None
        assert result.error is None