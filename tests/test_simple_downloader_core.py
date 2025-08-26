"""测试 SimpleDownloader 类的核心功能"""

import pandas as pd
from unittest.mock import Mock, patch

from neo.downloader.simple_downloader import SimpleDownloader
from neo.helpers.interfaces import IRateLimitManager
from neo.downloader.fetcher_builder import FetcherBuilder
from neo.task_bus.types import TaskType


class TestSimpleDownloader:
    """测试 SimpleDownloader 类基本功能"""

    def test_init(self):
        """测试初始化"""
        mock_rate_limit_manager = Mock(spec=IRateLimitManager)
        mock_fetcher_builder = Mock(spec=FetcherBuilder)
        downloader = SimpleDownloader(
            rate_limit_manager=mock_rate_limit_manager,
            fetcher_builder=mock_fetcher_builder,
        )

        assert isinstance(downloader, SimpleDownloader)
        assert downloader.rate_limit_manager is not None
        assert downloader.fetcher_builder is not None


class TestSimpleDownloaderFetchData:
    """测试 SimpleDownloader._fetch_data 方法的业务逻辑"""

    def setup_method(self):
        """测试前设置"""
        self.mock_rate_limit_manager = Mock(spec=IRateLimitManager)
        self.mock_fetcher_builder = Mock(spec=FetcherBuilder)
        self.downloader = SimpleDownloader(
            rate_limit_manager=self.mock_rate_limit_manager,
            fetcher_builder=self.mock_fetcher_builder,
        )

    def test_fetch_data_calls_builder_with_fixed_date(self):
        """测试 _fetch_data 是否使用固定的起始日期调用 builder"""
        # 模拟 fetcher 返回数据
        test_data = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240316"]})
        mock_fetcher = Mock(return_value=test_data)
        self.mock_fetcher_builder.build_by_task.return_value = mock_fetcher

        # 执行测试
        result = self.downloader._fetch_data("stock_daily", "000001.SZ")

        # 验证结果
        assert result is not None
        pd.testing.assert_frame_equal(result, test_data)

        # 验证 fetcher_builder 使用固定的起始日期被调用
        self.mock_fetcher_builder.build_by_task.assert_called_once_with(
            task_type="stock_daily", symbol="000001.SZ", start_date="19901218"
        )
        mock_fetcher.assert_called_once()

    def test_fetch_data_fetcher_exception(self):
        """测试 fetcher 抛出异常的场景"""
        # 模拟 fetcher 抛出异常
        mock_fetcher = Mock(side_effect=Exception("网络错误"))
        self.mock_fetcher_builder.build_by_task.return_value = mock_fetcher

        # 执行测试，应该重新抛出异常
        with patch.object(pd, 'Timestamp', side_effect=Exception("Should not be called")):
             try:
                self.downloader._fetch_data("stock_daily", "000001.SZ")
                assert False, "应该重新抛出异常"
             except Exception as e:
                assert str(e) == "网络错误"


class TestSimpleDownloaderDownload:
    """测试 SimpleDownloader.download 方法"""

    def setup_method(self):
        """测试前设置"""
        self.mock_rate_limit_manager = Mock(spec=IRateLimitManager)
        self.mock_fetcher_builder = Mock(spec=FetcherBuilder)
        self.downloader = SimpleDownloader(
            rate_limit_manager=self.mock_rate_limit_manager,
            fetcher_builder=self.mock_fetcher_builder,
        )

    def test_download_success(self):
        """测试成功下载"""
        # 模拟 fetcher_builder 返回一个函数，该函数返回数据
        mock_fetcher = Mock(return_value=pd.DataFrame({'a': [1]}))
        self.mock_fetcher_builder.build_by_task.return_value = mock_fetcher
        
        result = self.downloader.download("stock_basic", "000001.SZ")
        
        assert result is not None
        self.mock_fetcher_builder.build_by_task.assert_called_once_with("stock_basic", symbol="000001.SZ")
        mock_fetcher.assert_called_once()

    def test_download_rate_limiting_exception(self):
        """测试 fetcher_builder 抛出异常"""
        # 模拟 fetcher_builder 抛出异常
        self.mock_fetcher_builder.build_by_task.side_effect = Exception(
            "Fetcher build failed"
        )

        # 执行下载
        result = self.downloader.download("stock_basic", "000001.SZ")

        # 验证结果
        assert result is None
        self.mock_fetcher_builder.build_by_task.assert_called_once_with(
            "stock_basic", symbol="000001.SZ"
        )

    def test_download_fetcher_exception(self):
        """测试 fetcher 执行异常"""
        # 模拟 fetcher 函数执行时抛出异常
        mock_fetcher = Mock(side_effect=Exception("Fetch failed"))
        self.mock_fetcher_builder.build_by_task.return_value = mock_fetcher
        
        result = self.downloader.download("stock_basic", "000001.SZ")
        
        assert result is None
        self.mock_fetcher_builder.build_by_task.assert_called_once_with(
            "stock_basic", symbol="000001.SZ"
        )
        mock_fetcher.assert_called_once()
