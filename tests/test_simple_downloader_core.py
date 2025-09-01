"""测试 SimpleDownloader 类的核心功能"""

import pandas as pd
from unittest.mock import Mock, patch

from neo.downloader.simple_downloader import SimpleDownloader
from neo.helpers.interfaces import IRateLimitManager
from neo.downloader.fetcher_builder import FetcherBuilder
# TaskType 已移除，现在使用字符串表示任务类型


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

    def test_cleanup(self):
        """测试清理方法"""
        mock_rate_limit_manager = Mock(spec=IRateLimitManager)
        mock_fetcher_builder = Mock(spec=FetcherBuilder)
        downloader = SimpleDownloader(
            rate_limit_manager=mock_rate_limit_manager,
            fetcher_builder=mock_fetcher_builder,
        )

        # 执行清理方法，不应抛出异常
        downloader.cleanup()


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
        mock_fetcher = Mock(return_value=pd.DataFrame({"a": [1]}))
        self.mock_fetcher_builder.build_by_task.return_value = mock_fetcher

        result = self.downloader.download("stock_basic", "000001.SZ")

        assert result is not None
        self.mock_fetcher_builder.build_by_task.assert_called_once_with(
            "stock_basic", symbol="000001.SZ"
        )
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

    def test_download_with_kwargs(self):
        """测试带额外参数的下载"""
        mock_fetcher = Mock(return_value=pd.DataFrame({"a": [1]}))
        self.mock_fetcher_builder.build_by_task.return_value = mock_fetcher

        # 使用额外参数调用
        result = self.downloader.download(
            "stock_basic", "000001.SZ", start_date="20240101"
        )

        assert result is not None
        self.mock_fetcher_builder.build_by_task.assert_called_once_with(
            "stock_basic", symbol="000001.SZ", start_date="20240101"
        )
        mock_fetcher.assert_called_once()

    def test_download_with_rate_limit_manager_exception(self):
        """测试速率限制管理器抛出异常的情况"""
        # 模拟速率限制管理器抛出异常
        self.mock_rate_limit_manager.apply_rate_limiting.side_effect = Exception(
            "Rate limit error"
        )

        result = self.downloader.download("stock_basic", "000001.SZ")

        # 应该返回 None 并记录错误
        assert result is None
        self.mock_rate_limit_manager.apply_rate_limiting.assert_called_once()

    # 新增测试用例：测试 download 方法成功获取数据并验证日志
    def test_download_success_with_logging(self):
        """测试成功下载并验证日志"""
        mock_fetcher = Mock(return_value=pd.DataFrame({"a": [1, 2, 3]}))
        self.mock_fetcher_builder.build_by_task.return_value = mock_fetcher

        with patch("neo.downloader.simple_downloader.logger") as mock_logger:
            result = self.downloader.download("stock_basic", "000001.SZ")

            assert result is not None
            # No debug log in download success path, so no assertion here.
            # For now, just ensure no error logs are made.
            mock_logger.error.assert_not_called()


# 新增测试类：测试 SimpleDownloader 中未覆盖的方法和边界条件
class TestSimpleDownloaderAdditional:
    """测试 SimpleDownloader 类的其他方法和边界条件"""

    def setup_method(self):
        """测试前设置"""
        self.mock_rate_limit_manager = Mock(spec=IRateLimitManager)
        self.mock_fetcher_builder = Mock(spec=FetcherBuilder)
        self.downloader = SimpleDownloader(
            rate_limit_manager=self.mock_rate_limit_manager,
            fetcher_builder=self.mock_fetcher_builder,
        )

    def test_download_none_result(self):
        """测试 download 方法返回 None 的情况"""
        mock_fetcher = Mock(return_value=None)
        self.mock_fetcher_builder.build_by_task.return_value = mock_fetcher

        result = self.downloader.download("stock_basic", "000001.SZ")

        assert result is None
        self.mock_fetcher_builder.build_by_task.assert_called_once_with(
            "stock_basic", symbol="000001.SZ"
        )
        mock_fetcher.assert_called_once()

    def test_download_empty_dataframe(self):
        """测试 download 方法返回空 DataFrame 的情况"""
        mock_fetcher = Mock(return_value=pd.DataFrame())
        self.mock_fetcher_builder.build_by_task.return_value = mock_fetcher

        result = self.downloader.download("stock_basic", "000001.SZ")

        assert result is not None
        assert result.empty
        self.mock_fetcher_builder.build_by_task.assert_called_once_with(
            "stock_basic", symbol="000001.SZ"
        )
        mock_fetcher.assert_called_once()
