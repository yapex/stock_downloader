"""测试 SimpleDownloader 类的核心功能"""

import pandas as pd
from unittest.mock import Mock, patch

from neo.downloader.simple_downloader import SimpleDownloader
from neo.database.operator import DBOperator
from neo.helpers.interfaces import IRateLimitManager
from neo.downloader.fetcher_builder import FetcherBuilder
from neo.task_bus.types import TaskType


class TestSimpleDownloader:
    """测试 SimpleDownloader 类基本功能"""

    def test_init_with_default_params(self):
        """测试使用默认参数初始化"""
        mock_rate_limit_manager = Mock(spec=IRateLimitManager)
        mock_fetcher_builder = Mock(spec=FetcherBuilder)
        downloader = SimpleDownloader(
            rate_limit_manager=mock_rate_limit_manager,
            fetcher_builder=mock_fetcher_builder,
        )

        assert isinstance(downloader, SimpleDownloader)
        assert downloader.rate_limit_manager is not None
        assert downloader.fetcher_builder is not None

    def test_init_with_custom_db_operator(self):
        """测试使用自定义 db_operator 初始化"""
        mock_db_operator = Mock(spec=DBOperator)
        mock_rate_limit_manager = Mock(spec=IRateLimitManager)
        mock_fetcher_builder = Mock(spec=FetcherBuilder)

        downloader = SimpleDownloader(
            db_operator=mock_db_operator,
            rate_limit_manager=mock_rate_limit_manager,
            fetcher_builder=mock_fetcher_builder,
        )

        assert downloader.rate_limit_manager is not None
        assert downloader.fetcher_builder is not None


class TestSimpleDownloaderFetchData:
    """测试 SimpleDownloader._fetch_data 方法的业务逻辑"""

    def setup_method(self):
        """测试前设置"""
        self.mock_db_operator = Mock(spec=DBOperator)
        # 确保 get_max_date 方法无论传入什么参数都返回 None
        self.mock_db_operator.get_max_date = Mock(return_value=None)
        self.mock_rate_limit_manager = Mock(spec=IRateLimitManager)
        self.mock_fetcher_builder = Mock(spec=FetcherBuilder)
        self.downloader = SimpleDownloader(
            db_operator=self.mock_db_operator,
            rate_limit_manager=self.mock_rate_limit_manager,
            fetcher_builder=self.mock_fetcher_builder,
        )

    def test_fetch_data_with_existing_max_date(self):
        """测试数据库中有最新日期时的场景"""
        # 模拟数据库返回最新日期
        self.mock_db_operator.get_max_date.return_value = "20240315"

        # 模拟 fetcher 返回数据
        test_data = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240316"]})
        mock_fetcher = Mock(return_value=test_data)
        self.mock_fetcher_builder.build_by_task.return_value = mock_fetcher

        # 执行测试
        result = self.downloader._fetch_data("stock_daily", "000001.SZ")

        # 验证结果
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

        # 验证调用了数据库获取最新日期
        self.mock_db_operator.get_max_date.assert_called_once_with("stock_daily")

        # 验证使用正确的日期调用了 fetcher_builder（最新日期加一天）
        self.mock_fetcher_builder.build_by_task.assert_called_once_with(
            task_type="stock_daily", symbol="000001.SZ", start_date="20240316"
        )

        # 验证调用了 fetcher
        mock_fetcher.assert_called_once()

    def test_fetch_data_with_no_max_date(self):
        """测试数据库中没有最新日期时使用默认值的场景"""
        # 模拟数据库返回 "19901218"（空表的默认返回值）
        self.mock_db_operator.get_max_date.return_value = "19901218"

        # 模拟 fetcher 返回数据
        test_data = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240316"]})
        mock_fetcher = Mock(return_value=test_data)
        self.mock_fetcher_builder.build_by_task.return_value = mock_fetcher

        # 执行测试
        result = self.downloader._fetch_data("stock_basic", "000001.SZ")

        # 验证结果
        assert result is not None
        assert isinstance(result, pd.DataFrame)

        # 验证调用了数据库获取最新日期
        self.mock_db_operator.get_max_date.assert_called_once_with("stock_basic")

        # 验证使用数据库返回的日期加一天调用了 fetcher_builder
        self.mock_fetcher_builder.build_by_task.assert_called_once_with(
            task_type="stock_basic", symbol="000001.SZ", start_date="19901219"
        )

        # 验证调用了 fetcher
        mock_fetcher.assert_called_once()

    def test_fetch_data_returns_empty_dataframe(self):
        """测试 fetcher 返回空 DataFrame 的场景"""
        # 模拟数据库返回最新日期
        self.mock_db_operator.get_max_date.return_value = "20240315"

        # 模拟 fetcher 返回空数据
        empty_data = pd.DataFrame()
        mock_fetcher = Mock(return_value=empty_data)
        self.mock_fetcher_builder.build_by_task.return_value = mock_fetcher

        # 执行测试
        result = self.downloader._fetch_data("stock_daily", "000001.SZ")

        # 验证结果
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_fetch_data_returns_none(self):
        """测试 fetcher 返回 None 的场景"""
        # 模拟数据库返回最新日期
        self.mock_db_operator.get_max_date.return_value = "20240315"

        # 模拟 fetcher 返回 None
        mock_fetcher = Mock(return_value=None)
        self.mock_fetcher_builder.build_by_task.return_value = mock_fetcher

        # 执行测试
        result = self.downloader._fetch_data("stock_daily", "000001.SZ")

        # 验证结果
        assert result is None

    def test_fetch_data_fetcher_exception(self):
        """测试 fetcher 抛出异常的场景"""
        # 模拟数据库返回最新日期
        self.mock_db_operator.get_max_date.return_value = "20240315"

        # 模拟 fetcher 抛出异常
        mock_fetcher = Mock(side_effect=Exception("网络错误"))
        self.mock_fetcher_builder.build_by_task.return_value = mock_fetcher

        # 执行测试，应该重新抛出异常
        try:
            self.downloader._fetch_data("stock_daily", "000001.SZ")
            assert False, "应该重新抛出异常"
        except Exception as e:
            assert str(e) == "网络错误"

    def test_fetch_data_build_by_task_exception(self):
        """测试 build_by_task 抛出异常的场景"""
        # 模拟数据库返回最新日期
        self.mock_db_operator.get_max_date.return_value = "20240315"

        # 模拟 build_by_task 抛出异常
        self.mock_fetcher_builder.build_by_task.side_effect = Exception("构建失败")

        # 执行测试，应该重新抛出异常
        try:
            self.downloader._fetch_data("stock_daily", "000001.SZ")
            assert False, "应该重新抛出异常"
        except Exception as e:
            assert str(e) == "构建失败"


class TestSimpleDownloaderDownload:
    """测试 SimpleDownloader.download 方法"""

    def setup_method(self):
        """测试前设置"""
        self.mock_db_operator = Mock(spec=DBOperator)
        # 确保 get_max_date 方法无论传入什么参数都返回 None
        self.mock_db_operator.get_max_date = Mock(return_value=None)
        self.mock_rate_limit_manager = Mock(spec=IRateLimitManager)
        self.mock_fetcher_builder = Mock(spec=FetcherBuilder)
        self.downloader = SimpleDownloader(
            db_operator=self.mock_db_operator,
            rate_limit_manager=self.mock_rate_limit_manager,
            fetcher_builder=self.mock_fetcher_builder,
        )

    def test_download_none_data(self):
        """测试下载返回None"""
        # 模拟数据库返回 None（没有历史数据）
        self.mock_db_operator.get_max_date.return_value = None

        # 模拟返回None
        mock_fetcher = Mock(return_value=None)
        self.downloader.fetcher_builder.build_by_task = Mock(return_value=mock_fetcher)

        # 执行下载
        result = self.downloader.download("stock_basic", "000001.SZ")

        # 验证结果
        assert result is None

        # 验证调用了速率限制
        self.mock_rate_limit_manager.apply_rate_limiting.assert_called_once_with(
            TaskType.stock_basic
        )

    def test_download_rate_limiting_exception(self):
        """测试速率限制异常"""
        # 模拟速率限制抛出异常
        self.mock_rate_limit_manager.apply_rate_limiting.side_effect = Exception(
            "Rate limit exceeded"
        )

        # 执行下载
        result = self.downloader.download("stock_basic", "000001.SZ")

        # 验证结果
        assert result is None

        # 验证调用了速率限制
        self.mock_rate_limit_manager.apply_rate_limiting.assert_called_once_with(
            TaskType.stock_basic
        )

    def test_download_fetcher_exception(self):
        """测试数据获取异常"""
        # 模拟数据库返回 None（没有历史数据）
        self.mock_db_operator.get_max_date.return_value = None

        # 模拟数据获取抛出异常
        mock_fetcher = Mock(side_effect=Exception("Fetch failed"))
        self.downloader.fetcher_builder.build_by_task = Mock(return_value=mock_fetcher)

        # 执行下载
        result = self.downloader.download("stock_basic", "000001.SZ")

        # 验证结果
        assert result is None

        # 验证调用了速率限制
        self.mock_rate_limit_manager.apply_rate_limiting.assert_called_once_with(
            TaskType.stock_basic
        )

    def test_download_build_fetcher_exception(self):
        """测试构建fetcher异常"""
        # 模拟数据库返回 None（没有历史数据）
        self.mock_db_operator.get_max_date.return_value = None

        # 模拟构建fetcher抛出异常
        with patch.object(
            self.downloader.fetcher_builder,
            "build_by_task",
            side_effect=Exception("Build fetcher failed"),
        ):
            # 执行下载
            result = self.downloader.download("stock_basic", "000001.SZ")

            # 验证结果
            assert result is None

            # 验证调用了速率限制
            self.mock_rate_limit_manager.apply_rate_limiting.assert_called_once_with(
                TaskType.stock_basic
            )