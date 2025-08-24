"""测试 SimpleDownloader 类"""

import pandas as pd
from unittest.mock import Mock, patch

from neo.downloader.simple_downloader import SimpleDownloader
from neo.database.operator import DBOperator
from neo.helpers.interfaces import IRateLimitManager
from neo.downloader.fetcher_builder import FetcherBuilder
from neo.task_bus.types import TaskType
from neo.container import AppContainer


class TestSimpleDownloader:
    """测试 SimpleDownloader 类"""

    def test_create_default(self):
        """测试 create_default 工厂方法"""
        downloader = SimpleDownloader.create_default()

        # 验证实例创建成功
        assert isinstance(downloader, SimpleDownloader)

        # 验证实例具有预期的属性和方法
        assert hasattr(downloader, "rate_limit_manager")
        assert hasattr(downloader, "fetcher_builder")
        assert hasattr(downloader, "download")

        # 验证 rate_limit_manager 已正确初始化
        assert downloader.rate_limit_manager is not None

        # 验证 fetcher_builder 已正确初始化
        assert downloader.fetcher_builder is not None

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


class TestSimpleDownloaderContainer:
    """测试从 AppContainer 获取 SimpleDownloader 的相关行为"""

    def test_get_downloader_from_container(self):
        """测试从容器中获取 SimpleDownloader 实例"""
        container = AppContainer()
        downloader = container.downloader()

        # 验证实例类型
        assert isinstance(downloader, SimpleDownloader)

        # 验证依赖注入正确
        assert downloader.rate_limit_manager is not None
        assert downloader.fetcher_builder is not None

        # 验证具有预期的方法
        assert hasattr(downloader, "download")

    def test_container_provides_different_instances(self):
        """测试容器以工厂模式提供不同的 SimpleDownloader 实例"""
        container = AppContainer()
        downloader1 = container.downloader()
        downloader2 = container.downloader()

        # 验证是不同的实例（工厂模式）
        assert downloader1 is not downloader2
        assert isinstance(downloader1, SimpleDownloader)
        assert isinstance(downloader2, SimpleDownloader)

    def test_container_downloader_functionality(self):
        """测试从容器获取的 SimpleDownloader 功能是否正常"""
        container = AppContainer()
        downloader = container.downloader()

        # 验证可以调用 download 方法（即使可能因为网络或配置问题失败）
        # 这里主要验证方法存在且可调用，不验证具体结果
        assert callable(downloader.download)

        # 验证依赖的组件都已正确注入
        assert downloader.rate_limit_manager is not None
        assert downloader.fetcher_builder is not None

    def test_different_containers_have_different_singletons(self):
        """测试不同容器实例会创建不同的单例组件"""
        container1 = AppContainer()
        container2 = AppContainer()

        downloader1 = container1.downloader()
        downloader2 = container2.downloader()

        # 验证不同容器的下载器是不同实例
        assert downloader1 is not downloader2

        # 验证不同容器的速率限制管理器是不同实例
        # （因为 RateLimitManager 在容器中配置为 Singleton，但是容器级别的单例）
        rate_manager1 = container1.rate_limit_manager()
        rate_manager2 = container2.rate_limit_manager()
        assert rate_manager1 is not rate_manager2

    def test_same_container_shares_singleton_components(self):
        """测试同一容器内的单例组件共享"""
        container = AppContainer()

        # 获取两个下载器实例
        downloader1 = container.downloader()
        downloader2 = container.downloader()

        # 验证下载器是不同实例（工厂模式）
        assert downloader1 is not downloader2

        # 但它们共享同一个速率限制管理器（单例）
        assert downloader1.rate_limit_manager is downloader2.rate_limit_manager

        # 直接从容器获取的速率限制管理器也应该是同一个实例
        rate_manager = container.rate_limit_manager()
        assert downloader1.rate_limit_manager is rate_manager
        assert downloader2.rate_limit_manager is rate_manager

    def test_container_downloader_with_mocked_dependencies(self):
        """测试容器中下载器与模拟依赖的集成"""
        container = AppContainer()

        # 模拟速率限制管理器
        mock_rate_manager = Mock(spec=IRateLimitManager)
        container.rate_limit_manager.override(mock_rate_manager)

        try:
            downloader = container.downloader()

            # 验证使用了模拟的速率限制管理器
            assert downloader.rate_limit_manager is mock_rate_manager

            # 验证其他依赖仍然正常
            assert downloader.fetcher_builder is not None

        finally:
            # 清理覆盖
            container.rate_limit_manager.reset_override()


class TestSimpleDownloaderOriginal:
    """测试 SimpleDownloader 类的原有功能"""

    def setup_method(self):
        """测试前设置"""
        self.mock_db_operator = Mock(spec=DBOperator)
        self.mock_rate_limit_manager = Mock(spec=IRateLimitManager)
        self.mock_fetcher_builder = Mock(spec=FetcherBuilder)
        self.downloader = SimpleDownloader(
            db_operator=self.mock_db_operator,
            rate_limit_manager=self.mock_rate_limit_manager,
            fetcher_builder=self.mock_fetcher_builder,
        )

    def test_download_success(self):
        """测试成功下载数据"""
        # 准备测试数据
        test_data = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"]}
        )

        # 模拟 fetcher_builder.build_by_task 返回一个函数
        mock_fetcher = Mock(return_value=test_data)
        self.downloader.fetcher_builder.build_by_task = Mock(return_value=mock_fetcher)

        # 执行下载
        result = self.downloader.download("stock_basic", "000001.SZ")

        # 验证结果
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["ts_code"] == "000001.SZ"

        # 验证调用了速率限制
        self.mock_rate_limit_manager.apply_rate_limiting.assert_called_once_with(
            TaskType.stock_basic
        )

        # 验证调用了数据获取
        self.downloader.fetcher_builder.build_by_task.assert_called_once_with(
            task_type=TaskType.stock_basic, symbol="000001.SZ"
        )
        mock_fetcher.assert_called_once()

    def test_download_empty_data(self):
        """测试下载返回空数据"""
        # 模拟返回空数据
        empty_data = pd.DataFrame()
        mock_fetcher = Mock(return_value=empty_data)
        self.downloader.fetcher_builder.build_by_task = Mock(return_value=mock_fetcher)

        # 执行下载
        result = self.downloader.download("stock_basic", "000001.SZ")

        # 验证结果
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

        # 验证调用了速率限制
        self.mock_rate_limit_manager.apply_rate_limiting.assert_called_once_with(
            TaskType.stock_basic
        )

    def test_download_none_data(self):
        """测试下载返回None"""
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

    def test_apply_rate_limiting_with_different_task_types(self):
        """测试不同任务类型的速率限制"""
        # 测试不同的任务类型
        task_types = ["stock_basic", "stock_daily", "income_statement", "balance_sheet"]
        expected_enums = [
            TaskType.stock_basic,
            TaskType.stock_daily,
            TaskType.income_statement,
            TaskType.balance_sheet,
        ]

        for task_type, expected_enum in zip(task_types, expected_enums):
            # 重置mock
            self.mock_rate_limit_manager.reset_mock()

            # 模拟数据获取成功
            test_data = pd.DataFrame({"test": ["data"]})
            mock_fetcher = Mock(return_value=test_data)
            self.downloader.fetcher_builder.build_by_task = Mock(
                return_value=mock_fetcher
            )

            # 执行下载
            self.downloader.download(task_type, "000001.SZ")

            # 验证速率限制调用了正确的枚举
            self.mock_rate_limit_manager.apply_rate_limiting.assert_called_once_with(
                expected_enum
            )

    def test_fetch_data_with_different_task_types(self):
        """测试不同任务类型的数据获取"""
        # 测试不同的任务类型
        task_types = ["stock_basic", "stock_daily", "income_statement"]
        expected_enums = [
            TaskType.stock_basic,
            TaskType.stock_daily,
            TaskType.income_statement,
        ]

        for task_type, expected_enum in zip(task_types, expected_enums):
            # 重置mock
            self.mock_rate_limit_manager.reset_mock()

            # 模拟数据获取成功
            test_data = pd.DataFrame({"test": ["data"]})
            mock_fetcher = Mock(return_value=test_data)
            self.downloader.fetcher_builder.build_by_task = Mock(
                return_value=mock_fetcher
            )

            # 执行下载
            self.downloader.download(task_type, "000001.SZ")

            # 验证fetcher构建调用了正确的枚举
            self.downloader.fetcher_builder.build_by_task.assert_called_with(
                task_type=expected_enum, symbol="000001.SZ"
            )

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
