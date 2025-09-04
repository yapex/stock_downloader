import pytest
import pandas as pd
from unittest.mock import Mock, patch
from threading import Lock
from neo.downloader.fetcher_builder import TushareApiManager, FetcherBuilder
from neo.downloader.fetcher_builder import TaskTemplate

# TaskType 和 TaskTemplateRegistry 已移除，现在使用字符串和 schema 配置
from neo.containers import AppContainer


class TestTaskTemplate:
    """测试 TaskTemplate 数据类"""

    def test_task_template_creation(self):
        """测试任务模板创建"""
        template = TaskTemplate(
            base_object="pro",
            api_method="stock_basic",
            default_params={"exchange": "SSE"},
        )
        assert template.base_object == "pro"
        assert template.api_method == "stock_basic"
        assert template.default_params == {"exchange": "SSE"}

    def test_task_template_without_default_params(self):
        """测试没有默认参数的任务模板"""
        template = TaskTemplate(base_object="pro", api_method="stock_basic")
        assert template.base_object == "pro"
        assert template.api_method == "stock_basic"
        assert template.default_params == {}


# TestTaskType 类已删除，因为 TaskType 和 TaskTemplateRegistry 已被移除
# 现在使用字符串表示任务类型，配置通过 schema.toml 文件管理


class TestTushareApiManager:
    """测试 TushareApiManager 单例类"""
    
    def setup_method(self):
        """每个测试方法前的设置"""
        from tests.fixtures.mock_factory import MockFactory
        self.mock_factory = MockFactory()

    def teardown_method(self):
        """测试后清理单例状态"""
        TushareApiManager._instance = None
        TushareApiManager._lock = Lock()

    def test_singleton_pattern(self):
        """测试单例模式"""
        manager1 = TushareApiManager.get_instance()
        manager2 = TushareApiManager.get_instance()
        assert manager1 is manager2

    @patch("neo.downloader.fetcher_builder.ts")
    def test_get_api_function_pro(self, mock_ts):
        """测试获取 pro API 函数"""
        # 使用环境变量上下文管理器替代 patch
        env_context = self.mock_factory.create_environment_context({"TUSHARE_TOKEN": "test_token"})
        
        # 使用 MockFactory 创建 Tushare API mocks
        mocks = self.mock_factory.create_tushare_api_mock(
            token="test_token",
            api_functions={"stock_basic": None}  # 不需要返回值，只测试函数获取
        )
        
        # 设置 mock 对象
        mock_ts.return_value = mocks["ts"]
        mock_ts.pro_api = mocks["ts"].pro_api
        mock_ts.set_token = Mock()

        with env_context:
            # 重置单例实例
            TushareApiManager._instance = None
            TushareApiManager._lock = Lock()

            manager = TushareApiManager.get_instance()
            result = manager.get_api_function("pro", "stock_basic")

            # 验证结果
            assert result is mocks["api_functions"]["stock_basic"]
            mock_ts.set_token.assert_called_once_with("test_token")
            mock_ts.pro_api.assert_called_once()

    @patch("neo.downloader.fetcher_builder.ts")
    def test_get_api_function_direct(self, mock_ts):
        """测试获取直接 API 函数"""
        # 使用环境变量上下文管理器替代 patch
        env_context = self.mock_factory.create_environment_context({"TUSHARE_TOKEN": "test_token"})
        
        # 使用 MockFactory 创建 Tushare API mocks
        mocks = self.mock_factory.create_tushare_api_mock(
            token="test_token",
            api_functions={"get_hist_data": None}  # 不需要返回值，只测试函数获取
        )
        
        # 设置 mock 对象
        mock_ts.return_value = mocks["ts"]
        mock_ts.pro_api = mocks["ts"].pro_api
        mock_ts.set_token = Mock()
        # 为直接 API 设置属性
        setattr(mock_ts, "get_hist_data", mocks["ts"].get_hist_data)

        with env_context:
            # 重置单例实例
            TushareApiManager._instance = None
            TushareApiManager._lock = Lock()

            manager = TushareApiManager.get_instance()
            result = manager.get_api_function("ts", "get_hist_data")

            assert result is mocks["ts"].get_hist_data
            mock_ts.set_token.assert_called_once_with("test_token")


class TestFetcherBuilder:
    """测试 FetcherBuilder 类"""

    def setup_method(self):
        """测试前设置"""
        from tests.fixtures.mock_factory import MockFactory
        self.mock_factory = MockFactory()
        # 不初始化 builder，在各个测试中传入 mock 的 api_manager

    def test_init(self):
        """测试初始化"""
        builder = FetcherBuilder()
        assert builder.api_manager is not None
        assert isinstance(builder.api_manager, TushareApiManager)

    def test_build_by_task_invalid_type(self):
        """测试无效任务类型"""
        # 使用 mock API manager
        mock_api_manager = self.mock_factory.create_mock_api_manager()
        builder = FetcherBuilder(api_manager=mock_api_manager)
        
        with pytest.raises(ValueError, match="不支持的任务类型"):
            builder.build_by_task("invalid_type")

    @patch("neo.downloader.fetcher_builder.normalize_stock_code")
    def test_build_by_task_with_symbol(self, mock_normalize):
        """测试带股票代码的任务构建"""
        # 使用 MockFactory 创建 mocks
        api_mock = self.mock_factory.create_api_function_mock(
            pd.DataFrame({"data": [1, 2, 3]})
        )
        normalize_mock = self.mock_factory.create_normalize_stock_code_mock("600519.SH")
        mock_api_manager = self.mock_factory.create_mock_api_manager(api_mock)
        
        mock_normalize.return_value = normalize_mock.return_value
        
        builder = FetcherBuilder(api_manager=mock_api_manager)
        fetcher = builder.build_by_task("stock_basic", symbol="600519")
        result = fetcher()

        mock_normalize.assert_called_once_with("600519")
        mock_api_manager.get_api_function.assert_called_once_with("pro", "stock_basic")
        api_mock.assert_called_once_with(ts_code="600519.SH")
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.parametrize("task_type,expected_base_object,expected_api_method,expected_params", [
        ("stock_basic", "pro", "stock_basic", {}),
        ("stock_adj_hfq", "ts", "pro_bar", {"adj": "hfq"}),
    ])
    def test_build_by_task_without_symbol_parametrized(
        self, task_type, expected_base_object, expected_api_method, expected_params
    ):
        """测试不带股票代码的任务构建（参数化）"""
        # 使用 MockFactory 创建 API mock
        api_mock = self.mock_factory.create_api_function_mock(
            pd.DataFrame({"data": [1, 2, 3]})
        )
        mock_api_manager = self.mock_factory.create_mock_api_manager(api_mock)
        
        builder = FetcherBuilder(api_manager=mock_api_manager)
        fetcher = builder.build_by_task(task_type)
        result = fetcher()

        mock_api_manager.get_api_function.assert_called_once_with(expected_base_object, expected_api_method)
        if expected_params:
            # 验证默认参数被传递
            call_args = api_mock.call_args[1]
            for key, value in expected_params.items():
                assert call_args[key] == value
        else:
            # 没有默认参数，不传递任何参数
            api_mock.assert_called_once_with()
        assert isinstance(result, pd.DataFrame)

    @patch("neo.downloader.fetcher_builder.normalize_stock_code")
    def test_build_by_task_stock_basic_with_empty_symbol(
        self, mock_normalize
    ):
        """测试 stock_basic 任务传入空字符串时不调用 normalize_stock_code"""
        # 使用 MockFactory 创建 API mock
        api_mock = self.mock_factory.create_api_function_mock(
            pd.DataFrame({"data": [1, 2, 3]})
        )
        mock_api_manager = self.mock_factory.create_mock_api_manager(api_mock)
        
        builder = FetcherBuilder(api_manager=mock_api_manager)
        fetcher = builder.build_by_task("stock_basic", symbol="")
        result = fetcher()

        # 验证 normalize_stock_code 没有被调用
        mock_normalize.assert_not_called()
        mock_api_manager.get_api_function.assert_called_once_with("pro", "stock_basic")
        # stock_basic 任务不需要 ts_code 参数
        api_mock.assert_called_once_with()
        assert isinstance(result, pd.DataFrame)

    @patch("neo.downloader.fetcher_builder.normalize_stock_code")
    def test_build_by_task_with_overrides(self, mock_normalize):
        """测试带参数覆盖的任务构建"""
        mock_normalize.return_value = "600519.SH"
        # 使用 MockFactory 创建 API mock
        api_mock = self.mock_factory.create_api_function_mock(
            pd.DataFrame({"data": [1, 2, 3]})
        )
        mock_api_manager = self.mock_factory.create_mock_api_manager(api_mock)
        
        builder = FetcherBuilder(api_manager=mock_api_manager)
        fetcher = builder.build_by_task(
            "stock_basic", symbol="600519", exchange="SZSE", list_status="L"
        )
        result = fetcher()

        mock_normalize.assert_called_once_with("600519")
        mock_api_manager.get_api_function.assert_called_once_with("pro", "stock_basic")
        api_mock.assert_called_once_with(
            ts_code="600519.SH", exchange="SZSE", list_status="L"
        )
        assert isinstance(result, pd.DataFrame)

    # test_build_by_task_with_default_params 已被参数化测试替代

    def test_build_by_task_with_kwargs(self):
        """测试使用 kwargs 传递 start_date 等参数"""
        # 使用 MockFactory 创建 API mock
        api_mock = self.mock_factory.create_api_function_mock(
            pd.DataFrame({"data": [1]})
        )
        mock_api_manager = self.mock_factory.create_mock_api_manager(api_mock)
        
        builder = FetcherBuilder(api_manager=mock_api_manager)
        fetcher = builder.build_by_task(
            "stock_daily",
            symbol="600519.SH",
            start_date="20240101",
            end_date="20240131",
        )
        fetcher()

        # 验证传递给 Tushare API 的参数是否正确
        api_mock.assert_called_once_with(
            ts_code="600519.SH", start_date="20240101", end_date="20240131"
        )

    def test_execute_function_exception_handling(self):
        """测试执行函数异常处理"""
        # 使用 MockFactory 创建抛出异常的 API mock
        api_mock = self.mock_factory.create_api_function_mock()
        api_mock.side_effect = Exception("API调用失败")
        mock_api_manager = self.mock_factory.create_mock_api_manager(api_mock)
        
        builder = FetcherBuilder(api_manager=mock_api_manager)
        fetcher = builder.build_by_task("stock_basic")

        with pytest.raises(Exception, match="API调用失败"):
            fetcher()

    @patch("neo.downloader.fetcher_builder.normalize_stock_code")
    def test_parameter_merging(self, mock_normalize):
        """测试参数合并逻辑"""
        mock_normalize.return_value = "600519.SH"
        # 使用 MockFactory 创建 API mock
        api_mock = self.mock_factory.create_api_function_mock(
            pd.DataFrame({"data": [1]})
        )
        mock_api_manager = self.mock_factory.create_mock_api_manager(api_mock)
        
        builder = FetcherBuilder(api_manager=mock_api_manager)
        # 测试运行时参数覆盖默认参数
        fetcher = builder.build_by_task(
            "stock_daily",
            symbol="600519",
            start_date="20240101",  # 覆盖默认参数
            end_date="20240131",  # 新增参数
        )
        fetcher()

        call_args = api_mock.call_args[1]
        assert call_args["ts_code"] == "600519.SH"
        assert call_args["start_date"] == "20240101"
        assert call_args["end_date"] == "20240131"


class TestFetcherBuilderContainer:
    """测试从 Container 中获取 FetcherBuilder"""
    
    def setup_method(self):
        """每个测试方法前的设置"""
        from tests.fixtures.mock_factory import MockFactory
        self.mock_factory = MockFactory()

    def test_get_fetcher_builder_from_container(self):
        """测试从容器中获取 FetcherBuilder 实例"""
        container = AppContainer()
        fetcher_builder = container.fetcher_builder()

        assert isinstance(fetcher_builder, FetcherBuilder)
        assert fetcher_builder.api_manager is not None
        assert isinstance(fetcher_builder.api_manager, TushareApiManager)

    def test_container_provides_different_instances(self):
        """测试容器提供不同的 FetcherBuilder 实例（Factory 模式）"""
        container = AppContainer()
        fetcher_builder1 = container.fetcher_builder()
        fetcher_builder2 = container.fetcher_builder()

        # Factory 模式应该提供不同的实例
        assert fetcher_builder1 is not fetcher_builder2
        assert isinstance(fetcher_builder1, FetcherBuilder)
        assert isinstance(fetcher_builder2, FetcherBuilder)

    def test_container_fetcher_builder_functionality(self):
        """测试从容器获取的 FetcherBuilder 功能正常"""
        # 使用 MockFactory 创建 API mock
        api_mock = self.mock_factory.create_api_function_mock(
            pd.DataFrame({"data": [1, 2, 3]})
        )
        mock_api_manager = self.mock_factory.create_mock_api_manager(api_mock)

        container = AppContainer()
        # 覆盖容器中的 FetcherBuilder 工厂方法
        container.fetcher_builder = lambda: FetcherBuilder(api_manager=mock_api_manager)
        
        fetcher_builder = container.fetcher_builder()

        # 测试构建任务功能
        fetcher = fetcher_builder.build_by_task("stock_basic")
        result = fetcher()

        mock_api_manager.get_api_function.assert_called_once_with("pro", "stock_basic")
        api_mock.assert_called_once_with()
        assert isinstance(result, pd.DataFrame)

    def test_multiple_containers_share_singleton_api_manager(self):
        """测试多个容器共享单例 API 管理器"""
        container1 = AppContainer()
        container2 = AppContainer()

        fetcher_builder1 = container1.fetcher_builder()
        fetcher_builder2 = container2.fetcher_builder()

        # API 管理器应该是同一个单例实例
        assert fetcher_builder1.api_manager is fetcher_builder2.api_manager
