"""测试数据获取器构建器模块"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
import os

from src.downloader2.factories.fetcher_builder import (
    FetcherBuilder,
    TushareApiManager,
    TaskTemplate,
    TaskType,
)


class TestTaskTemplate:
    """测试任务模板"""

    def test_task_template_creation(self):
        """测试任务模板创建"""
        template = TaskTemplate(api_method="test_method")

        assert template.api_method == "test_method"
        assert template.base_object == "pro"
        assert template.default_params == {}

    def test_task_template_with_custom_params(self):
        """测试带自定义参数的任务模板"""
        custom_params = {"param1": "value1", "param2": "value2"}
        template = TaskTemplate(
            api_method="test_method",
            base_object="ts",
            default_params=custom_params,
        )

        assert template.base_object == "ts"
        assert template.default_params == custom_params


class TestTushareApiManager:
    """测试 Tushare API 管理器"""

    def setup_method(self):
        """每个测试方法前的设置"""
        # 重置单例实例
        TushareApiManager._instance = None

    @patch("src.downloader2.factories.fetcher_builder.get_config")
    @patch("src.downloader2.factories.fetcher_builder.ts")
    def test_singleton_pattern(self, mock_ts, mock_get_config):
        """测试单例模式"""
        # 模拟配置
        mock_config = Mock()
        mock_config.tushare.token = "test_token"
        mock_get_config.return_value = mock_config

        # 模拟 tushare API
        mock_pro_api = Mock()
        mock_ts.pro_api.return_value = mock_pro_api

        # 获取两个实例
        manager1 = TushareApiManager.get_instance()
        manager2 = TushareApiManager.get_instance()

        # 验证是同一个实例
        assert manager1 is manager2

        # 验证 API 初始化只调用一次
        mock_ts.set_token.assert_called_once_with("test_token")
        mock_ts.pro_api.assert_called_once()

    @patch("src.downloader2.factories.fetcher_builder.get_config")
    @patch("src.downloader2.factories.fetcher_builder.ts")
    def test_get_api_function(self, mock_ts, mock_get_config):
        """测试获取 API 函数"""
        # 模拟配置
        mock_config = Mock()
        mock_config.tushare.token = "test_token"
        mock_get_config.return_value = mock_config

        # 模拟 tushare API
        mock_pro_api = Mock(spec=["stock_basic"])  # 只允许 stock_basic 属性
        mock_pro_api.stock_basic = Mock()
        mock_ts.pro_api.return_value = mock_pro_api

        manager = TushareApiManager.get_instance()

        # 测试获取存在的方法
        api_func = manager.get_api_function("pro", "stock_basic")
        assert api_func is mock_pro_api.stock_basic

        # 测试获取不存在的对象
        with pytest.raises(ValueError, match="不支持的 API 对象"):
            manager.get_api_function("invalid", "method")

        # 测试获取不存在的方法
        with pytest.raises(ValueError, match="没有方法"):
            manager.get_api_function("pro", "invalid_method")


class TestFetcherBuilder:
    """测试数据获取器构建器"""

    def setup_method(self):
        """每个测试方法前的设置"""
        # 重置单例实例
        TushareApiManager._instance = None

    @patch("src.downloader2.factories.fetcher_builder.TushareApiManager")
    def test_build_fetcher_success(self, mock_manager_class):
        """测试成功构建获取器"""
        # 模拟 API 管理器
        mock_manager = Mock()
        mock_api_func = Mock()
        mock_api_func.return_value = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "name": ["测试股票"]}
        )
        mock_manager.get_api_function.return_value = mock_api_func
        mock_manager_class.get_instance.return_value = mock_manager

        builder = FetcherBuilder()
        fetcher = builder.build_by_task(TaskType.STOCK_BASIC, "")

        # 验证返回的是函数
        assert callable(fetcher)

        # 执行函数并验证结果
        result = fetcher()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["ts_code"] == "000001.SZ"

        # 验证 API 调用
        mock_manager.get_api_function.assert_called_once_with("pro", "stock_basic")
        mock_api_func.assert_called_once_with()

    @patch("src.downloader2.factories.fetcher_builder.TushareApiManager")
    def test_build_fetcher_with_overrides(self, mock_manager_class):
        """测试带参数覆盖的构建器"""
        # 模拟 API 管理器
        mock_manager = Mock()
        mock_api_func = Mock()
        mock_api_func.return_value = pd.DataFrame({"ts_code": ["000001.SZ"]})
        mock_manager.get_api_function.return_value = mock_api_func
        mock_manager_class.get_instance.return_value = mock_manager

        builder = FetcherBuilder()
        fetcher = builder.build_by_task(
            TaskType.STOCK_BASIC, "", ts_code="000001.SZ", exchange="SZSE"
        )

        # 执行函数
        result = fetcher()

        # 验证 API 调用参数
        mock_api_func.assert_called_once_with(ts_code="000001.SZ", exchange="SZSE")

    def test_build_fetcher_invalid_task(self):
        """测试构建无效任务类型的获取器"""
        builder = FetcherBuilder()

        with pytest.raises(ValueError, match="不支持的任务类型"):
            builder.build_by_task("invalid_task")

    @patch("src.downloader2.factories.fetcher_builder.TushareApiManager")
    def test_build_stock_daily_fetcher(self, mock_manager_class):
        """测试构建股票日线行情获取器"""
        # 模拟 API 管理器
        mock_manager = Mock()
        mock_api_func = Mock()
        mock_api_func.return_value = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "close": [10.5]}
        )
        mock_manager.get_api_function.return_value = mock_api_func
        mock_manager_class.get_instance.return_value = mock_manager

        builder = FetcherBuilder()
        fetcher = builder.build_by_task(
            TaskType.STOCK_DAILY, "000001.SZ", start_date="20240101"
        )

        # 验证返回的是函数
        assert callable(fetcher)

        # 执行函数并验证结果
        result = fetcher()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["ts_code"] == "000001.SZ"

        # 验证 API 调用
        mock_manager.get_api_function.assert_called_once_with("pro", "daily")
        mock_api_func.assert_called_once_with(
            ts_code="000001.SZ", start_date="20240101"
        )

    @patch("src.downloader2.factories.fetcher_builder.TushareApiManager")
    def test_build_daily_bar_qfq_fetcher(self, mock_manager_class):
        """测试构建前复权日线行情获取器"""
        # 模拟 API 管理器
        mock_manager = Mock()
        mock_api_func = Mock()
        mock_api_func.return_value = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240101"],
                "close": [10.5],
                "adj_factor": [1.0],
            }
        )
        mock_manager.get_api_function.return_value = mock_api_func
        mock_manager_class.get_instance.return_value = mock_manager

        builder = FetcherBuilder()
        fetcher = builder.build_by_task(
            TaskType.DAILY_BAR_QFQ, "000001.SZ", start_date="20240101"
        )

        # 验证返回的是函数
        assert callable(fetcher)

        # 执行函数并验证结果
        result = fetcher()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["ts_code"] == "000001.SZ"

        # 验证 API 调用
        mock_manager.get_api_function.assert_called_once_with("ts", "pro_bar")
        mock_api_func.assert_called_once_with(
            ts_code="000001.SZ", start_date="20240101", adj="qfq"
        )

    @patch("src.downloader2.factories.fetcher_builder.TushareApiManager")
    def test_build_daily_bar_none_fetcher(self, mock_manager_class):
        """测试构建不复权日线行情获取器"""
        # 模拟 API 管理器
        mock_manager = Mock()
        mock_api_func = Mock()
        mock_api_func.return_value = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "close": [10.5]}
        )
        mock_manager.get_api_function.return_value = mock_api_func
        mock_manager_class.get_instance.return_value = mock_manager

        builder = FetcherBuilder()
        fetcher = builder.build_by_task(
            TaskType.DAILY_BAR_NONE, "000001.SZ", start_date="20240101"
        )

        # 验证返回的是函数
        assert callable(fetcher)

        # 执行函数并验证结果
        result = fetcher()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["ts_code"] == "000001.SZ"

        # 验证 API 调用
        mock_manager.get_api_function.assert_called_once_with("ts", "pro_bar")
        mock_api_func.assert_called_once_with(
            ts_code="000001.SZ", start_date="20240101", adj=None
        )

    @patch("src.downloader2.factories.fetcher_builder.TushareApiManager")
    def test_build_by_task(self, mock_manager_class):
        """测试构建指定股票代码的获取器"""
        # 模拟 API 管理器
        mock_manager = Mock()
        mock_api_func = Mock()
        mock_api_func.return_value = pd.DataFrame({"ts_code": ["000001.SZ"]})
        mock_manager.get_api_function.return_value = mock_api_func
        mock_manager_class.get_instance.return_value = mock_manager

        builder = FetcherBuilder()
        fetcher = builder.build_by_task(TaskType.STOCK_BASIC, "000001.SZ")

        # 执行函数
        result = fetcher()

        # 验证 API 调用参数包含 ts_code
        mock_api_func.assert_called_once_with(ts_code="000001.SZ")

    @patch("src.downloader2.factories.fetcher_builder.TushareApiManager")
    def test_build_balancesheet_fetcher(self, mock_manager_class):
        """测试构建资产负债表获取器"""
        # 模拟 API 管理器和返回数据
        mock_manager = Mock()
        mock_api_func = Mock()
        mock_api_func.return_value = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "ann_date": ["20240331"],
                "total_assets": [1000000],
            }
        )
        mock_manager.get_api_function.return_value = mock_api_func
        mock_manager_class.get_instance.return_value = mock_manager

        builder = FetcherBuilder()
        fetcher = builder.build_by_task(
            TaskType.BALANCESHEET, "000001.SZ", period="20240331"
        )

        # 执行函数
        result = fetcher()

        # 验证结果
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["ts_code"] == "000001.SZ"

        # 验证 API 调用
        mock_manager.get_api_function.assert_called_once_with("pro", "balancesheet")
        mock_api_func.assert_called_once_with(ts_code="000001.SZ", period="20240331")

    @patch("src.downloader2.factories.fetcher_builder.TushareApiManager")
    def test_build_income_fetcher(self, mock_manager_class):
        """测试构建利润表获取器"""
        # 模拟 API 管理器和返回数据
        mock_manager = Mock()
        mock_api_func = Mock()
        mock_api_func.return_value = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "ann_date": ["20240331"],
                "total_revenue": [500000],
            }
        )
        mock_manager.get_api_function.return_value = mock_api_func
        mock_manager_class.get_instance.return_value = mock_manager

        builder = FetcherBuilder()
        fetcher = builder.build_by_task(
            TaskType.INCOME, "000001.SZ", period="20240331"
        )

        # 执行函数
        result = fetcher()

        # 验证结果
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["ts_code"] == "000001.SZ"

        # 验证 API 调用
        mock_manager.get_api_function.assert_called_once_with("pro", "income")
        mock_api_func.assert_called_once_with(ts_code="000001.SZ", period="20240331")

    @patch("src.downloader2.factories.fetcher_builder.TushareApiManager")
    def test_build_cashflow_fetcher(self, mock_manager_class):
        """测试构建现金流量表获取器"""
        # 模拟 API 管理器和返回数据
        mock_manager = Mock()
        mock_api_func = Mock()
        mock_api_func.return_value = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "ann_date": ["20240331"],
                "n_cashflow_act": [200000],
            }
        )
        mock_manager.get_api_function.return_value = mock_api_func
        mock_manager_class.get_instance.return_value = mock_manager

        builder = FetcherBuilder()
        fetcher = builder.build_by_task(
            TaskType.CASHFLOW, "000001.SZ", period="20240331"
        )

        # 执行函数
        result = fetcher()

        # 验证结果
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["ts_code"] == "000001.SZ"

        # 验证 API 调用
        mock_manager.get_api_function.assert_called_once_with("pro", "cashflow")
        mock_api_func.assert_called_once_with(ts_code="000001.SZ", period="20240331")



class TestIntegration:
    """集成测试"""

    @pytest.mark.skipif(
        not os.getenv("TUSHARE_TOKEN"), reason="需要设置 TUSHARE_TOKEN 环境变量"
    )
    def test_real_api_call(self):
        """测试真实 API 调用（需要有效的 TUSHARE_TOKEN）"""
        # 重置单例实例
        TushareApiManager._instance = None

        # 使用环境变量中的 token 覆盖配置
        with patch(
            "src.downloader2.factories.fetcher_builder.get_config"
        ) as mock_get_config:
            mock_config = Mock()
            mock_config.tushare.token = os.getenv("TUSHARE_TOKEN")
            mock_get_config.return_value = mock_config

            # 构建并执行获取器
            builder = FetcherBuilder()
            fetcher = builder.build_by_task(TaskType.STOCK_BASIC, "000001.SZ")
            result = fetcher()

            # 验证结果
            assert isinstance(result, pd.DataFrame)
            assert not result.empty
            assert "ts_code" in result.columns
            assert "name" in result.columns
            assert result.iloc[0]["ts_code"] == "000001.SZ"
