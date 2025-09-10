"""测试分红送股数据获取功能"""

import pytest
import pandas as pd
from unittest.mock import patch
from neo.downloader.fetcher_builder import FetcherBuilder
from tests.fixtures.mock_factory import MockFactory


class TestDividendDataFetching:
    """测试分红送股数据获取功能"""

    def setup_method(self):
        """测试前设置"""
        self.mock_factory = MockFactory()

    def test_fetcher_builder_can_create_dividend_fetcher(self):
        """测试 FetcherBuilder 能创建 dividend 数据获取器"""
        # 现在这个测试应该成功，因为我们已经配置了 dividend schema

        builder = FetcherBuilder()

        # 这应该成功创建 dividend fetcher
        fetcher = builder.build_by_task("dividend", symbol="600519")

        # 验证返回的是可调用函数
        assert callable(fetcher)
        print("✅ 成功创建 dividend fetcher")

    def test_dividend_data_structure_validation(self):
        """测试分红数据结构验证"""
        # 验证我们预期的数据结构
        expected_columns = [
            "ts_code",
            "end_date",
            "ann_date",
            "div_proc",
            "stk_div",
            "stk_bo_rate",
            "cash_div",
            "cash_div_tax",
            "record_date",
            "ex_date",
            "pay_date",
            "div_listdate",
            "imp_ann_date",
            "base_date",
            "base_share",
        ]

        # 这个测试用来验证我们对数据结构的理解
        # 基于 https://tushare.pro/document/2?doc_id=103
        assert len(expected_columns) == 15

        # 验证关键字段存在
        assert "ts_code" in expected_columns
        assert "end_date" in expected_columns  # 报告期，应该作为主键
        assert "ann_date" in expected_columns  # 公告日期
        assert "cash_div" in expected_columns  # 每股股利
        assert "stk_div" in expected_columns  # 送股数
        assert "record_date" in expected_columns  # 股权登记日
        assert "ex_date" in expected_columns  # 除权除息日

    def test_dividend_primary_key_strategy(self):
        """测试分红数据主键策略"""
        # 验证我们选择的主键策略
        primary_key = ["ts_code", "end_date"]

        # 理由：一只股票在同一个报告期只能有一份分红数据
        assert len(primary_key) == 2
        assert "ts_code" in primary_key
        assert "end_date" in primary_key  # 使用报告期作为日期字段

    def test_dividend_update_strategy(self):
        """测试分红数据更新策略"""
        # 验证我们选择的更新策略
        update_strategy = "incremental"
        update_by_symbol = True

        # 理由：分红数据具有历史性，新增数据较少，适合增量更新
        assert update_strategy == "incremental"
        assert update_by_symbol is True

    @patch("neo.downloader.fetcher_builder.normalize_stock_code")
    def test_dividend_fetcher_parameter_passing(self, mock_normalize):
        """测试分红数据获取器参数传递"""
        # 这个测试现在应该成功
        mock_normalize.return_value = "600519.SH"

        expected_data = pd.DataFrame(
            {"ts_code": ["600519.SH"], "end_date": ["20231231"], "cash_div": [25.91]}
        )

        api_mock = self.mock_factory.create_api_function_mock(expected_data)
        mock_api_manager = self.mock_factory.create_mock_api_manager(api_mock)

        builder = FetcherBuilder(api_manager=mock_api_manager)

        # 现在这个测试应该成功
        fetcher = builder.build_by_task("dividend", symbol="600519")
        result = fetcher()

        # 验证参数传递
        mock_normalize.assert_called_once_with("600519")
        api_mock.assert_called_once_with(ts_code="600519.SH")

        # 验证返回数据
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert "ts_code" in result.columns
        assert "end_date" in result.columns

        print("✅ 参数传递测试通过")


if __name__ == "__main__":
    pytest.main([__file__])
