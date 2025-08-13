"""测试StockBasicFields的from_series方法"""

import pandas as pd
import pytest
from src.downloader2.models.stock_basic import StockBasicFields


class TestStockBasicFieldsFromSeries:
    """测试StockBasicFields.from_series方法"""

    def test_from_series_with_complete_data(self):
        """测试使用完整数据创建StockBasicFields实例"""
        # 创建包含所有字段的Series
        data = {
            "ts_code": "000001.SZ",
            "symbol": "000001",
            "name": "平安银行",
            "area": "深圳",
            "industry": "银行",
            "fullname": "平安银行股份有限公司",
            "enname": "Ping An Bank Co., Ltd.",
            "cnspell": "PAYH",
            "market": "主板",
            "exchange": "SZSE",
            "curr_type": "CNY",
            "list_status": "L",
            "list_date": "19910403",
            "delist_date": None,
            "is_hs": "H",
            "act_name": "平安银行股份有限公司",
            "act_ent_type": "境内非国有法人",
        }
        series = pd.Series(data)

        # 调用from_series方法
        result = StockBasicFields.from_series(series)

        # 验证结果
        assert isinstance(result, StockBasicFields)
        assert result.ts_code == "000001.SZ"
        assert result.symbol == "000001"
        assert result.name == "平安银行"
        assert result.area == "深圳"
        assert result.industry == "银行"
        assert result.fullname == "平安银行股份有限公司"
        assert result.enname == "Ping An Bank Co., Ltd."
        assert result.cnspell == "PAYH"
        assert result.market == "主板"
        assert result.exchange == "SZSE"
        assert result.curr_type == "CNY"
        assert result.list_status == "L"
        assert result.list_date == "19910403"
        assert result.delist_date is None
        assert result.is_hs == "H"
        assert result.act_name == "平安银行股份有限公司"
        assert result.act_ent_type == "境内非国有法人"

    def test_from_series_with_partial_data(self):
        """测试使用部分数据创建StockBasicFields实例"""
        # 只包含部分字段的Series
        data = {
            "ts_code": "000002.SZ",
            "symbol": "000002",
            "name": "万科A",
            "area": "深圳",
            "industry": "房地产开发",
        }
        series = pd.Series(data)

        # 由于dataclass的所有字段都是必需的，部分数据应该引发TypeError
        with pytest.raises(TypeError):
            StockBasicFields.from_series(series)

    def test_from_series_with_empty_values(self):
        """测试包含空值的Series"""
        data = {
            "ts_code": "000003.SZ",
            "symbol": "000003",
            "name": "PT金田A",
            "area": "",
            "industry": None,
            "fullname": "",
            "enname": None,
            "cnspell": "",
            "market": "主板",
            "exchange": "SZSE",
            "curr_type": "CNY",
            "list_status": "D",
            "list_date": "19910403",
            "delist_date": "20031208",
            "is_hs": "N",
            "act_name": "",
            "act_ent_type": "",
        }
        series = pd.Series(data)

        # 调用from_series方法
        result = StockBasicFields.from_series(series)

        # 验证结果
        assert isinstance(result, StockBasicFields)
        assert result.ts_code == "000003.SZ"
        assert result.symbol == "000003"
        assert result.name == "PT金田A"
        assert result.area == ""
        assert result.industry is None
        assert result.fullname == ""
        assert result.enname is None
        assert result.cnspell == ""
        assert result.market == "主板"
        assert result.exchange == "SZSE"
        assert result.curr_type == "CNY"
        assert result.list_status == "D"
        assert result.list_date == "19910403"
        assert result.delist_date == "20031208"
        assert result.is_hs == "N"
        assert result.act_name == ""
        assert result.act_ent_type == ""

    def test_from_series_missing_required_fields(self):
        """测试缺少必需字段时的行为"""
        # 缺少大部分字段
        data = {"symbol": "000001", "name": "平安银行"}
        series = pd.Series(data)

        # 由于dataclass的所有字段都是必需的，缺少字段应该引发TypeError
        with pytest.raises(TypeError):
            StockBasicFields.from_series(series)

    def test_from_series_with_extra_fields(self):
        """测试Series包含额外字段时的行为"""
        data = {
            "ts_code": "000001.SZ",
            "symbol": "000001",
            "name": "平安银行",
            "area": "深圳",
            "industry": "银行",
            "fullname": "平安银行股份有限公司",
            "enname": "Ping An Bank Co., Ltd.",
            "cnspell": "PAYH",
            "market": "主板",
            "exchange": "SZSE",
            "curr_type": "CNY",
            "list_status": "L",
            "list_date": "19910403",
            "delist_date": None,
            "is_hs": "H",
            "act_name": "平安银行股份有限公司",
            "act_ent_type": "境内非国有法人",
            # 额外字段
            "extra_field1": "extra_value1",
            "extra_field2": "extra_value2",
        }
        series = pd.Series(data)

        # 调用from_series方法
        result = StockBasicFields.from_series(series)

        # 验证结果 - 额外字段应该被忽略
        assert isinstance(result, StockBasicFields)
        assert result.ts_code == "000001.SZ"
        assert result.symbol == "000001"
        assert result.name == "平安银行"
        # 验证额外字段不存在
        assert not hasattr(result, "extra_field1")
        assert not hasattr(result, "extra_field2")

    def test_from_series_with_empty_series(self):
        """测试空Series的行为"""
        series = pd.Series(dtype=object)

        # 由于dataclass的所有字段都是必需的，空Series应该引发TypeError
        with pytest.raises(TypeError):
            StockBasicFields.from_series(series)

    def test_from_series_field_mapping(self):
        """测试字段映射是否正确"""
        # 验证所有字段都能正确映射
        data = {
            "ts_code": "test_ts_code",
            "symbol": "test_symbol",
            "name": "test_name",
            "area": "test_area",
            "industry": "test_industry",
            "fullname": "test_fullname",
            "enname": "test_enname",
            "cnspell": "test_cnspell",
            "market": "test_market",
            "exchange": "test_exchange",
            "curr_type": "test_curr_type",
            "list_status": "test_list_status",
            "list_date": "test_list_date",
            "delist_date": "test_delist_date",
            "is_hs": "test_is_hs",
            "act_name": "test_act_name",
            "act_ent_type": "test_act_ent_type",
        }
        series = pd.Series(data)

        # 调用from_series方法
        result = StockBasicFields.from_series(series)

        # 验证所有字段都正确映射
        assert result.ts_code == "test_ts_code"
        assert result.symbol == "test_symbol"
        assert result.name == "test_name"
        assert result.area == "test_area"
        assert result.industry == "test_industry"
        assert result.fullname == "test_fullname"
        assert result.enname == "test_enname"
        assert result.cnspell == "test_cnspell"
        assert result.market == "test_market"
        assert result.exchange == "test_exchange"
        assert result.curr_type == "test_curr_type"
        assert result.list_status == "test_list_status"
        assert result.list_date == "test_list_date"
        assert result.delist_date == "test_delist_date"
        assert result.is_hs == "test_is_hs"
        assert result.act_name == "test_act_name"
        assert result.act_ent_type == "test_act_ent_type"
