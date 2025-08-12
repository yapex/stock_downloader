import pytest
import pandas as pd
from src.downloader2.models.stock_basic import StockBasicFields


class TestStockBasicFieldsToSeries:
    """测试 StockBasicFields.to_series 方法"""

    def test_to_series_with_complete_data(self):
        """测试完整数据转换为Series"""
        fields = StockBasicFields(
            ts_code="000001.SZ",
            symbol="000001",
            name="平安银行",
            area="深圳",
            industry="银行",
            full_name="平安银行股份有限公司",
            en_name="Ping An Bank Co., Ltd.",
            cnspell="PAYH",
            market="主板",
            exchange="SZSE",
            curr_type="CNY",
            list_status="L",
            list_date="19910403",
            delist_date=None,
            is_hs="S",
            act_name="平安银行股份有限公司",
            act_ent_type="境内非国有法人"
        )
        
        series = StockBasicFields.to_series(fields)
        
        assert isinstance(series, pd.Series)
        assert series["ts_code"] == "000001.SZ"
        assert series["symbol"] == "000001"
        assert series["name"] == "平安银行"
        assert series["area"] == "深圳"
        assert series["industry"] == "银行"
        assert series["fullname"] == "平安银行股份有限公司"
        assert series["enname"] == "Ping An Bank Co., Ltd."
        assert series["cnspell"] == "PAYH"
        assert series["market"] == "主板"
        assert series["exchange"] == "SZSE"
        assert series["curr_type"] == "CNY"
        assert series["list_status"] == "L"
        assert series["list_date"] == "19910403"
        assert pd.isna(series["delist_date"])
        assert series["is_hs"] == "S"
        assert series["act_name"] == "平安银行股份有限公司"
        assert series["act_ent_type"] == "境内非国有法人"

    def test_to_series_with_partial_data(self):
        """测试部分数据转换为Series"""
        fields = StockBasicFields(
            ts_code="000002.SZ",
            symbol="000002",
            name="万科A"
        )
        
        series = StockBasicFields.to_series(fields)
        
        assert isinstance(series, pd.Series)
        assert series["ts_code"] == "000002.SZ"
        assert series["symbol"] == "000002"
        assert series["name"] == "万科A"
        # 其他字段不应该出现在Series中，因为fields实例中没有这些属性
        assert "area" not in series.index
        assert "industry" not in series.index

    def test_to_series_with_none_values(self):
        """测试包含None值的数据转换为Series"""
        fields = StockBasicFields(
            ts_code="000003.SZ",
            symbol="000003",
            name="PT金田A",
            area=None,
            industry=None,
            delist_date="20040427"
        )
        
        series = StockBasicFields.to_series(fields)
        
        assert isinstance(series, pd.Series)
        assert series["ts_code"] == "000003.SZ"
        assert series["symbol"] == "000003"
        assert series["name"] == "PT金田A"
        assert pd.isna(series["area"])
        assert pd.isna(series["industry"])
        assert series["delist_date"] == "20040427"

    def test_to_series_with_empty_fields(self):
        """测试空字段实例转换为Series"""
        fields = StockBasicFields()
        
        series = StockBasicFields.to_series(fields)
        
        assert isinstance(series, pd.Series)
        assert len(series) == 0  # 没有任何字段

    def test_to_series_with_extra_attributes(self):
        """测试包含额外属性的字段实例转换为Series"""
        fields = StockBasicFields(
            ts_code="000004.SZ",
            symbol="000004",
            name="国华网安",
            extra_field="这是额外字段"  # 这个字段不在StockBasicFields常量中定义
        )
        
        series = StockBasicFields.to_series(fields)
        
        assert isinstance(series, pd.Series)
        assert series["ts_code"] == "000004.SZ"
        assert series["symbol"] == "000004"
        assert series["name"] == "国华网安"
        # extra_field不应该出现在Series中，因为它不在类常量中定义
        assert "extra_field" not in series.index

    def test_to_series_field_mapping(self):
        """测试字段名映射是否正确"""
        fields = StockBasicFields(
            ts_code="000005.SZ",
            symbol="000005",
            name="世纪星源",
            full_name="深圳世纪星源股份有限公司",
            en_name="Shenzhen Century Star Source Co., Ltd.",
            cnspell="SJXY",
            market="主板",
            exchange="SZSE",
            curr_type="CNY",
            list_status="L",
            list_date="19910403",
            delist_date=None,
            is_hs="S",
            act_name="深圳世纪星源股份有限公司",
            act_ent_type="境内非国有法人"
        )
        
        series = StockBasicFields.to_series(fields)
        
        # 验证字段名映射
        assert "fullname" in series.index  # full_name -> fullname
        assert "enname" in series.index    # en_name -> enname
        assert series["fullname"] == "深圳世纪星源股份有限公司"
        assert series["enname"] == "Shenzhen Century Star Source Co., Ltd."

    def test_to_series_roundtrip_conversion(self):
        """测试往返转换：from_series -> to_series"""
        # 创建原始Series
        original_data = {
            "ts_code": "000006.SZ",
            "symbol": "000006",
            "name": "深振业A",
            "area": "深圳",
            "industry": "房地产开发",
            "fullname": "深圳市振业(集团)股份有限公司",
            "enname": "Shenzhen Zhenye (Group) Co., Ltd.",
            "cnspell": "SZYA",
            "market": "主板",
            "exchange": "SZSE",
            "curr_type": "CNY",
            "list_status": "L",
            "list_date": "19920427",
            "delist_date": None,
            "is_hs": "S",
            "act_name": "深圳市振业(集团)股份有限公司",
            "act_ent_type": "境内非国有法人"
        }
        original_series = pd.Series(original_data)
        
        # from_series -> to_series
        fields = StockBasicFields.from_series(original_series)
        result_series = StockBasicFields.to_series(fields)
        
        # 验证往返转换的一致性
        for key in original_data.keys():
            if pd.isna(original_data[key]):
                assert pd.isna(result_series[key])
            else:
                assert result_series[key] == original_data[key]