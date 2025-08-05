import pandas as pd
import pytest
import warnings
import tempfile
import sys
from pathlib import Path

# 添加 src 到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from downloader.storage import ParquetStorage


def test_concat_future_warning_comprehensive():
    """全面测试确保在各种情况下都不会产生 FutureWarning"""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(tmpdir)
        data_type = "test"
        entity_id = "000001"
        date_col = "trade_date"
        
        # 测试1: 保存空 DataFrame
        empty_df = pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "volume"])
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            storage.save(empty_df, data_type, entity_id, date_col)
            
            # 检查是否有 FutureWarning
            future_warnings = [warning for warning in w if issubclass(warning.category, FutureWarning)]
            assert len(future_warnings) == 0, f"保存空 DataFrame 时不应该有 FutureWarning，但得到了: {[str(warn.message) for warn in future_warnings]}"
        
        # 测试2: 在已有空数据的基础上保存新数据
        df = pd.DataFrame({
            "trade_date": ["20230101", "20230102"],
            "open": [10.0, 11.0],
            "high": [11.0, 12.0],
            "low": [9.0, 10.0],
            "close": [10.5, 11.5],
            "volume": [1000, 1100]
        })
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            storage.save(df, data_type, entity_id, date_col)
            
            # 检查是否有 FutureWarning
            future_warnings = [warning for warning in w if issubclass(warning.category, FutureWarning)]
            assert len(future_warnings) == 0, f"在空数据基础上保存新数据时不应该有 FutureWarning，但得到了: {[str(warn.message) for warn in future_warnings]}"
        
        # 测试3: 在已有数据基础上保存空 DataFrame
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            storage.save(empty_df, data_type, entity_id, date_col)
            
            # 检查是否有 FutureWarning
            future_warnings = [warning for warning in w if issubclass(warning.category, FutureWarning)]
            assert len(future_warnings) == 0, f"在已有数据基础上保存空 DataFrame 时不应该有 FutureWarning，但得到了: {[str(warn.message) for warn in future_warnings]}"
        
        # 测试4: 保存有数据的 DataFrame（正常情况）
        df2 = pd.DataFrame({
            "trade_date": ["20230103", "20230104"],
            "open": [12.0, 13.0],
            "high": [13.0, 14.0],
            "low": [11.0, 12.0],
            "close": [12.5, 13.5],
            "volume": [1200, 1300]
        })
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            storage.save(df2, data_type, entity_id, date_col)
            
            # 检查是否有 FutureWarning
            future_warnings = [warning for warning in w if issubclass(warning.category, FutureWarning)]
            assert len(future_warnings) == 0, f"保存有数据的 DataFrame 时不应该有 FutureWarning，但得到了: {[str(warn.message) for warn in future_warnings]}"


def test_concat_future_warning_with_all_na_dataframe():
    """测试确保在处理全 NA DataFrame 时不会产生 FutureWarning"""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(tmpdir)
        data_type = "test"
        entity_id = "000002"
        date_col = "trade_date"
        
        # 创建一个全 NA 的 DataFrame
        na_df = pd.DataFrame({
            "trade_date": [None, None],
            "open": [None, None],
            "high": [None, None],
            "low": [None, None],
            "close": [None, None],
            "volume": [None, None]
        })
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            storage.save(na_df, data_type, entity_id, date_col)
            
            # 检查是否有 FutureWarning
            future_warnings = [warning for warning in w if issubclass(warning.category, FutureWarning)]
            assert len(future_warnings) == 0, f"保存全 NA DataFrame 时不应该有 FutureWarning，但得到了: {[str(warn.message) for warn in future_warnings]}"