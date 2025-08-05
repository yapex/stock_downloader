import pandas as pd
import pytest
import warnings
import tempfile
import os
import sys
from pathlib import Path

# 添加 src 到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from downloader.storage import ParquetStorage


def test_concat_future_warning():
    """测试确保在连接空 DataFrame 时不会产生 FutureWarning"""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(tmpdir)
        data_type = "test"
        entity_id = "000001"
        date_col = "trade_date"
        
        # 创建一个空的 DataFrame
        empty_df = pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "volume"])
        
        # 确保没有 FutureWarning
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            storage.save(empty_df, data_type, entity_id, date_col)
            
            # 检查是否有 FutureWarning
            future_warnings = [warning for warning in w if issubclass(warning.category, FutureWarning)]
            assert len(future_warnings) == 0, f"不应该有 FutureWarning，但得到了: {[str(warn.message) for warn in future_warnings]}"
            
        # 创建一个有数据的 DataFrame
        df = pd.DataFrame({
            "trade_date": ["20230101", "20230102"],
            "open": [10.0, 11.0],
            "high": [11.0, 12.0],
            "low": [9.0, 10.0],
            "close": [10.5, 11.5],
            "volume": [1000, 1100]
        })
        
        # 再次保存，测试连接操作
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            storage.save(df, data_type, entity_id, date_col)
            
            # 检查是否有 FutureWarning
            future_warnings = [warning for warning in w if issubclass(warning.category, FutureWarning)]
            assert len(future_warnings) == 0, f"不应该有 FutureWarning，但得到了: {[str(warn.message) for warn in future_warnings]}"