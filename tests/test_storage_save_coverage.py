import pandas as pd
import pytest
import warnings
import tempfile
import sys
from pathlib import Path

# 添加 src 到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from downloader.storage import ParquetStorage


def test_storage_save_method_coverage():
    """测试确保 storage.py 中 save 方法的关键路径被覆盖"""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(tmpdir)
        data_type = "test"
        entity_id = "000001"
        date_col = "trade_date"
        
        # 测试完整的代码路径:
        # 1. 保存初始数据 (file_path 不存在的情况)
        df1 = pd.DataFrame({
            "trade_date": ["20230101", "20230102"],
            "open": [10.0, 11.0],
            "high": [11.0, 12.0],
            "low": [9.0, 10.0],
            "close": [10.5, 11.5],
            "volume": [1000, 1100]
        })
        
        # 确保没有 FutureWarning
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            storage.save(df1, data_type, entity_id, date_col)
            
            # 检查是否有 FutureWarning
            future_warnings = [warning for warning in w if issubclass(warning.category, FutureWarning)]
            assert len(future_warnings) == 0
        
        # 2. 保存更多数据 (file_path 存在，且两个 DataFrame 都非空)
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
            assert len(future_warnings) == 0
        
        # 3. 保存空数据 (file_path 存在，新数据为空)
        empty_df = pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "volume"])
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            storage.save(empty_df, data_type, entity_id, date_col)
            
            # 检查是否有 FutureWarning
            future_warnings = [warning for warning in w if issubclass(warning.category, FutureWarning)]
            assert len(future_warnings) == 0

        # 4. 测试去重和排序路径
        df3 = pd.DataFrame({
            "trade_date": ["20230101", "20230105"],  # 包含重复日期
            "open": [10.0, 14.0],
            "high": [11.0, 15.0],
            "low": [9.0, 13.0],
            "close": [10.5, 14.5],
            "volume": [1000, 1400]
        })
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            storage.save(df3, data_type, entity_id, date_col)
            
            # 检查是否有 FutureWarning
            future_warnings = [warning for warning in w if issubclass(warning.category, FutureWarning)]
            assert len(future_warnings) == 0