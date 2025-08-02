import pandas as pd
import pytest
import shutil
from pathlib import Path

from downloader.storage import ParquetStorage


@pytest.fixture(scope="function")
def temp_storage():
    """创建一个函数级别的临时存储实例和目录。"""
    test_data_path = Path("./test_data_pytest")
    if test_data_path.exists():
        shutil.rmtree(test_data_path)
    storage_instance = ParquetStorage(base_path=test_data_path)
    yield storage_instance
    if test_data_path.exists():
        shutil.rmtree(test_data_path)


@pytest.fixture
def mock_daily_df_part1():
    """提供第一批模拟的日线数据。"""
    return pd.DataFrame(
        {
            "ts_code": ["600519.SH"] * 3,
            "trade_date": ["20231009", "20231010", "20231011"],
            "open": [1800.0, 1810.0, 1820.0],
            "close": [1805.0, 1815.0, 1825.0],
        }
    )


@pytest.fixture
def mock_daily_df_part2():
    """提供第二批（增量）模拟日线数据。"""
    return pd.DataFrame(
        {
            "ts_code": ["600519.SH"] * 3,
            "trade_date": ["20231011", "20231012", "20231013"],
            "open": [1821.0, 1830.0, 1840.0],
            "close": [1826.0, 1835.0, 1845.0],
        }
    )


def test_storage_initial_save_and_get_date(temp_storage, mock_daily_df_part1):
    """测试初次保存和随后的日期获取功能。"""
    storage = temp_storage
    df = mock_daily_df_part1
    ts_code = "600519.SH"
    data_type = "daily_qfq"

    storage.save(df, data_type, ts_code, date_col="trade_date")

    file_path = storage._get_file_path(data_type, ts_code)
    assert file_path.exists()
    saved_df = pd.read_parquet(file_path)
    pd.testing.assert_frame_equal(
        df.sort_values(by="trade_date").reset_index(drop=True), saved_df
    )

    latest_date = storage.get_latest_date(data_type, ts_code, date_col="trade_date")
    assert latest_date == "20231011"


def test_storage_incremental_save(
    temp_storage, mock_daily_df_part1, mock_daily_df_part2
):
    """测试增量更新功能，包括去重。"""
    storage = temp_storage
    ts_code = "600519.SH"
    data_type = "daily_qfq"

    storage.save(mock_daily_df_part1, data_type, ts_code, date_col="trade_date")
    storage.save(mock_daily_df_part2, data_type, ts_code, date_col="trade_date")

    file_path = storage._get_file_path(data_type, ts_code)
    updated_df = pd.read_parquet(file_path)

    assert len(updated_df) == 5
    assert updated_df["trade_date"].is_unique
    updated_row = updated_df[updated_df["trade_date"] == "20231011"]
    assert updated_row["open"].iloc[0] == 1821.0
