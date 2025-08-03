import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import argparse

# --- 共享的 Mock 对象 ---


@pytest.fixture
def mock_fetcher():
    """一个可供所有测试使用的、模拟的Fetcher实例。"""
    fetcher = MagicMock()
    fetcher.fetch_stock_list.return_value = pd.DataFrame({"ts_code": ["000001.SZ"]})
    fetcher.fetch_daily_history.return_value = pd.DataFrame(
        {"trade_date": ["20230102"]}
    )
    fetcher.fetch_daily_basic_by_code.return_value = pd.DataFrame(
        {"trade_date": ["20230102"]}
    )
    fetcher.fetch_daily_basic.return_value = pd.DataFrame({"ts_code": ["000001.SZ"]})
    return fetcher


@pytest.fixture
def mock_storage():
    """一个可供所有测试使用的、模拟的Storage实例。"""
    storage = MagicMock()
    storage.get_latest_date.return_value = "20230101"
    storage.save.return_value = None
    storage.overwrite.return_value = None
    # 模拟_get_file_path返回一个MagicMock对象，该对象具有exists方法
    mock_path = MagicMock()
    mock_path.exists.return_value = True
    storage._get_file_path.return_value = mock_path
    return storage


@pytest.fixture
def mock_args():
    """一个可供所有测试使用的、模拟的命令行参数。"""
    return argparse.Namespace(force=False)
