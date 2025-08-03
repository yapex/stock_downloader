import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import argparse

from downloader.engine import DownloadEngine


# Mock 类定义在模块级别
class MockDailyHandler:
    def __init__(self, *args):
        pass

    def execute(self, **kwargs):
        pass


class MockStockListHandler:
    def __init__(self, *args):
        pass

    def execute(self, **kwargs):
        pass


@pytest.fixture
def mock_fetcher():
    """一个模拟的Fetcher，为不同测试提供数据。"""
    fetcher = MagicMock()
    fetcher.fetch_stock_list.return_value = pd.DataFrame(
        {"ts_code": ["from_api_01", "from_api_02"]}
    )
    fetcher.fetch_daily_history.return_value = pd.DataFrame(
        {"trade_date": ["20230101"]}
    )
    return fetcher


@pytest.fixture
def mock_storage():
    """一个模拟的Storage，特别是为 'all' 模式模拟股票列表文件的读取。"""
    storage = MagicMock()
    # 模拟从Parquet文件读取的股票列表，这对于测试 'all' 模式至关重要
    mock_stock_list_from_file = pd.DataFrame(
        {"ts_code": ["from_file_A", "from_file_B", "from_file_C"]}
    )

    # 我们需要模拟 pandas.read_parquet 的行为
    with patch(
        "pandas.read_parquet", MagicMock(return_value=mock_stock_list_from_file)
    ):
        mock_path = MagicMock()
        mock_path.exists.return_value = True
        storage._get_file_path.return_value = mock_path
        yield storage


@pytest.fixture
def base_args():
    return argparse.Namespace(force=False)


# ==========================================================
#                      测试用例
# ==========================================================


def test_engine_discovers_tasks(monkeypatch):
    """测试引擎能否正确发现并加载entry_points。"""
    mock_ep = MagicMock()
    mock_ep.name = "mock_task"
    mock_ep.load.return_value = MockDailyHandler
    monkeypatch.setattr("downloader.engine.entry_points", lambda group: [mock_ep])
    engine = DownloadEngine({}, None, None, None)
    assert "mock_task" in engine.task_registry
    assert engine.task_registry["mock_task"] == MockDailyHandler


# === 新增：测试“指定symbols列表”模式 ===
def test_engine_run_with_specific_symbols(mock_fetcher, mock_storage, base_args):
    """
    测试当 config.downloader.symbols 是一个具体列表时，
    引擎是否会使用这个列表作为 target_symbols。
    """
    specific_symbols = ["000001.SZ", "600519.SH"]
    config = {
        "downloader": {"symbols": specific_symbols},
        "tasks": [
            {
                "name": "Test Daily",
                "enabled": True,
                "type": "daily",
                "update_strategy": "incremental",
            }
        ],
        "defaults": {},
    }

    mock_daily_handler_class = MagicMock()
    mock_registry = {"daily": mock_daily_handler_class}

    engine = DownloadEngine(config, mock_fetcher, mock_storage, base_args)

    with patch.object(engine, "task_registry", mock_registry):
        engine.run()

        mock_daily_handler_class.assert_called_once()
        execute_mock = mock_daily_handler_class.return_value.execute
        execute_mock.assert_called_once()

        # 核心断言：验证传递给 execute 的 target_symbols 正是我们在 config 中指定的列表
        _, kwargs = execute_mock.call_args
        assert kwargs.get("target_symbols") == specific_symbols


# === 新增：测试“all”模式 ===
def test_engine_run_with_symbols_all(mock_fetcher, mock_storage, base_args):
    """
    测试当 config.downloader.symbols 是 "all" 时，
    引擎是否会从 storage 加载列表并作为 target_symbols。
    """
    config = {
        "downloader": {"symbols": "all"},
        "tasks": [
            {
                "name": "Test Daily",
                "enabled": True,
                "type": "daily",
                "update_strategy": "incremental",
            }
        ],
        "defaults": {},
    }

    mock_daily_handler_class = MagicMock()
    mock_registry = {"daily": mock_daily_handler_class}

    engine = DownloadEngine(config, mock_fetcher, mock_storage, base_args)

    with patch.object(engine, "task_registry", mock_registry):
        engine.run()

        mock_daily_handler_class.assert_called_once()
        execute_mock = mock_daily_handler_class.return_value.execute
        execute_mock.assert_called_once()

        # 核心断言：验证传递的 target_symbols 是从 mock_storage (模拟文件) 中读取的
        _, kwargs = execute_mock.call_args
        expected_symbols_from_file = ["from_file_A", "from_file_B", "from_file_C"]
        assert kwargs.get("target_symbols") == expected_symbols_from_file
