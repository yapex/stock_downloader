import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import argparse
from datetime import datetime, timedelta

from downloader.engine import DownloadEngine


# Mock 类定义在模块级别
class MockDailyHandler:
    def __init__(self, *args):
        pass

    def execute(self):
        pass


class MockStockListHandler:
    def __init__(self, *args):
        pass

    def execute(self):
        pass


@pytest.fixture
def mock_fetcher():
    fetcher = MagicMock()
    fetcher.fetch_stock_list.return_value = pd.DataFrame(
        {"ts_code": ["000001.SZ", "600519.SH"]}
    )
    fetcher.fetch_daily_history.return_value = pd.DataFrame(
        {"trade_date": ["20230101"]}
    )
    return fetcher


@pytest.fixture
def mock_storage():
    storage = MagicMock()
    # 模拟股票列表文件存在且可读
    mock_stock_list_df = pd.DataFrame(
        {"ts_code": ["000001.SZ", "600519.SH", "300059.SZ"]}
    )
    with patch("pandas.read_parquet", MagicMock(return_value=mock_stock_list_df)):
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


def test_engine_dispatches_with_specific_symbols(mock_fetcher, mock_storage, base_args):
    """测试当配置为具体symbols列表时，任务能否被正确分发。"""
    # ---> 核心修正：为任务添加 'update_strategy' <---
    config = {
        "downloader": {"symbols": ["000001.SZ"]},
        "tasks": [
            {
                "name": "Test Daily",
                "enabled": True,
                "type": "daily",
                "update_strategy": "incremental",  # <--- 加上这一行
            }
        ],
        "defaults": {},  # 加上一个空的defaults，避免KeyError
    }
    mock_daily_handler_class = MagicMock()
    mock_registry = {"daily": mock_daily_handler_class}

    engine = DownloadEngine(config, mock_fetcher, mock_storage, base_args)

    with patch.object(engine, "task_registry", mock_registry):
        engine.run()

        mock_daily_handler_class.assert_called_once()
        call_args, _ = mock_daily_handler_class.call_args
        passed_config = call_args[0]
        assert passed_config["target_symbols"] == ["000001.SZ"]
        mock_daily_handler_class.return_value.execute.assert_called_once()


def test_engine_handles_symbols_all_correctly(mock_fetcher, mock_storage, base_args):
    """【回归测试】测试当配置为 symbols: "all" 时，能否正确加载列表并注入任务。"""
    # ---> 核心修正：为任务添加 'update_strategy' <---
    config = {
        "downloader": {"symbols": "all"},
        "tasks": [
            {
                "name": "Test Daily",
                "enabled": True,
                "type": "daily",
                "update_strategy": "incremental",  # <--- 加上这一行
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
        call_args, _ = mock_daily_handler_class.call_args
        passed_config = call_args[0]

        expected_symbols = ["000001.SZ", "600519.SH", "300059.SZ"]
        assert passed_config["target_symbols"] == expected_symbols
        mock_daily_handler_class.return_value.execute.assert_called_once()
