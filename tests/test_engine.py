from unittest.mock import MagicMock, patch
import pandas as pd

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


# (conftest.py 中的 fixtures 不需要修改)

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


def test_engine_run_with_specific_symbols(mock_fetcher, mock_storage, mock_args):
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

    engine = DownloadEngine(config, mock_fetcher, mock_storage, mock_args)

    with patch.object(engine, "task_registry", mock_registry):
        engine.run()

        mock_daily_handler_class.assert_called_once()
        execute_mock = mock_daily_handler_class.return_value.execute
        execute_mock.assert_called_once()

        _, kwargs = execute_mock.call_args
        assert kwargs.get("target_symbols") == specific_symbols


# ===================================================================
#           核心修正：在测试函数内部进行 patch
# ===================================================================
def test_engine_run_with_symbols_all(mock_fetcher, mock_storage, mock_args):
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

    engine = DownloadEngine(config, mock_fetcher, mock_storage, mock_args)

    # 模拟从 DuckDBStorage 读取的股票列表
    expected_symbols_from_file = ["from_file_A", "from_file_B", "from_file_C"]
    mock_stock_list_df = pd.DataFrame({"ts_code": expected_symbols_from_file})

    # 模拟 DuckDBStorage 的方法
    mock_storage.table_exists.return_value = True
    mock_storage.query.return_value = mock_stock_list_df

    with patch.object(engine, "task_registry", mock_registry):
        engine.run()

        mock_daily_handler_class.assert_called_once()
        execute_mock = mock_daily_handler_class.return_value.execute
        execute_mock.assert_called_once()

        _, kwargs = execute_mock.call_args
        assert kwargs.get("target_symbols") == expected_symbols_from_file
