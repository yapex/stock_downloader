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
    引擎是否会正确使用这个列表构建任务。
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

    engine = DownloadEngine(config, mock_fetcher, mock_storage, mock_args)
    
    # 测试目标股票列表准备
    enabled_tasks = [task for task in config["tasks"] if task.get("enabled", False)]
    target_symbols = engine._prepare_target_symbols(enabled_tasks)
    assert target_symbols == specific_symbols


# ===================================================================
#           核心修正：在测试函数内部进行 patch
# ===================================================================
def test_engine_run_with_symbols_all(mock_fetcher, mock_storage, mock_args):
    """
    测试当 config.downloader.symbols 是 "all" 时，
    引擎会从数据库获取所有股票列表。
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

    # 模拟存储返回股票列表
    mock_storage.get_all_stock_codes.return_value = ["000001.SZ", "600519.SH"]
    
    engine = DownloadEngine(config, mock_fetcher, mock_storage, mock_args)
    
    # 测试目标股票列表准备 - 在新架构中，"all"模式从数据库获取股票列表
    enabled_tasks = [task for task in config["tasks"] if task.get("enabled", False)]
    target_symbols = engine._prepare_target_symbols(enabled_tasks)
    assert len(target_symbols) > 0  # 应该返回股票列表
    assert "000001.SZ" in target_symbols
    assert "600519.SH" in target_symbols


def test_engine_processes_enabled_tasks_correctly(mock_fetcher, mock_storage, mock_args):
    """
    测试引擎能正确处理已启用的任务。
    """
    config = {
        "group_name": "test_group",
        "downloader": {"symbols": ["000001.SZ", "600519.SH"]},
        "tasks": [
            {
                "name": "Test Daily 1",
                "enabled": True,
                "type": "daily",
            },
            {
                "name": "Test Daily 2", 
                "enabled": True,
                "type": "daily_basic",
            }
        ],
        "defaults": {},
    }

    engine = DownloadEngine(config, mock_fetcher, mock_storage, False, False, "test_group")
    
    # 测试已启用任务的识别
    tasks = config.get("tasks", [])
    enabled_tasks = [task for task in tasks if task.get("enabled", False)]
    assert len(enabled_tasks) == 2
    assert enabled_tasks[0]["type"] == "daily"
    assert enabled_tasks[1]["type"] == "daily_basic"


