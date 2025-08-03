import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import argparse
from datetime import datetime, timedelta

from downloader.engine import DownloadEngine


# Mock 类定义在模块级别，以便被引用
class MockDailyHandler:
    pass


class MockStockListHandler:
    pass


@pytest.fixture
def mock_fetcher():
    return MagicMock()


@pytest.fixture
def mock_storage():
    return MagicMock()


@pytest.fixture
def base_config():
    return {
        "defaults": {"symbols": ["000001.SZ"]},
        "tasks": [
            {"name": "更新A股列表", "enabled": True, "type": "stock_list"},
            {"name": "日K线-前复权", "enabled": True, "type": "daily", "adjust": "qfq"},
        ],
    }


@pytest.fixture
def base_args():
    return argparse.Namespace(force=False)


def test_engine_discovers_tasks(monkeypatch):
    """测试引擎能否正确发现并加载entry_points。"""
    mock_ep = MagicMock()
    mock_ep.name = "mock_task"
    mock_ep.load.return_value = MockDailyHandler

    monkeypatch.setattr("downloader.engine.entry_points", lambda group: [mock_ep])

    engine = DownloadEngine({}, None, None, None)

    assert "mock_task" in engine.task_registry
    assert engine.task_registry["mock_task"] == MockDailyHandler


# ==========================================================
#                      核心修正部分
# ==========================================================
def test_engine_run_dispatches_tasks(
    base_config, mock_fetcher, mock_storage, base_args
):
    """测试引擎的run方法是否会正确地分发任务到处理器。"""

    # 1. 准备：直接用 MagicMock 来模拟处理器类
    mock_daily_handler_class = MagicMock()
    mock_stock_list_handler_class = MagicMock()

    # 2. 准备：模拟的注册表，其值就是我们的 mock 对象
    mock_registry = {
        "daily": mock_daily_handler_class,
        "stock_list": mock_stock_list_handler_class,
    }

    engine = DownloadEngine(base_config, mock_fetcher, mock_storage, base_args)

    # 3. 只需 patch 引擎的 task_registry 属性
    with patch.object(engine, "task_registry", mock_registry):
        # 4. 行动
        engine.run()

        # 5. 断言
        # 验证处理器“类”被调用（即实例化）了一次
        mock_daily_handler_class.assert_called_once()
        # 验证返回的“实例”的 execute 方法被调用了一次
        mock_daily_handler_class.return_value.execute.assert_called_once()

        mock_stock_list_handler_class.assert_called_once()
        mock_stock_list_handler_class.return_value.execute.assert_called_once()
