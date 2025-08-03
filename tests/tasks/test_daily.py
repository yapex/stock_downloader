import pytest
from unittest.mock import MagicMock
from datetime import datetime, timedelta
import pandas as pd

from downloader.tasks.daily import DailyTaskHandler


def test_daily_task_handler_executes_correctly(mock_fetcher, mock_storage, mock_args):
    """
    测试DailyTaskHandler是否会为target_symbols中的每个股票调用fetcher和storage。
    """
    task_config = {"name": "Test Daily", "type": "daily", "adjust": "qfq"}
    target_symbols = ["000001.SZ", "600519.SH"]

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage, mock_args)
    handler.execute(target_symbols=target_symbols)

    assert mock_fetcher.fetch_daily_history.call_count == len(target_symbols)
    assert mock_storage.save.call_count == len(target_symbols)


def test_daily_handler_skips_if_no_symbols_provided(
    mock_fetcher, mock_storage, mock_args
):
    """
    测试当target_symbols为空时，处理器是否会优雅地跳过执行。
    """
    task_config = {"name": "Test Daily", "type": "daily"}
    target_symbols = []

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage, mock_args)
    handler.execute(target_symbols=target_symbols)

    mock_fetcher.fetch_daily_history.assert_not_called()
    mock_storage.save.assert_not_called()


# --- 新增的增量逻辑专项测试 ---


def test_daily_task_handler_calculates_incremental_start_date_correctly(
    mock_fetcher, mock_storage, mock_args
):
    """
    【专项测试】验证增量更新时，是否能正确计算出下一次请求的 start_date。
    """
    task_config = {"name": "Test Daily Inc", "type": "daily", "adjust": "qfq"}
    target_symbols = ["000001.SZ"]

    mock_storage.get_latest_date.return_value = "20231110"

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage, mock_args)
    handler.execute(target_symbols=target_symbols)

    mock_fetcher.fetch_daily_history.assert_called_once()

    # ---> 核心修正：从位置参数 (args) 中获取 start_date <---
    call_args, call_kwargs = mock_fetcher.fetch_daily_history.call_args
    # args 是一个元组: (ts_code, start_date, end_date, adjust)
    # start_date 是第二个参数，索引为 1
    actual_start_date = call_args[1]

    assert actual_start_date == "20231111"


def test_daily_task_handler_handles_no_existing_data(
    mock_fetcher, mock_storage, mock_args
):
    """
    【专项测试】验证当 storage 中无历史数据时，start_date 是否为默认的早期日期。
    """
    task_config = {"name": "Test Daily Full", "type": "daily", "adjust": "qfq"}
    target_symbols = ["000001.SZ"]

    mock_storage.get_latest_date.return_value = None

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage, mock_args)
    handler.execute(target_symbols=target_symbols)

    mock_fetcher.fetch_daily_history.assert_called_once()

    # ---> 核心修正：从位置参数 (args) 中获取 start_date <---
    call_args, call_kwargs = mock_fetcher.fetch_daily_history.call_args
    actual_start_date = call_args[1]

    assert actual_start_date == "19901219"


def test_daily_task_handler_skips_if_data_is_up_to_date(
    mock_fetcher, mock_storage, mock_args
):
    """
    【专项测试】验证当本地数据已经是最新时，是否会跳过网络请求。
    """
    task_config = {"name": "Test Daily Skip", "type": "daily", "adjust": "qfq"}
    target_symbols = ["000001.SZ"]

    # 模拟 storage 中的最新日期就是今天
    today = datetime.now().strftime("%Y%m%d")
    mock_storage.get_latest_date.return_value = today

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage, mock_args)
    handler.execute(target_symbols=target_symbols)

    mock_fetcher.fetch_daily_history.assert_not_called()
