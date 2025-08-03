import pandas as pd
from unittest.mock import patch
from downloader.tasks.daily_basic import DailyBasicTaskHandler


def test_daily_basic_task_handler_executes_correctly(
    mock_fetcher, mock_storage, mock_args
):
    """
    测试 DailyBasicTaskHandler 是否会为 target_symbols 中的每个股票调用正确的 fetcher 方法。
    """
    task_config = {"name": "Test Daily Basic", "type": "daily_basic"}
    target_symbols = ["000001.SZ", "600519.SH"]

    handler = DailyBasicTaskHandler(task_config, mock_fetcher, mock_storage, mock_args)
    handler.execute(target_symbols=target_symbols)

    # ---> 核心修正：断言正确的方法名 <---
    assert mock_fetcher.fetch_daily_basic.call_count == len(target_symbols)
    assert mock_storage.save.call_count == len(target_symbols)
    assert mock_storage.get_latest_date.call_count == len(target_symbols)
