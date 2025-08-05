import pytest
from unittest.mock import MagicMock
import pandas as pd
from downloader.tasks.daily import DailyTaskHandler


def test_network_error_handling_and_retry(mock_fetcher, mock_storage, mock_args):
    """
    测试网络错误处理和重试机制。
    """
    task_config = {"name": "Test Daily", "type": "daily", "adjust": "qfq"}
    target_symbols = ["000001.SZ", "600519.SH", "000002.SZ"]

    # 模拟网络错误
    mock_fetcher.fetch_daily_history.side_effect = [
        pd.DataFrame({"trade_date": ["20230102"]}),  # 第一个成功
        ConnectionError("Network connection failed"),  # 第二个网络错误
        TimeoutError("Request timeout"),  # 第三个超时错误
        pd.DataFrame({"trade_date": ["20230103"]}),  # 第二个重试成功
        ConnectionError("Network connection failed"),  # 第三个重试失败
    ]

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage, mock_args)
    handler.execute(target_symbols=target_symbols)

    # 验证调用次数：初始3次 + 重试2次 = 5次
    assert mock_fetcher.fetch_daily_history.call_count == 5
    # 第一个成功 + 第二个重试成功 = 2次保存
    assert mock_storage.save.call_count == 2


def test_network_error_retry_success(mock_fetcher, mock_storage, mock_args):
    """
    测试网络错误重试成功的情况。
    """
    task_config = {"name": "Test Daily", "type": "daily", "adjust": "qfq"}
    target_symbols = ["600519.SH"]

    # 第一次调用失败，第二次调用成功
    mock_fetcher.fetch_daily_history.side_effect = [
        ConnectionError("Network connection failed"),  # 第一次网络错误
        pd.DataFrame({"trade_date": ["20230102"]}),  # 重试成功
    ]

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage, mock_args)
    handler.execute(target_symbols=target_symbols)

    # 验证调用次数：初始1次 + 重试1次 = 2次
    assert mock_fetcher.fetch_daily_history.call_count == 2
    # 重试成功应该保存
    assert mock_storage.save.call_count == 1


def test_network_error_retry_failure(mock_fetcher, mock_storage, mock_args):
    """
    测试网络错误重试仍然失败的情况。
    """
    task_config = {"name": "Test Daily", "type": "daily", "adjust": "qfq"}
    target_symbols = ["600519.SH"]

    # 两次都失败
    mock_fetcher.fetch_daily_history.side_effect = [
        ConnectionError("Network connection failed"),  # 第一次网络错误
        ConnectionError("Network connection failed"),  # 重试仍然失败
    ]

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage, mock_args)
    handler.execute(target_symbols=target_symbols)

    # 验证调用次数：初始1次 + 重试1次 = 2次
    assert mock_fetcher.fetch_daily_history.call_count == 2
    # 重试失败不应该保存
    assert mock_storage.save.call_count == 0