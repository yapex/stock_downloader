import pandas as pd
from downloader.tasks.daily import DailyTaskHandler
from src.downloader.storage import TableNames


def test_network_error_handling_and_retry(mock_fetcher, mock_storage):
    """
    测试网络错误处理。现在重试逻辑在fetcher层，task层只调用一次。
    """
    task_config = {"name": "Test Daily", "type": TableNames.DAILY_DATA, "adjust": "qfq"}
    target_symbols = ["000001.SZ", "600519.SH", "000002.SZ"]

    # 模拟各种错误场景
    mock_fetcher.fetch_daily_history.side_effect = [
        pd.DataFrame({"trade_date": ["20230102"]}),  # 第一个成功
        ConnectionError("Network connection failed"),  # 第二个网络错误（fetcher层会重试）
        TimeoutError("Request timeout"),  # 第三个超时错误（fetcher层会重试）
    ]

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
    handler.execute(target_symbols=target_symbols)

    # 验证调用次数：每个股票只调用一次，共3次
    assert mock_fetcher.fetch_daily_history.call_count == 3
    # 只有第一个成功保存
    assert mock_storage.save_daily.call_count == 1


def test_network_error_retry_success(mock_fetcher, mock_storage):
    """
    测试模拟fetcher层重试成功的情况。
    """
    task_config = {"name": "Test Daily", "type": "daily", "adjust": "qfq"}
    target_symbols = ["600519.SH"]

    # 模拟fetcher层重试成功，最终返回数据
    mock_fetcher.fetch_daily_history.return_value = pd.DataFrame({"trade_date": ["20230102"]})

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
    handler.execute(target_symbols=target_symbols)

    # 验证调用次数：每个股票只调用一次
    assert mock_fetcher.fetch_daily_history.call_count == 1
    # 成功应该保存
    assert mock_storage.save_daily.call_count == 1


def test_network_error_retry_failure(mock_fetcher, mock_storage):
    """
    测试模拟fetcher层重试仍然失败的情况。
    """
    task_config = {"name": "Test Daily", "type": "daily", "adjust": "qfq"}
    target_symbols = ["600519.SH"]

    # 模拟fetcher层重试后仍然失败，返回None
    mock_fetcher.fetch_daily_history.return_value = None

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
    handler.execute(target_symbols=target_symbols)

    # 验证调用次数：每个股票只调用一次
    assert mock_fetcher.fetch_daily_history.call_count == 1
    # 失败不应该保存
    assert mock_storage.save_daily.call_count == 0
