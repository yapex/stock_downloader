from downloader.tasks.daily import DailyTaskHandler


def test_daily_task_handler_executes_correctly(mock_fetcher, mock_storage, mock_args):
    """
    测试DailyTaskHandler是否会为target_symbols中的每个股票调用fetcher和storage。
    """
    task_config = {"name": "Test Daily", "type": "daily", "adjust": "qfq"}
    target_symbols = ["000001.SZ", "600519.SH"]

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage, mock_args)
    handler.execute(target_symbols=target_symbols)

    # 验证fetcher为列表中的每只股票都被调用了
    assert mock_fetcher.fetch_daily_history.call_count == len(target_symbols)
    # 验证storage.save也被相应调用了
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

    # 验证fetcher和storage完全没有被调用
    mock_fetcher.fetch_daily_history.assert_not_called()
    mock_storage.save.assert_not_called()
