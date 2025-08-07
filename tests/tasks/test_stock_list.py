from downloader.tasks.stock_list import StockListTaskHandler


def test_stock_list_handler_executes_correctly(mock_fetcher, mock_storage):
    """
    测试StockListTaskHandler的核心执行逻辑。
    """
    task_config = {
        "name": "Test SL",
        "type": "stock_list",
        "update_strategy": "cooldown",
    }

    # 模拟文件不存在，以确保下载逻辑被触发
    mock_storage._get_file_path.return_value.exists.return_value = False
    # 模拟没有最后更新时间，以跳过冷却期检查
    mock_storage.get_table_last_updated.return_value = None

    handler = StockListTaskHandler(task_config, mock_fetcher, mock_storage)
    handler.execute()  # 它不接收kwargs

    # 验证调用了正确的fetcher和storage方法
    mock_fetcher.fetch_stock_list.assert_called_once()
    mock_storage.overwrite.assert_called_once()
