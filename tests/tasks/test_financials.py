from downloader.tasks.financials import FinancialsTaskHandler
from unittest.mock import call


def test_financials_handler_executes_correctly(mock_fetcher, mock_storage, mock_args):
    """
    测试 FinancialsTaskHandler 是否会为每个股票调用正确的 fetcher 方法。
    """
    # 准备一个利润表的任务配置
    task_config = {
        "name": "Test Income",
        "type": "financials",
        "statement_type": "income",  # 指定要下载利润表
        "date_col": "ann_date",
    }
    target_symbols = ["000001.SZ", "600519.SH"]

    handler = FinancialsTaskHandler(task_config, mock_fetcher, mock_storage, mock_args)
    handler.execute(target_symbols=target_symbols)

    # 验证 fetch_income 方法被正确调用了N次
    assert mock_fetcher.fetch_income.call_count == len(target_symbols)
    # 验证其他财报方法没有被调用
    mock_fetcher.fetch_balancesheet.assert_not_called()
    mock_fetcher.fetch_cashflow.assert_not_called()
    # 验证 storage.save 被调用了
    assert mock_storage.save.call_count == len(target_symbols)
