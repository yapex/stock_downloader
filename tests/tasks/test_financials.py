from downloader.tasks.financials import FinancialsTaskHandler
from unittest.mock import ANY  # 导入 ANY 用于匹配不关心的参数


def test_financials_handler_executes_correctly(mock_fetcher, mock_storage):
    """
    测试 FinancialsTaskHandler 是否会为每个股票调用正确的 fetcher 方法，
    并传递所有必需参数。
    """
    task_config = {
        "name": "Test Income",
        "type": "financials",
        "statement_type": "income",
        "date_col": "ann_date",
    }
    target_symbols = ["000001.SZ", "600519.SH"]

    handler = FinancialsTaskHandler(task_config, mock_fetcher, mock_storage)
    handler.execute(target_symbols=target_symbols)

    # 验证 fetch_income 方法被正确调用了N次
    assert mock_fetcher.fetch_income.call_count == len(target_symbols)

    # ---> 核心修正：验证调用时的参数 <---
    # 我们可以抽查第一次调用
    mock_fetcher.fetch_income.assert_any_call(
        ts_code="000001.SZ",
        start_date=ANY,  # 我们不关心start_date的具体值，只关心它存在
        end_date=ANY,  # 验证end_date参数被传递了
    )

    # 验证其他财报方法没有被调用
    mock_fetcher.fetch_balancesheet.assert_not_called()
    mock_fetcher.fetch_cashflow.assert_not_called()

    # 验证 storage.save_financial_data 被调用了
    assert mock_storage.save_financial_data.call_count == len(target_symbols)
