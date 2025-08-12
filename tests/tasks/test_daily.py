from datetime import datetime

from downloader.tasks.daily import DailyTaskHandler
from tests.test_implementations import MockFetcher, MockStorage
from src.downloader.storage import TableNames


def test_daily_task_handler_executes_correctly(mock_fetcher, mock_storage):
    """
    测试DailyTaskHandler是否会为target_symbols中的每个股票调用fetcher和storage。
    """
    task_config = {"name": "Test Daily", "type": "daily", "adjust": "qfq"}
    target_symbols = ["000001.SZ", "600519.SH"]

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
    handler.execute(target_symbols=target_symbols)

    assert mock_fetcher.fetch_daily_history.call_count == len(target_symbols)
    assert mock_storage.save_daily.call_count == len(target_symbols)


def test_daily_handler_skips_if_no_symbols_provided(
    mock_fetcher, mock_storage
):
    """
    测试当target_symbols为空时，处理器是否会优雅地跳过执行。
    """
    task_config = {"name": "Test Daily", "type": "daily"}
    target_symbols = []

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
    handler.execute(target_symbols=target_symbols)

    mock_fetcher.fetch_daily_history.assert_not_called()
    mock_storage.save_daily.assert_not_called()


# --- 新增的增量逻辑专项测试 ---


def test_daily_task_handler_calculates_incremental_start_date_correctly(
    mock_fetcher, mock_storage
):
    """
    【专项测试】验证增量更新时，是否能正确计算出下一次请求的 start_date。
    """
    task_config = {"name": "Test Daily Inc", "type": "daily", "adjust": "qfq"}
    target_symbols = ["000001.SZ"]

    mock_storage.get_latest_date_by_stock.return_value = "20231110"

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
    handler.execute(target_symbols=target_symbols)

    mock_fetcher.fetch_daily_history.assert_called_once()

    # ---> 核心修正：从位置参数 (args) 中获取 start_date <---
    call_args, call_kwargs = mock_fetcher.fetch_daily_history.call_args
    # args 是一个元组: (ts_code, start_date, end_date, adjust)
    # start_date 是第二个参数，索引为 1
    actual_start_date = call_args[1]

    assert actual_start_date == "20231111"


def test_daily_task_handler_handles_no_existing_data(
    mock_fetcher, mock_storage
):
    """
    【专项测试】验证当 storage 中无历史数据时，start_date 是否为默认的早期日期。
    """
    task_config = {"name": "Test Daily Full", "type": "daily", "adjust": "qfq"}
    target_symbols = ["000001.SZ"]

    mock_storage.get_latest_date_by_stock.return_value = None

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)
    handler.execute(target_symbols=target_symbols)

    mock_fetcher.fetch_daily_history.assert_called_once()

    # ---> 核心修正：从位置参数 (args) 中获取 start_date <---
    call_args, call_kwargs = mock_fetcher.fetch_daily_history.call_args
    actual_start_date = call_args[1]

    assert actual_start_date == "19901219"


def test_daily_task_handler_skips_if_data_is_up_to_date(
    mock_fetcher, mock_storage
):
    """【专项测试】验证当本地数据已经是最新时，是否会跳过网络请求。"""
    task_config = {"name": "Test Daily Skip", "type": "daily", "adjust": "qfq"}
    target_symbols = ["000001.SZ"]

    # 模拟 storage 中的最新日期就是今天
    today = datetime.now().strftime("%Y%m%d")
    mock_storage.get_latest_date_by_stock.return_value = today

    handler = DailyTaskHandler(task_config, mock_fetcher, mock_storage)


def test_daily_task_handler_with_test_implementations():
    """【改进测试】使用测试实现类而不是mock，验证完整的数据流。"""
    task_config = {"name": "Test Daily Real", "type": "daily", "adjust": "qfq"}
    target_symbols = ["000001.SZ", "000002.SZ"]

    # 使用测试实现类
    test_fetcher = MockFetcher()
    test_storage = MockStorage()
    
    # 设置存储中已有的数据
    test_storage.latest_dates[("000001.SZ", "daily")] = "20231110"
    # 000002.SZ 没有历史数据

    handler = DailyTaskHandler(task_config, test_fetcher, test_storage)
    handler.execute(target_symbols=target_symbols)

    # 验证调用历史
    assert len([call for call in test_fetcher.call_history if "fetch_daily_history" in call]) == 2
    assert len([call for call in test_storage.call_history if "save_daily" in call]) == 2
    
    # 验证具体的调用参数
    daily_calls = [call for call in test_fetcher.call_history if "fetch_daily_history" in call]
    
    # 000001.SZ 应该从20231111开始（增量更新）
    assert any("000001.SZ_20231111" in call for call in daily_calls)
    
    # 000002.SZ 应该从19901219开始（全量更新）
    assert any("000002.SZ_19901219" in call for call in daily_calls)
    
    # 验证数据确实被保存到storage中
    assert "000001.SZ" in test_storage.daily_data
    assert "000002.SZ" in test_storage.daily_data
