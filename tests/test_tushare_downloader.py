import pytest
import time
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch, Mock, call
import pandas as pd
from box import Box

# 假设您的代码文件路径如下
from downloader2.producer.tushare_downloader import TushareDownloader
from downloader2.producer.fetcher_builder import TaskType

# --- 常量定义 ---
SUCCESS_SYM = "000001.SZ"
FAIL_SYM = "666666.SH"
EMPTY_SYM = "000002.SZ"
SUCCESS_DATA = pd.DataFrame({"ts_code": [SUCCESS_SYM], "close": [10.0]})
EMPTY_DATA = pd.DataFrame()


class TestTushareDownloaderBusinessLogic:
    """TushareDownloader 核心业务逻辑测试"""

    @pytest.fixture
    def executor(self):
        ex = ThreadPoolExecutor(max_workers=2)
        yield ex
        ex.shutdown(wait=True)

    @pytest.fixture
    def downloader(self, executor):
        with patch("downloader2.producer.tushare_downloader.FetcherBuilder"):
            mock_event_bus = Mock()
            instance = TushareDownloader(
                symbols=[SUCCESS_SYM, FAIL_SYM, EMPTY_SYM],
                task_type=TaskType.STOCK_DAILY,
                data_queue=Queue(),
                executor=executor,
                event_bus=mock_event_bus,
            )
            instance._fetching_by_symbol = Mock()
            yield instance
            instance._shutdown(wait=False, timeout=1.0)

    # ... 其他测试用例保持不变，它们是正确的 ...

    def test_happy_path_successful_download(self, downloader):
        downloader._fetching_by_symbol.return_value = SUCCESS_DATA
        downloader._process_symbol(SUCCESS_SYM)
        task = downloader.data_queue.get()
        assert task.data.equals(SUCCESS_DATA)
        assert downloader.successful_symbols == 1
        assert downloader.processed_symbols == 1

    def test_handling_of_empty_dataframe(self, downloader):
        downloader._fetching_by_symbol.return_value = EMPTY_DATA
        downloader._process_symbol(EMPTY_SYM)
        assert downloader.data_queue.empty()
        assert downloader.successful_symbols == 1
        assert downloader.processed_symbols == 1

    def test_retry_once_and_then_succeed(self, downloader):
        downloader._fetching_by_symbol.side_effect = [
            Exception("网络错误"),
            SUCCESS_DATA,
        ]
        downloader._process_symbol(SUCCESS_SYM)
        assert downloader.retry_counts.get(SUCCESS_SYM) == 1
        assert downloader.processed_symbols == 0
        retry_symbol = downloader.task_queue.get()
        downloader._process_symbol(retry_symbol)
        assert downloader.data_queue.qsize() == 1
        assert downloader.successful_symbols == 1
        assert downloader.processed_symbols == 1

    def test_failure_after_max_retries(self, downloader):
        downloader.max_retries = 1
        downloader._fetching_by_symbol.side_effect = Exception("持续错误")
        downloader._process_symbol(FAIL_SYM)  # 第一次
        downloader._process_symbol(FAIL_SYM)  # 第二次 (放弃)
        assert downloader.data_queue.empty()
        assert downloader.failed_symbols == 1
        assert downloader.processed_symbols == 1

    # ==================================================================
    # === 唯一的修正点在这里 ===
    # ==================================================================
    def test_full_workflow_integration(self, downloader):
        """
        测试场景：一个完整的端到端流程，混合成功和失败的任务。
        """

        def smart_fetcher(symbol):
            if symbol == SUCCESS_SYM:
                return SUCCESS_DATA
            if symbol == EMPTY_SYM:
                return EMPTY_DATA
            if symbol == FAIL_SYM:
                raise Exception("无效的符号")
            return pd.DataFrame()

        downloader.max_retries = 0  # 简化测试，失败不重试
        downloader._fetching_by_symbol.side_effect = smart_fetcher

        # --- 关键修正：移除下面这行代码 ---
        # downloader._populate_symbol_queue()  # <--- 移除此行！

        # --- 行动 (Act) ---
        # start() 会自己调用 _populate_symbol_queue()，确保任务只添加一次
        downloader.start()

        downloader.task_queue.join()
        downloader.stop()

        # --- 断言 (Assert) ---
        assert downloader.successful_symbols == 2
        assert downloader.failed_symbols == 1
        assert downloader.processed_symbols == 3
        assert downloader.data_queue.qsize() == 1
        task = downloader.data_queue.get()
        assert task.data["ts_code"].iloc[0] == SUCCESS_SYM
