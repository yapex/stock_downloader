"""测试新的 TushareDownloader 实现

验证基于新架构的实现是否与原有测试兼容。
"""

import pytest
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch, Mock, call
import pandas as pd
from box import Box

from downloader.producer.tushare_downloader import TushareDownloader
from downloader.producer.fetcher_builder import TaskType
from downloader.producer.huey_tasks import process_fetched_data

# 测试数据
SUCCESS_SYM = "000001.SZ"
FAIL_SYM = "666666.SH"
EMPTY_SYM = "000002.SZ"
SUCCESS_DATA = pd.DataFrame({"ts_code": [SUCCESS_SYM], "close": [10.0]})
EMPTY_DATA = pd.DataFrame()


class TestTushareDownloaderBusinessLogic:
    """测试新 TushareDownloader 的业务逻辑"""

    @pytest.fixture
    def executor(self):
        """线程池执行器（兼容性）"""
        with ThreadPoolExecutor(max_workers=2) as executor:
            yield executor

    @pytest.fixture
    def downloader(self, executor, huey_immediate):
        """创建下载器实例"""
        symbols = [SUCCESS_SYM, EMPTY_SYM, FAIL_SYM]
        downloader = TushareDownloader(
            symbols=symbols,
            task_type=TaskType.STOCK_DAILY,
            executor=executor,
            max_workers=2,
            max_retries=3,
        )

        # Mock FetcherBuilder 的 build_by_task 方法
        with patch.object(
            downloader.task_executor.download_task.fetcher_builder, "build_by_task"
        ) as mock_build:
            downloader._mock_build = mock_build
            yield downloader

    def test_happy_path_successful_download(self, downloader, huey_immediate):
        """测试成功下载的快乐路径"""
        with patch(
            "downloader.producer.tushare_downloader.process_fetched_data"
        ) as mock_task:
            # Mock fetcher 返回成功数据
            mock_fetcher = Mock(return_value=SUCCESS_DATA)
            downloader._mock_build.return_value = mock_fetcher

            downloader._process_symbol(SUCCESS_SYM)
            mock_task.assert_called_once_with(
                SUCCESS_SYM, TaskType.STOCK_DAILY.name, SUCCESS_DATA.to_dict()
            )
            assert downloader.successful_symbols == 1
            assert downloader.processed_symbols == 1

    def test_handling_of_empty_dataframe(self, downloader, huey_immediate):
        """测试空数据框的处理"""
        with patch(
            "downloader.producer.tushare_downloader.process_fetched_data"
        ) as mock_task:
            # Mock fetcher 返回空数据
            mock_fetcher = Mock(return_value=EMPTY_DATA)
            downloader._mock_build.return_value = mock_fetcher

            downloader._process_symbol(EMPTY_SYM)
            mock_task.assert_not_called()  # 空数据不应该触发任务
            assert downloader.successful_symbols == 1
            assert downloader.processed_symbols == 1

    def test_retry_once_and_then_succeed(self, downloader, huey_immediate):
        """测试重试一次后成功"""
        with patch(
            "downloader.producer.tushare_downloader.process_fetched_data"
        ) as mock_task:
            # Mock fetcher 第一次失败，第二次成功
            mock_fetcher = Mock(
                side_effect=[
                    Exception("网络错误"),
                    SUCCESS_DATA,
                ]
            )
            downloader._mock_build.return_value = mock_fetcher

            downloader._process_symbol(SUCCESS_SYM)
            assert downloader.retry_counts.get(SUCCESS_SYM) == 1
            assert downloader.processed_symbols == 0
            retry_symbol = downloader.task_queue.get()
            downloader._process_symbol(retry_symbol)
            mock_task.assert_called_once_with(
                SUCCESS_SYM, TaskType.STOCK_DAILY.name, SUCCESS_DATA.to_dict()
            )
            assert downloader.successful_symbols == 1
            assert downloader.processed_symbols == 1

    def test_failure_after_max_retries(self, downloader, huey_immediate):
        """测试达到最大重试次数后失败"""
        with patch(
            "downloader.producer.tushare_downloader.process_fetched_data"
        ) as mock_task:
            downloader.max_retries = 1
            # Mock fetcher 持续失败
            mock_fetcher = Mock(side_effect=Exception("持续错误"))
            downloader._mock_build.return_value = mock_fetcher

            downloader._process_symbol(FAIL_SYM)  # 第一次
            downloader._process_symbol(FAIL_SYM)  # 第二次 (放弃)
            mock_task.assert_not_called()  # 失败的任务不应该触发 Huey 任务
            assert downloader.failed_symbols == 1
            assert downloader.processed_symbols == 1

    def test_full_workflow_integration(self, downloader, huey_immediate):
        """测试完整的端到端流程"""
        with patch(
            "downloader.producer.tushare_downloader.process_fetched_data"
        ) as mock_task:

            def smart_fetcher():
                # 根据调用次数返回不同结果
                call_count = smart_fetcher.call_count
                smart_fetcher.call_count += 1

                if call_count == 0:  # SUCCESS_SYM
                    return SUCCESS_DATA
                elif call_count == 1:  # EMPTY_SYM
                    return EMPTY_DATA
                elif call_count == 2:  # FAIL_SYM
                    raise Exception("无效的符号")
                return pd.DataFrame()

            smart_fetcher.call_count = 0
            downloader.max_retries = 0  # 简化测试，失败不重试

            # Mock fetcher 使用智能函数
            mock_fetcher = Mock(side_effect=smart_fetcher)
            downloader._mock_build.return_value = mock_fetcher

            # 启动下载器
            downloader.start()

            # 等待任务完成
            time.sleep(0.5)  # 给任务一些时间完成

            # 停止下载器
            downloader.stop()

            # 验证结果
            stats = downloader.get_stats()
            assert stats["successful_symbols"] == 2  # SUCCESS_SYM 和 EMPTY_SYM
            assert stats["failed_symbols"] == 1  # FAIL_SYM
            assert stats["processed_symbols"] == 3

            # 验证只有成功的非空数据触发了 Huey 任务
            mock_task.assert_called_once_with(
                SUCCESS_SYM, TaskType.STOCK_DAILY.name, SUCCESS_DATA.to_dict()
            )

    def test_start_stop_lifecycle(self, downloader, huey_immediate):
        """测试启动停止生命周期"""
        assert not downloader.is_running()

        downloader.start()
        assert downloader.is_running()

        downloader.stop()
        time.sleep(0.1)  # 给停止操作一些时间
        assert not downloader.is_running()

    def test_stats_tracking(self, downloader, huey_immediate):
        """测试统计信息跟踪"""
        stats = downloader.get_stats()
        assert stats["successful_symbols"] == 0
        assert stats["failed_symbols"] == 0
        assert stats["processed_symbols"] == 0
        assert stats["total_symbols"] == 3

        # 模拟处理一个成功的符号
        mock_fetcher = Mock(return_value=SUCCESS_DATA)
        downloader._mock_build.return_value = mock_fetcher
        downloader._process_symbol(SUCCESS_SYM)

        stats = downloader.get_stats()
        assert stats["successful_symbols"] == 1
        assert stats["processed_symbols"] == 1

    def test_compatibility_methods(self, downloader, huey_immediate):
        """测试兼容性方法"""
        # 测试 _populate_symbol_queue
        downloader._populate_symbol_queue()
        assert downloader.task_queue.qsize() == 3

        # 测试 _shutdown
        downloader._shutdown(wait=False)
        assert not downloader.is_running()
