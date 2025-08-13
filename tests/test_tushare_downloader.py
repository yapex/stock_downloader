import pytest
import threading
import time
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch
import pandas as pd
from box import Box

from downloader2.tushare_downloader import TushareDownloader
from downloader2.factories.fetcher_builder import TaskType


class TestTushareDownloader:
    """TushareDownloader 核心功能测试"""

    def setup_method(self):
        """设置测试环境"""
        self.symbols = ["000001.SZ", "000002.SZ", "600000.SH"]
        self.task_type = TaskType.STOCK_DAILY
        self.data_queue = Queue()
        self.executor = ThreadPoolExecutor(max_workers=2)

        with patch("downloader2.tushare_downloader.FetcherBuilder"):
            self.downloader = TushareDownloader(
                symbols=self.symbols,
                task_type=self.task_type,
                data_queue=self.data_queue,
                executor=self.executor,
            )

    def teardown_method(self):
        """清理测试环境"""
        try:
            self.downloader.shutdown(timeout=2.0)
        except Exception:
            pass
        self.executor.shutdown(wait=False)

    def test_initialization(self):
        """测试初始化"""
        assert self.downloader.symbols == self.symbols
        assert self.downloader.task_type == self.task_type
        assert self.downloader.data_queue == self.data_queue
        assert self.downloader.executor == self.executor
        assert isinstance(self.downloader.task_queue, Queue)
        assert self.downloader.worker_count == 4
        assert len(self.downloader.worker_threads) == 0
        assert self.downloader.rate_limiter is not None
        assert self.downloader.retry_counts == {}
        assert self.downloader.max_retries == 2

    def test_populate_symbol_queue(self):
        """测试队列填充"""
        self.downloader._populate_symbol_queue()

        # 验证队列中的内容
        queue_items = []
        while not self.downloader.task_queue.empty():
            try:
                item = self.downloader.task_queue.get_nowait()
                queue_items.append(item)
            except Empty:
                break

        # 应该包含所有 symbols 和 worker_count 个 None
        symbols_in_queue = [item for item in queue_items if item is not None]
        sentinels_in_queue = [item for item in queue_items if item is None]

        assert set(symbols_in_queue) == set(self.symbols)
        assert len(sentinels_in_queue) == self.downloader.worker_count

    def test_start_creates_worker_threads(self):
        """测试启动时创建工作线程"""
        # Mock _fetching_by_symbol 以避免实际调用
        mock_df = Mock()
        mock_df.empty = True
        self.downloader._fetching_by_symbol = Mock(return_value=mock_df)

        self.downloader.start()

        # 验证线程创建
        assert len(self.downloader.worker_threads) == self.downloader.worker_count

        # 验证线程命名
        for i, thread in enumerate(self.downloader.worker_threads):
            assert thread.name == f"TushareWorker-{i}"
            assert not thread.daemon  # 确保不是守护线程

    def test_worker_processes_symbols(self):
        """测试工作线程处理 symbols"""
        # Mock _fetching_by_symbol 返回测试数据
        test_data = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        self.downloader._fetching_by_symbol = Mock(return_value=test_data)

        # 手动添加一个 symbol 到队列
        self.downloader.task_queue.put("TEST.SH")
        self.downloader.task_queue.put(None)  # 结束标记

        # 创建并启动一个工作线程
        worker_thread = threading.Thread(target=self.downloader._worker)
        worker_thread.start()
        worker_thread.join(timeout=2.0)

        # 验证数据被放入数据队列
        assert not self.data_queue.empty()
        task = self.data_queue.get()
        assert isinstance(task, Box)
        assert task.task_type == self.task_type.value
        assert task.data.equals(test_data)

        # 验证 _fetching_by_symbol 被调用
        self.downloader._fetching_by_symbol.assert_called_once_with("TEST.SH")

    def test_worker_handles_exceptions(self):
        """测试工作线程异常处理"""
        # Mock _fetching_by_symbol 抛出异常
        self.downloader._fetching_by_symbol = Mock(side_effect=Exception("测试异常"))
        
        # 监控 task_done 调用
        original_task_done = self.downloader.task_queue.task_done
        task_done_count = 0
        
        def mock_task_done():
            nonlocal task_done_count
            task_done_count += 1
            original_task_done()
        
        self.downloader.task_queue.task_done = mock_task_done
        
        # 手动添加一个 symbol 到队列
        self.downloader.task_queue.put("TEST.SH")
        
        # 创建并启动一个工作线程
        worker_thread = threading.Thread(target=self.downloader._worker)
        worker_thread.start()
        
        # 等待足够长的时间让重试完成
        time.sleep(3.0)
        
        # 添加结束标记
        self.downloader.task_queue.put(None)
        worker_thread.join(timeout=5.0)
        
        # 验证异常不会阻止线程正常结束
        assert not worker_thread.is_alive()
        
        # 验证 task_done 被正确调用
        # 应该是2次：1次最终放弃处理 + 1次None结束标记
        # (重试时不调用task_done，只有最终放弃时才调用)
        assert task_done_count == 2
        
        # 验证数据队列为空（异常时不应添加数据）
        assert self.data_queue.empty()

    def test_retry_mechanism(self):
        """测试重试机制"""
        # 创建一个会失败2次然后成功的mock
        call_count = 0
        test_data = pd.DataFrame({"col1": [1]})
        
        def mock_fetching_with_retries(symbol):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:  # 前两次调用失败
                raise Exception(f"测试异常 - 第{call_count}次调用")
            return test_data  # 第三次调用成功
        
        self.downloader._fetching_by_symbol = mock_fetching_with_retries
        
        # 手动添加一个 symbol 到队列
        test_symbol = "TEST.SH"
        self.downloader.task_queue.put(test_symbol)
        
        # 创建并启动一个工作线程
        worker_thread = threading.Thread(target=self.downloader._worker)
        worker_thread.start()
        
        # 等待足够长的时间让重试完成
        time.sleep(3.0)
        
        # 添加结束标记
        self.downloader.task_queue.put(None)
        worker_thread.join(timeout=5.0)
        
        # 验证线程正常结束
        assert not worker_thread.is_alive()
        
        # 验证重试机制工作正常
        assert call_count == 3  # 应该被调用3次（2次失败 + 1次成功）
        
        # 验证最终成功后数据被添加到队列
        assert not self.data_queue.empty()
        result = self.data_queue.get()
        assert result.task_type == self.task_type.value
        assert not result.data.empty
        
        # 验证重试计数被清理
        assert test_symbol not in self.downloader.retry_counts

    def test_retry_mechanism_max_retries_exceeded(self):
        """测试超过最大重试次数的情况"""
        # Mock _fetching_by_symbol 总是抛出异常
        self.downloader._fetching_by_symbol = Mock(side_effect=Exception("持续失败"))
        
        # 手动添加一个 symbol 到队列
        test_symbol = "FAIL.SH"
        self.downloader.task_queue.put(test_symbol)
        
        # 创建并启动一个工作线程
        worker_thread = threading.Thread(target=self.downloader._worker)
        worker_thread.start()
        
        # 等待足够长的时间让重试完成
        time.sleep(3.0)
        
        # 添加结束标记
        self.downloader.task_queue.put(None)
        worker_thread.join(timeout=5.0)
        
        # 验证线程正常结束
        assert not worker_thread.is_alive()
        
        # 验证被调用了3次（1次初始 + 2次重试）
        assert self.downloader._fetching_by_symbol.call_count == 3
        
        # 验证数据队列为空（因为所有尝试都失败了）
        assert self.data_queue.empty()
        
        # 验证重试计数被清理
        assert test_symbol not in self.downloader.retry_counts

    def test_shutdown_with_running_threads(self):
        """测试优雅关闭功能"""
        # Mock _fetching_by_symbol 返回测试数据
        test_data = pd.DataFrame({"col1": [1]})
        self.downloader._fetching_by_symbol = Mock(return_value=test_data)

        # 启动下载器
        self.downloader.start()

        # 等待一小段时间让线程启动
        time.sleep(0.1)

        # 关闭下载器
        result = self.downloader.shutdown(timeout=5.0)

        # 验证关闭成功
        assert result is True

        # 验证所有线程都已结束
        for thread in self.downloader.worker_threads:
            assert not thread.is_alive()

    def test_complete_workflow(self):
        """测试完整工作流程"""

        # Mock _fetching_by_symbol 返回不同的数据
        def mock_fetch(symbol):
            return pd.DataFrame(
                {"symbol": [symbol], "price": [100 if "1" in symbol else 200]}
            )

        self.downloader._fetching_by_symbol = mock_fetch

        # 启动下载器
        self.downloader.start()

        # 等待完成
        success = self.downloader.shutdown(timeout=5.0)

        # 验证结果
        assert success

        # 收集所有数据
        results = []
        while not self.data_queue.empty():
            task = self.data_queue.get()
            results.append(task)

        # 验证数据正确性
        assert len(results) == len(self.symbols)
        for task in results:
            assert task.task_type == self.task_type.value
            assert isinstance(task.data, pd.DataFrame)
            assert not task.data.empty
