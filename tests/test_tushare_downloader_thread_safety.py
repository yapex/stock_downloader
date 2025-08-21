import pytest
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch, MagicMock
from queue import Queue

from downloader2.producer.tushare_downloader import TushareDownloader
from downloader2.producer.fetcher_builder import TaskType
from downloader2.interfaces.task_handler import TaskEventType


class TestTushareDownloaderThreadSafety:
    """测试 TushareDownloader 的线程安全性"""
    
    @pytest.fixture
    def mock_event_bus(self):
        """模拟事件总线"""
        return Mock()
    
    @pytest.fixture
    def mock_executor(self):
        """模拟线程池执行器"""
        executor = Mock(spec=ThreadPoolExecutor)
        executor._max_workers = 2
        return executor
    
    @pytest.fixture
    def downloader(self, mock_event_bus, mock_executor):
        """创建测试用的下载器"""
        symbols = ['000001.SZ', '000002.SZ', '000003.SZ']
        return TushareDownloader(
            symbols=symbols,
            task_type=TaskType.STOCK_DAILY,
            executor=mock_executor,
            event_bus=mock_event_bus
        )
    
    def test_concurrent_stop_calls(self, downloader):
        """测试并发停止调用的线程安全性"""
        exceptions = []
        
        def stop_downloader():
            try:
                downloader.stop()
            except Exception as e:
                exceptions.append(e)
        
        # 创建多个线程同时调用 stop
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=stop_downloader)
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=5.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"停止时发生异常: {exceptions}"
        assert downloader._stop_called.is_set(), "停止标志应该被设置"
        
        # 验证事件只被发布一次
        assert downloader.event_bus.publish.call_count == 1, "事件应该只被发布一次"
    
    def test_progress_update_thread_safety(self, downloader):
        """测试进度更新的线程安全性"""
        exceptions = []
        
        # 模拟真实场景：每个线程处理不同的 symbol
        def process_symbols(start_idx, count):
            try:
                for i in range(count):
                    symbol_idx = start_idx + i
                    if symbol_idx < len(downloader.symbols):
                        # 模拟处理成功
                        downloader._update_progress('success')
                    time.sleep(0.001)
            except Exception as e:
                exceptions.append(e)
        
        threads = []
        symbols_per_thread = 1
        for i in range(min(3, len(downloader.symbols))):
            thread = threading.Thread(target=process_symbols, args=(i, symbols_per_thread))
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=5.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"进度更新时发生异常: {exceptions}"
        assert downloader.processed_symbols <= downloader.total_symbols, "处理数量不应超过总数量"
        assert downloader.processed_symbols == downloader.successful_symbols + downloader.failed_symbols, "进度计数应该一致"
    
    def test_completion_check_thread_safety(self, downloader):
        """测试完成检查的线程安全性"""
        exceptions = []
        
        def check_completion():
            try:
                for _ in range(20):
                    downloader._check_completion()
                    time.sleep(0.001)
            except Exception as e:
                exceptions.append(e)
        
        # 同时更新进度和检查完成状态
        def process_symbols(count):
            try:
                for i in range(min(count, len(downloader.symbols))):
                    downloader._update_progress('success')
                    time.sleep(0.001)
            except Exception as e:
                exceptions.append(e)
        
        threads = []
        # 创建检查完成状态的线程
        for _ in range(2):
            thread = threading.Thread(target=check_completion)
            threads.append(thread)
        
        # 创建更新进度的线程
        thread = threading.Thread(target=process_symbols, args=(2,))
        threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=5.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"完成检查时发生异常: {exceptions}"
        assert downloader.processed_symbols <= downloader.total_symbols, "处理数量不应超过总数量"
    
    def test_worker_thread_safety(self, downloader):
        """测试工作线程的线程安全性"""
        # 填充任务队列
        for symbol in downloader.symbols:
            downloader.task_queue.put(symbol)
        
        exceptions = []
        
        def worker_wrapper():
            try:
                # 模拟工作线程行为
                downloader._worker()
            except Exception as e:
                exceptions.append(e)
        
        # 模拟 _process_symbol 方法
        def mock_process_symbol(symbol):
            time.sleep(0.01)  # 模拟处理时间
            downloader._update_progress('success')
        
        downloader._process_symbol = mock_process_symbol
        
        # 创建多个工作线程
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=worker_wrapper)
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待一段时间后停止
        time.sleep(0.5)
        downloader._shutdown_event.set()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=5.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"工作线程执行时发生异常: {exceptions}"
    
    def test_rate_limiter_thread_safety(self, downloader):
        """测试限流器的线程安全性"""
        exceptions = []
        
        def apply_rate_limiting():
            try:
                downloader._apply_rate_limiting()
            except Exception as e:
                exceptions.append(e)
        
        # 创建多个线程同时应用限流
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=apply_rate_limiting)
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=10.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"限流时发生异常: {exceptions}"
    
    def test_retry_mechanism_thread_safety(self, downloader):
        """测试重试机制的线程安全性"""
        exceptions = []
        
        def handle_error(symbol):
            try:
                error = Exception(f"测试错误 {symbol}")
                downloader._handle_fetch_error(symbol, error)
            except Exception as e:
                exceptions.append(e)
        
        # 创建多个线程同时处理错误
        threads = []
        for i, symbol in enumerate(downloader.symbols):
            thread = threading.Thread(target=handle_error, args=(symbol,))
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=5.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"错误处理时发生异常: {exceptions}"
    
    def test_shutdown_thread_safety(self, downloader):
        """测试关闭操作的线程安全性"""
        exceptions = []
        
        def shutdown_downloader():
            try:
                downloader._shutdown(wait=False)
            except Exception as e:
                exceptions.append(e)
        
        # 创建多个线程同时关闭
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=shutdown_downloader)
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=5.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"关闭时发生异常: {exceptions}"
        assert downloader._shutdown_event.is_set(), "关闭事件应该被设置"
    
    def test_queue_operations_thread_safety(self, downloader):
        """测试队列操作的线程安全性"""
        exceptions = []
        
        def populate_queue():
            try:
                downloader._populate_symbol_queue()
            except Exception as e:
                exceptions.append(e)
        
        def consume_queue():
            try:
                while not downloader.task_queue.empty():
                    try:
                        symbol = downloader.task_queue.get(timeout=0.1)
                        downloader.task_queue.task_done()
                    except:
                        break
            except Exception as e:
                exceptions.append(e)
        
        # 创建生产者和消费者线程
        producer_threads = []
        consumer_threads = []
        
        for _ in range(2):
            producer_thread = threading.Thread(target=populate_queue)
            consumer_thread = threading.Thread(target=consume_queue)
            producer_threads.append(producer_thread)
            consumer_threads.append(consumer_thread)
        
        # 启动所有线程
        for thread in producer_threads + consumer_threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in producer_threads + consumer_threads:
            thread.join(timeout=5.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"队列操作时发生异常: {exceptions}"
    
    def test_event_publishing_thread_safety(self, downloader):
        """测试事件发布的线程安全性"""
        # 模拟多个线程同时触发事件发布
        exceptions = []
        
        def trigger_event_publishing():
            try:
                # 模拟完成状态
                downloader.processed_symbols = downloader.total_symbols
                downloader.successful_symbols = downloader.total_symbols
                downloader._check_completion()
            except Exception as e:
                exceptions.append(e)
        
        # 创建多个线程
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=trigger_event_publishing)
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=5.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"事件发布时发生异常: {exceptions}"
        # 验证事件只被发布一次（由于 _stop_called 防护）
        assert downloader.event_bus.publish.call_count <= 1, "事件发布次数应该被控制"
    
    def test_data_processing_thread_safety(self, downloader):
        """测试数据处理的线程安全性"""
        exceptions = []
        processed_data = []
        data_lock = threading.Lock()
        
        def process_data(start_idx, count):
            try:
                for i in range(count):
                    symbol_idx = start_idx + i
                    if symbol_idx < len(downloader.symbols):
                        # 模拟数据处理
                        symbol = downloader.symbols[symbol_idx]
                        downloader._update_progress('success')
                        with data_lock:
                            processed_data.append(symbol)
                    time.sleep(0.001)
            except Exception as e:
                exceptions.append(e)
        
        threads = []
        symbols_per_thread = 1
        for i in range(min(3, len(downloader.symbols))):
            thread = threading.Thread(target=process_data, args=(i, symbols_per_thread))
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=5.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"数据处理时发生异常: {exceptions}"
        assert downloader.processed_symbols <= downloader.total_symbols, "处理数量不应超过总数量"
        assert len(processed_data) == downloader.processed_symbols, "处理的数据数量应该与进度一致"
    
    def test_memory_consistency(self, downloader):
        """测试内存一致性"""
        # 测试在多线程环境下状态变量的一致性
        exceptions = []
        
        def worker(worker_id, iterations):
            try:
                for i in range(iterations):
                    # 交替成功和失败，确保不超过总数
                    if downloader.processed_symbols < downloader.total_symbols:
                        result = 'success' if i % 2 == 0 else 'failed'
                        downloader._update_progress(result)
                    time.sleep(0.001)
            except Exception as e:
                exceptions.append(e)
        
        threads = []
        iterations_per_thread = min(10, downloader.total_symbols // 3 + 1)
        for i in range(3):
            thread = threading.Thread(target=worker, args=(i, iterations_per_thread))
            threads.append(thread)
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join(timeout=5.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"内存一致性测试时发生异常: {exceptions}"
        # 验证状态一致性
        assert downloader.processed_symbols == downloader.successful_symbols + downloader.failed_symbols, "进度计数应该一致"
        assert downloader.processed_symbols <= downloader.total_symbols, "处理数量不应超过总数量"