import pytest
import threading
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch, MagicMock

from downloader2.producer.downloader_manager import DownloaderManager
from downloader2.producer.fetcher_builder import TaskType
from downloader2.interfaces.task_handler import TaskEventType


class TestDownloaderManagerThreadSafety:
    """测试 DownloaderManager 的线程安全性"""
    
    @pytest.fixture
    def mock_config(self):
        """模拟配置"""
        # 模拟配置对象
        mock_config = Mock()
        mock_config.task_groups = {
            'test_group': ['stock_basic', 'stock_daily']
        }
        
        # 模拟 FetcherBuilder 和股票数据
        mock_fetcher = Mock()
        mock_df = pd.DataFrame({'ts_code': ['000001.SZ', '600519.SH']})
        mock_fetcher.return_value = mock_df
        
        mock_fetcher_builder = Mock()
        mock_fetcher_builder.build_by_task.return_value = mock_fetcher
        
        # 模拟 TushareDownloader
        mock_downloader = Mock()
        mock_downloader.start = Mock()
        mock_downloader.stop = Mock()
        mock_downloader.task_type = Mock()
        mock_downloader.task_type.value = 'test_task'
        
        with patch('downloader2.producer.downloader_manager.config', mock_config), \
             patch('downloader2.producer.downloader_manager.task_groups', mock_config.task_groups), \
             patch('downloader2.producer.downloader_manager.FetcherBuilder', return_value=mock_fetcher_builder), \
             patch('downloader2.producer.downloader_manager.TushareDownloader', return_value=mock_downloader), \
             patch.object(DownloaderManager, '_register_signal_handlers'), \
             patch.object(DownloaderManager, '_restore_signal_handlers'):
            yield mock_config
    
    @pytest.fixture
    def mock_fetcher_builder(self):
        """模拟 FetcherBuilder"""
        with patch('downloader2.producer.downloader_manager.FetcherBuilder') as mock_builder:
            mock_instance = Mock()
            mock_fetcher = Mock()
            mock_fetcher.return_value = Mock(empty=False, ts_code=['000001.SZ', '000002.SZ'])
            mock_instance.build_by_task.return_value = mock_fetcher
            mock_builder.return_value = mock_instance
            yield mock_builder
    
    @pytest.fixture
    def mock_downloader(self):
        """模拟 TushareDownloader"""
        with patch('downloader2.producer.downloader_manager.TushareDownloader') as mock_downloader_class:
            mock_downloader = Mock()
            mock_downloader.task_type = TaskType.STOCK_BASIC
            mock_downloader.start = Mock()
            mock_downloader.stop = Mock()
            mock_downloader_class.return_value = mock_downloader
            yield mock_downloader
    
    def test_concurrent_start_calls(self, mock_config, mock_fetcher_builder, mock_downloader):
        """测试并发启动调用的线程安全性"""
        # 提供 symbols 参数，避免需要获取股票列表
        manager = DownloaderManager('test_group', symbols=['000001.SZ', '600519.SH'])
        
        results = []
        exceptions = []
        
        def start_manager():
            try:
                result = manager.start()
                results.append(result)
            except Exception as e:
                exceptions.append(e)
        
        # 创建多个线程同时调用 start
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=start_manager)
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=5.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"启动时发生异常: {exceptions}"
        assert manager.is_started, "管理器应该已启动"
        assert len(results) == 5, "所有启动调用都应该返回结果"
        
        # 清理
        manager.stop()
    
    def test_concurrent_stop_calls(self, mock_config, mock_fetcher_builder, mock_downloader):
        """测试并发停止调用的线程安全性"""
        manager = DownloaderManager('test_group', symbols=['000001.SZ', '600519.SH'])
        manager.start()
        
        exceptions = []
        
        def stop_manager():
            try:
                manager.stop()
            except Exception as e:
                exceptions.append(e)
        
        # 创建多个线程同时调用 stop
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=stop_manager)
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=5.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"停止时发生异常: {exceptions}"
        assert manager.is_stopped, "管理器应该已停止"
        assert manager.is_shutdown_complete, "管理器应该完全关闭"
    
    def test_start_stop_race_condition(self, mock_config, mock_fetcher_builder, mock_downloader):
        """测试启动和停止的竞态条件"""
        manager = DownloaderManager('test_group', symbols=['000001.SZ', '600519.SH'])
        
        start_exceptions = []
        stop_exceptions = []
        
        def start_manager():
            try:
                manager.start()
            except Exception as e:
                start_exceptions.append(e)
        
        def stop_manager():
            try:
                time.sleep(0.1)  # 稍微延迟以增加竞态条件的可能性
                manager.stop()
            except Exception as e:
                stop_exceptions.append(e)
        
        # 同时启动和停止
        start_thread = threading.Thread(target=start_manager)
        stop_thread = threading.Thread(target=stop_manager)
        
        start_thread.start()
        stop_thread.start()
        
        start_thread.join(timeout=5.0)
        stop_thread.join(timeout=5.0)
        
        # 验证没有异常
        assert len(start_exceptions) == 0, f"启动时发生异常: {start_exceptions}"
        assert len(stop_exceptions) == 0, f"停止时发生异常: {stop_exceptions}"
    
    def test_event_handler_thread_safety(self, mock_config, mock_fetcher_builder, mock_downloader):
        """测试事件处理器的线程安全性"""
        manager = DownloaderManager('test_group', symbols=['000001.SZ', '600519.SH'])
        manager.start()
        
        # 模拟多个下载器同时完成
        mock_senders = []
        for i in range(3):
            mock_sender = Mock()
            mock_sender.task_type = Mock()
            mock_sender.task_type.value = f'task_{i}'
            mock_senders.append(mock_sender)
        
        exceptions = []
        
        def trigger_event(sender):
            try:
                manager._on_downloader_finished(
                    sender,
                    total_task_count=10,
                    processed_task_count=10,
                    successful_task_count=10,
                    failed_task_count=0
                )
            except Exception as e:
                exceptions.append(e)
        
        # 创建多个线程同时触发事件
        threads = []
        for sender in mock_senders:
            thread = threading.Thread(target=trigger_event, args=(sender,))
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=5.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"事件处理时发生异常: {exceptions}"
        
        # 清理
        manager.stop()
    
    def test_signal_handler_registration_thread_safety(self, mock_config, mock_fetcher_builder, mock_downloader):
        """测试信号处理器注册的线程安全性"""
        manager = DownloaderManager('test_group', symbols=['000001.SZ', '600519.SH'])
        
        exceptions = []
        call_count = 0
        
        def register_signals():
            nonlocal call_count
            try:
                # 模拟信号处理器注册逻辑
                with manager._state_lock:
                    if not manager._signal_handlers_registered:
                        manager._signal_handlers_registered = True
                        call_count += 1
            except Exception as e:
                exceptions.append(e)
        
        # 创建多个线程同时注册信号处理器
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=register_signals)
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=5.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"信号处理器注册时发生异常: {exceptions}"
        assert manager._signal_handlers_registered, "信号处理器应该已注册"
        assert call_count == 1, f"信号处理器应该只注册一次，实际注册了 {call_count} 次"
        
        # 清理
        manager.stop()
    
    def test_context_manager_thread_safety(self, mock_config, mock_fetcher_builder, mock_downloader):
        """测试上下文管理器的线程安全性"""
        exceptions = []
        
        def use_context_manager():
            try:
                with DownloaderManager('test_group', symbols=['000001.SZ', '600519.SH']) as manager:
                    manager.start()
                    time.sleep(0.1)
            except Exception as e:
                exceptions.append(e)
        
        # 创建多个线程同时使用上下文管理器
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=use_context_manager)
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=10.0)
        
        # 验证结果
        assert len(exceptions) == 0, f"上下文管理器使用时发生异常: {exceptions}"
    
    def test_wait_for_stop_timeout(self, mock_config, mock_fetcher_builder, mock_downloader):
        """测试等待停止的超时功能"""
        manager = DownloaderManager('test_group', symbols=['000001.SZ', '600519.SH'])
        
        # 测试超时
        start_time = time.time()
        result = manager.wait_for_stop(timeout=0.5)
        end_time = time.time()
        
        assert not result, "应该超时返回 False"
        assert 0.4 <= (end_time - start_time) <= 0.7, "超时时间应该接近设定值"
        
        # 测试正常停止
        def stop_after_delay():
            time.sleep(0.2)
            manager._stop_event.set()
        
        stop_thread = threading.Thread(target=stop_after_delay)
        stop_thread.start()
        
        start_time = time.time()
        result = manager.wait_for_stop(timeout=1.0)
        end_time = time.time()
        
        stop_thread.join()
        
        assert result, "应该在超时前收到停止信号"
        assert (end_time - start_time) < 0.5, "应该在延迟后立即返回"
    
    def test_resource_cleanup_on_exception(self, mock_config, mock_fetcher_builder):
        """测试异常情况下的资源清理"""
        with patch('downloader2.producer.downloader_manager.TushareDownloader') as mock_downloader_class:
            # 模拟下载器启动失败
            mock_downloader = Mock()
            mock_downloader.task_type = TaskType.STOCK_BASIC
            mock_downloader.start.side_effect = Exception("启动失败")
            mock_downloader.stop = Mock()
            mock_downloader_class.return_value = mock_downloader
            
            manager = DownloaderManager('test_group', symbols=['000001.SZ', '600519.SH'])
            
            # 启动应该失败
            with pytest.raises(Exception, match="启动失败"):
                manager.start()
            
            # 验证资源被清理
            assert manager.executor is None, "线程池应该被清理"
            assert not manager._signal_handlers_registered, "信号处理器应该被恢复"
    
    def test_deadlock_prevention(self, mock_config, mock_fetcher_builder, mock_downloader):
        """测试死锁预防机制"""
        manager = DownloaderManager('test_group', symbols=['000001.SZ', '600519.SH'])
        manager.start()
        
        # 模拟在事件处理中调用停止
        original_handler = manager._on_downloader_finished
        
        def handler_with_stop(sender, **kwargs):
            # 在事件处理中调用停止，测试是否会死锁
            threading.Thread(target=manager.stop, daemon=True).start()
            return original_handler(sender, **kwargs)
        
        manager._on_downloader_finished = handler_with_stop
        
        # 触发事件
        mock_sender = Mock()
        mock_sender.task_type = Mock()
        mock_sender.task_type.value = 'test_task'
        
        start_time = time.time()
        manager._on_downloader_finished(mock_sender)
        
        # 等待停止完成
        manager.wait_for_stop(timeout=5.0)
        end_time = time.time()
        
        # 验证没有死锁（在合理时间内完成）
        assert (end_time - start_time) < 3.0, "操作应该在合理时间内完成，没有死锁"
        assert manager.is_stopped, "管理器应该已停止"