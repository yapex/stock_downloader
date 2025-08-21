"""DownloaderManager 单元测试"""

import pytest
import threading
import time
from unittest.mock import Mock, patch, MagicMock
from concurrent.futures import Future

from downloader.manager.downloader_manager import DownloaderManager, DownloadStats
from downloader.task.types import DownloadTaskConfig, TaskResult, TaskPriority
from downloader.task.task_scheduler import TaskTypeConfig
from downloader.producer.fetcher_builder import TaskType
from downloader.task.interfaces import IDownloadTask


class MockTaskExecutor(IDownloadTask):
    """模拟任务执行器"""
    
    def __init__(self, success_rate: float = 1.0, execution_delay: float = 0.01, deterministic: bool = True):
        self.success_rate = success_rate
        self.execution_delay = execution_delay
        self.execution_count = 0
        self.executed_configs = []
        self.deterministic = deterministic
        self._call_results = []  # 预定义的结果序列
    
    def set_call_results(self, results: list[bool]):
        """设置预定义的调用结果"""
        self._call_results = results
    
    def execute(self, config: DownloadTaskConfig) -> TaskResult:
        """模拟任务执行"""
        time.sleep(self.execution_delay)  # 模拟执行时间
        
        self.execution_count += 1
        self.executed_configs.append(config)
        
        # 确定是否成功
        if self.deterministic and self._call_results:
            # 使用预定义的结果
            result_index = (self.execution_count - 1) % len(self._call_results)
            success = self._call_results[result_index]
        elif self.deterministic:
            # 确定性结果：基于成功率
            success = self.success_rate >= 1.0
        else:
            # 随机结果
            import random
            success = random.random() < self.success_rate
        
        if success:
            return TaskResult(
                config=config,
                success=True,
                data={'mock': 'data'}
            )
        else:
            return TaskResult(
                config=config,
                success=False,
                error=Exception("模拟执行失败")
            )


class TestDownloadStats:
    """DownloadStats 测试"""
    
    def test_initial_stats(self):
        """测试初始统计信息"""
        stats = DownloadStats()
        
        assert stats.total_tasks == 0
        assert stats.completed_tasks == 0
        assert stats.successful_tasks == 0
        assert stats.failed_tasks == 0
        assert stats.retry_tasks == 0
        assert stats.success_rate == 0.0
        assert not stats.is_complete
    
    def test_success_rate_calculation(self):
        """测试成功率计算"""
        stats = DownloadStats(
            total_tasks=10,
            completed_tasks=8,
            successful_tasks=6,
            failed_tasks=2
        )
        
        assert stats.success_rate == 0.75  # 6/8
    
    def test_is_complete(self):
        """测试完成状态"""
        stats = DownloadStats(total_tasks=5, completed_tasks=3)
        assert not stats.is_complete
        
        stats.completed_tasks = 5
        assert stats.is_complete
        
        stats.completed_tasks = 6  # 超过总数也算完成
        assert stats.is_complete


class TestDownloaderManager:
    """DownloaderManager 测试"""
    
    def test_initialization(self):
        """测试初始化"""
        manager = DownloaderManager(max_workers=2)
        
        assert manager.max_workers == 2
        assert manager.task_executor is not None
        assert manager.scheduler is not None
        assert not manager.is_running
        assert manager.stats.total_tasks == 0
    
    def test_initialization_with_custom_executor(self):
        """测试使用自定义执行器初始化"""
        mock_executor = MockTaskExecutor()
        manager = DownloaderManager(task_executor=mock_executor)
        
        assert manager.task_executor is mock_executor
    
    def test_add_download_tasks(self):
        """测试添加下载任务"""
        manager = DownloaderManager()
        symbols = ['000001', '000002', '000003']
        
        manager.add_download_tasks(symbols, TaskType.STOCK_DAILY)
        
        assert manager.stats.total_tasks == 3
        assert not manager.scheduler.is_empty()
    
    def test_start_and_stop(self):
        """测试启动和停止"""
        manager = DownloaderManager()
        
        # 测试启动
        manager.start()
        assert manager.is_running
        assert manager._executor is not None
        
        # 测试停止
        manager.stop()
        assert not manager.is_running
        assert manager._executor is None
    
    def test_start_already_running(self):
        """测试重复启动"""
        manager = DownloaderManager()
        manager.start()
        
        with pytest.raises(RuntimeError, match="已经在运行"):
            manager.start()
        
        manager.stop()
    
    def test_run_without_start(self):
        """测试未启动就运行"""
        manager = DownloaderManager()
        
        with pytest.raises(RuntimeError, match="未启动"):
            manager.run()
    
    def test_run_empty_tasks(self):
        """测试运行空任务列表"""
        manager = DownloaderManager(enable_progress_bar=False)
        manager.start()
        
        stats = manager.run()
        
        assert stats.total_tasks == 0
        assert stats.completed_tasks == 0
        
        manager.stop()
    
    def test_successful_execution(self):
        """测试成功执行任务"""
        mock_executor = MockTaskExecutor(success_rate=1.0, execution_delay=0.001)
        manager = DownloaderManager(
            max_workers=2,
            task_executor=mock_executor,
            enable_progress_bar=False
        )
        
        symbols = ['000001', '000002']
        manager.add_download_tasks(symbols, TaskType.STOCK_DAILY)
        
        manager.start()
        stats = manager.run()
        manager.stop()
        
        assert stats.total_tasks == 2
        assert stats.completed_tasks == 2
        assert stats.successful_tasks == 2
        assert stats.failed_tasks == 0
        assert stats.success_rate == 1.0
        assert mock_executor.execution_count == 2
    
    def test_failed_execution(self):
        """测试失败执行任务"""
        mock_executor = MockTaskExecutor(success_rate=0.0, execution_delay=0.001)
        # 设置所有调用都失败
        mock_executor.set_call_results([False, False, False])
        
        manager = DownloaderManager(
            max_workers=2,
            task_executor=mock_executor,
            enable_progress_bar=False
        )
        
        symbols = ['000001']
        manager.add_download_tasks(symbols, TaskType.STOCK_DAILY, max_retries=2)
        
        manager.start()
        stats = manager.run()
        manager.stop()
        
        assert stats.total_tasks == 1
        assert stats.completed_tasks == 1
        assert stats.successful_tasks == 0
        assert stats.failed_tasks == 1
        assert stats.success_rate == 0.0
        # 应该执行 1 + 2 次重试 = 3 次
        assert mock_executor.execution_count == 3
    
    def test_mixed_execution_results(self):
        """测试混合执行结果"""
        mock_executor = MockTaskExecutor(success_rate=0.5, execution_delay=0.001)
        # 使用单线程执行确保执行顺序可预测
        # 设置结果：第1个任务成功，第2个任务失败后重试成功，第3个任务失败后重试失败，第4个任务失败后重试成功
        mock_executor.set_call_results([True, False, False, False, True, False, True])
        
        manager = DownloaderManager(
            max_workers=1,  # 使用单线程确保执行顺序
            task_executor=mock_executor,
            enable_progress_bar=False
        )
        
        symbols = ['000001', '000002', '000003', '000004']
        manager.add_download_tasks(symbols, TaskType.STOCK_DAILY, max_retries=1)
        
        manager.start()
        stats = manager.run()
        manager.stop()
        
        assert stats.total_tasks == 4
        assert stats.completed_tasks == 4
        # 预期：任务1成功，任务2重试后成功，任务3重试后失败，任务4重试后成功 = 3成功1失败
        assert stats.successful_tasks == 3
        assert stats.failed_tasks == 1
        assert stats.success_rate == 0.75
    
    def test_managed_execution_context(self):
        """测试上下文管理器"""
        mock_executor = MockTaskExecutor(success_rate=1.0, execution_delay=0.001)
        manager = DownloaderManager(
            task_executor=mock_executor,
            enable_progress_bar=False
        )
        
        symbols = ['000001']
        manager.add_download_tasks(symbols, TaskType.STOCK_DAILY)
        
        with manager.managed_execution():
            assert manager.is_running
            stats = manager.run()
        
        assert not manager.is_running
        assert stats.successful_tasks == 1
    
    def test_get_stats(self):
        """测试获取统计信息"""
        manager = DownloaderManager()
        manager.add_download_tasks(['000001'], TaskType.STOCK_DAILY)
        
        stats = manager.get_stats()
        assert isinstance(stats, DownloadStats)
        assert stats.total_tasks == 1
        assert stats.completed_tasks == 0
    
    def test_shutdown_during_execution(self):
        """测试执行过程中关闭"""
        # 创建一个执行时间较长的模拟执行器
        mock_executor = MockTaskExecutor(success_rate=1.0, execution_delay=0.1)
        manager = DownloaderManager(
            max_workers=1,
            task_executor=mock_executor,
            enable_progress_bar=False
        )
        
        symbols = ['000001', '000002']
        manager.add_download_tasks(symbols, TaskType.STOCK_DAILY)
        
        manager.start()
        
        # 在另一个线程中延迟关闭
        def delayed_stop():
            time.sleep(0.05)  # 等待任务开始执行
            manager.stop()
        
        stop_thread = threading.Thread(target=delayed_stop)
        stop_thread.start()
        
        stats = manager.run()
        stop_thread.join()
        
        # 由于提前关闭，可能不是所有任务都完成
        assert stats.completed_tasks <= stats.total_tasks
    
    def test_task_type_config(self):
        """测试任务类型配置"""
        task_type_config = TaskTypeConfig({
            TaskType.STOCK_BASIC: TaskPriority.HIGH,
            TaskType.STOCK_DAILY: TaskPriority.LOW
        })
        
        manager = DownloaderManager(task_type_config=task_type_config)
        
        # 添加不同优先级的任务
        manager.add_download_tasks(['000001'], TaskType.STOCK_BASIC)
        manager.add_download_tasks(['000002'], TaskType.STOCK_DAILY)
        
        # 获取所有任务，应该按优先级排序
        all_tasks = manager.scheduler.get_all_tasks()
        assert len(all_tasks) == 2
        
        # 第一个任务应该是高优先级的 STOCK_BASIC
        assert all_tasks[0].task_type == TaskType.STOCK_BASIC
        assert all_tasks[1].task_type == TaskType.STOCK_DAILY
    
    @patch('downloader.manager.downloader_manager.signal.signal')
    def test_signal_handling(self, mock_signal):
        """测试信号处理"""
        manager = DownloaderManager()
        manager.start()
        
        # 验证信号处理器已注册
        assert mock_signal.call_count == 2  # SIGINT 和 SIGTERM
        
        manager.stop()
        
        # 验证信号处理器已恢复
        assert mock_signal.call_count == 4  # 注册 + 恢复
    
    def test_thread_safety(self):
        """测试线程安全性"""
        manager = DownloaderManager()
        
        def add_tasks():
            for i in range(10):
                manager.add_download_tasks([f'00000{i}'], TaskType.STOCK_DAILY)
        
        # 并发添加任务
        threads = [threading.Thread(target=add_tasks) for _ in range(3)]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # 验证所有任务都被正确添加
        assert manager.stats.total_tasks == 30
        assert not manager.scheduler.is_empty()