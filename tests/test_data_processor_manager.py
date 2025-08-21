import pytest
import time
import threading
from unittest.mock import Mock, patch, MagicMock
from concurrent.futures import Future

from downloader.consumer.data_processor_manager import DataProcessorManager
from downloader.consumer.interfaces import IBatchSaver, IDataProcessor
from downloader.task.types import TaskResult, DownloadTaskConfig, TaskPriority
from downloader.producer.fetcher_builder import TaskType
import pandas as pd


class MockBatchSaver:
    """模拟批量保存器"""

    def __init__(self, success: bool = True):
        self.success = success
        self.saved_batches = []

    def save_batch(self, task_type: TaskType, data: pd.DataFrame) -> bool:
        """模拟批量保存数据"""
        self.saved_batches.append((task_type, data.copy()))
        return self.success


class MockDataProcessor:
    """模拟数据处理器"""

    def __init__(self):
        self.processed_results = []
        self.flush_called = False
        self.stats = {"processed_count": 0, "failed_count": 0, "pending_count": 0}

    def process_task_result(self, task_result: TaskResult) -> None:
        """模拟处理任务结果"""
        self.processed_results.append(task_result)
        if task_result.success:
            self.stats["processed_count"] += 1
        else:
            self.stats["failed_count"] += 1

    def flush_pending_data(self) -> None:
        """模拟刷新待处理数据"""
        self.flush_called = True
        self.stats["pending_count"] = 0

    def get_stats(self) -> dict:
        """获取统计信息"""
        return self.stats.copy()


class TestDataProcessorManager:
    """测试DataProcessorManager类"""

    @pytest.fixture
    def batch_saver(self):
        """批量保存器fixture"""
        return MockBatchSaver()

    @pytest.fixture
    def data_processor_manager(self, batch_saver):
        """数据处理管理器fixture"""
        with patch('downloader.consumer.data_processor_manager.get_config') as mock_config:
            # 模拟配置
            mock_config.return_value.huey.db_file = ":memory:"
            mock_config.return_value.huey.immediate = True

            return DataProcessorManager(
                batch_saver=batch_saver, num_workers=2, batch_size=10, poll_interval=0.1
            )

    def test_init(self, batch_saver):
        """测试初始化"""
        with patch("downloader.consumer.data_processor_manager.get_config") as mock_config:
            mock_config.return_value.huey.db_file = ":memory:"
            mock_config.return_value.huey.immediate = True

            manager = DataProcessorManager(
                batch_saver=batch_saver, num_workers=3, batch_size=50, poll_interval=2.0
            )

            assert manager.batch_saver == batch_saver
            assert manager.num_workers == 3
            assert manager.batch_size == 50
            assert manager.poll_interval == 2.0
            assert not manager._running
            assert manager.executor is None
            assert len(manager.workers) == 0
            assert len(manager.data_processors) == 0

    def test_start_and_stop(self, data_processor_manager):
        """测试启动和停止"""
        # 模拟 DataProcessor 创建
        with patch(
            "downloader.consumer.data_processor_manager.DataProcessor"
        ) as mock_dp_class:
            mock_processors = [MockDataProcessor() for _ in range(2)]
            mock_dp_class.side_effect = mock_processors

            # 启动
            data_processor_manager.start()

            assert data_processor_manager._running is True
            assert data_processor_manager.executor is not None
            assert len(data_processor_manager.workers) == 2
            assert len(data_processor_manager.data_processors) == 2
            assert data_processor_manager._stats["start_time"] is not None

            # 停止
            data_processor_manager.stop()

            assert data_processor_manager._running is False
            assert data_processor_manager.executor is None
            assert len(data_processor_manager.workers) == 0
            assert len(data_processor_manager.data_processors) == 0

            # 验证所有处理器的 flush_pending_data 被调用
            for processor in mock_processors:
                assert processor.flush_called is True

    def test_start_already_running(self, data_processor_manager):
        """测试重复启动"""
        with patch("downloader.consumer.data_processor_manager.DataProcessor"):
            data_processor_manager.start()

            # 再次启动应该被忽略
            with patch("downloader.consumer.data_processor_manager.logger") as mock_logger:
                data_processor_manager.start()
                mock_logger.warning.assert_called_with(
                    "DataProcessorManager is already running"
                )

            data_processor_manager.stop()

    def test_stop_not_running(self, data_processor_manager):
        """测试停止未运行的管理器"""
        with patch("downloader.consumer.data_processor_manager.logger") as mock_logger:
            data_processor_manager.stop()
            mock_logger.warning.assert_called_with("DataProcessorManager is not running")

    def test_get_status_not_started(self, data_processor_manager):
        """测试获取未启动状态"""
        status = data_processor_manager.get_status()

        assert status["running"] is False
        assert status["num_workers"] == 2
        assert status["batch_size"] == 10
        assert status["poll_interval"] == 0.1
        assert status["processed_count"] == 0
        assert status["failed_count"] == 0
        assert status["start_time"] is None
        assert "uptime" not in status
        assert status["processors"] == []

    def test_get_status_running(self, data_processor_manager):
        """测试获取运行状态"""
        with patch(
            "downloader.consumer.data_processor_manager.DataProcessor"
        ) as mock_dp_class:
            mock_processors = [MockDataProcessor() for _ in range(2)]
            mock_dp_class.side_effect = mock_processors

            # 设置处理器统计
            mock_processors[0].stats = {
                "processed_count": 5,
                "failed_count": 1,
                "pending_count": 2,
            }
            mock_processors[1].stats = {
                "processed_count": 3,
                "failed_count": 0,
                "pending_count": 1,
            }

            data_processor_manager.start()

            # 短暂等待确保启动完成
            time.sleep(0.05)

            status = data_processor_manager.get_status()

            assert status["running"] is True
            assert status["start_time"] is not None
            assert "uptime" in status
            assert status["uptime"] >= 0
            assert len(status["processors"]) == 2
            assert status["processors"][0]["processed_count"] == 5
            assert status["processors"][1]["processed_count"] == 3

            data_processor_manager.stop()

    def test_update_worker_stats(self, data_processor_manager):
        """测试更新工作线程统计"""
        # 初始化工作线程状态
        data_processor_manager._stats["workers_status"][0] = {
            "status": "running",
            "processed": 0,
            "failed": 0,
            "last_activity": time.time(),
        }

        # 更新处理统计
        data_processor_manager._update_worker_stats(0, "processed")

        assert data_processor_manager._stats["processed_count"] == 1
        assert data_processor_manager._stats["workers_status"][0]["processed"] == 1

        # 更新失败统计
        data_processor_manager._update_worker_stats(0, "failed")

        assert data_processor_manager._stats["failed_count"] == 1
        assert data_processor_manager._stats["workers_status"][0]["failed"] == 1

    def test_context_manager(self, batch_saver):
        """测试上下文管理器"""
        with patch("downloader.consumer.data_processor_manager.get_config") as mock_config:
            mock_config.return_value.huey.db_file = ":memory:"
            mock_config.return_value.huey.immediate = True

            with patch("downloader.consumer.data_processor_manager.DataProcessor"):
                with DataProcessorManager(batch_saver, num_workers=1) as manager:
                    assert manager._running is True

                assert manager._running is False

    def test_worker_loop_no_tasks(self, data_processor_manager):
        """测试工作线程循环（无任务）"""
        mock_processor = MockDataProcessor()

        # 模拟 _get_task_result_from_queue 返回 None
        with patch.object(
            data_processor_manager, "_get_task_result_from_queue", return_value=None
        ):
            # 启动工作线程，短暂运行后停止
            data_processor_manager._running = True

            # 在单独线程中运行工作循环
            def run_worker():
                data_processor_manager._worker_loop(0, mock_processor)

            worker_thread = threading.Thread(target=run_worker)
            worker_thread.start()

            # 短暂等待
            time.sleep(0.2)

            # 停止工作线程
            data_processor_manager._running = False
            data_processor_manager._shutdown_event.set()

            worker_thread.join(timeout=1.0)

            # 验证没有处理任何任务
            assert len(mock_processor.processed_results) == 0

    def test_worker_loop_with_tasks(self, data_processor_manager):
        """测试工作线程循环（有任务）"""
        mock_processor = MockDataProcessor()

        # 创建测试任务结果
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        data = pd.DataFrame({"ts_code": ["600519.SH"], "close": [100.0]})
        task_result = TaskResult(config=config, success=True, data=data)

        # 模拟 _get_task_result_from_queue 返回任务结果
        call_count = 0

        def mock_get_task():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:  # 返回2个任务
                return task_result
            return None  # 之后返回None

        with patch.object(
            data_processor_manager, "_get_task_result_from_queue", side_effect=mock_get_task
        ):
            data_processor_manager._running = True

            # 在单独线程中运行工作循环
            def run_worker():
                data_processor_manager._worker_loop(0, mock_processor)

            worker_thread = threading.Thread(target=run_worker)
            worker_thread.start()

            # 等待处理任务
            time.sleep(0.3)

            # 停止工作线程
            data_processor_manager._running = False
            data_processor_manager._shutdown_event.set()

            worker_thread.join(timeout=1.0)

            # 验证处理了任务
            assert len(mock_processor.processed_results) == 2
            assert mock_processor.stats["processed_count"] == 2

    def test_worker_loop_exception_handling(self, data_processor_manager):
        """测试工作线程异常处理"""
        mock_processor = MockDataProcessor()

        # 模拟处理器抛出异常
        def mock_process_error(task_result):
            raise Exception("Processing error")

        mock_processor.process_task_result = mock_process_error

        # 创建测试任务结果
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        task_result = TaskResult(config=config, success=True, data=pd.DataFrame())

        # 模拟获取任务
        call_count = 0

        def mock_get_task():
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                return task_result
            return None

        with patch.object(
            data_processor_manager, "_get_task_result_from_queue", side_effect=mock_get_task
        ):
            data_processor_manager._running = True

            # 在单独线程中运行工作循环
            def run_worker():
                data_processor_manager._worker_loop(0, mock_processor)

            worker_thread = threading.Thread(target=run_worker)
            worker_thread.start()

            # 等待处理
            time.sleep(0.3)

            # 停止工作线程
            data_processor_manager._running = False
            data_processor_manager._shutdown_event.set()

            worker_thread.join(timeout=1.0)

            # 验证异常被处理，工作线程继续运行
            assert data_processor_manager._stats["failed_count"] >= 1

    def test_get_task_result_from_queue_placeholder(self, data_processor_manager):
        """测试从队列获取任务结果的占位符方法"""
        # 当前实现应该返回 None
        result = data_processor_manager._get_task_result_from_queue()
        assert result is None
