"""ConsumerManager - 消费管理器

负责整体消费流程的协调和管理，包括线程池管理、任务分发、监控统计等。
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Dict, List, Optional, Any
from queue import Queue, Empty

from huey import SqliteHuey
from ..config import get_config
from .interfaces import IDataProcessorManager, IDataProcessor, IBatchSaver
from .data_processor import DataProcessor
from .batch_saver import BatchSaver
from ..task.types import TaskResult, DownloadTaskConfig
from ..producer.fetcher_builder import TaskType

logger = logging.getLogger(__name__)


class DataProcessorManager(IDataProcessorManager):
    """数据处理管理器

    负责整体消费流程的协调和管理，包括：
    - 线程池管理和任务分发
    - 监控和统计信息收集
    - 优雅关闭处理
    - 配置加载
    """

    def __init__(
        self,
        batch_saver: IBatchSaver,
        num_workers: int = 4,
        batch_size: int = 100,
        poll_interval: float = 1.0,
    ):
        """初始化消费管理器

        Args:
            batch_saver: 批量保存器实例
            num_workers: 工作线程数量
            batch_size: 批量处理大小
            poll_interval: 队列轮询间隔（秒）
        """
        self.batch_saver = batch_saver
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.poll_interval = poll_interval

        # 初始化 Huey 实例
        config = get_config()
        self.huey = SqliteHuey(
            filename=config.huey.db_file, immediate=config.huey.immediate
        )

        # 线程管理
        self.executor: Optional[ThreadPoolExecutor] = None
        self.workers: List[Future] = []
        self.data_processors: List[IDataProcessor] = []

        # 状态管理
        self._running = False
        self._shutdown_event = threading.Event()
        self._stats_lock = threading.Lock()
        self._stats = {
            "processed_count": 0,
            "failed_count": 0,
            "start_time": None,
            "workers_status": {},
        }

        logger.info(f"DataProcessorManager initialized with {num_workers} workers")

    def start(self) -> None:
        """启动消费进程"""
        if self._running:
            logger.warning("DataProcessorManager is already running")
            return

        logger.info("Starting DataProcessorManager...")
        self._running = True
        self._shutdown_event.clear()

        # 记录启动时间
        with self._stats_lock:
            self._stats["start_time"] = time.time()

        # 创建数据处理器实例
        self.data_processors = [
            DataProcessor(self.batch_saver, self.batch_size)
            for _ in range(self.num_workers)
        ]

        # 启动线程池
        self.executor = ThreadPoolExecutor(
            max_workers=self.num_workers, thread_name_prefix="consumer-worker"
        )

        # 启动工作线程
        for i, processor in enumerate(self.data_processors):
            future = self.executor.submit(self._worker_loop, i, processor)
            self.workers.append(future)

        logger.info(f"DataProcessorManager started with {self.num_workers} workers")

    def stop(self) -> None:
        """停止消费进程"""
        if not self._running:
            logger.warning("DataProcessorManager is not running")
            return

        logger.info("Stopping DataProcessorManager...")
        self._running = False
        self._shutdown_event.set()

        # 刷新所有处理器的待处理数据
        for processor in self.data_processors:
            try:
                processor.flush_pending_data()
            except Exception as e:
                logger.error(f"Error flushing processor data: {e}")

        # 等待所有工作线程完成
        if self.executor:
            self.executor.shutdown(wait=True)

        # 清理资源
        self.workers.clear()
        self.data_processors.clear()
        self.executor = None

        logger.info("DataProcessorManager stopped")

    def get_status(self) -> dict:
        """获取消费状态"""
        with self._stats_lock:
            status = self._stats.copy()

        # 添加运行时状态
        status.update(
            {
                "running": self._running,
                "num_workers": self.num_workers,
                "batch_size": self.batch_size,
                "poll_interval": self.poll_interval,
            }
        )

        # 计算运行时间
        if status["start_time"]:
            status["uptime"] = time.time() - status["start_time"]

        # 添加处理器统计
        processor_stats = []
        for processor in self.data_processors:
            try:
                processor_stats.append(processor.get_stats())
            except Exception as e:
                logger.error(f"Error getting processor stats: {e}")
                processor_stats.append({"error": str(e)})

        status["processors"] = processor_stats

        return status

    def _worker_loop(self, worker_id: int, processor: IDataProcessor) -> None:
        """工作线程主循环

        Args:
            worker_id: 工作线程ID
            processor: 数据处理器实例
        """
        logger.info(f"Worker {worker_id} started")

        # 更新工作线程状态
        with self._stats_lock:
            self._stats["workers_status"][worker_id] = {
                "status": "running",
                "processed": 0,
                "failed": 0,
                "last_activity": time.time(),
            }

        try:
            while self._running and not self._shutdown_event.is_set():
                try:
                    # 从 Huey 队列获取任务结果
                    # 注意：这里需要实现从 Huey 队列获取 TaskResult 的逻辑
                    # 目前 Huey 任务是 process_fetched_data，需要修改为返回 TaskResult
                    task_result = self._get_task_result_from_queue()

                    if task_result is None:
                        # 没有任务，短暂休眠
                        time.sleep(self.poll_interval)
                        continue

                    # 处理任务结果
                    processor.process_task_result(task_result)

                    # 更新统计信息
                    self._update_worker_stats(worker_id, "processed")

                except Exception as e:
                    logger.error(f"Worker {worker_id} error: {e}")
                    self._update_worker_stats(worker_id, "failed")

                    # 短暂休眠避免错误循环
                    time.sleep(1.0)

        except Exception as e:
            logger.error(f"Worker {worker_id} fatal error: {e}")

        finally:
            # 更新工作线程状态
            with self._stats_lock:
                if worker_id in self._stats["workers_status"]:
                    self._stats["workers_status"][worker_id]["status"] = "stopped"

            logger.info(f"Worker {worker_id} stopped")

    def _get_task_result_from_queue(self) -> Optional[TaskResult]:
        """从 Huey 队列获取任务结果

        注意：这是一个占位符方法，需要根据实际的 Huey 任务结构来实现。
        目前的 process_fetched_data 任务需要修改为返回 TaskResult。

        Returns:
            TaskResult 或 None（如果队列为空）
        """
        # TODO: 实现从 Huey 队列获取 TaskResult 的逻辑
        # 这需要修改现有的 Huey 任务结构

        # 临时实现：返回 None 表示队列为空
        return None

    def _update_worker_stats(self, worker_id: int, stat_type: str) -> None:
        """更新工作线程统计信息

        Args:
            worker_id: 工作线程ID
            stat_type: 统计类型（'processed' 或 'failed'）
        """
        with self._stats_lock:
            # 更新全局统计
            if stat_type == "processed":
                self._stats["processed_count"] += 1
            elif stat_type == "failed":
                self._stats["failed_count"] += 1

            # 更新工作线程统计
            if worker_id in self._stats["workers_status"]:
                worker_stats = self._stats["workers_status"][worker_id]
                if stat_type in worker_stats:
                    worker_stats[stat_type] += 1
                worker_stats["last_activity"] = time.time()

    def __enter__(self):
        """上下文管理器入口"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.stop()
