"""基于新架构的 TushareDownloader 实现

使用 DownloaderManager + DownloadTask 替代原有的复杂线程管理逻辑。
"""

import logging
import threading
from typing import List, Optional, Dict, Any
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

from downloader.manager.downloader_manager import (
    DownloaderManager,
    DownloadStats,
)
from downloader.task.download_task import DownloadTask
from downloader.task.types import DownloadTaskConfig, TaskResult
from downloader.task.task_scheduler import TaskTypeConfig
from downloader.producer.fetcher_builder import TaskType
from downloader.producer.huey_tasks import process_fetched_data
from downloader.interfaces.download_task import IDownloadTask

logger = logging.getLogger(__name__)


class TushareTaskExecutor(IDownloadTask):
    """Tushare 任务执行器

    包装 DownloadTask 并添加 Huey 任务处理逻辑。
    """

    def __init__(self):
        self.download_task = DownloadTask()

    def execute(self, config: DownloadTaskConfig) -> TaskResult:
        """执行下载任务并处理结果"""
        result = self.download_task.execute(config)

        if result.success and result.data is not None and not result.data.empty:
            # 只有成功且数据非空时才触发 Huey 任务
            try:
                process_fetched_data(
                    config.symbol, config.task_type.name, result.data.to_dict()
                )
                logger.debug(f"已触发 Huey 任务处理: {config.symbol}")
            except Exception as e:
                logger.error(f"触发 Huey 任务失败: {config.symbol}, error: {e}")
                # 不影响主任务的成功状态

        return result


class TushareDownloader:
    """基于新架构的 TushareDownloader

    保持与原有 TushareDownloader 相同的接口，但使用新的内部实现。
    """

    def __init__(
        self,
        symbols: List[str],
        task_type: TaskType,
        executor: Optional[ThreadPoolExecutor] = None,
        event_bus: Optional[Any] = None,
        max_workers: int = 4,
        max_retries: int = 3,
        **kwargs,
    ):
        """初始化下载器

        Args:
            symbols: 股票代码列表
            task_type: 任务类型
            executor: 线程池执行器（为了兼容性保留，但不使用）
            event_bus: 事件总线（为了兼容性保留，但不使用）
            max_workers: 最大工作线程数
            max_retries: 最大重试次数
            **kwargs: 其他参数
        """
        self.symbols = symbols
        self.task_type = task_type
        self.max_retries = max_retries

        # 兼容性属性
        self.task_queue = Queue()
        self.retry_counts: Dict[str, int] = {}

        # 统计属性
        self._successful_symbols = 0
        self._failed_symbols = 0
        self._processed_symbols = 0
        self._lock = threading.RLock()

        # 创建新的下载管理器
        self.task_executor = TushareTaskExecutor()
        self.manager = DownloaderManager(
            max_workers=max_workers,
            task_executor=self.task_executor,
            enable_progress_bar=False,  # 保持与原有行为一致
        )

        # 状态管理
        self._running = False
        self._started = False

    @property
    def successful_symbols(self) -> int:
        """成功处理的符号数量"""
        with self._lock:
            return self._successful_symbols

    @property
    def failed_symbols(self) -> int:
        """失败的符号数量"""
        with self._lock:
            return self._failed_symbols

    @property
    def processed_symbols(self) -> int:
        """已处理的符号数量"""
        with self._lock:
            return self._processed_symbols

    def start(self) -> None:
        """启动下载器"""
        if self._started:
            logger.warning("TushareDownloader 已经启动")
            return

        logger.info(f"启动 TushareDownloader，符号数量: {len(self.symbols)}")

        # 添加下载任务
        self.manager.add_download_tasks(
            self.symbols, self.task_type, max_retries=self.max_retries
        )

        # 启动管理器
        self.manager.start()
        self._started = True
        self._running = True

        # 在后台线程中运行任务
        self._run_thread = threading.Thread(target=self._run_tasks, daemon=True)
        self._run_thread.start()

    def stop(self) -> None:
        """停止下载器"""
        if not self._running:
            return

        logger.info("停止 TushareDownloader")
        self._running = False

        # 停止管理器
        self.manager.stop()

        # 等待运行线程结束
        if hasattr(self, "_run_thread") and self._run_thread.is_alive():
            self._run_thread.join(timeout=5.0)

    def _run_tasks(self) -> None:
        """在后台运行所有任务"""
        try:
            stats = self.manager.run()
            self._update_stats_from_manager(stats)
        except Exception as e:
            logger.error(f"运行任务时发生错误: {e}")
        finally:
            self._running = False

    def _update_stats_from_manager(self, stats: DownloadStats) -> None:
        """从管理器统计信息更新本地统计"""
        with self._lock:
            self._successful_symbols = stats.successful_tasks
            self._failed_symbols = stats.failed_tasks
            self._processed_symbols = stats.completed_tasks

    def _shutdown(self, wait: bool = True, timeout: float = 5.0) -> None:
        """关闭下载器（兼容性方法）"""
        self.stop()

        if wait and hasattr(self, "_run_thread"):
            self._run_thread.join(timeout=timeout)

    def _process_symbol(self, symbol: str) -> None:
        """处理单个符号（兼容性方法）

        这个方法主要用于测试兼容性。在新架构中，符号处理由管理器自动完成。
        """
        # 获取当前重试次数
        current_retry_count = self.retry_counts.get(symbol, 0)

        # 创建任务配置
        config = DownloadTaskConfig(
            symbol=symbol,
            task_type=self.task_type,
            max_retries=0,  # 单次执行，重试逻辑由此方法控制
        )

        # 执行任务
        result = self.task_executor.execute(config)

        # 更新统计信息
        with self._lock:
            if result.success:
                self._successful_symbols += 1
                self._processed_symbols += 1
                # 成功时清除重试计数
                if symbol in self.retry_counts:
                    del self.retry_counts[symbol]
            else:
                # 增加重试计数
                self.retry_counts[symbol] = current_retry_count + 1

                # 如果达到最大重试次数，标记为失败并处理
                if self.retry_counts[symbol] > self.max_retries:
                    self._failed_symbols += 1
                    self._processed_symbols += 1
                else:
                    # 还有重试机会，添加到队列
                    self.task_queue.put(symbol)

    def _populate_symbol_queue(self) -> None:
        """填充符号队列（兼容性方法）"""
        for symbol in self.symbols:
            self.task_queue.put(symbol)

    def is_running(self) -> bool:
        """检查是否正在运行"""
        return self._running

    def get_stats(self) -> Dict[str, int]:
        """获取统计信息"""
        with self._lock:
            return {
                "successful_symbols": self._successful_symbols,
                "failed_symbols": self._failed_symbols,
                "processed_symbols": self._processed_symbols,
                "total_symbols": len(self.symbols),
            }
