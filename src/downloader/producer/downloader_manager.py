"""重构后的下载管理器

基于任务扁平化设计，使用 as_completed + tqdm 进度跟踪。
"""

from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from dataclasses import dataclass
from typing import List, Dict, Optional, Callable, Any
import logging
import signal
import threading
from contextlib import contextmanager
from tqdm import tqdm

from downloader.task.download_task import DownloadTask
from downloader.task.types import DownloadTaskConfig, TaskResult, TaskPriority
from downloader.task.task_scheduler import (
    TaskScheduler,
    TaskTypeConfig,
    create_task_configs,
)
from downloader.task.types import TaskType
from downloader.producer.interfaces import IDownloaderManager
from downloader.task.interfaces import IDownloadTask
from downloader.config import get_config
from pathlib import Path
from downloader.database.schema_loader import SchemaLoader

logger = logging.getLogger(__name__)


def _build_task_name_to_type_mapping() -> Dict[str, TaskType]:
    """动态构建任务名称到 TaskType 的映射"""
    schema_loader = SchemaLoader()
    mapping = {}

    for table_name in schema_loader.get_table_names():
        task_type = getattr(TaskType, table_name.upper(), None)
        if task_type is not None:
            mapping[table_name] = task_type

    return mapping


# 任务名称到 TaskType 的映射
TASK_NAME_TO_TYPE = _build_task_name_to_type_mapping()

# 优先级数值到 TaskPriority 的映射
PRIORITY_VALUE_TO_ENUM = {
    0: TaskPriority.HIGH,
    1: TaskPriority.MEDIUM,
    2: TaskPriority.LOW,
}


def create_task_type_config_from_config(
    config_path: Optional[Path] = None,
) -> TaskTypeConfig:
    """从配置文件创建任务类型配置

    Args:
        config_path: 配置文件路径，如果为 None 则使用默认配置文件

    Returns:
        TaskTypeConfig: 基于配置文件的任务类型配置
    """
    config = get_config(config_path)
    custom_priorities = {}

    # 检查是否存在 download_tasks 配置
    if hasattr(config, "download_tasks") and config.download_tasks:
        for task_name, task_config in config.download_tasks.items():
            if task_name in TASK_NAME_TO_TYPE and hasattr(task_config, "priority"):
                task_type = TASK_NAME_TO_TYPE[task_name]
                priority_value = task_config.priority

                # 将数值优先级转换为 TaskPriority 枚举
                if priority_value in PRIORITY_VALUE_TO_ENUM:
                    priority_enum = PRIORITY_VALUE_TO_ENUM[priority_value]
                    custom_priorities[task_type] = priority_enum
                    logger.info(f"设置任务 {task_name} 优先级为 {priority_enum.name}")
                else:
                    logger.warning(
                        f"未知的优先级值 {priority_value} for task {task_name}"
                    )

    return TaskTypeConfig(custom_priorities)


def get_task_types_from_group(
    group: str, config_path: Optional[Path] = None
) -> List[TaskType]:
    """根据任务组名称获取任务类型列表

    Args:
        group: 任务组名称
        config_path: 配置文件路径，如果为 None 则使用默认配置文件

    Returns:
        List[TaskType]: 任务类型列表
    """
    config = get_config(config_path)

    if group not in config.task_groups:
        raise ValueError(f"未找到任务组: {group}")

    task_names = config.task_groups[group]
    task_types = []

    for task_name in task_names:
        if task_name not in TASK_NAME_TO_TYPE:
            raise ValueError(f"未知的任务类型: {task_name}")
        task_types.append(TASK_NAME_TO_TYPE[task_name])

    return task_types


@dataclass
class DownloadStats:
    """下载统计信息"""

    total_tasks: int = 0
    completed_tasks: int = 0
    successful_tasks: int = 0
    failed_tasks: int = 0
    retry_tasks: int = 0

    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.completed_tasks == 0:
            return 0.0
        return self.successful_tasks / self.completed_tasks

    @property
    def is_complete(self) -> bool:
        """是否完成"""
        if self.total_tasks == 0:
            return False
        return self.completed_tasks >= self.total_tasks


class DownloaderManager(IDownloaderManager):
    """重构后的下载管理器

    基于任务扁平化设计，简化了架构：
    1. 使用 TaskScheduler 管理任务优先级
    2. 使用 DownloadTask 执行具体下载
    3. 使用 as_completed + tqdm 跟踪进度
    4. 简化的生命周期管理
    """

    def __init__(
        self,
        max_workers: int = 4,
        task_executor: Optional[IDownloadTask] = None,
        task_type_config: Optional[TaskTypeConfig] = None,
        enable_progress_bar: bool = True,
        config_path: Optional[Path] = None,
    ):
        """初始化下载管理器

        Args:
            max_workers: 最大工作线程数
            task_executor: 任务执行器，如果为 None 则使用默认的 DownloadTask
            task_type_config: 任务类型配置，如果为 None 则使用默认配置
            enable_progress_bar: 是否启用进度条
            config_path: 配置文件路径，如果为 None 则使用默认配置文件
        """
        self.max_workers = max_workers
        self.task_executor = task_executor or DownloadTask()
        self.scheduler = TaskScheduler(task_type_config)
        self.enable_progress_bar = enable_progress_bar
        self.config_path = config_path

        # 状态管理
        self._executor: Optional[ThreadPoolExecutor] = None
        self._running = False
        self._shutdown_requested = False
        self._lock = threading.RLock()

        # 统计信息
        self.stats = DownloadStats()

        # 信号处理
        self._original_sigint_handler = None
        self._original_sigterm_handler = None

    @classmethod
    def create_from_config(
        cls,
        task_executor: Optional[IDownloadTask] = None,
        max_workers: Optional[int] = None,
        enable_progress_bar: Optional[bool] = None,
        config_path: Optional[Path] = None,
    ) -> "DownloaderManager":
        """从配置文件创建下载管理器

        Args:
            task_executor: 任务执行器，如果为 None 则使用默认的 DownloadTask
            max_workers: 最大工作线程数，如果为 None 则从配置文件读取
            enable_progress_bar: 是否启用进度条，如果为 None 则从配置文件读取
            config_path: 配置文件路径，如果为 None 则使用默认配置文件

        Returns:
            配置好的 DownloaderManager 实例
        """
        config = get_config(config_path)
        task_type_config = create_task_type_config_from_config(config_path)

        # 从配置文件读取参数，如果未提供的话
        if max_workers is None:
            downloader_config = getattr(config, "downloader", {})
            max_workers = getattr(downloader_config, "max_workers", 4)

        if enable_progress_bar is None:
            downloader_config = getattr(config, "downloader", {})
            enable_progress_bar = getattr(
                downloader_config, "enable_progress_bar", True
            )

        return cls(
            max_workers=max_workers,
            task_executor=task_executor,
            task_type_config=task_type_config,
            enable_progress_bar=enable_progress_bar,
            config_path=config_path,
        )

    def add_download_tasks(
        self, symbols: List[str], task_type: TaskType, **kwargs
    ) -> None:
        """添加下载任务

        Args:
            symbols: 股票代码列表
            task_type: 任务类型
            **kwargs: 传递给 create_task_configs 的额外参数
        """
        configs = create_task_configs(symbols, task_type, **kwargs)
        self.scheduler.add_tasks(configs)

        with self._lock:
            self.stats.total_tasks += len(configs)

        logger.info(f"添加了 {len(configs)} 个 {task_type.name} 任务")

    def download_group(
        self, group: str, symbols: Optional[List[str]] = None, **kwargs
    ) -> DownloadStats:
        """下载指定任务组的数据

        Args:
            group: 任务组名称
            symbols: 股票代码列表，如果为 None 则使用 ["all"]
            **kwargs: 传递给任务的额外参数

        Returns:
            下载统计信息
        """
        # 获取任务类型列表
        task_types = get_task_types_from_group(group, self.config_path)

        # 使用默认股票代码列表
        if symbols is None:
            symbols = ["all"]

        # 添加所有任务
        for task_type in task_types:
            self.add_download_tasks(symbols=symbols, task_type=task_type, **kwargs)

        # 执行下载
        self.start()
        try:
            return self.run()
        finally:
            self.stop()

    def start(self) -> None:
        """启动下载管理器"""
        with self._lock:
            if self._running:
                raise RuntimeError("DownloaderManager 已经在运行")

            if self._shutdown_requested:
                raise RuntimeError("DownloaderManager 已经关闭，无法重新启动")

            logger.info(f"启动下载管理器，最大工作线程数: {self.max_workers}")

            # 创建线程池
            self._executor = ThreadPoolExecutor(max_workers=self.max_workers)

            # 注册信号处理器
            self._register_signal_handlers()

            self._running = True

    def run(self) -> DownloadStats:
        """运行所有下载任务

        Returns:
            下载统计信息
        """
        if not self._running:
            raise RuntimeError("DownloaderManager 未启动，请先调用 start()")

        if self.scheduler.is_empty():
            logger.warning("没有任务需要执行")
            return self.stats

        logger.info(f"开始执行 {self.stats.total_tasks} 个下载任务")

        try:
            # 提交所有任务到线程池
            futures = self._submit_all_tasks()

            # 使用 as_completed 跟踪任务完成情况
            self._process_completed_tasks(futures)

            logger.info(
                f"所有任务执行完成，成功: {self.stats.successful_tasks}, 失败: {self.stats.failed_tasks}"
            )

        except KeyboardInterrupt:
            logger.info("收到中断信号，正在停止...")
            self._shutdown_requested = True
        except Exception as e:
            logger.error(f"执行任务时发生错误: {e}")
            raise
        finally:
            self._cleanup()

        return self.stats

    def stop(self) -> None:
        """停止下载管理器"""
        with self._lock:
            if not self._running:
                return

            logger.info("正在停止下载管理器...")
            self._shutdown_requested = True
            self._cleanup()

    def _submit_all_tasks(self) -> List[Future[TaskResult]]:
        """提交所有任务到线程池

        Returns:
            Future 对象列表
        """
        futures = []

        # 获取所有任务并提交
        all_tasks = self.scheduler.get_all_tasks()
        for config in all_tasks:
            if self._shutdown_requested:
                break

            future = self._executor.submit(self._execute_task_with_retry, config)
            futures.append(future)

        logger.info(f"已提交 {len(futures)} 个任务到线程池")
        return futures

    def _process_completed_tasks(self, futures: List[Future[TaskResult]]) -> None:
        """处理已完成的任务

        Args:
            futures: Future 对象列表
        """
        # 创建进度条
        progress_bar = None
        if self.enable_progress_bar:
            progress_bar = tqdm(
                total=len(futures),
                desc="下载进度",
                unit="task",
                ncols=80,
                leave=False,
                dynamic_ncols=True,
                miniters=1,
            )

        try:
            # 使用 as_completed 处理完成的任务
            for future in as_completed(futures):
                if self._shutdown_requested:
                    break

                try:
                    result = future.result()
                    self._update_stats(result)

                    if progress_bar:
                        progress_bar.update(1)

                except Exception as e:
                    logger.error(f"处理任务结果时发生错误: {e}")
                    with self._lock:
                        self.stats.completed_tasks += 1
                        self.stats.failed_tasks += 1

                    if progress_bar:
                        progress_bar.update(1)

        finally:
            if progress_bar:
                progress_bar.close()

    def _execute_task_with_retry(self, config: DownloadTaskConfig) -> TaskResult:
        """执行任务（包含重试逻辑）

        Args:
            config: 任务配置

        Returns:
            任务执行结果
        """
        retry_count = 0
        last_result = None

        while retry_count <= config.max_retries:
            if self._shutdown_requested:
                # 如果请求关闭，返回失败结果
                return TaskResult(
                    config=config,
                    success=False,
                    error=Exception("任务被中断"),
                    retry_count=retry_count,
                )

            result = self.task_executor.execute(config)

            if result.success:
                result.retry_count = retry_count
                return result

            # 任务失败，准备重试
            retry_count += 1
            last_result = result

            if retry_count <= config.max_retries:
                logger.warning(
                    f"任务失败，准备第 {retry_count} 次重试: {config.task_type.name}, "
                    f"symbol: {config.symbol}, error: {result.error}"
                )
                with self._lock:
                    self.stats.retry_tasks += 1

        # 所有重试都失败了
        if last_result:
            last_result.retry_count = retry_count - 1
            return last_result

        # 这种情况不应该发生，但为了安全起见
        return TaskResult(
            config=config,
            success=False,
            error=Exception("未知错误"),
            retry_count=retry_count - 1,
        )

    def _update_stats(self, result: TaskResult) -> None:
        """更新统计信息

        Args:
            result: 任务执行结果
        """
        with self._lock:
            self.stats.completed_tasks += 1

            if result.success:
                self.stats.successful_tasks += 1
            else:
                self.stats.failed_tasks += 1
                logger.warning(
                    f"任务最终失败: {result.config.task_type.name}, "
                    f"symbol: {result.config.symbol}, "
                    f"retries: {result.retry_count}, "
                    f"error: {result.error}"
                )

    def _register_signal_handlers(self) -> None:
        """注册信号处理器"""
        try:

            def signal_handler(signum, frame):
                logger.info(f"收到信号 {signum}，正在优雅关闭...")
                self._shutdown_requested = True

            self._original_sigint_handler = signal.signal(signal.SIGINT, signal_handler)
            self._original_sigterm_handler = signal.signal(
                signal.SIGTERM, signal_handler
            )
        except ValueError:
            # 在非主线程中无法注册信号处理器，忽略此错误
            logger.debug("无法注册信号处理器（非主线程）")
            self._original_sigint_handler = None
            self._original_sigterm_handler = None

    def _restore_signal_handlers(self) -> None:
        """恢复原始信号处理器"""
        try:
            if self._original_sigint_handler is not None:
                signal.signal(signal.SIGINT, self._original_sigint_handler)
                self._original_sigint_handler = None

            if self._original_sigterm_handler is not None:
                signal.signal(signal.SIGTERM, self._original_sigterm_handler)
                self._original_sigterm_handler = None
        except ValueError:
            # 在非主线程中无法恢复信号处理器，忽略此错误
            logger.debug("无法恢复信号处理器（非主线程）")
            self._original_sigint_handler = None
            self._original_sigterm_handler = None

    def _cleanup(self) -> None:
        """清理资源"""
        with self._lock:
            if not self._running:
                return

            logger.info("正在清理资源...")

            # 关闭线程池
            if self._executor:
                self._executor.shutdown(wait=True, cancel_futures=True)
                self._executor = None

            # 恢复信号处理器
            self._restore_signal_handlers()

            self._running = False

            logger.info("资源清理完成")

    @contextmanager
    def managed_execution(self):
        """上下文管理器，自动管理启动和清理

        使用示例:
            with manager.managed_execution():
                stats = manager.run()
        """
        self.start()
        try:
            yield self
        finally:
            self.stop()

    @property
    def is_running(self) -> bool:
        """是否正在运行"""
        with self._lock:
            return self._running

    @property
    def is_shutdown_requested(self) -> bool:
        """是否请求关闭"""
        return self._shutdown_requested

    def get_stats(self) -> DownloadStats:
        """获取当前统计信息"""
        with self._lock:
            return DownloadStats(
                total_tasks=self.stats.total_tasks,
                completed_tasks=self.stats.completed_tasks,
                successful_tasks=self.stats.successful_tasks,
                failed_tasks=self.stats.failed_tasks,
                retry_tasks=self.stats.retry_tasks,
            )
