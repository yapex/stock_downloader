好的，非常乐意为您审核和重构这段代码。

您对代码的观察非常敏锐，现有架构确实存在一些可以优化的地方。`DownloaderManager` 承担了过多的职责，包括线程管理、信号处理、任务编排和状态监控，而 `TushareDownloader` 内部又实现了一套自己的工作线程和完成逻辑。这种“管理器管理管理器”的模式导致了双层的复杂性，使得生命周期和状态同步变得困难。

### 现有代码优点分析

在重构之前，我们先肯定一下当前代码的优点：

1.  **目标明确**: 实现了基于配置的任务分组下载，逻辑是完整的。
2.  **考虑了健壮性**: 包含了信号处理、重试机制和速率限制，这些都是生产级代码需要考虑的。
3.  **事件驱动**: 使用事件总线（Event Bus）来解耦进度处理（`TqdmTaskHandler`）和下载器状态，这是一个很好的设计模式。
4.  **资源管理**: 使用了 `ThreadPoolExecutor` 和上下文管理器（`__enter__` / `__exit__`）来管理资源，确保线程池能被正确关闭。

### 可改进的关键点

正如您所指出的，主要问题在于 **架构和业务逻辑的纠缠** 以及 **过度的复杂性**：

1.  **复杂的生命周期管理**: `DownloaderManager` 有自己的 `start/stop` 和状态（`_started`, `_stop_event`），`TushareDownloader` 也有自己的 `start/stop` 和状态（`_shutdown_event`, `_completion_checked`）。这种嵌套的生命周期管理容易出错且难以理解。
2.  **职责耦合**: `DownloaderManager` 既负责创建下载器（业务逻辑），又负责管理线程池（底层架构）。`TushareDownloader` 既负责单个下载任务，又负责管理一个“微型”的消费者队列和工作线程池（通过向外部 `executor` 提交任务实现）。
3.  **不必要的并发模型**: `TushareDownloader` 内部通过 `task_queue` 和多个 `_worker` 形成了一个生产者-消费者模型。但实际上，所有的下载任务（比如下载'000001.SZ'的日线）都是同质的，完全可以被“扁平化”地提交给一个线程池，而不需要在每个 `TushareDownloader` 内部再维护一个队列。
4.  **状态同步复杂**: 完成状态的判断逻辑分散在两处。`TushareDownloader` 判断自己是否完成，然后发送事件；`DownloaderManager` 监听这些事件，再判断所有下载器是否都完成了。这增加了系统的复杂性和潜在的竞态条件。

---

### 重构建议：任务扁平化与职责分离

核心思想是简化并发模型，将两层管理结构（Manager -> Downloader -> Worker）拍平为一层（Manager -> Tasks）。

1.  **重新定义 `DownloaderManager` 的职责**:

    - **任务规划器 (Task Planner)**: 它的核心职责是解析配置，生成一个包含所有原子下载任务的 **扁平列表**。例如 `[ (task_type_A, symbol_1), (task_type_A, symbol_2), (task_type_B, symbol_1), ... ]`。
    - **执行控制器 (Execution Controller)**: 管理一个 `ThreadPoolExecutor`，将这些原子任务提交执行，并负责优雅地启动、监控和关闭。

2.  **简化 `TushareDownloader`**:

    - 将其改造成一个无状态的、可复用的 **任务执行器 (Task Runner)**，或者直接将其核心逻辑提取成一个函数 `execute_download_task`。
    - 移除其内部的 `Queue`、`_worker` 线程、`start/stop` 方法和复杂的完成判断逻辑。它只需要接收一个原子任务（如 `task_type` 和 `symbol`），执行它，然后返回结果或抛出异常。

3.  **简化完成与停止逻辑**:
    - 当所有任务都被提交到线程池后，`DownloaderManager` 可以使用 `concurrent.futures` 提供的工具（如 `as_completed`）来跟踪整体进度。
    - 当所有 `future` 对象都完成后，任务自然结束，管理器就可以自动停止。
    - 信号处理逻辑可以直接作用于线程池的 `shutdown` 方法，大大简化停止流程。

### 重构后的代码示例

以下是基于上述思想重构后的代码。我将 `TushareDownloader` 的核心功能提取到一个更简单的类 `DownloadTaskRunner` 中，并重写 `DownloaderManager`。

#### 1. `DownloadTaskRunner` (替代 `TushareDownloader`)

这个类只负责执行单个下载任务，包含获取、重试和速率限制的逻辑。

```python
# downloader/task_runner.py (新文件或重构 TushareDownloader)
import logging
import time
import pandas as pd
from pyrate_limiter import Duration, InMemoryBucket, Limiter, Rate

from downloader.producer.fetcher_builder import FetcherBuilder, TaskType
from .huey_tasks import process_fetched_data

logger = logging.getLogger(__name__)

# 可以作为模块级单例或传入
# Rate(190, Duration.MINUTE) -> 每分钟190次
rate_limiter = Limiter(
    InMemoryBucket([Rate(190, Duration.MINUTE)]),
    raise_when_fail=False,
    max_delay=Duration.MINUTE * 2,
)

class DownloadTask:
    """定义一个独立的下载任务单元"""
    def __init__(self, task_type: TaskType, symbol: str):
        self.task_type = task_type
        self.symbol = symbol

    def __call__(self, max_retries: int = 2):
        """使任务对象可调用，执行下载逻辑"""
        fetcher_builder = FetcherBuilder()

        for attempt in range(max_retries + 1):
            try:
                # 应用速率限制
                rate_limiter.try_acquire(self.task_type.value, 1)

                # 构建 fetcher
                # STOCK_BASIC 任务类型特殊处理
                fetcher_symbol = self.symbol if self.task_type != TaskType.STOCK_BASIC else None
                fetcher = fetcher_builder.build_by_task(self.task_type, fetcher_symbol)

                # 执行下载
                df = fetcher()

                # 处理数据
                if df is not None and not df.empty:
                    # 使用 Huey 异步处理
                    process_fetched_data(self.symbol, self.task_type.name, df.to_dict())

                logger.debug(f"成功处理任务: {self.task_type.value} for {self.symbol}")
                return {"status": "success", "task": self}

            except Exception as e:
                logger.warning(
                    f"处理任务 {self.task_type.value} for {self.symbol} 失败 (尝试 {attempt + 1}/{max_retries + 1}): {e}"
                )
                if attempt >= max_retries:
                    logger.error(
                        f"任务 {self.task_type.value} for {self.symbol} 已达到最大重试次数，放弃。"
                    )
                    # 可以在这里返回更详细的错误信息
                    return {"status": "failed", "task": self, "error": str(e)}
                time.sleep(1) # 重试前稍作等待
```

#### 2. `DownloaderManager` (重构后)

这个类现在更加清晰，只负责任务的规划和执行。

```python
# downloader/manager.py (重构 DownloaderManager)
import signal
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

from tqdm import tqdm

from downloader.config import get_config
from downloader.producer.fetcher_builder import FetcherBuilder, TaskType
# 假设 DownloadTask 在这里
from .task_runner import DownloadTask

logger = logging.getLogger(__name__)

class DownloaderManager:
    """
    下载器管理器（重构版）
    负责规划、执行和监控所有下载任务。
    """
    def __init__(self, task_group: str, symbols: Optional[List[str]] = None, max_workers: int = 10):
        config = get_config()
        if task_group not in config.task_groups:
            raise ValueError(f"任务组 '{task_group}' 不存在。")

        self.task_group_names = config.task_groups[task_group]
        self.initial_symbols = symbols
        self.max_workers = max_workers

        self.executor: Optional[ThreadPoolExecutor] = None
        self._shutdown_event = threading.Event()
        self._original_handlers = {}

    def _register_signal_handlers(self):
        """注册信号处理器，以便优雅地关闭"""
        def signal_handler(signum, frame):
            logger.info(f"接收到信号 {signum}，开始优雅关闭...")
            self.stop()

        try:
            self._original_handlers[signal.SIGINT] = signal.signal(signal.SIGINT, signal_handler)
            self._original_handlers[signal.SIGTERM] = signal.signal(signal.SIGTERM, signal_handler)
            logger.debug("信号处理器已注册")
        except (OSError, ValueError) as e:
            logger.warning(f"在非主线程注册信号处理器失败: {e}")

    def _restore_signal_handlers(self):
        """恢复原始信号处理器"""
        for signum, handler in self._original_handlers.items():
            if handler:
                try:
                    signal.signal(signum, handler)
                except (OSError, ValueError) as e:
                     logger.warning(f"恢复信号处理器 {signum} 失败: {e}")
        logger.debug("信号处理器已恢复")

    def _prepare_tasks(self) -> List[DownloadTask]:
        """
        准备所有需要执行的原子任务列表（任务扁平化）
        """
        # 任务名到 TaskType 的映射
        task_mapping = {
            "stock_basic": TaskType.STOCK_BASIC,
            "stock_daily": TaskType.STOCK_DAILY,
            "stock_adj_qfq": TaskType.DAILY_BAR_QFQ,
            "balance_sheet": TaskType.BALANCESHEET,
            "income_statement": TaskType.INCOME,
            "cash_flow": TaskType.CASHFLOW,
        }

        # 如果没有提供 symbols，则先执行 stock_basic_fetcher 获取全部 symbols
        symbols = self.initial_symbols
        if symbols is None:
            logger.info("未提供symbols，正在获取全量股票列表...")
            stock_basic_fetcher = FetcherBuilder().build_by_task(TaskType.STOCK_BASIC)
            try:
                df = stock_basic_fetcher()
                if df is None or df.empty:
                    raise ValueError("获取股票基本信息失败")
                symbols = df["ts_code"].tolist()
                logger.info(f"成功获取 {len(symbols)} 只股票。")
            except Exception as e:
                logger.error(f"获取股票列表失败: {e}")
                raise

        tasks = []
        for task_name in self.task_group_names:
            task_type = task_mapping.get(task_name)
            if not task_type:
                logger.warning(f"未知的任务类型 '{task_name}'，已跳过。")
                continue

            if task_type == TaskType.STOCK_BASIC:
                # STOCK_BASIC 是一个单一任务，不需要遍历 symbols
                tasks.append(DownloadTask(task_type, symbol="stock_basic_all"))
            else:
                for symbol in symbols:
                    tasks.append(DownloadTask(task_type, symbol))

        return tasks

    def run(self):
        """
        启动管理器，执行所有任务直到完成。
        这是一个阻塞方法。
        """
        if self._shutdown_event.is_set():
            logger.warning("管理器已经运行并关闭，无法再次启动。")
            return

        self._register_signal_handlers()

        all_tasks = self._prepare_tasks()
        if not all_tasks:
            logger.warning("没有生成任何下载任务，程序退出。")
            return

        logger.info(f"共计 {len(all_tasks)} 个下载任务待执行。")
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="DownloadWorker")

        successful_tasks = 0
        failed_tasks = 0

        try:
            # 使用 as_completed 来实时处理已完成的任务，并用 tqdm 显示进度条
            futures = {self.executor.submit(task): task for task in all_tasks}

            with tqdm(total=len(all_tasks), desc="下载进度") as pbar:
                for future in as_completed(futures):
                    if self._shutdown_event.is_set():
                        # 如果收到停止信号，取消未完成的 future
                        future.cancel()
                        pbar.update(1)
                        continue

                    try:
                        result = future.result()
                        if result and result.get("status") == "success":
                            successful_tasks += 1
                        else:
                            failed_tasks += 1
                    except Exception as e:
                        failed_tasks += 1
                        task = futures[future]
                        logger.error(f"任务 {task.task_type.value} for {task.symbol} 执行时抛出未捕获异常: {e}")

                    pbar.update(1)

        finally:
            logger.info(f"所有任务执行完毕。成功: {successful_tasks}, 失败: {failed_tasks}.")
            self.stop()

    def stop(self):
        """停止管理器并清理资源"""
        if not self._shutdown_event.is_set():
            self._shutdown_event.set()
            logger.info("正在关闭线程池...")
            if self.executor:
                # cancel_futures=True 仅在 Python 3.9+ 支持
                # 对于旧版本，我们已经在循环中处理了取消
                self.executor.shutdown(wait=True, cancel_futures=True)
            logger.info("线程池已关闭。")
            self._restore_signal_handlers()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

```

### 重构后的优势

1.  **架构清晰**: `DownloaderManager` 的职责是“规划与执行”，`DownloadTask` 的职责是“完成一项具体工作”。职责单一，符合单一职责原则。
2.  **代码简化**: 移除了所有复杂的双层状态管理（`_started`, `_stop_event`, `_completion_lock`, `_on_downloader_finished` 等），代码量大幅减少，逻辑更直接。
3.  **更强的内聚性**: 下载、重试、限流的逻辑都内聚在 `DownloadTask` 中，易于单独测试和维护。
4.  **易于理解和维护**: “准备一个任务列表，然后把它扔进线程池执行”这个模型比之前嵌套的管理模型要简单得多，新人接手也更容易。
5.  **进度反馈更直接**: 通过 `tqdm` 和 `as_completed`，可以非常简单地实现一个精确到单个任务的进度条，而不再需要依赖复杂的事件系统。
6.  **自动停止**: `run` 方法在所有任务完成后会自然退出 `try...finally` 块，并调用 `stop()`，完美实现了“所有任务完成后能自动停止”的需求，无需任何事件监听。

希望这份详细的审核和重构建议对您有帮助！
