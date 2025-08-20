# filename: downloader2/tushare_downloader.py

from concurrent.futures import ThreadPoolExecutor
import logging
from queue import Empty, Queue
import threading

import pandas as pd
from pyrate_limiter import Duration, InMemoryBucket, Limiter, Rate

from downloader2.producer.fetcher_builder import FetcherBuilder, TaskType
from downloader2.interfaces.event_bus import IEventBus
from downloader2.interfaces.task_handler import TaskEventType
from .huey_tasks import process_fetched_data

logger = logging.getLogger(__name__)


class TushareDownloader:  # 按给定symbol列表下载数据
    TASK_TYPE: TaskType

    def __init__(
        self,
        symbols: list[str],
        task_type: TaskType,
        executor: ThreadPoolExecutor,
        event_bus: IEventBus,
    ):
        # 检查symbols是否为空列表
        if not symbols and task_type != TaskType.STOCK_BASIC:
            logger.warning(
                f"警告：任务类型 {task_type.value} 的symbols列表为空，该下载器将不会执行任何任务"
            )
        elif not symbols and task_type == TaskType.STOCK_BASIC:
            logger.warning(
                f"警告：任务类型 {task_type.value} 的symbols列表为空，该下载器将不会执行任何任务"
            )

        self.symbols = symbols
        self.task_type = task_type
        self.fetcher_builder = FetcherBuilder()
        self.executor = executor

        self.event_bus = event_bus

        self.task_queue: Queue = Queue()
        self.worker_count = executor._max_workers
        self.retry_counts: dict[str, int] = {}
        self.max_retries = 2

        self._shutdown_event = threading.Event()

        # 计算正确的总任务数：对于STOCK_BASIC任务，总任务数是1（如果symbols不为空）；其他任务是symbols数量
        if task_type == TaskType.STOCK_BASIC:
            self.total_symbols = 1 if symbols else 0
        else:
            self.total_symbols = len(symbols)
        self.processed_symbols = 0
        self.successful_symbols = 0
        self.failed_symbols = 0
        self._progress_lock = threading.Lock()
        self._completion_checked = threading.Event()  # 使用 Event 防止重复检查完成状态
        self._stop_called = threading.Event()  # 防止 stop 方法被多次调用

        self.rate_limiter = Limiter(
            InMemoryBucket([Rate(190, Duration.MINUTE)]),
            raise_when_fail=False,
            max_delay=Duration.MINUTE * 2,
        )

    # ==================================================================
    # === 核心修正部分 ===
    # ==================================================================

    def _update_progress(self, status: str) -> None:
        """
        更新进度。此方法只在任务生命周期结束时（最终成功或失败）被调用。

        Args:
            status: 最终状态 ('success' 或 'failed')
        """
        with self._progress_lock:
            # 关键修正：无条件增加 processed_symbols，因为它只在任务完成时被调用。
            self.processed_symbols += 1
            if status == "success":
                self.successful_symbols += 1
            elif status == "failed":
                self.failed_symbols += 1

    def _handle_successful_fetch(self, symbol: str, df: pd.DataFrame) -> None:
        """处理成功获取的数据"""
        if not df.empty:
            # 使用 Huey 任务替代 data_queue.put
            process_fetched_data(symbol, self.task_type.name, df.to_dict())

        # 关键修正：在任务成功时，调用简化的进度更新方法。
        self._update_progress("success")

        # 计算正确的总任务数：对于STOCK_BASIC任务，总任务数是1；其他任务是symbols数量
        total_task_count = (
            1 if self.task_type == TaskType.STOCK_BASIC else len(self.symbols)
        )

        # 发布任务成功事件
        self.event_bus.publish(
            TaskEventType.TASK_SUCCEEDED.value,
            self,
            symbol=symbol,
            total_task_count=total_task_count,
            processed_task_count=self.processed_symbols,
            successful_task_count=self.successful_symbols,
            failed_task_count=self.failed_symbols,
            task_left_count=self.task_queue.qsize(),
            task_type=self.task_type.value,
        )

        self.retry_counts.pop(symbol, None)

    def _abandon_symbol(self, symbol: str, error: Exception) -> None:
        """放弃处理symbol"""
        # 关键修正：在任务被放弃时，调用简化的进度更新方法。
        self._update_progress("failed")

        # 发布任务失败事件
        self.event_bus.publish(
            TaskEventType.TASK_FAILED.value,
            self,
            symbol=symbol,
            error=str(error),
            task_queue_size=self.task_queue.qsize(),
            task_type=self.task_type.value,
        )

        logger.error(
            f"处理 symbol {symbol} 时发生错误: {error}，已达到最大重试次数 {self.max_retries}，放弃处理"
        )
        self.retry_counts.pop(symbol, None)

    # ==================================================================
    # === 其余代码保持不变，逻辑是健全的 ===
    # ==================================================================

    def start(self):
        """启动下载器，将任务提交给线程池"""
        # 如果symbols为空，根据任务类型决定是否启动
        if not self.symbols:
            if self.task_type == TaskType.STOCK_BASIC:
                # STOCK_BASIC任务类型，即使symbols为空也可以启动，但需要警告
                logger.warning(
                    f"任务类型 {self.task_type.value} 的symbols列表为空，但仍将启动下载器"
                )
            else:
                # 其他任务类型，symbols为空时不启动
                logger.warning(
                    f"任务类型 {self.task_type.value} 的symbols列表为空，跳过启动"
                )
                return

        self._populate_symbol_queue()
        # 发布任务开始事件
        self.event_bus.publish(
            TaskEventType.TASK_STARTED.value,
            self,
            total_task_count=self.total_symbols,
            task_type=self.task_type.value,
        )
        for _ in range(self.worker_count):
            self.executor.submit(self._worker)

    def stop(self):
        """停止下载器，发送结束信号给所有工作线程"""
        # 确保 stop 方法只被执行一次
        if self._stop_called.is_set():
            return
        self._stop_called.set()

        self._shutdown()
        self.event_bus.publish(
            TaskEventType.TASK_FINISHED.value,
            self,
            total_task_count=self.total_symbols,
            processed_task_count=self.processed_symbols,
            successful_task_count=self.successful_symbols,
            failed_task_count=self.failed_symbols,
            task_type=self.task_type.value,
        )

    def _worker(self):
        """工作线程主循环"""
        logger.debug(
            f"工作线程启动 - 任务类型: {self.task_type.value}, 队列大小: {self.task_queue.qsize()}"
        )
        while not self._shutdown_event.is_set():
            try:
                symbol = self.task_queue.get(timeout=1.0)
                logger.debug(
                    f"工作线程获取到任务: {symbol} - 任务类型: {self.task_type.value}"
                )
                try:
                    self._process_symbol(symbol)
                finally:
                    # 这个 finally 块是正确的，确保了 get() 和 task_done() 的配对
                    self.task_queue.task_done()

                # 检查是否所有任务都已完成
                self._check_completion()

            except Empty:
                logger.debug(
                    f"工作线程等待超时 - 任务类型: {self.task_type.value}, 队列大小: {self.task_queue.qsize()}"
                )
                # 队列为空时也检查是否所有任务都已完成
                self._check_completion()
                continue
            except Exception as e:
                logger.error(f"工作线程发生未知严重错误: {e}")
        logger.debug(f"工作线程结束 - 任务类型: {self.task_type.value}")

    def _check_completion(self):
        """检查是否所有任务都已完成，如果是则自动停止下载器"""
        # 如果已经检查过完成状态，直接返回
        if self._completion_checked.is_set():
            return

        with self._progress_lock:
            # 再次检查，防止竞态条件
            if self._completion_checked.is_set():
                return

            # 检查是否所有任务都已处理完成
            if (
                self.processed_symbols >= self.total_symbols
                and self.task_queue.qsize() == 0
                and self.total_symbols > 0
            ):
                self._completion_checked.set()  # 原子性标记已检查
                logger.info(
                    f"任务类型 {self.task_type.value} 的所有任务已完成，自动停止下载器"
                )
                self._shutdown_event.set()  # 设置停止信号
                # 在单独的线程中调用stop方法，避免死锁
                import threading

                threading.Thread(target=self.stop, daemon=True).start()

    def _process_symbol(self, symbol: str):
        """处理单个symbol"""
        try:
            self._apply_rate_limiting()
            df = self._fetching_by_symbol(symbol)
            self._handle_successful_fetch(symbol, df)
        except Exception as e:
            self._handle_fetch_error(symbol, e)

    def _apply_rate_limiting(self) -> None:
        """应用速率限制"""
        logger.debug(f"Rate limiting check for symbol: {self.task_type.value}")
        self.rate_limiter.try_acquire(self.task_type.value, 1)

    def _handle_fetch_error(self, symbol: str, error: Exception):
        """处理获取数据时的错误"""
        current_retries = self.retry_counts.get(symbol, 0)
        if current_retries < self.max_retries:
            self._schedule_retry(symbol, error, current_retries)
        else:
            self._abandon_symbol(symbol, error)

    def _schedule_retry(self, symbol: str, error: Exception, current_retries: int):
        """安排重试"""
        self.retry_counts[symbol] = current_retries + 1
        logger.warning(
            f"处理 symbol {symbol} 时发生错误: {error}，第 {current_retries + 1} 次重试"
        )
        self.task_queue.put(symbol)

    def _populate_symbol_queue(self):
        """将symbols数据放入内部队列"""
        logger.debug(
            f"开始填充任务队列 - 任务类型: {self.task_type.value}, 股票数量: {len(self.symbols)}"
        )

        # 特殊处理：对于STOCK_BASIC任务类型，只放入一个空字符串作为任务
        # 因为STOCK_BASIC任务不需要symbol参数，可以直接获取所有股票的基本信息
        if self.task_type == TaskType.STOCK_BASIC:
            self.task_queue.put("")
            logger.debug(f"STOCK_BASIC任务类型特殊处理：只添加一个空symbol任务")
        else:
            # 其他任务类型正常处理，遍历symbols列表
            for symbol in self.symbols:
                self.task_queue.put(symbol)

        logger.debug(
            f"任务队列填充完成 - 任务类型: {self.task_type.value}, 队列大小: {self.task_queue.qsize()}"
        )

    def _shutdown(self, wait: bool = True, timeout: float = 30.0):
        """优雅地关闭下载器"""
        logger.debug("开始关闭下载器...")
        self._shutdown_event.set()
        if not wait:
            while not self.task_queue.empty():
                try:
                    self.task_queue.get_nowait()
                    self.task_queue.task_done()
                except Empty:
                    break

        # 在多线程测试中，主线程不应在这里阻塞等待 join
        # executor 的关闭由外部（测试夹具）负责
        logger.debug("下载器关闭信号已发送。")

    def _fetching_by_symbol(self, symbol: str) -> pd.DataFrame:
        # 对于空字符串symbol（STOCK_BASIC特殊处理），不传入symbol参数
        if symbol == "" and self.task_type == TaskType.STOCK_BASIC:
            fetcher = self.fetcher_builder.build_by_task(self.task_type)
            logger.debug(f"STOCK_BASIC任务类型特殊处理：不传入symbol参数")
        else:
            fetcher = self.fetcher_builder.build_by_task(self.task_type, symbol)
        return fetcher()


if __name__ == "__main__":
    pass
