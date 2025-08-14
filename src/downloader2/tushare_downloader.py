# filename: downloader2/tushare_downloader.py

from concurrent.futures import ThreadPoolExecutor
import logging
from queue import Empty, Queue
import threading

from box import Box
import pandas as pd
from pyrate_limiter import Duration, InMemoryBucket, Limiter, Rate

from downloader2.factories.fetcher_builder import FetcherBuilder, TaskType
from downloader2.interfaces.event_bus import IEventBus
from downloader2.interfaces.event_handler import EventType

logger = logging.getLogger(__name__)


class TushareDownloader:
    TASK_TYPE: TaskType

    def __init__(
        self,
        symbols: list[str],
        task_type: TaskType,
        data_queue: Queue,
        executor: ThreadPoolExecutor,
        event_bus: IEventBus,
    ):
        self.symbols = symbols
        self.task_type = task_type
        self.fetcher_builder = FetcherBuilder()
        self.data_queue = data_queue
        self.executor = executor

        self.event_bus = event_bus

        self.task_queue: Queue = Queue()
        self.worker_count = executor._max_workers
        self.retry_counts: dict[str, int] = {}
        self.max_retries = 2

        self._shutdown_event = threading.Event()

        self.total_symbols = len(symbols)
        self.processed_symbols = 0
        self.successful_symbols = 0
        self.failed_symbols = 0
        self._progress_lock = threading.Lock()

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
            task = Box(
                {"symbol": symbol, "task_type": self.task_type.value, "data": df}
            )
            self.data_queue.put(task)

        # 关键修正：在任务成功时，调用简化的进度更新方法。
        self._update_progress("success")

        # 发布任务成功事件
        self.event_bus.publish(
            EventType.TASK_SUCCEEDED.value,
            self,
            symbol=symbol,
            total_task_count=len(self.symbols),
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
            EventType.TASK_FAILED.value,
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
        self._populate_symbol_queue()
        # 发布任务开始事件
        self.event_bus.publish(
            EventType.TASK_STARTED.value,
            self,
            total_task_count=len(self.symbols),
            task_type=self.task_type.value,
        )
        for _ in range(self.worker_count):
            self.executor.submit(self._worker)

    def stop(self):
        """停止下载器，发送结束信号给所有工作线程"""
        self._shutdown()
        self.event_bus.publish(
            EventType.TASK_FINISHED.value,
            self,
            total_task_count=len(self.symbols),
            processed_task_count=self.processed_symbols,
            successful_task_count=self.successful_symbols,
            failed_task_count=self.failed_symbols,
            task_type=self.task_type.value,
        )

    def _worker(self):
        """工作线程主循环"""
        while not self._shutdown_event.is_set():
            try:
                symbol = self.task_queue.get(timeout=1.0)
                try:
                    self._process_symbol(symbol)
                finally:
                    # 这个 finally 块是正确的，确保了 get() 和 task_done() 的配对
                    self.task_queue.task_done()
            except Empty:
                continue
            except Exception as e:
                logger.error(f"工作线程发生未知严重错误: {e}")

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
        for symbol in self.symbols:
            self.task_queue.put(symbol)

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
        fetcher = self.fetcher_builder.build_by_task(self.task_type, symbol)
        return fetcher()


if __name__ == "__main__":
    pass
