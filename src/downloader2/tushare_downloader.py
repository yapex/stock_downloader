# filename: downloader2/tushare_downloader.py

from downloader2.factories.fetcher_builder import FetcherBuilder, TaskType
from queue import Queue, Empty
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading
from pyrate_limiter import Duration, Rate, Limiter, InMemoryBucket
import logging
from box import Box

logger = logging.getLogger(__name__)


class TushareDownloader:
    TASK_TYPE: TaskType

    def __init__(
        self,
        symbols: list[str],
        task_type: TaskType,
        data_queue: Queue,
        executor: ThreadPoolExecutor,
    ):
        self.symbols = symbols
        self.task_type = task_type
        self.fetcher_builder = FetcherBuilder()
        self.data_queue = data_queue
        self.executor = executor
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
        self.retry_counts.pop(symbol, None)

    def _abandon_symbol(self, symbol: str, error: Exception) -> None:
        """放弃处理symbol"""
        # 关键修正：在任务被放弃时，调用简化的进度更新方法。
        self._update_progress("failed")
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
        for _ in range(self.worker_count):
            self.executor.submit(self._worker)

    def stop(self):
        """停止下载器，发送结束信号给所有工作线程"""
        self._shutdown()

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
    from concurrent.futures import ThreadPoolExecutor
    from queue import Queue
    from downloader2.factories.fetcher_builder import TaskType

    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # 示例股票代码
    symbols = ["000001.SZ", "000002.SZ", "600519.SH"]

    # 创建队列和线程池
    data_queue = Queue()
    executor = ThreadPoolExecutor(max_workers=4)

    try:
        # 创建下载器实例
        downloader = TushareDownloader(
            symbols=symbols,
            task_type=TaskType.STOCK_DAILY,
            data_queue=data_queue,
            executor=executor,
        )

        # 启动下载
        logger.info(f"开始下载 {len(symbols)} 个股票的数据...")
        downloader.start()

        # 处理下载的数据
        downloaded_count = 0
        while downloaded_count < len(symbols):
            try:
                task = data_queue.get(timeout=60)  # 60秒超时
                if task is not None:
                    symbol = task.symbol
                    df = task.data
                    if df is not None and not df.empty:
                        logger.info(f"成功下载 {symbol} 的数据，共 {len(df)} 条记录")
                    else:
                        logger.warning(f"股票 {symbol} 没有数据")
                downloaded_count += 1
                data_queue.task_done()
            except Exception as e:
                logger.error(f"处理数据时出错: {e}")
                break

        logger.info("数据下载完成")

    except KeyboardInterrupt:
        logger.error("用户中断下载")
    except Exception as e:
        logger.error(f"下载过程中出错: {e}")
    finally:
        # 清理资源
        if "downloader" in locals():
            downloader.stop()
        executor.shutdown(wait=True)
