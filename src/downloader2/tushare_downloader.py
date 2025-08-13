from downloader2.factories.fetcher_builder import FetcherBuilder, TaskType
from queue import Queue
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
        self.executor: ThreadPoolExecutor = executor
        self.task_queue: Queue = Queue()
        self.worker_threads: list[threading.Thread] = []
        self.worker_count = 4
        self.retry_counts: dict[str, int] = {}  # 记录每个symbol的重试次数
        self.max_retries = 2  # 最大重试次数

        # 创建速率限制器：每分钟最多190次调用
        self.rate_limiter = Limiter(
            InMemoryBucket([Rate(190, Duration.MINUTE)]),
            raise_when_fail=False,  # 阻塞模式，不抛出异常
            max_delay=Duration.MINUTE * 2,  # 最大等待时间2分钟
        )

    def start(self):
        """启动下载器，创建线程池并将symbols放入内部队列"""
        self._populate_symbol_queue()

        # 启动多个线程，从task_queue中获取symbol，调用_fetching_by_symbol
        for i in range(self.worker_count):
            worker_thread = threading.Thread(
                target=self._worker, name=f"TushareWorker-{i}"
            )
            worker_thread.daemon = False  # 确保主线程等待工作线程完成
            worker_thread.start()
            self.worker_threads.append(worker_thread)

    def stop(self):
        """停止下载器，发送结束信号给所有工作线程"""
        self.shutdown()

    def _worker(self):
        """工作线程函数，从队列中获取symbol并处理"""
        while True:
            symbol = self.task_queue.get()
            if symbol is None:
                self.task_queue.task_done()
                break

            should_retry = self._process_symbol(symbol)
            if not should_retry:
                self.task_queue.task_done()

    def _process_symbol(self, symbol: str) -> bool:
        """处理单个symbol，返回是否需要重试"""
        try:
            self._apply_rate_limiting(symbol)
            df = self._fetching_by_symbol(symbol)
            self._handle_successful_fetch(symbol, df)
            return False
        except Exception as e:
            return self._handle_fetch_error(symbol, e)

    def _apply_rate_limiting(self, symbol: str) -> None:
        """应用速率限制"""
        logger.debug(f"Rate limiting check for symbol: {symbol}")
        self.rate_limiter.try_acquire(
            self.task_type.value, 1
        )  # 使用task_type作为identity，权重为1

    def _handle_successful_fetch(self, symbol: str, df: pd.DataFrame) -> None:
        """处理成功获取的数据"""
        if not df.empty:
            task = Box(
                {
                    "task_type": self.task_type.value,
                    "data": df,
                }
            )
            self.data_queue.put(task)
            # 成功处理后清理重试计数
            self.retry_counts.pop(symbol, None)

    def _handle_fetch_error(self, symbol: str, error: Exception) -> bool:
        """处理获取数据时的错误，返回是否需要重试"""
        current_retries = self.retry_counts.get(symbol, 0)
        
        if current_retries < self.max_retries:
            return self._schedule_retry(symbol, error, current_retries)
        else:
            self._abandon_symbol(symbol, error)
            return False

    def _schedule_retry(self, symbol: str, error: Exception, current_retries: int) -> bool:
        """安排重试"""
        self.retry_counts[symbol] = current_retries + 1
        logger.warning(f"处理 symbol {symbol} 时发生错误: {error}，第 {current_retries + 1} 次重试")
        self.task_queue.put(symbol)  # 重新加入队列尾部
        return True

    def _abandon_symbol(self, symbol: str, error: Exception) -> None:
        """放弃处理symbol"""
        logger.error(f"处理 symbol {symbol} 时发生错误: {error}，已达到最大重试次数 {self.max_retries}，放弃处理")
        # 清理重试计数
        self.retry_counts.pop(symbol, None)

    def _populate_symbol_queue(self):
        """将symbols数据放入内部队列"""
        for symbol in self.symbols:
            self.task_queue.put(symbol)

        # 为每个工作线程添加结束标记，确保所有线程都能收到停止信号
        for _ in range(self.worker_count):
            self.task_queue.put(None)

    def shutdown(self, timeout: float = 30.0) -> bool:
        """优雅关闭所有工作线程

        Args:
            timeout: 等待线程完成的超时时间（秒）

        Returns:
            bool: 如果所有线程在超时时间内完成则返回True，否则返回False
        """
        logger.debug(f"开始关闭 {len(self.worker_threads)} 个工作线程...")
        
        start_time = self._wait_for_tasks_completion(timeout)
        all_finished = self._wait_for_workers_to_finish(timeout, start_time)
        
        self._log_shutdown_result(all_finished)
        return all_finished

    def _wait_for_tasks_completion(self, timeout: float) -> float:
        """等待队列任务完成
        
        Args:
            timeout: 超时时间
            
        Returns:
            float: 开始时间
        """
        import time
        start_time = time.time()
        
        if self.worker_threads:
            try:
                # 使用带超时的 join 等待队列中的所有任务完成
                import threading

                queue_join_thread = threading.Thread(target=self.task_queue.join)
                queue_join_thread.daemon = True  # 设置为守护线程
                queue_join_thread.start()
                queue_join_thread.join(timeout=timeout)

                if queue_join_thread.is_alive():
                    logger.warning(f"队列任务未能在 {timeout} 秒内完成")
                    # 不返回 False，继续尝试关闭线程
                else:
                    elapsed = time.time() - start_time
                    logger.debug(f"所有队列任务已完成，耗时 {elapsed:.2f} 秒")
            except Exception as e:
                logger.error(f"等待队列任务完成时发生错误: {e}")
                # 不返回 False，继续尝试关闭线程
        
        return start_time

    def _wait_for_workers_to_finish(self, timeout: float, start_time: float) -> bool:
        """等待所有工作线程完成
        
        Args:
            timeout: 超时时间
            start_time: 开始时间
            
        Returns:
            bool: 是否所有线程都完成
        """
        import time
        
        # 等待所有工作线程完成
        all_finished = True
        remaining_timeout = max(
            5.0,
            timeout - (time.time() - start_time)
            if "start_time" in locals()
            else timeout,
        )

        for thread in self.worker_threads:
            thread.join(
                timeout=remaining_timeout / len(self.worker_threads)
                if self.worker_threads
                else 5.0
            )
            if thread.is_alive():
                logger.warning(f"线程 {thread.name} 在等待后仍在运行")
                all_finished = False
            else:
                logger.debug(f"线程 {thread.name} 已完成")
        
        return all_finished

    def _log_shutdown_result(self, all_finished: bool) -> None:
        """记录关闭结果
        
        Args:
            all_finished: 是否所有线程都完成
        """
        if all_finished:
            logger.debug("所有工作线程已优雅关闭")
        else:
            logger.warning("部分工作线程未能在超时时间内完成")

    def _fetching_by_symbol(self, symbol: str) -> pd.DataFrame:
        fetcher = self.fetcher_builder.build_by_task(self.task_type, symbol)
        return fetcher()


if __name__ == "__main__":
    import time

    print("=== TushareDownloader 实际运行测试 ===")
    print("注意：Token 将由 FetcherBuilder 自动从配置文件获取")

    # 测试用例
    symbols = ["600519.SH", "002023.SZ"]
    task_type = TaskType.STOCK_DAILY
    data_queue = Queue()
    executor = ThreadPoolExecutor(max_workers=4)

    print(f"准备下载 {len(symbols)} 个股票的数据: {symbols}")
    print(f"任务类型: {task_type}")

    downloader = TushareDownloader(
        symbols=symbols,
        task_type=task_type,
        data_queue=data_queue,
        executor=executor,
    )

    print("启动下载器...")
    downloader.start()

    # 等待一段时间让下载器工作
    print("等待下载完成...")
    time.sleep(10)

    # 检查数据队列中的结果
    print("\n=== 下载结果 ===")
    result_count = 0
    while not data_queue.empty():
        try:
            task = data_queue.get_nowait()
            result_count += 1
            print(f"任务 {result_count}:")
            print(f"  - 任务类型: {task.task_type}")
            print(f"  - 数据行数: {len(task.data)}")
            if not task.data.empty:
                print(
                    f"  - 股票代码: {task.data['ts_code'].iloc[0] if 'ts_code' in task.data.columns else 'N/A'}"
                )
                print(f"  - 数据列: {list(task.data.columns)}")
            print()
        except Exception as e:
            print(f"读取结果时出错: {e}")
            break

    print(f"总共获取到 {result_count} 个结果")

    # 优雅关闭
    print("\n关闭下载器...")
    success = downloader.shutdown(timeout=10.0)
    print(f"关闭{'成功' if success else '失败'}")

    # 清理资源
    executor.shutdown(wait=True)
    print("测试完成")
