线程使用和停止方面的问题分析
尽管整体设计不错，但在线程管理和关闭逻辑上，存在一些关键问题和可以优化的地方。

ThreadPoolExecutor 的误用:
问题: 类在 __init__ 中接收一个 executor: ThreadPoolExecutor 对象，但在整个类中从未使用过它。相反，代码通过 threading.Thread 手动创建和管理线程 (self.worker_threads)。
影响: 这造成了代码的混乱和误导。使用者会以为传入的 executor 会被使用，但实际上它完全被忽略了。这不仅浪费了资源，也与代码的意图相悖。
task_queue.join() 的阻塞风险 (严重Bug):
问题: _worker 方法中的 task_queue.task_done() 调用位置不正确。当一个任务需要重试时 (_process_symbol 返回 True)，task_done() 没有被调用。
影响: queue.join() 的作用是阻塞，直到队列中每个被 get() 的项目都有一个对应的 task_done() 被调用。在重试场景下，一个任务被 get() 了，但对应的 task_done() 永远不会被调用，这会导致 self.task_queue.join() 在 _wait_for_tasks_completion 方法中永久阻塞，直到超时。这是导致关闭逻辑失败的主要原因。
复杂的关闭逻辑:
问题: shutdown 方法中的关闭流程过于复杂。它在一个新线程里调用 task_queue.join() 来模拟超时，这是一种复杂的变通方法（workaround），通常意味着底层设计有问题。
影响: 代码难以理解和维护。优雅关闭的核心应该是清晰地通知线程停止，并等待它们结束，而不是依赖这种复杂的队列阻塞技巧。
重复的“毒丸” (Poison Pill):
问题: 在 _populate_symbol_queue 和 shutdown 两个方法中都向队列放入了 None。
影响: 虽然这不一定会导致错误，但这是一种冗余设计。一个清晰的关闭信号应该只在需要关闭时发送一次。如果在 start() 时就放入，那么一旦所有初始任务完成，线程就会退出，即使后续还有可能动态添加任务（虽然当前代码没这个功能）。在 shutdown 时放入是更标准的做法。

改进建议
核心的改进思路是：真正利用 ThreadPoolExecutor 来管理线程生命周期，并修正 task_done() 的调用逻辑。这会让代码变得更简单、更健壮、也更符合Python的最佳实践。

以下是具体的改进方案和重构后的代码：

改进点：
移除手动线程管理: 完全删除 self.worker_threads 和手动创建 threading.Thread 的部分。让 ThreadPoolExecutor 来负责这一切。
正确使用 ThreadPoolExecutor: 在 start 方法中使用 executor.submit() 来提交工作任务。
修正 task_done() Bug: 在 _worker 循环中，无论任务成功、失败还是重试，只要 get() 了一个任务，就应该在处理完毕后（包括重新入队后）调用 task_done()。
引入更可靠的停止信号: 使用 threading.Event 作为停止信号。这比“毒丸”模式更明确，可以立即通知所有线程停止，而不用等待它们处理完队列中剩余的任务。
简化 shutdown 逻辑: 关闭流程简化为：设置停止事件 -> 等待队列任务完成（可选但推荐） -> 关闭 executor。executor.shutdown(wait=True) 会自动处理等待线程完成

'''python
from downloader2.factories.fetcher_builder import FetcherBuilder, TaskType
from queue import Queue, Empty
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading
from pyrate_limiter import Duration, Rate, Limiter, InMemoryBucket
import logging
from box import Box
import time

logger = logging.getLogger(__name__)

class ImprovedTushareDownloader:
    TASK_TYPE: TaskType

    def __init__(
        self,
        symbols: list[str],
        task_type: TaskType,
        data_queue: Queue,
        worker_count: int = 4, # 直接在这里指定工作线程数
    ):
        self.symbols = symbols
        self.task_type = task_type
        self.fetcher_builder = FetcherBuilder()
        self.data_queue = data_queue
        self.task_queue: Queue = Queue()
        self.worker_count = worker_count
        
        # 使用内部的Executor，使组件更独立
        self.executor = ThreadPoolExecutor(max_workers=self.worker_count, thread_name_prefix="TushareWorker")
        
        # 引入一个Event作为清晰的关闭信号
        self._shutdown_event = threading.Event()

        self.retry_counts: dict[str, int] = {}
        self.max_retries = 2

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

    def _update_progress(self, symbol: str, status: str) -> None:
        with self._progress_lock:
            # 注意：重试的任务不应计入processed
            if self.retry_counts.get(symbol, 0) == 0:
                 self.processed_symbols += 1
            if status == "success":
                self.successful_symbols += 1
            elif status == "failed":
                self.failed_symbols += 1
    
    def start(self):
        """启动下载器，将任务提交给线程池"""
        self._populate_symbol_queue()
        
        # 使用ThreadPoolExecutor提交任务，它会自动管理线程
        for _ in range(self.worker_count):
            self.executor.submit(self._worker)

    def shutdown(self, wait: bool = True, timeout: float = 30.0):
        """
        优雅地关闭下载器。
        
        Args:
            wait (bool): 是否等待所有在队列中的任务完成。
            timeout (float): 等待的最长时间（秒）。
        """
        logger.info("开始关闭下载器...")
        
        # 1. 发送关闭信号，让工作线程不再接受新任务
        self._shutdown_event.set()

        # 2. (可选) 清空队列，防止线程在等待get()时阻塞
        # 这会让所有已在队列中但未开始处理的任务被丢弃
        if not wait:
            while not self.task_queue.empty():
                try:
                    self.task_queue.get_nowait()
                    self.task_queue.task_done() # 保持计数平衡
                except Empty:
                    break
        
        # 3. 等待队列中已开始的任务完成
        # 因为task_done()的修正，这里不再会无限期阻塞
        # 我们仍然可以加一个超时来增加鲁棒性
        queue_join_thread = threading.Thread(target=self.task_queue.join)
        queue_join_thread.start()
        queue_join_thread.join(timeout=timeout)
        if queue_join_thread.is_alive():
            logger.warning(f"队列任务未能在 {timeout} 秒内完成。")

        # 4. 关闭线程池
        # executor.shutdown会等待所有已提交的任务完成（除非设置wait=False）
        self.executor.shutdown(wait=True)
        logger.info("所有工作线程已关闭。")

    def _worker(self):
        """工作线程函数，从队列中获取symbol并处理"""
        # 检查关闭信号，并且队列不为空
        while not self._shutdown_event.is_set():
            try:
                # 使用超时get，避免在shutdown时永久阻塞
                symbol = self.task_queue.get(timeout=1.0)
                
                try:
                    self._process_symbol(symbol)
                finally:
                    # 关键修正：无论成功、失败还是重试，
                    # 只要get()了一个任务，就必须调用task_done()
                    self.task_queue.task_done()
            
            except Empty:
                # 队列为空，是正常情况，继续循环等待或检查关闭信号
                continue
            except Exception as e:
                logger.error(f"工作线程发生未知错误: {e}")


    def _process_symbol(self, symbol: str):
        """处理单个symbol"""
        try:
            self._apply_rate_limiting()
            df = self._fetching_by_symbol(symbol)
            self._handle_successful_fetch(symbol, df)
        except Exception as e:
            self._handle_fetch_error(symbol, e)

    def _apply_rate_limiting(self):
        logger.debug(f"Rate limiting check for symbol: {self.task_type.value}")
        self.rate_limiter.try_acquire(self.task_type.value, 1)

    def _handle_successful_fetch(self, symbol: str, df: pd.DataFrame):
        if not df.empty:
            task = Box({"task_type": self.task_type.value, "data": df})
            self.data_queue.put(task)
        self._update_progress(symbol, "success")
        self.retry_counts.pop(symbol, None)

    def _handle_fetch_error(self, symbol: str, error: Exception):
        current_retries = self.retry_counts.get(symbol, 0)
        if current_retries < self.max_retries:
            self.retry_counts[symbol] = current_retries + 1
            logger.warning(f"处理 symbol {symbol} 时发生错误: {error}，第 {current_retries + 1} 次重试")
            self.task_queue.put(symbol)  # 重新加入队列
        else:
            self._update_progress(symbol, "failed")
            logger.error(f"处理 symbol {symbol} 时发生错误: {error}，已达最大重试次数，放弃处理")
            self.retry_counts.pop(symbol, None)

    def _populate_symbol_queue(self):
        """将symbols数据放入内部队列"""
        for symbol in self.symbols:
            self.task_queue.put(symbol)

    def _fetching_by_symbol(self, symbol: str) -> pd.DataFrame:
        fetcher = self.fetcher_builder.build_by_task(self.task_type, symbol)
        return fetcher()

# --- 测试代码保持不变，但实例化时有变化 ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')

    print("=== ImprovedTushareDownloader 实际运行测试 ===")
    
    symbols = ["600519", "002023", "000001", "non_existent_symbol", "600036"]
    task_type = TaskType.STOCK_DAILY
    data_queue = Queue()

    print(f"准备下载 {len(symbols)} 个股票的数据: {symbols}")
    print(f"任务类型: {task_type}")

    # 注意这里的实例化方式变化了
    downloader = ImprovedTushareDownloader(
        symbols=symbols,
        task_type=task_type,
        data_queue=data_queue,
        worker_count=4, # 直接在这里传入线程数
    )

    print("启动下载器...")
    downloader.start()

    # 等待下载完成的一种方式：监控队列和进度
    while downloader.processed_symbols < downloader.total_symbols:
        print(f"进度: {downloader.processed_symbols}/{downloader.total_symbols} | "
              f"成功: {downloader.successful_symbols} | "
              f"失败: {downloader.failed_symbols}")
        time.sleep(2)
    print("所有任务已处理完毕。")


    print("\n=== 下载结果 ===")
    result_count = 0
    while not data_queue.empty():
        # ... (其余部分与原代码相同) ...
        try:
            task = data_queue.get_nowait()
            result_count += 1
            print(f"  - 股票代码: {task.data['ts_code'].iloc[0] if 'ts_code' in task.data.columns else 'N/A'}")
        except Exception as e:
            pass # 省略输出细节

    print(f"总共获取到 {result_count} 个结果")

    # 优雅关闭
    print("\n关闭下载器...")
    downloader.shutdown(timeout=10.0)
    print("测试完成")
'''