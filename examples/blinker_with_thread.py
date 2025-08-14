import threading
import time
import random
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Protocol, Callable, Any

# 使用 blinker 作为我们接口的一个具体实现
from blinker import Signal

# =======================================================================
# 1. 接口设计 (Protocol)
# =======================================================================


class EventBus(Protocol):
    """
    定义一个事件总线的“协议”或“接口”。
    任何遵循此接口的类都可以被我们的系统使用，从而将系统与
    具体的事件库（如 blinker）解耦。
    """

    def subscribe(self, event_name: str, handler: Callable[..., None]) -> None:
        """
        订阅一个事件。当该事件发布时，指定的 handler 将被调用。

        Args:
            event_name: 事件的唯一名称 (例如 'task:succeeded').
            handler: 一个可调用的函数，用于处理事件。
        """
        ...  # 在 Protocol 中，我们只定义方法签名，不实现

    def publish(self, event_name: str, sender: Any, **kwargs) -> None:
        """
        发布（或发送）一个事件。

        Args:
            event_name: 要发布的事件的名称。
            sender: 发送事件的对象或实体。
            **kwargs: 附带到事件中的任意数据。
        """
        ...


# =======================================================================
# 2. 接口的具体实现 (BlinkerEventBus)
# =======================================================================


class BlinkerEventBus:
    """
    使用 'blinker' 库来实现我们的 EventBus 接口。
    这个类封装了所有 blinker 的细节。
    """

    def __init__(self):
        # 将 blinker 的 Signal 对象存储在字典中，与事件名称关联
        self._signals: dict[str, Signal] = {}

    def _get_or_create_signal(self, event_name: str) -> Signal:
        """一个内部辅助方法，用于获取或创建 blinker 信号"""
        if event_name not in self._signals:
            self._signals[event_name] = Signal(f"Signal for {event_name}")
        return self._signals[event_name]

    def subscribe(self, event_name: str, handler: Callable[..., None]) -> None:
        """使用 blinker.Signal.connect() 来实现订阅"""
        signal = self._get_or_create_signal(event_name)
        signal.connect(handler)

    def publish(self, event_name: str, sender: Any, **kwargs) -> None:
        """使用 blinker.Signal.send() 来实现发布"""
        signal = self._get_or_create_signal(event_name)
        signal.send(sender, **kwargs)


# =======================================================================
# 3. 重构后的应用程序代码
# =======================================================================

# --- A. 使用字符串常量定义事件名称 ---
TASK_STARTED = "task:started"
TASK_SUCCEEDED = "task:succeeded"
TASK_FAILED = "task:failed"


# --- B. 信号处理器保持不变，因为它本来就很通用 ---
class SignalHandler:
    def __init__(self):
        self.success_count = 0
        self.failure_count = 0
        self._lock = threading.Lock()

    def on_task_succeeded(self, sender, **kwargs):
        with self._lock:
            self.success_count += 1
        task_id = kwargs.get("task_id", "N/A")
        logging.info(f"[HANDLER] 任务成功: {task_id}")

    def on_task_failed(self, sender, **kwargs):
        with self._lock:
            self.failure_count += 1
        task_id = kwargs.get("task_id", "N/A")
        error = kwargs.get("error", "未知错误")
        logging.error(f"[HANDLER] 任务失败: {task_id} - 原因: {error}")

    def get_total_received(self):
        with self._lock:
            return self.success_count + self.failure_count


# --- C. 核心处理器现在依赖于 EventBus 接口，而不是具体实现 ---
class AsyncTaskProcessor:
    # 注意：构造函数现在接收一个符合 EventBus 协议的对象
    def __init__(
        self, task_queue: Queue, executor: ThreadPoolExecutor, event_bus: EventBus
    ):
        self.task_queue = task_queue
        self.executor = executor
        self.event_bus = event_bus  # 存储对事件总线接口的引用
        self._shutdown_event = threading.Event()

    def start(self):
        for _ in range(self.executor._max_workers):
            self.executor.submit(self._worker)

    def shutdown(self):
        self._shutdown_event.set()
        logging.info("关闭信号已发送至所有工作线程...")

    def _worker(self):
        thread_name = threading.current_thread().name
        logging.info(f"工作线程 {thread_name} 已启动。")

        while not self._shutdown_event.is_set():
            try:
                task_id = self.task_queue.get(timeout=0.5)
                try:
                    # 使用接口发布事件，而不是直接调用 blinker
                    self.event_bus.publish(TASK_STARTED, self, task_id=task_id)

                    logging.debug(f"线程 {thread_name} 开始处理任务: {task_id}")
                    time.sleep(random.uniform(0.1, 0.5))

                    if random.random() > 0.15:
                        self.event_bus.publish(TASK_SUCCEEDED, self, task_id=task_id)
                    else:
                        self.event_bus.publish(
                            TASK_FAILED, self, task_id=task_id, error="模拟的随机错误"
                        )

                finally:
                    self.task_queue.task_done()
            except Empty:
                continue

        logging.info(f"工作线程 {thread_name} 已正常关闭。")


# --- 4. 主程序入口 ---
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )

    NUM_TASKS = 50
    NUM_WORKERS = 5

    # --- 设置 ---
    task_queue = Queue()
    executor = ThreadPoolExecutor(
        max_workers=NUM_WORKERS, thread_name_prefix="TaskWorker"
    )

    # 1. 实例化具体实现
    #    这是唯一一处代码知道我们正在使用 "Blinker" 的地方。
    event_bus: EventBus = BlinkerEventBus()

    # 2. 实例化处理器，通过“依赖注入”传入 event_bus
    signal_handler = SignalHandler()
    #    AsyncTaskProcessor 只知道它得到了一个符合 EventBus 接口的东西。
    processor = AsyncTaskProcessor(task_queue, executor, event_bus)

    # 3. 使用接口进行订阅
    event_bus.subscribe(TASK_SUCCEEDED, signal_handler.on_task_succeeded)
    event_bus.subscribe(TASK_FAILED, signal_handler.on_task_failed)

    logging.info(f"主线程：准备分发 {NUM_TASKS} 个任务。系统已与 Blinker 解耦。")

    # --- 填充任务队列 ---
    for i in range(NUM_TASKS):
        task_queue.put(f"Task-{i + 1:02d}")

    # --- 启动和关闭流程（与之前相同） ---
    processor.start()
    task_queue.join()
    logging.info("主线程：所有任务已处理完毕。")

    processor.shutdown()
    executor.shutdown(wait=True)

    # --- 验证结果 ---
    logging.info("=" * 40)
    logging.info("验证结果：")
    total_received = signal_handler.get_total_received()
    logging.info(f"  - 接收到的成功/失败信号总数: {total_received}")
    logging.info(f"  - 原始任务总数: {NUM_TASKS}")

    assert total_received == NUM_TASKS
    logging.info("验证成功：通过抽象接口运行的事件总线在多线程中依然可靠！")
