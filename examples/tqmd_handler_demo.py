import time
import random
from concurrent.futures import ThreadPoolExecutor

# --- 从您的项目中导入或在此处定义接口和事件类型 ---
from downloader2.interfaces.event_bus import IEventBus
from downloader2.interfaces.task_handler import ITaskHandler
from downloader2.producer.event_bus import BlinkerEventBus
from downloader2.interfaces.task_handler import TaskEventType


# 为了演示的可运行性，我们在此处重新定义它们
from downloader2.producer.tqmd_task_handler import TqdmTaskHandler


def simulate_downloader(symbols: list, event_bus: IEventBus):
    """一个模拟 TushareDownloader 行为的函数"""
    total_tasks = len(symbols)
    successful_count = 0
    failed_count = 0

    # 1. 发布任务开始事件
    event_bus.publish(
        TaskEventType.TASK_STARTED,
        "DownloaderSimulator",
        total_task_count=total_tasks,
        task_type="StockData",
    )

    # 2. 使用线程池模拟并发处理
    with ThreadPoolExecutor(max_workers=4) as executor:
        for symbol in symbols:
            # 模拟处理每个 symbol
            time.sleep(random.uniform(0.05, 0.2))

            if random.random() > 0.1:  # 90% 成功率
                successful_count += 1
                event_bus.publish(
                    TaskEventType.TASK_SUCCEEDED,
                    "DownloaderSimulator",
                    symbol=symbol,
                    successful_task_count=successful_count,
                    failed_task_count=failed_count,
                    processed_task_count=successful_count + failed_count,
                )
            else:
                failed_count += 1
                event_bus.publish(
                    TaskEventType.TASK_FAILED,
                    "DownloaderSimulator",
                    symbol=symbol,
                    error="Random network failure",
                    successful_task_count=successful_count,
                    failed_task_count=failed_count,
                    processed_task_count=successful_count + failed_count,
                )

    # 3. 所有任务处理完毕后，发布任务完成事件
    event_bus.publish(
        TaskEventType.TASK_FINISHED,
        "DownloaderSimulator",
        total_task_count=total_tasks,
        successful_task_count=successful_count,
        failed_task_count=failed_count,
        processed_task_count=successful_count + failed_count,
    )


if __name__ == "__main__":
    # 1. 实例化事件总线和处理器
    event_bus: IEventBus = BlinkerEventBus()
    task_handler: ITaskHandler = TqdmTaskHandler(event_bus)

    # 2. 准备模拟数据并运行模拟器
    symbols_to_download = [f"SYM{i:03d}" for i in range(50)]

    simulate_downloader(symbols_to_download, event_bus)
