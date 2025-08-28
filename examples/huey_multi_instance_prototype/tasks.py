"""任务定义模块 (修正版)

修正了任务间数据传递的逻辑，使其更贴近真实管道场景。
"""

import time
import logging
from .config import huey_fast, huey_slow

# 配置日志，方便观察
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


# --- 慢速任务定义 ---
@huey_slow.task()
def process_data_task(task_type: str, symbol: str, data: dict):
    """模拟一个缓慢的、消费数据的处理任务。"""
    data_size = data.get("size", 0)
    logging.info(
        f"[SLOW_WORKER] 🐌 开始保存 {symbol}_{task_type} (数据大小: {data_size}b)..."
    )
    # 模拟一个长时间的数据库写入操作
    time.sleep(2)
    logging.info(f"[SLOW_WORKER] ✅ 完成保存 {symbol}_{task_type}.")
    return f"Saved {symbol}"


# --- 快速任务定义 ---
@huey_fast.task()
def download_task(task_type: str, symbol: str):
    """模拟一个快速的下载任务，完成后将数据传递给慢速任务。"""
    logging.info(f"[FAST_WORKER] 🚀 开始下载 {symbol}_{task_type}...")

    # 模拟速率限制/网络延迟
    time.sleep(0.05)

    # 模拟下载后产生的数据
    downloaded_data = {"size": 1024, "content": "...dummy data..."}

    logging.info(f"[FAST_WORKER] 📥 下载完成 {symbol}_{task_type}. 提交保存任务.")

    # 手动将下一个任务提交到“慢速”队列中，并把数据作为参数传递
    process_data_task(task_type=task_type, symbol=symbol, data=downloaded_data)

    return f"Downloaded {symbol}"
