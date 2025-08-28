"""任务生产者"""

import time
import logging
from .tasks import download_task

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# --- 模拟的业务参数 ---
TOTAL_TASKS = 1200
TASK_TYPES = [
    "stock_daily",
    "stock_adj_qfq",
    "daily_basic",
    "balance_sheet",
    "income_statement",
    "cash_flow",
]


def run_producer():
    """运行生产者，提交所有任务。"""
    logging.info(f"🔥 开始提交 {TOTAL_TASKS} 个下载任务...")
    start_time = time.time()

    for i in range(TOTAL_TASKS):
        # 模拟轮流使用不同的业务类型
        task_type = TASK_TYPES[i % len(TASK_TYPES)]
        symbol = f"SH{600000 + i}"
        # 将下载任务提交到“快速”队列
        download_task(task_type=task_type, symbol=symbol)

    end_time = time.time()
    logging.info(
        f"✅ 所有 {TOTAL_TASKS} 个任务已提交，耗时 {end_time - start_time:.2f} 秒."
    )


if __name__ == "__main__":
    run_producer()
