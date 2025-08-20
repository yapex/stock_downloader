#!/usr/bin/env python3

import sys
import time
import random
import os
from huey_prototype import huey, process_data

def run_producer(producer_id):
    """
    独立的生产者函数，导入tasks而不是直接定义
    """
    print(f"--- ▶️  启动 Producer [{producer_id}] (PID: {os.getpid()}) ---")

    for i in range(5):
        task_data = {"item_id": f"item-{i}", "value": random.randint(100, 999)}

        print(f"[{producer_id}] ➡️  生成任务 {i}，数据: {task_data}。发送至总线...")

        # 核心：直接调用被装饰的函数，Huey 会自动将任务添加到队列
        process_data(producer_id=producer_id, data=task_data)

        time.sleep(random.uniform(0.1, 0.5))

    print(f"---⏹️  Producer [{producer_id}] 已发送完所有任务。 ---")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python producer.py <ID>")
        sys.exit(1)
    
    producer_id = f"Producer-{sys.argv[1]}"
    run_producer(producer_id)
