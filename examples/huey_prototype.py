import time
import random
import os
import sys
from huey import SqliteHuey

# --- 1. 配置任务总线 (Configuration) ---
# 这是整个架构的核心。所有进程都将通过这个文件进行通信。
HUEY_DB_FILE = "task_bus.db"

# 【【【 这就是修正之处 】】】
# 显式地将 immediate=False，确保 Huey 始终以异步队列模式运行。
huey = SqliteHuey(filename=HUEY_DB_FILE, immediate=False)


# --- 2. 消费者逻辑 (Consumer Logic) ---
# 这个函数定义了“消费者”的工作内容。
@huey.task(retries=2, retry_delay=5)
def process_data(producer_id, data):
    """
    这个任务从队列中获取数据并进行处理。
    它由 Consumer 进程执行。
    """
    consumer_pid = os.getpid()

    print(
        f"[CONSUMER PID: {consumer_pid: >5}] "
        f"接收到来自 [{producer_id}] 的任务。 正在处理数据: {data}..."
    )

    # 模拟耗时的I/O操作
    time.sleep(random.uniform(1, 3))

    print(f"[CONSUMER PID: {consumer_pid: >5}] ✅ 完成了来自 [{producer_id}] 的任务。")


# --- 3. 生产者逻辑 (Producer Logic) ---
def run_producer(producer_id):
    """
    这个函数模拟一个生产者。它只负责生成任务并发送到总线。
    它由 Producer 进程执行。
    """
    print(f"--- ▶️  启动 Producer [{producer_id}] (PID: {os.getpid()}) ---")

    for i in range(5):
        task_data = {"item_id": f"item-{i}", "value": random.randint(100, 999)}

        print(f"[{producer_id}] ➡️  生成任务 {i}，数据: {task_data}。发送至总线...")

        # 核心：直接调用被装饰的函数，Huey 会自动将任务添加到队列
        process_data(producer_id=producer_id, data=task_data)

        time.sleep(random.uniform(0.1, 0.5))

    print(f"---⏹️  Producer [{producer_id}] 已发送完所有任务。 ---")


# --- 4. 主程序入口 (Main Entrypoint) ---
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("\n" + "=" * 50)
        print("错误: 请指定一个角色。")
        print("用法:")
        print("  启动消费者进程池: uv run huey_prototype.py consumer")
        print("  启动一个生产者:   uv run producer.py <ID>")
        print("=" * 50 + "\n")
        sys.exit(1)

    role = sys.argv[1]

    if role == "consumer":
        print("--- ⚙️  正在启动 Huey 消费者进程池 (4个workers)... ---")
        print(f"--- 监听任务总线: {HUEY_DB_FILE} ---")
        print("--- 按下 Ctrl+C 停止运行。 ---")

        # 使用 uv run 启动 Huey consumer
        import subprocess
        subprocess.run([
            "uv", "run", "huey_consumer.py", 
            "huey_prototype.huey", 
            "-w", "4", 
            "-k", "process"
        ])

    else:
        print(f"错误: 未知的角色 '{role}'")
        print("请使用 'uv run producer.py <ID>' 启动生产者")
        sys.exit(1)
