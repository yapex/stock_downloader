import threading
import time
import random
from pyrate_limiter import Duration, Rate, Limiter, BucketFullException


# --- 1. 定义一个模拟的 API 调用函数 ---
# 这是我们想要保护的资源，确保它不会被过度调用。
def call_api(worker_id: int):
    """一个模拟的函数，代表一次 API 调用。"""
    thread_name = threading.current_thread().name
    print(
        f"✅ [{time.time():.2f}] API 调用成功! (来自: {thread_name}, Worker {worker_id})"
    )


# --- 2. 设置限流规则和限流器 ---
# 定义多个速率限制规则：
# - 突发速率: 1秒内最多2次调用 (用于应对瞬时高峰)
# - 持续速率: 10秒内最多5次调用 (用于控制长期平均速率)
rates = [Rate(2, Duration.SECOND), Rate(5, 10 * Duration.SECOND)]

# 创建一个单一的、线程安全的限流器实例。
# 这个实例将在所有线程之间共享。
# 默认使用 MemoryListBucket，它是线程安全的。
limiter = Limiter(*rates)

# 定义一个共享的标识符。所有线程都将竞争这个标识符的速率配额。
# 在真实应用中，这可能是 'user_id', 'ip_address', 'api_key' 等。
SHARED_ITEM_NAME = "shared_api_key"


# --- 3. 定义 "Fail-Fast" 策略的工作线程 ---
def fail_fast_worker(worker_id: int):
    """
    这个工作线程使用 try_acquire。
    如果速率超限，它会捕获异常并立即放弃本次调用。
    """
    thread_name = threading.current_thread().name
    # 每个线程尝试发起 3 次 API 调用
    for i in range(3):
        try:
            # 尝试获取一个令牌。如果桶满了，会立即抛出 BucketFullException。
            limiter.try_acquire(SHARED_ITEM_NAME)

            # 如果没有抛出异常，说明获取成功，可以执行调用
            call_api(worker_id)

        except BucketFullException:
            # 捕获异常，表示速率超限
            print(
                f"❌ [{time.time():.2f}] 速率超限，调用被拒绝! (来自: {thread_name}, Worker {worker_id})"
            )
            # 在真实应用中，你可以在这里记录日志、返回错误信息或将任务放入延迟队列。

        # 模拟请求之间的一些随机间隔
        time.sleep(random.uniform(0.1, 0.4))


# --- 4. 定义 "Blocking" 策略的工作线程 ---
def blocking_worker(worker_id: int):
    """
    这个工作线程使用 wait。
    如果速率超限，它会自动暂停（阻塞），直到可以执行为止。
    """
    thread_name = threading.current_thread().name
    # 每个线程最终都会成功完成 2 次调用
    for i in range(2):
        print(
            f"⏳ [{time.time():.2f}] {thread_name} (Worker {worker_id}) 正在等待获取调用许可..."
        )

        # 此调用将阻塞，直到速率限制允许它继续进行。它永远不会抛出异常。
        limiter.wait(SHARED_ITEM_NAME)

        # 一旦 wait 返回，就可以安全地执行调用
        call_api(worker_id)

        # 模拟调用成功后的一些处理工作
        time.sleep(random.uniform(0.5, 1.0))


# --- 5. 主执行逻辑 ---
if __name__ == "__main__":
    # =================================================================
    # 第一部分: 演示 Fail-Fast (try_acquire)
    # =================================================================
    print("=" * 60)
    print("🚀 Part 1: 演示 Fail-Fast (try_acquire) 策略 (4个线程并发)")
    print(f"规则: {rates}")
    print("=" * 60)

    threads = []
    for i in range(4):  # 启动4个线程，制造并发冲突
        thread = threading.Thread(
            target=fail_fast_worker, name=f"FailFast-T{i + 1}", args=(i + 1,)
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print("\n Fail-Fast 演示结束。请注意，许多调用被立即'拒绝'了。\n")

    # 等待足够长的时间（>10秒），以确保限流器的桶完全清空，以便进行下一次演示。
    print("...等待 11 秒，让限流规则重置...\n")
    time.sleep(11)

    # =================================================================
    # 第二部分: 演示 Blocking (wait)
    # =================================================================
    print("=" * 60)
    print("🧘 Part 2: 演示 Blocking (wait) 策略 (4个线程并发)")
    print(f"规则: {rates}")
    print("=" * 60)

    threads = []
    for i in range(4):  # 再次启动4个线程
        thread = threading.Thread(
            target=blocking_worker, name=f"Blocking-T{i + 1}", args=(i + 1,)
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print("\n Blocking 演示结束。请注意，所有调用最终都成功了，但执行时间被拉长了。")
