#!/usr/bin/env python3
"""使用 asyncio 的 Huey 演示"""

import asyncio
import time
from huey.contrib.asyncio import aget_result
from huey.consumer import Consumer
from config import huey
from tasks import download_task, slow_task

# 全局 consumer 变量
consumer = None
consumer_task = None


async def start_consumer():
    """在主线程中启动 Huey consumer"""
    global consumer, consumer_task

    def run_consumer_sync():
        """同步运行 consumer"""
        # 启动多线程 Consumer，支持真正的并发执行
        consumer = Consumer(huey, workers=4, worker_type="thread")
        consumer.run()

    # 在 executor 中运行 consumer，避免阻塞主线程
    loop = asyncio.get_event_loop()
    consumer_task = loop.run_in_executor(None, run_consumer_sync)

    print("🚀 Huey Consumer 已启动 (4个工作线程)")
    # 给 consumer 一点时间启动
    await asyncio.sleep(0.5)


async def stop_consumer():
    """停止 Huey consumer"""
    global consumer, consumer_task
    if consumer:
        consumer.stop()
        print("🛑 Huey Consumer 已停止")

    if consumer_task:
        try:
            consumer_task.cancel()
            await asyncio.sleep(0.1)  # 给一点时间让任务清理
        except asyncio.CancelledError:
            pass


async def test_chain_tasks():
    """测试链式任务"""
    print("🎯 测试链式任务 (使用 asyncio)")
    print()

    print("📋 提交链式任务...")
    print()

    # 提交多个任务
    symbols = ["AAPL", "TSLA", "GOOGL"]
    tasks = []

    for symbol in symbols:
        print(f"📤 提交任务: {symbol}")
        task = download_task(symbol)
        tasks.append(task)

    print("\n⏳ 等待任务执行完成...")
    print()

    # 异步等待所有任务完成
    start_time = time.time()
    try:
        # 使用 aget_result 异步获取所有任务结果
        results = await asyncio.gather(*[aget_result(task) for task in tasks])
    except Exception as e:
        print(f"❌ 任务失败: {e}")
        results = []
    end_time = time.time()

    print("\n" + "=" * 40)
    print(f"✅ 所有链式任务执行完成! 总耗时: {end_time - start_time:.2f}s")
    print(f"📊 完成任务数: {len(results)}")
    print(f"📊 结果: {results}")


async def test_concurrent_tasks():
    """测试并发任务执行"""
    print("\n🎯 测试并发任务执行 (使用 asyncio)")
    print()

    # 测试1: 快速提交多个任务，观察并发效果
    print("📋 测试1: 快速提交5个任务 (每个3秒)")
    submit_start = time.time()

    tasks = []
    for i in range(5):
        task = slow_task(3)
        tasks.append(task)
        current_time = time.time() - submit_start
        print(f"  📤 [{current_time:.3f}s] 提交任务 {i + 1}")

    submit_end = time.time()
    print(f"\n✅ 所有任务提交完成，耗时: {submit_end - submit_start:.3f}s")
    print("\n⏳ 开始获取任务结果...")

    # 使用 aget_result 异步获取所有任务结果
    result_start = time.time()
    results = await asyncio.gather(*[aget_result(task) for task in tasks])

    total_time = time.time() - submit_start
    print("\n📊 并发执行统计:")
    print(f"  📤 提交阶段: {submit_end - submit_start:.3f}s")
    print(f"  ⏳ 执行阶段: {time.time() - result_start:.3f}s")
    print(f"  🎯 总耗时: {total_time:.3f}s")
    print(f"  📈 理论串行时间: {5 * 3:.1f}s")
    print(f"  🚀 并发效率: {(15 / total_time * 100):.1f}%")
    print(f"  📊 结果: {results}")

    # 测试2: 对比串行执行
    print("\n" + "=" * 50)
    print("📋 测试2: 串行执行对比 (3个任务，每个2秒)")
    serial_start = time.time()

    for i in range(3):
        task_start = time.time()
        task = slow_task(2)
        print(f"  📤 [{time.time() - serial_start:.3f}s] 提交并等待任务 {i + 1}")
        result = task()
        task_end = time.time()
        print(
            f"  ✅ [{task_end - serial_start:.3f}s] 任务 {i + 1} 完成: {result} (单任务耗时: {task_end - task_start:.3f}s)"
        )

    serial_total = time.time() - serial_start
    print("\n📊 串行执行统计:")
    print(f"  🎯 总耗时: {serial_total:.3f}s")
    print(f"  📈 理论时间: {3 * 2:.1f}s")


async def main():
    """主函数"""
    print("🚀 MemoryHuey 异步演示")
    print("=" * 50)

    # 启动 consumer
    await start_consumer()

    try:
        # 测试链式任务
        await test_chain_tasks()

        # 等待一下
        await asyncio.sleep(2)

        # 测试并发任务
        await test_concurrent_tasks()

    finally:
        # 停止 consumer
        await stop_consumer()

    print("\n" + "=" * 50)
    print("✅ 所有测试完成!")
    print("👋 演示结束")


if __name__ == "__main__":
    asyncio.run(main())
