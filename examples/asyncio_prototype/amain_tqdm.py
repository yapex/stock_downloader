#!/usr/bin/env python3
"""使用 tqdm 进度条的 Huey 演示"""

import asyncio
import time
from tqdm.asyncio import tqdm
from huey.contrib.asyncio import aget_result
from huey.consumer import Consumer
from config import huey
from tasks_tqdm import download_task_with_progress, slow_task_with_progress

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


async def test_chain_tasks_with_progress():
    """测试链式任务（使用 tqdm 进度条）"""
    print("🎯 测试链式任务 (使用 tqdm 进度条)")
    print()

    # 提交多个任务
    symbols = ["AAPL", "TSLA", "GOOGL", "MSFT", "AMZN"]
    tasks = []

    # 任务提交进度条
    with tqdm(total=len(symbols), desc="📤 提交任务", unit="task") as submit_pbar:
        for symbol in symbols:
            task = download_task_with_progress(symbol)
            tasks.append(task)
            submit_pbar.set_postfix_str(f"当前: {symbol}")
            submit_pbar.update(1)
            await asyncio.sleep(0.1)  # 模拟提交间隔

    print("\n⏳ 等待任务执行完成...")

    # 任务执行进度条
    start_time = time.time()
    results = []

    with tqdm(total=len(tasks), desc="✅ 执行任务", unit="task") as exec_pbar:
        # 使用 asyncio.as_completed 来实时更新进度
        for coro in asyncio.as_completed([aget_result(task) for task in tasks]):
            try:
                result = await coro
                results.append(result)
                exec_pbar.set_postfix_str(f"完成: {result['symbol']}")
                exec_pbar.update(1)
            except Exception as e:
                exec_pbar.set_postfix_str(f"失败: {str(e)[:20]}")
                exec_pbar.update(1)

    end_time = time.time()

    print("\n" + "=" * 50)
    print(f"✅ 所有链式任务执行完成! 总耗时: {end_time - start_time:.2f}s")
    print(f"📊 完成任务数: {len(results)}")
    print(f"📊 成功率: {len(results) / len(symbols) * 100:.1f}%")


async def test_concurrent_tasks_with_progress():
    """测试并发任务执行（使用 tqdm 进度条）"""
    print("\n🎯 测试并发任务执行 (使用 tqdm 进度条)")
    print()

    # 测试1: 快速提交多个任务，观察并发效果
    print("📋 测试1: 快速提交8个任务 (每个2-4秒)")
    submit_start = time.time()

    tasks = []
    durations = [2, 3, 2, 4, 3, 2, 3, 4]  # 不同的任务持续时间

    # 任务提交进度条
    with tqdm(total=len(durations), desc="📤 提交慢任务", unit="task") as submit_pbar:
        for i, duration in enumerate(durations):
            task = slow_task_with_progress(duration, f"Task-{i + 1}")
            tasks.append(task)
            submit_pbar.set_postfix_str(f"任务{i + 1} ({duration}s)")
            submit_pbar.update(1)
            await asyncio.sleep(0.05)  # 快速提交

    submit_end = time.time()
    print(f"\n✅ 所有任务提交完成，耗时: {submit_end - submit_start:.3f}s")
    print("\n⏳ 开始获取任务结果...")

    # 任务执行进度条
    result_start = time.time()
    results = []

    with tqdm(total=len(tasks), desc="⚡ 并发执行", unit="task") as exec_pbar:
        for coro in asyncio.as_completed([aget_result(task) for task in tasks]):
            try:
                result = await coro
                results.append(result)
                # 从结果中提取任务名称
                task_name = result.split(",")[0] if "," in result else result[:10]
                exec_pbar.set_postfix_str(f"完成: {task_name}")
                exec_pbar.update(1)
            except Exception as e:
                exec_pbar.set_postfix_str(f"失败: {str(e)[:15]}")
                exec_pbar.update(1)

    total_time = time.time() - submit_start
    execution_time = time.time() - result_start

    print("\n📊 并发执行统计:")
    print(f"  📤 提交阶段: {submit_end - submit_start:.3f}s")
    print(f"  ⏳ 执行阶段: {execution_time:.3f}s")
    print(f"  🎯 总耗时: {total_time:.3f}s")
    print(f"  📈 理论串行时间: {sum(durations):.1f}s")
    print(f"  🚀 并发效率: {(sum(durations) / total_time * 100):.1f}%")
    print(f"  📊 成功任务数: {len(results)}/{len(tasks)}")


async def test_progress_monitoring():
    """测试进度监控功能"""
    print("\n🎯 测试进度监控功能")
    print()

    # 创建一个长时间运行的任务来演示进度监控
    print("📋 启动长时间任务 (10秒)")

    task = slow_task_with_progress(10, "LongTask")

    # 模拟进度监控
    with tqdm(total=100, desc="🔄 任务进度", unit="%") as pbar:
        start_time = time.time()

        # 每0.5秒更新一次进度
        while True:
            elapsed = time.time() - start_time
            progress = min(int(elapsed / 10 * 100), 100)

            pbar.n = progress
            pbar.set_postfix_str(f"已用时: {elapsed:.1f}s")
            pbar.refresh()

            if progress >= 100:
                break

            await asyncio.sleep(0.5)

    # 获取任务结果
    try:
        result = await aget_result(task)
        print(f"\n✅ 长时间任务完成: {result}")
    except Exception as e:
        print(f"\n❌ 长时间任务失败: {e}")


async def main():
    """主函数"""
    print("🚀 MemoryHuey + tqdm 进度条演示")
    print("=" * 60)

    # 启动 consumer
    await start_consumer()

    try:
        # 测试链式任务
        await test_chain_tasks_with_progress()

        # 等待一下
        await asyncio.sleep(1)

        # 测试并发任务
        await test_concurrent_tasks_with_progress()

        # 等待一下
        await asyncio.sleep(1)

        # 测试进度监控
        await test_progress_monitoring()

    finally:
        # 停止 consumer
        await stop_consumer()

    print("\n" + "=" * 60)
    print("✅ 所有测试完成!")
    print("👋 tqdm 进度条演示结束")


if __name__ == "__main__":
    asyncio.run(main())
