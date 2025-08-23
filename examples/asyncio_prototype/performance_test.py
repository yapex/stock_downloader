#!/usr/bin/env python3
"""性能对比测试：展示多线程Consumer的并发优势"""

import asyncio
import time
from config import huey
from tasks import slow_task
from huey.contrib.asyncio import aget_result
from huey.consumer import Consumer

# 全局变量
consumer = None
consumer_task = None


async def start_consumer(workers=1):
    """在主线程中启动 Huey consumer"""
    global consumer, consumer_task

    def run_consumer_sync():
        """同步运行 consumer"""
        global consumer
        # 创建Consumer
        consumer = Consumer(huey, workers=workers, worker_type="thread")
        consumer.run()

    # 在 executor 中运行 consumer，避免阻塞主线程
    loop = asyncio.get_event_loop()
    consumer_task = loop.run_in_executor(None, run_consumer_sync)

    print(f"🚀 Huey Consumer 已启动 ({workers}个工作线程)")
    # 给 consumer 一点时间启动
    await asyncio.sleep(0.5)


async def stop_consumer():
    """停止 Huey consumer"""
    global consumer, consumer_task
    if consumer:
        try:
            consumer.stop()
        except:
            pass  # 忽略停止时的错误
        print("🛑 Huey Consumer 已停止")

    if consumer_task:
        try:
            consumer_task.cancel()
            await asyncio.sleep(0.1)  # 给一点时间让任务清理
        except asyncio.CancelledError:
            pass


async def test_performance(workers, task_count=5, task_duration=2):
    """测试不同worker数量的性能"""
    print(
        f"\n📊 测试配置: {workers}个工作线程, {task_count}个任务, 每个任务{task_duration}秒"
    )

    # 启动consumer
    await start_consumer(workers)

    # 提交任务
    start_time = time.time()
    tasks = [slow_task(task_duration) for _ in range(task_count)]
    submit_time = time.time() - start_time

    # 等待结果
    execution_start = time.time()
    await asyncio.gather(*[aget_result(task) for task in tasks])
    execution_time = time.time() - execution_start

    total_time = time.time() - start_time
    theoretical_time = task_count * task_duration
    efficiency = theoretical_time / total_time * 100

    print(f"  📤 任务提交: {submit_time:.3f}s")
    print(f"  ⏳ 任务执行: {execution_time:.3f}s")
    print(f"  🎯 总耗时: {total_time:.3f}s")
    print(f"  📈 理论串行时间: {theoretical_time}s")
    print(f"  🚀 并发效率: {efficiency:.1f}%")

    # 停止consumer
    await stop_consumer()
    await asyncio.sleep(0.5)  # 等待consumer停止

    return {
        "workers": workers,
        "total_time": total_time,
        "efficiency": efficiency,
        "speedup": theoretical_time / total_time,
    }


async def main():
    """主函数：对比不同worker数量的性能"""
    print("🎯 Huey 多线程Consumer性能对比测试")
    print("=" * 50)

    results = []

    # 测试不同的worker数量
    for workers in [1, 2, 4]:
        result = await test_performance(workers, task_count=6, task_duration=2)
        results.append(result)
        await asyncio.sleep(1)  # 间隔

    # 输出对比结果
    print("\n📊 性能对比总结:")
    print("=" * 50)
    print(f"{'Workers':<8} {'总耗时':<10} {'并发效率':<10} {'加速比':<8}")
    print("-" * 40)

    for result in results:
        print(
            f"{result['workers']:<8} {result['total_time']:<10.3f} {result['efficiency']:<10.1f}% {result['speedup']:<8.2f}x"
        )

    # 结论
    best_result = max(results, key=lambda x: x["efficiency"])
    print(f"\n🏆 最佳配置: {best_result['workers']}个工作线程")
    print(f"🚀 最高效率: {best_result['efficiency']:.1f}%")
    print(f"⚡ 最大加速: {best_result['speedup']:.2f}倍")

    print("\n✅ 性能测试完成!")


if __name__ == "__main__":
    asyncio.run(main())
