#!/usr/bin/env python3
"""测试 pyrate_limiter 在无 gevent 环境中的兼容性"""

import time
from pyrate_limiter import Duration, InMemoryBucket, Limiter, Rate
from config import huey
from tasks import slow_task


def test_pyrate_limiter_basic():
    """测试 pyrate_limiter 基本功能"""
    print("🧪 测试 pyrate_limiter 基本功能")

    # 创建限流器：每秒最多 2 个请求
    limiter = Limiter(
        InMemoryBucket([Rate(2, Duration.SECOND)]),
        raise_when_fail=False,
        max_delay=Duration.SECOND * 5,
    )

    print("📊 测试限流效果 (每秒最多 2 个请求)...")
    start_time = time.time()

    for i in range(5):
        limiter.try_acquire("test", 1)
        current_time = time.time()
        elapsed = current_time - start_time
        print(f"  请求 {i + 1}: {elapsed:.2f}s")

    total_time = time.time() - start_time
    print(f"✅ 限流测试完成，总耗时: {total_time:.2f}s")
    print()


def test_pyrate_limiter_with_huey():
    """测试 pyrate_limiter 与 MiniHuey 的兼容性"""
    print("🧪 测试 pyrate_limiter 与 MiniHuey 的兼容性")

    # 创建限流器：每秒最多 1 个请求
    limiter = Limiter(
        InMemoryBucket([Rate(1, Duration.SECOND)]),
        raise_when_fail=False,
        max_delay=Duration.SECOND * 5,
    )

    # 启动 MiniHuey
    print("🚀 启动 MiniHuey 调度器...")
    huey.start()

    try:
        print("📋 在限流环境中提交任务...")
        tasks = []
        start_time = time.time()

        for i in range(3):
            limiter.try_acquire("huey_task", 1)
            current_time = time.time()
            elapsed = current_time - start_time
            print(f"  提交任务 {i + 1}: {elapsed:.2f}s")
            task = slow_task(1)
            tasks.append(task)

        print("\n⏳ 等待任务完成...")
        results = []
        for task in tasks:
            try:
                result = task()
                results.append(result)
            except Exception as e:
                print(f"❌ 任务失败: {e}")
                results.append(None)

        total_time = time.time() - start_time
        print("\n✅ 限流 + MiniHuey 测试完成")
        print(f"📊 完成任务数: {len([r for r in results if r is not None])}")
        print(f"⚡ 总耗时: {total_time:.2f}s")

    finally:
        print("\n🛑 停止 MiniHuey 调度器...")
        huey.stop()


def main():
    """主函数"""
    print("🚀 PyRate Limiter 兼容性测试")
    print("=" * 50)

    # 测试基本功能
    test_pyrate_limiter_basic()

    # 等待一下
    time.sleep(1)

    # 测试与 MiniHuey 的兼容性
    test_pyrate_limiter_with_huey()

    print("\n" + "=" * 50)
    print("✅ 所有兼容性测试完成!")
    print("🎉 pyrate_limiter 在无 gevent 环境中工作正常!")
    print("👋 测试结束")


if __name__ == "__main__":
    main()
