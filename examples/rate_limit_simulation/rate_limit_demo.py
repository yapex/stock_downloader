#!/usr/bin/env python3
"""速率限制核心验证演示

直接在代码中配置不同任务类型的速率限制，验证pyrate-limiter的工作效果。
"""

import asyncio
import time
from typing import Dict
from pyrate_limiter import Limiter, InMemoryBucket, Rate, Duration


class SimpleRateLimiter:
    """简化的速率限制器"""

    def __init__(self):
        # 直接在代码中配置不同任务类型的速率限制
        self.rate_configs = {
            "stock_basic": (5, 10),  # 5次/10秒
            "stock_daily": (3, 10),  # 3次/10秒
            "cash_flow": (8, 10),  # 8次/10秒
        }
        self.limiters: Dict[str, Limiter] = {}
        print("SimpleRateLimiter 初始化完成")

    def _get_limiter(self, task_type: str) -> Limiter:
        """获取指定任务类型的速率限制器"""
        if task_type not in self.limiters:
            count, seconds = self.rate_configs.get(task_type, (5, 10))
            rate = Rate(count, Duration.SECOND * seconds)
            self.limiters[task_type] = Limiter(
                InMemoryBucket([rate]),
                raise_when_fail=False,
                max_delay=Duration.SECOND * seconds * 2,
            )
            print(
                f"创建速率限制器 - 任务类型: {task_type}, 速率限制: {count} 请求/{seconds} 秒"
            )
        return self.limiters[task_type]

    def acquire(self, task_type: str) -> bool:
        """尝试获取权限"""
        limiter = self._get_limiter(task_type)
        return limiter.try_acquire(task_type)

    async def wait_and_acquire(self, task_type: str, timeout: int = 30) -> bool:
        """异步等待并获取权限"""
        limiter = self._get_limiter(task_type)
        start_time = time.time()

        while time.time() - start_time < timeout:
            if limiter.try_acquire(task_type):
                return True
            await asyncio.sleep(0.1)

        return False


def test_basic_rate_limiting():
    """测试基本速率限制功能"""
    print("=== 测试基本速率限制功能 ===")

    rate_limiter = SimpleRateLimiter()
    task_type = "stock_basic"  # 5次/10秒

    print(f"\n测试任务类型: {task_type}")
    print("预期: 前5次请求立即成功，第6次请求需要等待")

    start_time = time.time()
    success_count = 0

    # 连续发送8个请求
    for i in range(8):
        request_start = time.time()

        if rate_limiter.acquire(task_type):
            elapsed = time.time() - request_start
            success_count += 1
            print(f"请求 {i + 1}: 成功 (等待时间: {elapsed:.2f}秒)")
        else:
            elapsed = time.time() - request_start
            print(f"请求 {i + 1}: 被限制 (等待时间: {elapsed:.2f}秒)")

    total_time = time.time() - start_time
    print(f"\n结果: {success_count}/8 请求成功，总耗时: {total_time:.2f}秒")


def test_different_task_types():
    """测试不同任务类型的速率限制"""
    print("\n=== 测试不同任务类型的速率限制 ===")

    rate_limiter = SimpleRateLimiter()

    for task_type in ["stock_basic", "stock_daily", "cash_flow"]:
        print(f"\n测试任务类型: {task_type}")
        count, seconds = rate_limiter.rate_configs[task_type]
        print(f"配置: {count} 请求/{seconds} 秒")

        success_count = 0
        test_requests = count + 2  # 比限制多发2个请求

        for i in range(test_requests):
            start_time = time.time()

            if rate_limiter.acquire(task_type):
                elapsed = time.time() - start_time
                success_count += 1
                print(f"  请求 {i + 1}: 成功 (等待: {elapsed:.2f}秒)")
            else:
                elapsed = time.time() - start_time
                print(f"  请求 {i + 1}: 被限制 (等待: {elapsed:.2f}秒)")

        print(f"  结果: {success_count}/{test_requests} 请求成功")


async def test_async_wait():
    """测试异步等待功能"""
    print("\n=== 测试异步等待功能 ===")

    rate_limiter = SimpleRateLimiter()
    task_type = "stock_daily"  # 3次/10秒

    print(f"测试任务类型: {task_type}")

    # 先耗尽令牌桶
    for i in range(3):
        rate_limiter.acquire(task_type)
        print(f"耗尽令牌 {i + 1}/3")

    # 异步等待获取权限
    print("\n开始异步等待获取权限...")
    start_time = time.time()

    success = await rate_limiter.wait_and_acquire(task_type, timeout=15)
    elapsed = time.time() - start_time

    if success:
        print(f"异步等待成功! 等待时间: {elapsed:.2f}秒")
    else:
        print(f"异步等待超时! 等待时间: {elapsed:.2f}秒")


def main():
    """主函数"""
    print("速率限制核心验证演示")
    print("=" * 50)

    # 测试基本功能
    test_basic_rate_limiting()

    # 测试不同任务类型
    test_different_task_types()

    # 测试异步功能
    print("\n开始异步测试...")
    asyncio.run(test_async_wait())

    print("\n=== 所有测试完成 ===")


if __name__ == "__main__":
    main()
