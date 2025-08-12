#!/usr/bin/env python3
"""
验证 pyrate-limiter 的不同阻塞实现方式
对比三种实现方式：
1. raise_when_fail=False (现有方式)
2. raise_when_fail=True + 手动重试
3. 直接使用 limiter.try_acquire() 方法
"""

import time
import threading
from datetime import datetime
from pyrate_limiter import Duration, Rate, Limiter, InMemoryBucket, BucketFullException

# 配置较小的限制以便快速测试
TEST_RATE_LIMIT = Rate(3, Duration.SECOND * 5)  # 每5秒最多3次调用

# 方式1：使用 raise_when_fail=False
limiter1 = Limiter(
    InMemoryBucket([TEST_RATE_LIMIT]),
    raise_when_fail=False,
    max_delay=Duration.SECOND * 10
)

# 方式2：使用 raise_when_fail=True (会抛出异常)
limiter2 = Limiter(
    InMemoryBucket([TEST_RATE_LIMIT]),
    raise_when_fail=True
)

# 方式3：直接使用 try_acquire 方法
limiter3 = Limiter(InMemoryBucket([TEST_RATE_LIMIT]))

# 获取装饰器
decorator1 = limiter1.as_decorator()
decorator2 = limiter2.as_decorator()


def log_with_time(message: str):
    """带时间戳的日志输出"""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    thread_id = threading.current_thread().ident
    print(f"[{timestamp}] Thread-{thread_id}: {message}")


def mapping_func(*args, **kwargs):
    """映射函数：返回 (identity, weight)"""
    return "test_method", 1


# 方式1：raise_when_fail=False
def test_function_1(call_id: int):
    """使用 raise_when_fail=False 的装饰器"""
    # 直接使用 try_acquire 方法，因为 raise_when_fail=False 会阻塞
    limiter1.try_acquire("test_method", 1)
    log_with_time(f"执行 test_function_1，调用ID: {call_id}")
    return f"result_1_{call_id}"


# 方式2：raise_when_fail=True + 手动重试
def test_function_2(call_id: int):
    """使用 raise_when_fail=True + 手动重试"""
    max_retries = 10
    retry_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            limiter2.try_acquire("test_method", 1)
            log_with_time(f"执行 test_function_2，调用ID: {call_id}")
            return f"result_2_{call_id}"
        except BucketFullException:
            if attempt < max_retries - 1:
                log_with_time(f"第 {attempt + 1} 次尝试失败，等待 {retry_delay} 秒后重试")
                time.sleep(retry_delay)
            else:
                log_with_time(f"达到最大重试次数，放弃调用 {call_id}")
                raise


# 方式3：直接使用 try_acquire 方法
def test_function_3(call_id: int):
    """使用 try_acquire 方法实现阻塞"""
    max_retries = 10
    retry_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            # 尝试获取配额
            limiter3.try_acquire("test_method", 1)
            log_with_time(f"执行 test_function_3，调用ID: {call_id}")
            return f"result_3_{call_id}"
        except BucketFullException:
            if attempt < max_retries - 1:
                log_with_time(f"第 {attempt + 1} 次尝试失败，等待 {retry_delay} 秒后重试")
                time.sleep(retry_delay)
            else:
                log_with_time(f"达到最大重试次数，放弃调用 {call_id}")
                raise


def test_rate_limiter(test_func, method_name: str, num_calls: int = 6):
    """测试速率限制器"""
    print(f"\n=== 测试 {method_name} ===")
    log_with_time(f"开始测试 {method_name}，将进行 {num_calls} 次调用")
    
    start_time = time.time()
    
    for i in range(num_calls):
        log_with_time(f"准备第 {i+1} 次调用")
        try:
            result = test_func(i+1)
            log_with_time(f"第 {i+1} 次调用完成，结果: {result}")
        except Exception as e:
            log_with_time(f"第 {i+1} 次调用异常: {e}")
    
    end_time = time.time()
    total_time = end_time - start_time
    log_with_time(f"{method_name} 测试完成，总耗时: {total_time:.2f} 秒")
    
    return total_time


def test_concurrent_access(test_func, method_name: str):
    """测试并发访问"""
    print(f"\n=== 并发测试 {method_name} ===")
    
    def worker(worker_id: int):
        for i in range(3):
            try:
                result = test_func(f"{worker_id}-{i+1}")
                log_with_time(f"Worker {worker_id} 调用 {i+1} 完成: {result}")
            except Exception as e:
                log_with_time(f"Worker {worker_id} 调用 {i+1} 异常: {e}")
    
    # 创建3个线程，每个线程调用3次
    threads = []
    start_time = time.time()
    
    for worker_id in range(3):
        thread = threading.Thread(target=worker, args=(worker_id,))
        threads.append(thread)
        thread.start()
    
    # 等待所有线程完成
    for thread in threads:
        thread.join()
    
    end_time = time.time()
    total_time = end_time - start_time
    log_with_time(f"{method_name} 并发测试完成，总耗时: {total_time:.2f} 秒")
    
    return total_time


def reset_limiters():
    """重置限制器状态"""
    global limiter1, limiter2, limiter3
    
    limiter1 = Limiter(
        InMemoryBucket([TEST_RATE_LIMIT]),
        raise_when_fail=False,
        max_delay=Duration.MINUTE * 2
    )
    
    limiter2 = Limiter(
        InMemoryBucket([TEST_RATE_LIMIT]),
        raise_when_fail=True
    )
    
    limiter3 = Limiter(InMemoryBucket([TEST_RATE_LIMIT]))
    
    print("\n限制器状态已重置")


def main():
    """主测试函数"""
    print("pyrate-limiter 阻塞方式对比测试")
    print(f"速率限制: {TEST_RATE_LIMIT.limit} 次调用 / {TEST_RATE_LIMIT.interval} 秒")
    
    # 测试1：顺序调用
    time1 = test_rate_limiter(test_function_1, "方式1: raise_when_fail=False")
    
    # 等待一段时间，让限制器重置
    print("\n等待6秒让限制器重置...")
    time.sleep(6)
    
    time2 = test_rate_limiter(test_function_2, "方式2: raise_when_fail=True + 装饰器")
    
    # 重置限制器
    reset_limiters()
    
    # 等待一段时间
    print("\n等待6秒...")
    time.sleep(6)
    
    time3 = test_rate_limiter(test_function_3, "方式3: try_acquire + 手动重试")
    
    # 重置限制器
    reset_limiters()
    
    # 等待一段时间
    print("\n等待6秒...")
    time.sleep(6)
    
    # 测试2：并发调用
    concurrent_time1 = test_concurrent_access(test_function_1, "方式1 (并发)")
    
    # 重置限制器
    reset_limiters()
    
    # 等待一段时间
    print("\n等待6秒...")
    time.sleep(6)
    
    concurrent_time3 = test_concurrent_access(test_function_3, "方式3 (并发)")
    
    # 总结
    print("\n=== 测试总结 ===")
    print(f"方式1 (raise_when_fail=False) 顺序调用耗时: {time1:.2f} 秒")
    print(f"方式2 (raise_when_fail=True + 装饰器) 顺序调用耗时: {time2:.2f} 秒")
    print(f"方式3 (try_acquire + 手动重试) 顺序调用耗时: {time3:.2f} 秒")
    print(f"方式1 并发调用耗时: {concurrent_time1:.2f} 秒")
    print(f"方式3 并发调用耗时: {concurrent_time3:.2f} 秒")
    
    # 分析结果
    print("\n=== 分析 ===")
    print(f"方式1 vs 方式3 顺序调用差异: {abs(time1 - time3):.2f} 秒")
    print(f"方式1 vs 方式3 并发调用差异: {abs(concurrent_time1 - concurrent_time3):.2f} 秒")
    
    if abs(time1 - time3) < 1.0:
        print("✅ 方式1和方式3在顺序调用时表现相似，都能实现阻塞")
    else:
        print(f"⚠️  方式1和方式3在顺序调用时表现不同")
    
    if abs(concurrent_time1 - concurrent_time3) < 1.0:
        print("✅ 方式1和方式3在并发调用时表现相似，都能实现全局阻塞")
    else:
        print(f"⚠️  方式1和方式3在并发调用时表现不同")


if __name__ == "__main__":
    main()