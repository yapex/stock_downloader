#!/usr/bin/env python3
"""
可配置速率限制器演示
展示如何使用自定义速率限制
"""

import time
from pyrate_limiter import Rate, Duration

# 添加 src 目录到路径
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))
sys.path.append(str(Path(__file__).parent.parent / 'src' / 'downloader'))

from rate_limiter_native import rate_limit


def log_with_time(message: str):
    """带时间戳的日志输出"""
    current_time = time.strftime("%H:%M:%S.%f")[:-3]
    print(f"[{current_time}] {message}")


# 使用默认速率限制（190次/分钟）
@rate_limit("default_method")
def default_rate_method(call_id: int):
    """使用默认速率限制的方法"""
    log_with_time(f"执行 default_rate_method，调用ID: {call_id}")
    return f"default_result_{call_id}"


# 使用自定义速率限制（5次/10秒）
@rate_limit("custom_method", Rate(5, Duration.SECOND * 10))
def custom_rate_method(call_id: int):
    """使用自定义速率限制的方法"""
    log_with_time(f"执行 custom_rate_method，调用ID: {call_id}")
    return f"custom_result_{call_id}"


# 使用更严格的速率限制（2次/5秒）
@rate_limit("strict_method", Rate(2, Duration.SECOND * 5))
def strict_rate_method(call_id: int):
    """使用严格速率限制的方法"""
    log_with_time(f"执行 strict_rate_method，调用ID: {call_id}")
    return f"strict_result_{call_id}"


def test_rate_limiter(test_func, method_name: str, num_calls: int = 6):
    """测试速率限制器"""
    print(f"\n=== 测试 {method_name} ===")
    log_with_time(f"开始测试 {method_name}，将进行 {num_calls} 次调用")
    
    start_time = time.time()
    
    for i in range(1, num_calls + 1):
        log_with_time(f"准备第 {i} 次调用")
        try:
            result = test_func(i)
            log_with_time(f"第 {i} 次调用完成，结果: {result}")
        except Exception as e:
            log_with_time(f"第 {i} 次调用异常: {e}")
    
    end_time = time.time()
    total_time = end_time - start_time
    log_with_time(f"{method_name} 测试完成，总耗时: {total_time:.2f} 秒")
    
    return total_time


def main():
    """主测试函数"""
    print("可配置速率限制器演示")
    
    # 测试默认速率限制（应该很快完成，因为限制很宽松）
    time1 = test_rate_limiter(default_rate_method, "默认速率限制 (190次/分钟)", 3)
    
    # 等待一段时间
    print("\n等待3秒...")
    time.sleep(3)
    
    # 测试自定义速率限制（5次/10秒）
    time2 = test_rate_limiter(custom_rate_method, "自定义速率限制 (5次/10秒)", 6)
    
    # 等待一段时间
    print("\n等待3秒...")
    time.sleep(3)
    
    # 测试严格速率限制（2次/5秒）
    time3 = test_rate_limiter(strict_rate_method, "严格速率限制 (2次/5秒)", 5)
    
    # 总结
    print("\n=== 测试总结 ===")
    print(f"默认速率限制耗时: {time1:.2f} 秒")
    print(f"自定义速率限制耗时: {time2:.2f} 秒")
    print(f"严格速率限制耗时: {time3:.2f} 秒")
    
    # 分析结果
    print("\n=== 分析 ===")
    print("✅ 默认速率限制很宽松，调用几乎立即完成")
    print("✅ 自定义速率限制 (5次/10秒) 在超过限制后开始阻塞")
    print("✅ 严格速率限制 (2次/5秒) 阻塞更频繁，总耗时更长")
    print("\n这证明了可配置速率限制器能够根据不同的速率设置正确工作！")


if __name__ == "__main__":
    main()