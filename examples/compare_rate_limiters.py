#!/usr/bin/env python3
"""
对比原生装饰器和自定义实现的速率限制器
"""

import time
import logging
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.downloader.rate_limiter import rate_limit as custom_rate_limit
from src.rate_limiter_native import rate_limit as native_rate_limit

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_custom_implementation():
    """测试自定义实现"""
    print("\n=== 测试自定义实现（阻塞等待策略） ===")
    
    @custom_rate_limit("custom_test")
    def custom_fetch_data():
        current_time = time.time()
        print(f"Custom implementation called at {current_time}")
        return "custom data"
    
    start_time = time.time()
    
    # 快速连续调用 5 次
    for i in range(5):
        result = custom_fetch_data()
        print(f"Call {i+1}: {result}")
    
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"Custom implementation total time: {elapsed:.2f} seconds")


def test_native_implementation():
    """测试原生实现"""
    print("\n=== 测试原生实现（内置阻塞策略） ===")
    
    @native_rate_limit("native_test")
    def native_fetch_data():
        current_time = time.time()
        print(f"Native implementation called at {current_time}")
        return "native data"
    
    start_time = time.time()
    
    # 快速连续调用 5 次
    for i in range(5):
        result = native_fetch_data()
        print(f"Call {i+1}: {result}")
    
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"Native implementation total time: {elapsed:.2f} seconds")


def test_independent_limits():
    """测试独立限制"""
    print("\n=== 测试不同方法的独立限制 ===")
    
    @native_rate_limit("method_a")
    def method_a():
        print(f"Method A called at {time.time()}")
        return "A"
    
    @native_rate_limit("method_b")
    def method_b():
        print(f"Method B called at {time.time()}")
        return "B"
    
    # 交替调用两个方法
    for i in range(3):
        result_a = method_a()
        result_b = method_b()
        print(f"Round {i+1}: A={result_a}, B={result_b}")


def main():
    """主函数"""
    print("开始对比测试...")
    
    # 测试自定义实现
    test_custom_implementation()
    
    # 等待一段时间，避免限制器状态影响
    time.sleep(1)
    
    # 测试原生实现
    test_native_implementation()
    
    # 测试独立限制
    test_independent_limits()
    
    print("\n=== 总结 ===")
    print("1. 自定义实现：使用 while 循环 + try-catch 实现阻塞等待")
    print("2. 原生实现：使用 pyrate-limiter 内置的阻塞机制")
    print("3. 两种实现都支持独立的方法限制")
    print("4. 原生实现更简洁，减少了重复造轮子")


if __name__ == "__main__":
    main()