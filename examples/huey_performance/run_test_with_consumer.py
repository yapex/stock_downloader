#!/usr/bin/env python3
"""
运行Huey性能测试的启动脚本
同时启动Huey消费者和执行性能测试
"""

import subprocess
import time
import signal
import sys
import os
from pathlib import Path

def run_huey_consumer():
    """启动Huey消费者进程"""
    cmd = [
        "uv", "run", "python", "-m", "huey.bin.huey_consumer", 
        "huey_config.huey", "-w", "4", "-k", "process"
    ]
    return subprocess.Popen(cmd, cwd=Path(__file__).parent)

def run_performance_test():
    """运行性能测试"""
    cmd = ["uv", "run", "python", "huey_performance_test.py"]
    return subprocess.run(cmd, cwd=Path(__file__).parent)

def main():
    print("🚀 启动Huey性能测试...")
    
    # 启动Huey消费者
    print("📡 启动Huey消费者...")
    consumer_process = run_huey_consumer()
    
    try:
        # 等待消费者启动
        print("⏳ 等待消费者启动...")
        time.sleep(3)
        
        # 运行性能测试
        print("🧪 开始性能测试...")
        result = run_performance_test()
        
        print("✅ 测试完成!")
        return result.returncode
        
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断测试")
        return 1
    finally:
        # 清理消费者进程
        print("🧹 清理消费者进程...")
        consumer_process.terminate()
        try:
            consumer_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            consumer_process.kill()
            consumer_process.wait()
        print("✨ 清理完成")

if __name__ == "__main__":
    sys.exit(main())