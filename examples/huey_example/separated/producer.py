#!/usr/bin/env python3
"""Producer - 任务生产者"""

from tasks import add, slow_task
import time


def main():
    print("=== Huey Producer 启动 ===")
    
    # 提交简单任务
    print("\n1. 提交简单加法任务")
    result1 = add(10, 20)
    print(f"任务已提交，任务ID: {result1.id}")
    
    # 提交延迟任务
    print("\n2. 提交延迟任务（2秒后执行）")
    result2 = add.schedule(args=(100, 200), delay=2)
    print(f"延迟任务已提交，任务ID: {result2.id}")
    
    # 提交耗时任务
    print("\n3. 提交耗时任务")
    result3 = slow_task(3)
    print(f"耗时任务已提交，任务ID: {result3.id}")
    
    # 等待并获取结果
    print("\n=== 等待任务结果 ===")
    
    print("\n等待简单任务结果...")
    try:
        res1 = result1.get(blocking=True, timeout=10)
        print(f"简单任务结果: {res1}")
    except Exception as e:
        print(f"获取简单任务结果失败: {e}")
    
    print("\n等待延迟任务结果...")
    try:
        res2 = result2.get(blocking=True, timeout=15)
        print(f"延迟任务结果: {res2}")
    except Exception as e:
        print(f"获取延迟任务结果失败: {e}")
    
    print("\n等待耗时任务结果...")
    try:
        res3 = result3.get(blocking=True, timeout=20)
        print(f"耗时任务结果: {res3}")
    except Exception as e:
        print(f"获取耗时任务结果失败: {e}")
    
    print("\n=== Producer 完成 ===")


if __name__ == '__main__':
    main()