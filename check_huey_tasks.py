#!/usr/bin/env python3
"""检查Huey任务状态"""

from neo.configs import huey
import logging

logging.basicConfig(level=logging.INFO)

def check_tasks():
    """检查Huey任务状态"""
    print("🔍 检查Huey任务状态...")
    
    # 检查pending任务
    pending = huey.pending_count()
    print(f"待处理任务数: {pending}")
    
    # 检查结果存储
    storage = huey.storage
    print(f"存储后端: {type(storage)}")
    
    # 如果是内存存储，可能没有持久化
    print("注意：如果使用内存存储，任务结果在进程结束后会丢失")

if __name__ == "__main__":
    check_tasks()
