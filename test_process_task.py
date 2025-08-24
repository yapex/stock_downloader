#!/usr/bin/env python3
"""直接测试数据处理任务"""

from neo.tasks.huey_tasks import process_data_task
from neo.task_bus.types import TaskType
import logging

# 配置详细日志
logging.basicConfig(level=logging.DEBUG)

def test_process_task():
    """测试数据处理任务"""
    print("🔍 测试数据处理任务...")
    
    task_type = TaskType.stock_basic
    symbol = "000001.SZ"
    
    print(f"任务类型: {task_type} (type: {type(task_type)})")
    print(f"股票代码: {symbol}")
    
    try:
        # 直接调用数据处理任务
        result = process_data_task(task_type, symbol)
        print(f"处理结果: {result}")
        
    except Exception as e:
        print(f"处理失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_process_task()
