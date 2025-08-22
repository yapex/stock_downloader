#!/usr/bin/env python3
"""测试 process_data_task 功能

验证数据处理任务是否能正确处理下载结果。
"""

import sys
import os
from pathlib import Path
import pandas as pd

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

from neo.tasks.huey_tasks import process_data_task
from neo.task_bus.types import TaskType


def test_process_data_task():
    """测试数据处理任务"""
    # 创建测试数据
    test_data = pd.DataFrame({
        'ts_code': ['000001.SZ'],
        'trade_date': ['20240101'],
        'open': [10.0],
        'high': [11.0],
        'low': [9.5],
        'close': [10.5],
        'vol': [1000000]
    })
    
    # 调用数据处理任务
    result = process_data_task.call_local('stock_daily', test_data)
    
    print(f"数据处理结果: {result}")
    assert result is True, "数据处理应该成功"





if __name__ == "__main__":
    test_process_data_task()