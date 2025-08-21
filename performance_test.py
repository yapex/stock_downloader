#!/usr/bin/env python3
"""性能测试脚本

用于测试 fetcher_builder.py 中的性能监控功能
"""

import sys
import os
from pathlib import Path

# 添加 src 目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from downloader.producer.fetcher_builder import TaskType, FetcherBuilder, TaskTypeRegistry

def test_performance_monitoring():
    """测试性能监控功能"""
    print("开始性能测试...")
    
    # 测试任务类型注册表初始化
    print("1. 测试任务类型注册表初始化")
    registry = TaskTypeRegistry.get_instance()
    
    # 测试枚举生成
    print("2. 测试枚举生成")
    task_type_enum = registry.get_task_type_enum()
    
    # 测试构建器初始化
    print("3. 测试构建器初始化")
    builder = FetcherBuilder()
    
    # 测试任务构建（不执行实际 API 调用）
    print("4. 测试任务构建")
    try:
        task_type = TaskType.STOCK_BASIC
        fetcher = builder.build_by_task(task_type, "600519")
        print(f"成功构建任务: {task_type}")
    except Exception as e:
        print(f"构建任务时出错: {e}")
    
    print("性能测试完成！")
    print("请检查 logs/performance/ 目录下的日志文件")

if __name__ == "__main__":
    test_performance_monitoring()