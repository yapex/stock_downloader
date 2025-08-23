#!/usr/bin/env python3
"""测试进度条显示问题

用于验证母子进度条的显示效果，特别是执行速度过快导致的显示问题。
"""

import asyncio
import time
from typing import List
from neo.tqmd import ProgressManager, ProgressTrackerFactory


def demo_current_issue():
    """演示当前的问题：子进度条total=1，一闪而过"""
    print("\n=== 当前问题演示：子进度条一闪而过 ===")
    
    factory = ProgressTrackerFactory()
    manager = ProgressManager(factory)
    
    # 模拟当前AppService的逻辑
    tasks = ["600519_stock_daily", "600519_stock_adj_qfq", "600519_daily_basic", 
             "002023_stock_daily", "002023_stock_adj_qfq", "002023_daily_basic"]
    
    # 启动母进度条
    manager.start_group_progress(len(tasks), "处理下载任务组")
    
    for i, task in enumerate(tasks):
        # 每个任务启动一个total=1的子进度条（这是问题所在）
        manager.start_task_progress(1, f"处理 {task}")
        
        # 模拟任务执行（很快完成）
        time.sleep(0.1)  # 实际任务可能更快
        
        # 立即完成子进度条
        manager.update_task_progress(1)
        manager.finish_task_progress()
        
        # 更新母进度条
        manager.update_group_progress(1, f"已完成 {i+1}/{len(tasks)} 个任务")
        
        time.sleep(0.2)  # 稍微延迟以便观察
    
    manager.finish_group_progress()
    print("当前问题演示完成！")


def demo_improved_solution():
    """演示改进方案：子进度条显示更多细节"""
    print("\n=== 改进方案演示：子进度条显示更多步骤 ===")
    
    factory = ProgressTrackerFactory()
    manager = ProgressManager(factory)
    
    # 模拟任务
    tasks = ["600519_stock_daily", "600519_stock_adj_qfq", "600519_daily_basic", 
             "002023_stock_daily", "002023_stock_adj_qfq", "002023_daily_basic"]
    
    # 启动母进度条
    manager.start_group_progress(len(tasks), "处理下载任务组")
    
    for i, task in enumerate(tasks):
        # 为每个任务设置更多步骤的子进度条
        steps = 5  # 模拟：准备->请求->下载->处理->保存
        manager.start_task_progress(steps, f"处理 {task}")
        
        # 模拟任务的多个步骤
        step_names = ["准备参数", "发送请求", "下载数据", "处理数据", "保存数据"]
        for step_idx, step_name in enumerate(step_names):
            time.sleep(0.3)  # 模拟每个步骤的执行时间
            manager.update_task_progress(1, f"{step_name}")
        
        # 完成当前任务的子进度条
        manager.finish_task_progress()
        
        # 更新母进度条
        manager.update_group_progress(1, f"已完成 {i+1}/{len(tasks)} 个任务")
    
    manager.finish_group_progress()
    print("改进方案演示完成！")


def demo_alternative_solution():
    """演示替代方案：不使用子进度条，只用母进度条"""
    print("\n=== 替代方案演示：只使用母进度条 ===")
    
    factory = ProgressTrackerFactory()
    manager = ProgressManager(factory)
    
    # 模拟任务
    tasks = ["600519_stock_daily", "600519_stock_adj_qfq", "600519_daily_basic", 
             "002023_stock_daily", "002023_stock_adj_qfq", "002023_daily_basic"]
    
    # 只启动母进度条，不使用子进度条
    manager.start_group_progress(len(tasks), "处理下载任务组")
    
    for i, task in enumerate(tasks):
        # 直接更新母进度条，显示当前任务信息
        manager.update_group_progress(0, f"正在处理 {task}")
        
        # 模拟任务执行
        time.sleep(0.5)
        
        # 完成任务，更新母进度条
        manager.update_group_progress(1, f"已完成 {task}")
    
    manager.finish_group_progress()
    print("替代方案演示完成！")


if __name__ == "__main__":
    print("🔍 进度条显示问题分析")
    
    # 演示当前问题
    demo_current_issue()
    
    # 演示改进方案
    demo_improved_solution()
    
    # 演示替代方案
    demo_alternative_solution()
    
    print("\n📝 总结：")
    print("1. 当前问题：子进度条total=1，执行太快，一闪而过")
    print("2. 改进方案：为每个任务设置多步骤的子进度条")
    print("3. 替代方案：只使用母进度条，显示当前任务状态")
    print("4. 建议：根据任务的实际复杂度选择合适的方案")