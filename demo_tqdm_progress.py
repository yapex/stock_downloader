#!/usr/bin/env python3
"""tqdm进度条演示脚本

演示单任务和组任务的进度条效果，验证母子进度条的嵌套显示。
"""

import time
import sys
import os
from typing import Optional
from tqdm import tqdm

# 直接实现演示所需的类，避免复杂的包导入
class IProgressTracker:
    """进度跟踪器接口"""
    def start_tracking(self, total: int, description: str = "") -> None:
        pass
    
    def update_progress(self, increment: int = 1, description: Optional[str] = None) -> None:
        pass
    
    def finish_tracking(self) -> None:
        pass


class TqdmProgressTracker(IProgressTracker):
    """基于tqdm的进度跟踪器"""
    
    def __init__(self, is_nested: bool = False):
        self._is_nested = is_nested
        self._pbar: Optional[tqdm] = None
    
    def start_tracking(self, total: int, description: str = "") -> None:
        """开始跟踪进度"""
        position = 1 if self._is_nested else 0
        leave = not self._is_nested  # 母进度条保留，子进度条清除
        
        self._pbar = tqdm(
            total=total,
            desc=description,
            position=position,
            leave=leave,
            ncols=80
        )
    
    def update_progress(self, increment: int = 1, description: Optional[str] = None) -> None:
        """更新进度"""
        if self._pbar is None:
            return
            
        self._pbar.update(increment)
        if description is not None:
            self._pbar.set_description(description)
    
    def finish_tracking(self) -> None:
        """完成进度跟踪"""
        if self._pbar is not None:
            self._pbar.close()
            self._pbar = None


class ProgressTrackerFactory:
    """进度跟踪器工厂"""
    
    def create_tracker(self, task_type: str, is_nested: bool = False) -> IProgressTracker:
        return TqdmProgressTracker(is_nested=is_nested)


class ProgressManager:
    """进度管理器"""
    
    def __init__(self, factory: ProgressTrackerFactory):
        self._factory = factory
        self._main_tracker: Optional[IProgressTracker] = None
        self._sub_tracker: Optional[IProgressTracker] = None
    
    def start_group_progress(self, total_groups: int, description: str = "处理组任务") -> None:
        """开始组级别进度跟踪（母进度条）"""
        self._main_tracker = self._factory.create_tracker("group", is_nested=False)
        self._main_tracker.start_tracking(total_groups, description)
    
    def start_task_progress(self, total_tasks: int, description: str = "处理任务") -> None:
        """开始任务级别进度跟踪（子进度条）"""
        is_nested = self._main_tracker is not None
        self._sub_tracker = self._factory.create_tracker("task", is_nested=is_nested)
        self._sub_tracker.start_tracking(total_tasks, description)
    
    def update_group_progress(self, increment: int = 1, description: Optional[str] = None) -> None:
        """更新组级别进度"""
        if self._main_tracker is not None:
            self._main_tracker.update_progress(increment, description)
    
    def update_task_progress(self, increment: int = 1, description: Optional[str] = None) -> None:
        """更新任务级别进度"""
        if self._sub_tracker is not None:
            self._sub_tracker.update_progress(increment, description)
    
    def finish_task_progress(self) -> None:
        """完成任务级别进度跟踪"""
        if self._sub_tracker is not None:
            self._sub_tracker.finish_tracking()
            self._sub_tracker = None
    
    def finish_group_progress(self) -> None:
        """完成组级别进度跟踪"""
        if self._main_tracker is not None:
            self._main_tracker.finish_tracking()
            self._main_tracker = None
    
    def finish_all(self) -> None:
        """完成所有进度跟踪"""
        self.finish_task_progress()
        self.finish_group_progress()


def demo_single_task_progress():
    """演示单任务进度条"""
    print("\n=== 单任务进度条演示 ===")
    
    factory = ProgressTrackerFactory()
    manager = ProgressManager(factory)
    
    # 模拟单个任务的进度
    total_steps = 10
    manager.start_task_progress(total_steps, "下载股票数据")
    
    for i in range(total_steps):
        time.sleep(0.3)  # 模拟工作
        manager.update_task_progress(1, f"处理第 {i+1} 步")
    
    manager.finish_task_progress()
    print("单任务完成！")


def demo_group_task_progress():
    """演示组任务进度条（母子进度条）"""
    print("\n=== 组任务进度条演示 ===")
    
    factory = ProgressTrackerFactory()
    manager = ProgressManager(factory)
    
    # 模拟3个组，每组5个任务
    total_groups = 3
    tasks_per_group = 5
    
    manager.start_group_progress(total_groups, "处理股票组")
    
    for group_idx in range(total_groups):
        # 开始当前组的任务进度
        manager.start_task_progress(tasks_per_group, f"组 {group_idx+1} 任务")
        
        for task_idx in range(tasks_per_group):
            time.sleep(0.2)  # 模拟任务执行
            manager.update_task_progress(1, f"任务 {task_idx+1}")
        
        # 完成当前组的任务
        manager.finish_task_progress()
        
        # 更新组进度
        manager.update_group_progress(1, f"完成组 {group_idx+1}")
        
        time.sleep(0.5)  # 组间间隔
    
    manager.finish_group_progress()
    print("组任务完成！")


def demo_realistic_download_scenario():
    """演示真实下载场景"""
    print("\n=== 真实下载场景演示 ===")
    
    factory = ProgressTrackerFactory()
    manager = ProgressManager(factory)
    
    # 模拟下载多只股票的多种数据类型
    stocks = ["000001", "000002", "600000"]
    data_types = ["daily", "weekly", "monthly"]
    
    manager.start_group_progress(len(stocks), "下载股票数据")
    
    for stock_idx, stock in enumerate(stocks):
        manager.start_task_progress(len(data_types), f"股票 {stock}")
        
        for data_idx, data_type in enumerate(data_types):
            time.sleep(0.4)  # 模拟网络请求和数据处理
            manager.update_task_progress(1, f"{data_type} 数据")
        
        manager.finish_task_progress()
        manager.update_group_progress(1, f"完成 {stock}")
    
    manager.finish_group_progress()
    print("下载完成！")


def demo_error_handling():
    """演示错误处理场景"""
    print("\n=== 错误处理演示 ===")
    
    factory = ProgressTrackerFactory()
    manager = ProgressManager(factory)
    
    try:
        manager.start_group_progress(2, "处理任务组")
        
        # 第一组正常完成
        manager.start_task_progress(3, "组 1")
        for i in range(3):
            time.sleep(0.2)
            manager.update_task_progress(1, f"任务 {i+1}")
        manager.finish_task_progress()
        manager.update_group_progress(1, "组 1 完成")
        
        # 第二组模拟异常
        manager.start_task_progress(3, "组 2")
        for i in range(2):  # 只完成2个任务
            time.sleep(0.2)
            manager.update_task_progress(1, f"任务 {i+1}")
        
        # 模拟异常发生
        raise Exception("模拟网络错误")
        
    except Exception as e:
        print(f"发生错误: {e}")
        print("清理进度条...")
        manager.finish_all()  # 确保所有进度条都被清理
    
    print("错误处理完成！")


def main():
    """主函数"""
    print("tqdm进度条集成演示")
    print("=" * 50)
    
    # 演示各种场景
    demo_single_task_progress()
    time.sleep(1)
    
    demo_group_task_progress()
    time.sleep(1)
    
    demo_realistic_download_scenario()
    time.sleep(1)
    
    demo_error_handling()
    
    print("\n所有演示完成！")


if __name__ == "__main__":
    main()