# -*- coding: utf-8 -*-
"""
实时进度显示管理器，使用 tqdm 监听任务进度。
"""

import threading
from typing import Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass, field
from tqdm import tqdm
from .models import TaskType
from .utils import get_logger

logger = get_logger(__name__)


class ProgressManager:
    """进度管理器，用于实时显示下载进度"""
    
    def __init__(self):
        self.total_tasks: int = 0
        self.completed_tasks: int = 0
        self.failed_tasks: int = 0
        self.current_task: str = ""
        
        self._pbar: Optional[tqdm] = None
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
    
    def initialize(self, total_tasks: int, description: str = "处理任务"):
        """初始化进度条"""
        with self._lock:
            self.total_tasks = total_tasks
            self.completed_tasks = 0
            self.failed_tasks = 0
            
            if self._pbar:
                self._pbar.close()
            
            self._pbar = tqdm(
                total=total_tasks,
                desc=description,
                unit="task",
                position=0,
                leave=True,
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
            )
    
    def update_progress(self, completed: int = None, failed: int = None, current_task: str = None):
        """更新进度"""
        with self._lock:
            if completed is not None:
                self.completed_tasks = completed
            if failed is not None:
                self.failed_tasks = failed
            if current_task is not None:
                self.current_task = current_task
            
            if self._pbar:
                # 更新进度条
                progress = self.completed_tasks + self.failed_tasks
                self._pbar.n = progress
                
                # 更新描述
                if self.failed_tasks > 0:
                    desc = f"处理任务 (失败: {self.failed_tasks})"
                else:
                    desc = "处理任务"
                
                if current_task:
                    desc += f" [{current_task}]"
                
                self._pbar.set_description(desc)
                self._pbar.refresh()
    
    def increment(self, success: bool = True, current_task: str = None):
        """增量更新进度"""
        with self._lock:
            if success:
                self.completed_tasks += 1
            else:
                self.failed_tasks += 1
            
            if current_task is not None:
                self.current_task = current_task
            
            if self._pbar:
                progress = self.completed_tasks + self.failed_tasks
                self._pbar.n = progress
                
                # 更新描述
                if self.failed_tasks > 0:
                    desc = f"处理任务 (失败: {self.failed_tasks})"
                else:
                    desc = "处理任务"
                
                if current_task:
                    desc += f" [{current_task}]"
                
                self._pbar.set_description(desc)
                self._pbar.refresh()
    
    def finish(self):
        """完成进度显示"""
        with self._lock:
            if self._pbar:
                self._pbar.n = self._pbar.total
                
                # 最终描述
                if self.failed_tasks > 0:
                    final_desc = f"完成 (成功: {self.completed_tasks}, 失败: {self.failed_tasks})"
                else:
                    final_desc = f"完成 (成功: {self.completed_tasks})"
                
                self._pbar.set_description(final_desc)
                self._pbar.refresh()
                self._pbar.close()
                self._pbar = None
    
    def get_stats(self) -> Dict[str, Any]:
        """获取当前统计信息"""
        with self._lock:
            return {
                'total_tasks': self.total_tasks,
                'completed_tasks': self.completed_tasks,
                'failed_tasks': self.failed_tasks,
                'current_task': self.current_task,
                'success_rate': (self.completed_tasks / max(1, self.total_tasks)) * 100
            }
    
    def print_warning(self, message: str):
        """打印警告信息（不影响进度条）"""
        if self._pbar:
            tqdm.write(f"⚠️  {message}")
        else:
            print(f"⚠️  {message}")
    
    def print_error(self, message: str):
        """打印错误信息（不影响进度条）"""
        if self._pbar:
            tqdm.write(f"❌ {message}")
        else:
            print(f"❌ {message}")
    
    def print_info(self, message: str):
        """打印信息（不影响进度条）"""
        if self._pbar:
            tqdm.write(f"ℹ️  {message}")
        else:
            print(f"ℹ️  {message}")


# 全局进度管理器实例
progress_manager = ProgressManager()
