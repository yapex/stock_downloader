"""任务调度器

基于优先级的任务调度机制，支持不同任务类型的优先级设置。
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Iterator
from queue import PriorityQueue
import threading
from enum import Enum

from downloader.task.types import DownloadTaskConfig, TaskPriority
from downloader.producer.fetcher_builder import TaskType


@dataclass
class PriorityTaskItem:
    """优先级任务项
    
    用于在优先级队列中排序任务。
    """
    priority: int  # 数值越小优先级越高
    sequence: int  # 同优先级时的序列号，确保 FIFO
    config: DownloadTaskConfig
    
    def __lt__(self, other):
        """定义排序规则：优先级优先，然后按序列号"""
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.sequence < other.sequence


class TaskTypeConfig:
    """任务类型配置
    
    定义不同任务类型的默认优先级。
    """
    
    # 默认任务类型优先级映射
    DEFAULT_PRIORITIES: Dict[TaskType, TaskPriority] = {
        TaskType.STOCK_BASIC: TaskPriority.HIGH,      # 基础数据优先级最高
        TaskType.STOCK_DAILY: TaskPriority.MEDIUM,    # 日线数据中等优先级
        TaskType.DAILY_BAR_QFQ: TaskPriority.MEDIUM,  # 复权数据中等优先级
        TaskType.DAILY_BAR_NONE: TaskPriority.MEDIUM, # 不复权数据中等优先级
        TaskType.BALANCESHEET: TaskPriority.LOW,      # 财务数据优先级较低
        TaskType.INCOME: TaskPriority.LOW,            # 利润表优先级较低
        TaskType.CASHFLOW: TaskPriority.LOW,          # 现金流量表优先级较低
    }
    
    def __init__(self, custom_priorities: Optional[Dict[TaskType, TaskPriority]] = None):
        """初始化任务类型配置
        
        Args:
            custom_priorities: 自定义优先级映射，会覆盖默认配置
        """
        self.priorities = self.DEFAULT_PRIORITIES.copy()
        if custom_priorities:
            self.priorities.update(custom_priorities)
    
    def get_priority(self, task_type: TaskType) -> TaskPriority:
        """获取任务类型的优先级"""
        return self.priorities.get(task_type, TaskPriority.MEDIUM)
    
    def set_priority(self, task_type: TaskType, priority: TaskPriority) -> None:
        """设置任务类型的优先级"""
        self.priorities[task_type] = priority


class TaskScheduler:
    """任务调度器
    
    基于优先级的任务调度器，支持动态添加任务和按优先级获取任务。
    """
    
    def __init__(self, task_type_config: Optional[TaskTypeConfig] = None):
        """初始化任务调度器
        
        Args:
            task_type_config: 任务类型配置，如果为 None 则使用默认配置
        """
        self.task_type_config = task_type_config or TaskTypeConfig()
        self._queue = PriorityQueue()
        self._sequence_counter = 0
        self._lock = threading.Lock()
        self._task_count = 0
    
    def add_task(self, config: DownloadTaskConfig) -> None:
        """添加任务到调度器
        
        Args:
            config: 下载任务配置
        """
        with self._lock:
            # 如果任务配置中的优先级是默认值，使用任务类型的默认优先级
            if config.priority == TaskPriority.MEDIUM:  # 默认值
                effective_priority = self.task_type_config.get_priority(config.task_type)
                # 创建新的配置对象，使用有效的优先级
                effective_config = DownloadTaskConfig(
                    symbol=config.symbol,
                    task_type=config.task_type,
                    priority=effective_priority,
                    max_retries=config.max_retries
                )
            else:
                effective_priority = config.priority
                effective_config = config
            
            # 创建优先级任务项
            priority_item = PriorityTaskItem(
                priority=effective_priority.value,
                sequence=self._sequence_counter,
                config=effective_config
            )
            
            self._queue.put(priority_item)
            self._sequence_counter += 1
            self._task_count += 1
    
    def add_tasks(self, configs: List[DownloadTaskConfig]) -> None:
        """批量添加任务
        
        Args:
            configs: 下载任务配置列表
        """
        for config in configs:
            self.add_task(config)
    
    def get_next_task(self, timeout: Optional[float] = None) -> Optional[DownloadTaskConfig]:
        """获取下一个任务
        
        Args:
            timeout: 超时时间（秒），None 表示阻塞等待
            
        Returns:
            下一个任务配置，如果队列为空且超时则返回 None
        """
        try:
            if timeout is None:
                priority_item = self._queue.get()
            else:
                priority_item = self._queue.get(timeout=timeout)
            
            with self._lock:
                self._task_count -= 1
            
            return priority_item.config
        except:
            return None
    
    def get_all_tasks(self) -> List[DownloadTaskConfig]:
        """获取所有剩余任务（非阻塞）
        
        Returns:
            所有剩余任务的配置列表
        """
        tasks = []
        while not self._queue.empty():
            try:
                priority_item = self._queue.get_nowait()
                tasks.append(priority_item.config)
            except:
                break
        
        with self._lock:
            self._task_count = 0
        
        return tasks
    
    def is_empty(self) -> bool:
        """检查调度器是否为空"""
        return self._queue.empty()
    
    def size(self) -> int:
        """获取队列中任务数量"""
        with self._lock:
            return self._task_count
    
    def clear(self) -> None:
        """清空所有任务"""
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except:
                break
        
        with self._lock:
            self._task_count = 0
            self._sequence_counter = 0


def create_task_configs(
    symbols: List[str], 
    task_type: TaskType, 
    priority: Optional[TaskPriority] = None,
    max_retries: int = 2
) -> List[DownloadTaskConfig]:
    """创建任务配置列表的便利函数
    
    Args:
        symbols: 股票代码列表
        task_type: 任务类型
        priority: 优先级，如果为 None 则使用默认优先级
        max_retries: 最大重试次数
        
    Returns:
        任务配置列表
    """
    configs = []
    
    # 特殊处理 STOCK_BASIC 任务
    if task_type == TaskType.STOCK_BASIC:
        config = DownloadTaskConfig(
            symbol="",  # STOCK_BASIC 不需要 symbol
            task_type=task_type,
            priority=priority or TaskPriority.MEDIUM,
            max_retries=max_retries
        )
        configs.append(config)
    else:
        # 其他任务类型为每个 symbol 创建一个任务
        for symbol in symbols:
            config = DownloadTaskConfig(
                symbol=symbol,
                task_type=task_type,
                priority=priority or TaskPriority.MEDIUM,
                max_retries=max_retries
            )
            configs.append(config)
    
    return configs