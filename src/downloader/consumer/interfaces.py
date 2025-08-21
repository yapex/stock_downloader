from typing import Protocol, List
import pandas as pd
from downloader.task.types import TaskType
from downloader.task.types import TaskResult


class IBatchSaver(Protocol):
    """批量保存器接口"""
    
    def save_batch(self, task_type: TaskType, data: pd.DataFrame) -> bool:
        """批量保存数据
        
        Args:
            task_type: 任务类型
            data: 要保存的数据
            
        Returns:
            bool: 保存是否成功
        """
        ...


class IDataProcessor(Protocol):
    """数据处理器接口"""
    
    def process_task_result(self, task_result: TaskResult) -> None:
        """处理单个任务结果
        
        Args:
            task_result: 任务执行结果
        """
        ...
    
    def flush_pending_data(self) -> None:
        """刷新待处理数据，强制保存所有缓存的数据"""
        ...


class IDataProcessorManager(Protocol):
    """数据处理管理器接口"""
    
    def start(self) -> None:
        """启动消费进程"""
        ...
    
    def stop(self) -> None:
        """停止消费进程"""
        ...
    
    def get_status(self) -> dict:
        """获取消费状态"""
        ...