"""生产者接口定义

定义生产者组件的核心接口，遵循KISS原则。
"""

from typing import Protocol, Optional, runtime_checkable
from queue import Queue

from ..models import DownloadTask, DataBatch



@runtime_checkable
class IProducer(Protocol):
    """生产者接口
    
    负责从任务队列获取任务并处理。
    """
    
    def start(self) -> None:
        """启动生产者
        
        启动后生产者开始从任务队列获取任务并处理。
        """
        ...
    
    def stop(self, timeout: float = 30.0) -> None:
        """停止生产者
        
        Args:
            timeout: 等待停止的超时时间（秒）
        """
        ...
    
    def submit_task(self, task: DownloadTask, timeout: float = 1.0) -> bool:
        """提交任务到队列
        
        Args:
            task: 要提交的任务
            timeout: 提交超时时间（秒）
            
        Returns:
            是否成功提交
            
        Raises:
            RuntimeError: 生产者未运行时提交任务
        """
        ...
    
    @property
    def is_running(self) -> bool:
        """检查是否正在运行"""
        ...