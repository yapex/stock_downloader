"""接口定义模块

定义项目中使用的各种接口协议。
"""

from abc import ABC, abstractmethod

from pyrate_limiter import Limiter

from neo.task_bus.types import TaskType


class IRateLimitManager(ABC):
    """速率限制管理器接口
    
    负责管理pyrate-limiter实例的生命周期，包括创建、获取和销毁限制器。
    """
    
    @abstractmethod
    def get_limiter(self, task_type: TaskType) -> Limiter:
        """获取指定任务类型的速率限制器
        
        Args:
            task_type: 任务类型枚举
            
        Returns:
            Limiter: 对应的速率限制器实例
        """
        pass
    
    @abstractmethod
    def apply_rate_limiting(self, task_type: TaskType) -> None:
        """对指定任务类型应用速率限制
        
        Args:
            task_type: 任务类型枚举
        """
        pass
    
    @abstractmethod
    def cleanup(self) -> None:
        """清理所有速率限制器资源
        
        释放所有创建的限制器和相关资源。
        """
        pass
    
    @abstractmethod
    def get_rate_limit_config(self, task_type: TaskType) -> int:
        """获取指定任务类型的速率限制配置
        
        Args:
            task_type: 任务类型枚举
            
        Returns:
            int: 每分钟允许的请求数
        """
        pass