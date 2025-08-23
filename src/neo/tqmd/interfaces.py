"""进度跟踪接口定义

定义进度跟踪相关的接口规范。
"""

from typing import Optional, Protocol


class IProgressTracker(Protocol):
    """进度跟踪器接口
    
    提供统一的进度跟踪能力，支持任务的开始、更新和完成。
    """
    
    def start_tracking(self, total: int, description: str = "") -> None:
        """开始跟踪进度
        
        Args:
            total: 总任务数量
            description: 进度条描述
        """
        ...
    
    def update_progress(self, increment: int = 1, description: Optional[str] = None) -> None:
        """更新进度
        
        Args:
            increment: 增量，默认为1
            description: 可选的描述更新
        """
        ...
    
    def finish_tracking(self) -> None:
        """完成进度跟踪
        
        清理资源，关闭进度条显示。
        """
        ...


class IProgressTrackerFactory(Protocol):
    """进度跟踪器工厂接口
    
    根据不同的任务类型创建合适的进度跟踪器。
    """
    
    def create_tracker(self, task_type: str, is_nested: bool = False) -> IProgressTracker:
        """创建进度跟踪器
        
        Args:
            task_type: 任务类型（如'single', 'group'等）
            is_nested: 是否为嵌套进度条（子进度条）
            
        Returns:
            IProgressTracker: 进度跟踪器实例
        """
        ...