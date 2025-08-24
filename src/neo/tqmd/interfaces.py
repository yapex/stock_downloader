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

    def update_progress(
        self, increment: int = 1, description: Optional[str] = None
    ) -> None:
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

    def create_tracker(
        self, task_type: str, is_nested: bool = False
    ) -> IProgressTracker:
        """创建进度跟踪器

        Args:
            task_type: 任务类型（如'single', 'group'等）
            is_nested: 是否为嵌套进度条（子进度条）

        Returns:
            IProgressTracker: 进度跟踪器实例
        """
        ...


class ITasksProgressTracker(Protocol):
    """任务进度跟踪器接口

    管理母子进度条的生命周期，支持按任务类型分组的进度显示。
    """

    def start_group_progress(
        self, total_tasks: int, description: str = "处理下载任务"
    ) -> None:
        """开始组级别进度跟踪（母进度条）

        Args:
            total_tasks: 总任务数
            description: 进度条描述
        """
        ...

    def start_task_type_progress(self, task_type: str, total_symbols: int) -> None:
        """开始任务类型级别进度跟踪（子进度条）

        Args:
            task_type: 任务类型名称
            total_symbols: 该任务类型需要处理的股票数量
        """
        ...

    def start_task_progress(
        self, total_tasks: int, description: str = "处理任务"
    ) -> None:
        """开始任务级别进度跟踪（子进度条）- 兼容性方法

        Args:
            total_tasks: 总任务数
            description: 进度条描述
        """
        ...

    def update_group_progress(
        self, increment: int = 1, description: Optional[str] = None
    ) -> None:
        """更新组级别进度

        Args:
            increment: 增量
            description: 可选的描述更新
        """
        ...

    def update_task_type_progress(
        self,
        task_type: str,
        increment: int = 1,
        completed: int = None,
        total: int = None,
    ) -> None:
        """更新任务类型级别进度

        Args:
            task_type: 任务类型名称
            increment: 增量
            completed: 已完成数量（用于更新描述）
            total: 总数量（用于更新描述）
        """
        ...

    def update_task_progress(
        self, increment: int = 1, description: Optional[str] = None
    ) -> None:
        """更新任务级别进度 - 兼容性方法

        Args:
            increment: 增量
            description: 可选的描述更新
        """
        ...

    def finish_task_type_progress(self, task_type: str) -> None:
        """完成任务类型级别进度跟踪

        Args:
            task_type: 任务类型名称
        """
        ...

    def finish_task_progress(self) -> None:
        """完成任务级别进度跟踪 - 兼容性方法"""
        ...

    def finish_group_progress(self) -> None:
        """完成组级别进度跟踪"""
        ...

    def finish_all(self) -> None:
        """完成所有进度跟踪"""
        ...
