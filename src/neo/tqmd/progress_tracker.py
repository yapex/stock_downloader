"""进度管理器实现

基于tqdm的进度跟踪实现，支持单任务和组任务的母子进度条。
"""

from typing import Optional

from tqdm import tqdm

from .interfaces import IProgressTracker, IProgressTrackerFactory


class TqdmProgressTracker:
    """基于tqdm的进度跟踪器

    实现IProgressTracker接口，提供tqdm进度条功能。
    """

    # 类变量：跟踪已分配的位置
    _next_position = 1

    def __init__(self, is_nested: bool = False):
        """初始化进度跟踪器

        Args:
            is_nested: 是否为嵌套进度条（子进度条）
        """
        self._is_nested = is_nested
        self._pbar: Optional[tqdm] = None
        self._position = 0  # 默认位置

    def start_tracking(self, total: int, description: str = "") -> None:
        """开始跟踪进度"""
        if self._is_nested:
            # 子进度条：分配下一个可用位置
            self._position = TqdmProgressTracker._next_position
            TqdmProgressTracker._next_position += 1
        else:
            # 母进度条：固定在位置0
            self._position = 0

        leave = not self._is_nested  # 母进度条保留，子进度条清除

        self._pbar = tqdm(
            total=total,
            desc=description,
            position=self._position,
            leave=leave,
            ncols=80,
        )

    def update_progress(
        self, increment: int = 1, description: Optional[str] = None
    ) -> None:
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

    @classmethod
    def reset_positions(cls) -> None:
        """重置位置计数器（用于新的进度会话）"""
        cls._next_position = 1


class ProgressTrackerFactory:
    """进度跟踪器工厂

    实现IProgressTrackerFactory接口，创建合适的进度跟踪器。
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
        return TqdmProgressTracker(is_nested=is_nested)


class TasksProgressTracker:
    """进度管理器

    管理母子进度条的生命周期，支持按任务类型分组的进度显示。
    1个母进度条 + 多个任务类型子进度条的架构。

    实现 ITasksProgressTracker 接口。
    """

    def __init__(self, factory: IProgressTrackerFactory):
        """初始化进度管理器

        Args:
            factory: 进度跟踪器工厂
        """
        self._factory = factory
        self._main_tracker: Optional[IProgressTracker] = None
        self._task_type_trackers: dict[
            str, IProgressTracker
        ] = {}  # 按任务类型的子进度条

    def start_group_progress(
        self, total_tasks: int, description: str = "处理下载任务"
    ) -> None:
        """开始组级别进度跟踪（母进度条）

        Args:
            total_tasks: 总任务数
            description: 进度条描述
        """
        self._main_tracker = self._factory.create_tracker("group", is_nested=False)
        self._main_tracker.start_tracking(total_tasks, description)

    def start_task_type_progress(self, task_type: str, total_symbols: int) -> None:
        """开始任务类型级别进度跟踪（子进度条）

        Args:
            task_type: 任务类型名称
            total_symbols: 该任务类型需要处理的股票数量
        """
        is_nested = self._main_tracker is not None
        tracker = self._factory.create_tracker("task_type", is_nested=is_nested)
        description = f"{task_type}: 0/{total_symbols}"
        tracker.start_tracking(total_symbols, description)
        self._task_type_trackers[task_type] = tracker

    def start_task_progress(
        self, total_tasks: int, description: str = "处理任务"
    ) -> None:
        """开始任务级别进度跟踪（子进度条）- 兼容性方法

        Args:
            total_tasks: 总任务数
            description: 进度条描述
        """
        # 为了向后兼容，保留此方法
        pass

    def update_group_progress(
        self, increment: int = 1, description: Optional[str] = None
    ) -> None:
        """更新组级别进度

        Args:
            increment: 增量
            description: 可选的描述更新
        """
        if self._main_tracker is not None:
            self._main_tracker.update_progress(increment, description)

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
        tracker = self._task_type_trackers.get(task_type)
        if tracker is not None:
            description = None
            if completed is not None and total is not None:
                description = f"{task_type}: {completed}/{total}"
            tracker.update_progress(increment, description)

    def update_task_progress(
        self, increment: int = 1, description: Optional[str] = None
    ) -> None:
        """更新任务级别进度 - 兼容性方法

        Args:
            increment: 增量
            description: 可选的描述更新
        """
        # 为了向后兼容，保留此方法
        pass

    def finish_task_type_progress(self, task_type: str) -> None:
        """完成任务类型级别进度跟踪

        Args:
            task_type: 任务类型名称
        """
        tracker = self._task_type_trackers.get(task_type)
        if tracker is not None:
            tracker.finish_tracking()
            del self._task_type_trackers[task_type]

    def finish_task_progress(self) -> None:
        """完成任务级别进度跟踪 - 兼容性方法"""
        # 为了向后兼容，保留此方法
        pass

    def finish_group_progress(self) -> None:
        """完成组级别进度跟踪"""
        if self._main_tracker is not None:
            self._main_tracker.finish_tracking()
            self._main_tracker = None

    def finish_all(self) -> None:
        """完成所有进度跟踪"""
        # 完成所有任务类型进度条
        for task_type in list(self._task_type_trackers.keys()):
            self.finish_task_type_progress(task_type)

        # 完成母进度条
        self.finish_group_progress()
