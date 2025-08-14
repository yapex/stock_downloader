from typing import Protocol, Callable, Any, runtime_checkable

from enum import Enum


class TaskEventType(Enum):
    """事件类型枚举"""

    TASK_STARTED = "task:started"
    TASK_PROGRESS = "task:progress"
    TASK_SUCCEEDED = "task:succeeded"
    TASK_FAILED = "task:failed"
    TASK_FINISHED = "task:finished"


@runtime_checkable
class ITaskHandler(Protocol):
    def on_started(self, sender: Any, **kwargs) -> None: ...

    def on_progress(self, sender: Any, **kwargs) -> None: ...

    def on_finished(self, sender: Any, **kwargs) -> None: ...

    def on_failed(self, sender: Any, **kwargs) -> None: ...

    def on_retry(self, sender: Any, **kwargs) -> None: ...
