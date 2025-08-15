from .interfaces.event_bus import IEventBus
from blinker import Signal
from typing import Callable, Any


class BlinkerEventBus(IEventBus):
    """
    使用 'blinker' 库来实现我们的 EventBus 接口。
    这个类封装了所有 blinker 的细节。
    """

    def __init__(self):
        # 将 blinker 的 Signal 对象存储在字典中，与事件名称关联
        self._signals: dict[str, Signal] = {}

    def _get_or_create_signal(self, event_name: str) -> Signal:
        """一个内部辅助方法，用于获取或创建 blinker 信号"""
        if event_name not in self._signals:
            self._signals[event_name] = Signal(f"Signal for {event_name}")
        return self._signals[event_name]

    def subscribe(self, event_name: str, handler: Callable[..., None]) -> None:
        """使用 blinker.Signal.connect() 来实现订阅"""
        signal = self._get_or_create_signal(event_name)
        signal.connect(handler)

    def publish(self, event_name: str, sender: Any, **kwargs) -> None:
        """使用 blinker.Signal.send() 来实现发布"""
        signal = self._get_or_create_signal(event_name)
        signal.send(sender, **kwargs)


class DownloaderProcessingHandler:
    def on_task_succeeded(self, sender: Any, task_id: int) -> None:
        print(f"任务 {task_id} 已成功完成！")

    def on_task_failed(self, sender: Any, task_id: int, error: str) -> None:
        print(f"任务 {task_id} 失败：{error}")
