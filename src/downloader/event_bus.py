"""事件总线实现

提供事件总线的具体实现。
"""

from typing import Any
from .interfaces.events import IEventBus, IEventListener


class SimpleEventBus(IEventBus):
    """简单的事件总线实现
    
    基于字典的内存事件总线，适用于单进程场景。
    """
    
    def __init__(self):
        self._listeners: dict[str, list[IEventListener]] = {}
    
    def subscribe(self, event_type: str, listener: IEventListener) -> None:
        """订阅事件"""
        if event_type not in self._listeners:
            self._listeners[event_type] = []
        self._listeners[event_type].append(listener)
    
    def unsubscribe(self, event_type: str, listener: IEventListener) -> None:
        """取消订阅事件"""
        if event_type in self._listeners:
            try:
                self._listeners[event_type].remove(listener)
                if not self._listeners[event_type]:
                    del self._listeners[event_type]
            except ValueError:
                pass  # 监听器不存在，忽略
    
    def publish(self, event_type: str, event_data: Any) -> None:
        """发布事件"""
        if event_type in self._listeners:
            for listener in self._listeners[event_type].copy():  # 复制列表避免并发修改
                try:
                    listener.on_event(event_type, event_data)
                except Exception as e:
                    # 记录错误但不影响其他监听器
                    from .utils import get_logger
                    logger = get_logger(__name__)
                    logger.error(f"Error in event listener for {event_type}: {e}")
    
    def clear(self) -> None:
        """清除所有订阅"""
        self._listeners.clear()