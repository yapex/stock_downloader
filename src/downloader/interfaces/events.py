"""事件系统接口定义

定义通用的事件系统接口，用于组件间的解耦通信。
"""

from typing import Protocol, Callable, Any, Optional
from abc import ABC, abstractmethod


class IEventListener(Protocol):
    """事件监听器接口"""
    
    def __call__(self, event_data: Any) -> None:
        """处理事件
        
        Args:
            event_data: 事件数据
        """
        ...


class IEventBus(Protocol):
    """事件总线接口
    
    提供事件的发布和订阅功能。
    """
    
    def subscribe(self, event_type: str, listener: IEventListener) -> None:
        """订阅事件
        
        Args:
            event_type: 事件类型
            listener: 事件监听器
        """
        ...
    
    def unsubscribe(self, event_type: str, listener: IEventListener) -> None:
        """取消订阅事件
        
        Args:
            event_type: 事件类型
            listener: 事件监听器
        """
        ...
    
    def publish(self, event_type: str, event_data: Any) -> None:
        """发布事件
        
        Args:
            event_type: 事件类型
            event_data: 事件数据
        """
        ...
    
    def clear(self) -> None:
        """清除所有订阅"""
        ...


class SimpleEventBus:
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
                    listener(event_data)
                except Exception as e:
                    # 记录错误但不影响其他监听器
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.error(f"Error in event listener for {event_type}: {e}")
    
    def clear(self) -> None:
        """清除所有订阅"""
        self._listeners.clear()