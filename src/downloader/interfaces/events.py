"""事件系统接口定义

定义通用的事件系统接口，用于组件间的解耦通信。
"""

from typing import Protocol, Any


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