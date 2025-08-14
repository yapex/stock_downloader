from typing import Protocol, Callable, Any, runtime_checkable


@runtime_checkable
class ISubscriber(Protocol):
    def subscribe(self, event_name: str, handler: Callable[..., None]) -> None:
        """
        订阅一个事件。当该事件发布时，指定的 handler 将被调用。

        Args:
            event_name: 事件的唯一名称 (例如 'task:succeeded').
            handler: 一个可调用的函数，用于处理事件。
        """
        ...


@runtime_checkable
class IPublisher(Protocol):
    def publish(self, event_name: str, sender: Any, **kwargs) -> None:
        """
        发布（或发送）一个事件。

        Args:
            event_name: 要发布的事件的名称。
            sender: 发送事件的对象或实体。
            **kwargs: 附带到事件中的任意数据。
        """
        ...


class IEventBus(ISubscriber, IPublisher):
    """
    定义一个事件总线的“协议”或“接口”。
    任何遵循此接口的类都可以被我们的系统使用，从而将系统与
    具体的事件库（如 blinker）解耦。
    """
