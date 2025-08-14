import pytest
from unittest.mock import Mock

# 假设您的代码文件路径如下
# from your_project.event_bus import BlinkerEventBus, EventBus
# 为了可运行，我们直接将 BlinkerEventBus 放在这里
from blinker import Signal


class BlinkerEventBus:
    def __init__(self):
        self._signals: dict[str, Signal] = {}

    def _get_or_create_signal(self, event_name: str) -> Signal:
        if event_name not in self._signals:
            self._signals[event_name] = Signal(f"Signal for {event_name}")
        return self._signals[event_name]

    def subscribe(self, event_name, handler):
        signal = self._get_or_create_signal(event_name)
        signal.connect(handler)

    def publish(self, event_name, sender, **kwargs):
        signal = self._get_or_create_signal(event_name)
        signal.send(sender, **kwargs)


# --- 结束代码定义 ---


@pytest.fixture
def event_bus() -> BlinkerEventBus:
    """提供一个干净的 BlinkerEventBus 实例供每个测试使用。"""
    return BlinkerEventBus()


@pytest.fixture
def mock_handler() -> Mock:
    """提供一个 Mock 对象作为事件处理器。"""
    return Mock()


class TestBlinkerEventBus:
    """
    为 BlinkerEventBus 编写的简洁单元测试。
    """

    def test_publish_calls_subscribed_handler(self, event_bus, mock_handler):
        """
        核心测试：验证订阅后发布，处理器能被正确调用并收到参数。
        """
        # 安排 (Arrange)
        event_name = "user:created"
        sender_obj = "TestSender"
        event_data = {"user_id": 123, "name": "Alice"}

        event_bus.subscribe(event_name, mock_handler)

        # 行动 (Act)
        event_bus.publish(event_name, sender=sender_obj, **event_data)

        # 断言 (Assert)
        # 验证 mock handler 被调用了一次，并且收到了正确的关键字参数
        mock_handler.assert_called_once_with(sender_obj, **event_data)

    def test_handler_is_not_called_for_different_event(self, event_bus, mock_handler):
        """
        测试事件隔离：验证处理器不会响应未订阅的事件。
        """
        # 安排 (Arrange)
        event_bus.subscribe("event_A", mock_handler)

        # 行动 (Act)
        event_bus.publish("event_B", sender=self)  # 发布一个不同的事件

        # 断言 (Assert)
        mock_handler.assert_not_called()

    def test_publish_calls_multiple_handlers(self, event_bus):
        """
        测试广播功能：验证发布一个事件会调用所有订阅它的处理器。
        """
        # 安排 (Arrange)
        event_name = "system:shutdown"
        handler_one = Mock()
        handler_two = Mock()

        event_bus.subscribe(event_name, handler_one)
        event_bus.subscribe(event_name, handler_two)

        # 行动 (Act)
        event_bus.publish(event_name, sender=self)

        # 断言 (Assert)
        handler_one.assert_called_once()
        handler_two.assert_called_once()

    def test_publish_to_unsubscribed_event_does_not_fail(self, event_bus, mock_handler):
        """
        测试健壮性：验证向一个没有任何订阅者的事件发布消息时，程序不会崩溃。
        """
        try:
            # 行动 (Act)
            event_bus.publish("no_subscribers_event", sender=self)
            # 断言 (Assert)
            mock_handler.assert_not_called()
        except Exception as e:
            pytest.fail(f"发布到未订阅的事件不应引发异常，但却引发了: {e}")
