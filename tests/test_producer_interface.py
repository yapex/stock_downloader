"""Producer接口测试

测试简化后的IProducer接口定义。
"""

import pytest
from queue import Queue
from typing import Optional

from src.downloader.interfaces import IProducer, ProducerEvents, IEventBus, IEventListener
from src.downloader.models import DownloadTask, DataBatch


class MockEventBus:
    """Mock事件总线实现"""
    
    def __init__(self):
        self.listeners = {}
        self.events = []
    
    def subscribe(self, event_type: str, listener: IEventListener) -> None:
        if event_type not in self.listeners:
            self.listeners[event_type] = []
        self.listeners[event_type].append(listener)
    
    def publish(self, event_type: str, data: any) -> None:
        self.events.append((event_type, data))
        if event_type in self.listeners:
            for listener in self.listeners[event_type]:
                listener.on_event(event_type, data)


class MockProducer:
    """Mock Producer实现"""
    
    def __init__(self, event_bus: IEventBus):
        if event_bus is None:
            raise ValueError("event_bus is required")
        self.event_bus = event_bus
        self._running = False
        self.task_queue = Queue()
    
    def start(self) -> None:
        self._running = True
    
    def stop(self, timeout: float = 30.0) -> None:
        self._running = False
    
    def submit_task(self, task: DownloadTask, timeout: float = 1.0) -> bool:
        if not self._running:
            raise RuntimeError("Producer is not running")
        try:
            self.task_queue.put(task, timeout=timeout)
            return True
        except:
            return False
    
    @property
    def is_running(self) -> bool:
        return self._running


def test_producer_interface_compliance():
    """测试Producer实现是否符合接口定义"""
    event_bus = MockEventBus()
    producer = MockProducer(event_bus)
    
    # 验证接口方法存在
    assert hasattr(producer, 'start')
    assert hasattr(producer, 'stop')
    assert hasattr(producer, 'submit_task')
    assert hasattr(producer, 'is_running')
    
    # 验证初始状态
    assert not producer.is_running
    
    # 测试启动
    producer.start()
    assert producer.is_running
    
    # 测试停止
    producer.stop()
    assert not producer.is_running


def test_producer_task_submission():
    """测试任务提交功能"""
    event_bus = MockEventBus()
    producer = MockProducer(event_bus)
    
    # 未启动时提交任务应该失败
    task = DownloadTask(
        symbol="000001.SZ",
        task_type="stock_list",
        params={"exchange": "SZ"}
    )
    
    with pytest.raises(RuntimeError, match="Producer is not running"):
        producer.submit_task(task)
    
    # 启动后提交任务应该成功
    producer.start()
    assert producer.submit_task(task)
    
    producer.stop()


def test_producer_events_constants():
    """测试事件类型常量定义"""
    assert hasattr(ProducerEvents, 'DATA_READY')
    assert hasattr(ProducerEvents, 'TASK_FAILED')
    assert hasattr(ProducerEvents, 'TASK_COMPLETED')
    
    assert ProducerEvents.DATA_READY == "producer.data_ready"
    assert ProducerEvents.TASK_FAILED == "producer.task_failed"
    assert ProducerEvents.TASK_COMPLETED == "producer.task_completed"


def test_event_bus_integration():
    """测试事件总线集成"""
    event_bus = MockEventBus()
    
    # 测试事件发布
    test_data = {"task_id": "test", "data": "sample"}
    event_bus.publish(ProducerEvents.DATA_READY, test_data)
    
    assert len(event_bus.events) == 1
    assert event_bus.events[0] == (ProducerEvents.DATA_READY, test_data)