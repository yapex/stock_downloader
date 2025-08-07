"""
测试队列管理器实现
验证内存队列管理器的各种功能
"""

import pytest
import pytest_asyncio
import asyncio
from unittest.mock import patch

from downloader.interfaces import TaskMessage, DataMessage, TaskType, Priority
from downloader.queue_manager import MemoryQueueManager, QueueManagerFactory


class TestMemoryQueueManager:
    """测试内存队列管理器"""
    
    @pytest_asyncio.fixture
    async def queue_manager(self):
        """创建队列管理器实例"""
        manager = MemoryQueueManager(
            task_queue_size=10,
            data_queue_size=20,
            queue_timeout=1.0
        )
        await manager.start()
        yield manager
        await manager.stop()
    
    @pytest.mark.asyncio
    async def test_queue_manager_lifecycle(self):
        """测试队列管理器生命周期"""
        manager = MemoryQueueManager()
        
        # 初始状态
        assert not manager.is_running
        
        # 启动
        await manager.start()
        assert manager.is_running
        
        # 停止
        await manager.stop()
        assert not manager.is_running
    
    @pytest.mark.asyncio
    async def test_task_queue_operations(self, queue_manager):
        """测试任务队列操作"""
        # 创建测试任务
        task1 = TaskMessage.create(TaskType.DAILY, {"symbol": "000001"}, Priority.NORMAL)
        task2 = TaskMessage.create(TaskType.DAILY_BASIC, {"symbol": "000002"}, Priority.HIGH)
        
        # 添加任务
        await queue_manager.put_task(task1)
        await queue_manager.put_task(task2)
        
        assert queue_manager.task_queue_size == 2
        
        # 获取任务（高优先级任务应该先返回）
        retrieved_task1 = await queue_manager.get_task()
        assert retrieved_task1.task_id == task2.task_id  # HIGH优先级
        assert retrieved_task1.priority == Priority.HIGH
        
        retrieved_task2 = await queue_manager.get_task()
        assert retrieved_task2.task_id == task1.task_id  # NORMAL优先级
        
        assert queue_manager.task_queue_size == 0
    
    @pytest.mark.asyncio
    async def test_data_queue_operations(self, queue_manager):
        """测试数据队列操作"""
        # 创建测试数据消息
        data1 = DataMessage.create("task-1", "daily", [{"close": 10.5}])
        data2 = DataMessage.create("task-2", "daily_basic", [{"pe": 15.2}])
        
        # 添加数据
        await queue_manager.put_data(data1)
        await queue_manager.put_data(data2)
        
        assert queue_manager.data_queue_size == 2
        
        # 获取数据（FIFO顺序）
        retrieved_data1 = await queue_manager.get_data()
        assert retrieved_data1.message_id == data1.message_id
        
        retrieved_data2 = await queue_manager.get_data()
        assert retrieved_data2.message_id == data2.message_id
        
        assert queue_manager.data_queue_size == 0
    
    @pytest.mark.asyncio
    async def test_task_priority_ordering(self, queue_manager):
        """测试任务优先级排序"""
        # 创建不同优先级的任务
        low_task = TaskMessage.create(TaskType.DAILY, {"symbol": "LOW"}, Priority.LOW)
        normal_task = TaskMessage.create(TaskType.DAILY, {"symbol": "NORMAL"}, Priority.NORMAL)
        high_task = TaskMessage.create(TaskType.DAILY, {"symbol": "HIGH"}, Priority.HIGH)
        
        # 按低->高->正常的顺序添加
        await queue_manager.put_task(low_task)
        await queue_manager.put_task(high_task)
        await queue_manager.put_task(normal_task)
        
        # 应该按高->正常->低的顺序取出
        task1 = await queue_manager.get_task()
        assert task1.params["symbol"] == "HIGH"
        
        task2 = await queue_manager.get_task()
        assert task2.params["symbol"] == "NORMAL"
        
        task3 = await queue_manager.get_task()
        assert task3.params["symbol"] == "LOW"
    
    @pytest.mark.asyncio
    async def test_queue_timeout(self, queue_manager):
        """测试队列超时"""
        # 空队列获取任务应该超时返回None
        task = await queue_manager.get_task(timeout=0.1)
        assert task is None
        
        # 空队列获取数据应该超时返回None
        data = await queue_manager.get_data(timeout=0.1)
        assert data is None
    
    @pytest.mark.asyncio
    async def test_queue_full_error(self):
        """测试队列满时的错误处理"""
        # 创建小容量队列
        manager = MemoryQueueManager(task_queue_size=1, data_queue_size=1)
        await manager.start()
        
        try:
            # 填满任务队列
            task1 = TaskMessage.create(TaskType.DAILY, {"symbol": "001"})
            await manager.put_task(task1)
            
            # 再添加一个任务应该抛出错误
            task2 = TaskMessage.create(TaskType.DAILY, {"symbol": "002"})
            with pytest.raises(RuntimeError, match="Task queue is full"):
                await manager.put_task(task2)
            
            # 填满数据队列
            data1 = DataMessage.create("task-1", "daily", [{"close": 10.5}])
            await manager.put_data(data1)
            
            # 再添加一个数据应该抛出错误
            data2 = DataMessage.create("task-2", "daily", [{"close": 11.5}])
            with pytest.raises(RuntimeError, match="Data queue is full"):
                await manager.put_data(data2)
                
        finally:
            await manager.stop()
    
    @pytest.mark.asyncio
    async def test_queue_not_running_error(self):
        """测试队列未运行时的错误处理"""
        manager = MemoryQueueManager()
        
        task = TaskMessage.create(TaskType.DAILY, {"symbol": "001"})
        data = DataMessage.create("task-1", "daily", [{"close": 10.5}])
        
        # 未启动时操作应该抛出错误
        with pytest.raises(RuntimeError, match="Queue manager is not running"):
            await manager.put_task(task)
            
        with pytest.raises(RuntimeError, match="Queue manager is not running"):
            await manager.put_data(data)
        
        # 获取操作应该返回None
        assert await manager.get_task() is None
        assert await manager.get_data() is None
    
    @pytest.mark.asyncio
    async def test_performance_metrics(self, queue_manager):
        """测试性能指标收集"""
        # 添加一些任务和数据
        task1 = TaskMessage.create(TaskType.DAILY, {"symbol": "001"})
        task2 = TaskMessage.create(TaskType.DAILY_BASIC, {"symbol": "002"})
        
        data1 = DataMessage.create("task-1", "daily", [{"close": 10.5}])
        
        await queue_manager.put_task(task1)
        await queue_manager.put_task(task2)
        await queue_manager.put_data(data1)
        
        # 获取指标
        metrics = queue_manager.get_metrics()
        
        assert metrics.tasks_total == 2
        assert metrics.data_messages_total == 1
        assert metrics.queue_sizes["task_queue"] == 2
        assert metrics.queue_sizes["data_queue"] == 1
        
        # 更新完成和失败计数
        queue_manager.update_task_completed()
        queue_manager.update_task_failed()
        
        updated_metrics = queue_manager.get_metrics()
        assert updated_metrics.tasks_completed == 1
        assert updated_metrics.tasks_failed == 1
    
    @pytest.mark.asyncio
    async def test_queue_full_ratio(self, queue_manager):
        """测试队列满载率"""
        # 初始满载率应为0
        assert queue_manager.task_queue_full_ratio == 0.0
        assert queue_manager.data_queue_full_ratio == 0.0
        
        # 添加一些项目
        tasks = [TaskMessage.create(TaskType.DAILY, {"symbol": f"{i:03d}"}) for i in range(5)]
        for task in tasks:
            await queue_manager.put_task(task)
        
        # 任务队列满载率应为5/10=0.5
        assert queue_manager.task_queue_full_ratio == 0.5
        
        data_items = [DataMessage.create(f"task-{i}", "daily", [{"close": i}]) for i in range(10)]
        for data in data_items:
            await queue_manager.put_data(data)
        
        # 数据队列满载率应为10/20=0.5
        assert queue_manager.data_queue_full_ratio == 0.5


class TestQueueManagerFactory:
    """测试队列管理器工厂"""
    
    def test_create_memory_queue_manager(self):
        """测试创建内存队列管理器"""
        config = {
            'task_queue_size': 500,
            'data_queue_size': 2000,
            'queue_timeout': 10.0
        }
        
        manager = QueueManagerFactory.create_memory_queue_manager(config)
        
        assert isinstance(manager, MemoryQueueManager)
        assert manager._task_queue_size == 500
        assert manager._data_queue_size == 2000
        assert manager._queue_timeout == 10.0
    
    def test_create_memory_queue_manager_defaults(self):
        """测试使用默认参数创建内存队列管理器"""
        manager = QueueManagerFactory.create_memory_queue_manager({})
        
        assert isinstance(manager, MemoryQueueManager)
        assert manager._task_queue_size == 1000  # 默认值
        assert manager._data_queue_size == 5000  # 默认值
        assert manager._queue_timeout == 30.0    # 默认值
    
    def test_create_queue_manager(self):
        """测试通用队列管理器创建"""
        config = {'task_queue_size': 100}
        
        manager = QueueManagerFactory.create_queue_manager("memory", config)
        assert isinstance(manager, MemoryQueueManager)
        
        # 测试不支持的类型
        with pytest.raises(ValueError, match="Unsupported queue type"):
            QueueManagerFactory.create_queue_manager("redis", config)


# 并发测试
class TestConcurrency:
    """测试并发场景"""
    
    @pytest.mark.asyncio
    async def test_concurrent_task_operations(self):
        """测试并发任务操作"""
        manager = MemoryQueueManager(task_queue_size=100)
        await manager.start()
        
        try:
            # 并发添加任务
            async def add_tasks():
                tasks = [
                    TaskMessage.create(TaskType.DAILY, {"symbol": f"{i:03d}"})
                    for i in range(20)
                ]
                await asyncio.gather(*[manager.put_task(task) for task in tasks])
            
            # 并发获取任务
            async def get_tasks():
                tasks = []
                for _ in range(20):
                    task = await manager.get_task(timeout=1.0)
                    if task:
                        tasks.append(task)
                return tasks
            
            # 同时执行添加和获取
            add_task = asyncio.create_task(add_tasks())
            get_task = asyncio.create_task(get_tasks())
            
            await add_task
            retrieved_tasks = await get_task
            
            # 应该获取到所有任务
            assert len(retrieved_tasks) == 20
            
        finally:
            await manager.stop()
    
    @pytest.mark.asyncio
    async def test_concurrent_data_operations(self):
        """测试并发数据操作"""
        manager = MemoryQueueManager(data_queue_size=100)
        await manager.start()
        
        try:
            # 并发添加数据
            async def add_data():
                data_items = [
                    DataMessage.create(f"task-{i}", "daily", [{"close": i}])
                    for i in range(15)
                ]
                await asyncio.gather(*[manager.put_data(data) for data in data_items])
            
            # 并发获取数据
            async def get_data():
                data_items = []
                for _ in range(15):
                    data = await manager.get_data(timeout=1.0)
                    if data:
                        data_items.append(data)
                return data_items
            
            # 同时执行添加和获取
            add_task = asyncio.create_task(add_data())
            get_task = asyncio.create_task(get_data())
            
            await add_task
            retrieved_data = await get_task
            
            # 应该获取到所有数据
            assert len(retrieved_data) == 15
            
        finally:
            await manager.stop()
