"""TaskScheduler 单元测试"""

import pytest
import threading
import time
from queue import Empty

from downloader.task.task_scheduler import (
    TaskScheduler,
    TaskTypeConfig,
    PriorityTaskItem,
    create_task_configs
)
from downloader.task.types import DownloadTaskConfig, TaskPriority
from downloader.producer.fetcher_builder import TaskType


class TestPriorityTaskItem:
    """测试 PriorityTaskItem"""
    
    def test_priority_ordering(self):
        """测试优先级排序"""
        config1 = DownloadTaskConfig("600519", TaskType.STOCK_DAILY, TaskPriority.HIGH)
        config2 = DownloadTaskConfig("000001", TaskType.STOCK_DAILY, TaskPriority.LOW)
        
        item1 = PriorityTaskItem(TaskPriority.HIGH.value, 1, config1)
        item2 = PriorityTaskItem(TaskPriority.LOW.value, 2, config2)
        
        # 高优先级应该小于低优先级（数值越小优先级越高）
        assert item1 < item2
        assert not item2 < item1
    
    def test_sequence_ordering_same_priority(self):
        """测试相同优先级时的序列号排序"""
        config1 = DownloadTaskConfig("600519", TaskType.STOCK_DAILY, TaskPriority.MEDIUM)
        config2 = DownloadTaskConfig("000001", TaskType.STOCK_DAILY, TaskPriority.MEDIUM)
        
        item1 = PriorityTaskItem(TaskPriority.MEDIUM.value, 1, config1)
        item2 = PriorityTaskItem(TaskPriority.MEDIUM.value, 2, config2)
        
        # 相同优先级时，序列号小的优先
        assert item1 < item2
        assert not item2 < item1


class TestTaskTypeConfig:
    """测试 TaskTypeConfig"""
    
    def test_default_priorities(self):
        """测试默认优先级"""
        config = TaskTypeConfig()
        
        assert config.get_priority(TaskType.STOCK_BASIC) == TaskPriority.HIGH
        assert config.get_priority(TaskType.STOCK_DAILY) == TaskPriority.MEDIUM
        assert config.get_priority(TaskType.BALANCESHEET) == TaskPriority.LOW
    
    def test_custom_priorities(self):
        """测试自定义优先级"""
        custom_priorities = {
            TaskType.STOCK_DAILY: TaskPriority.HIGH,
            TaskType.BALANCESHEET: TaskPriority.MEDIUM
        }
        config = TaskTypeConfig(custom_priorities)
        
        # 自定义优先级应该覆盖默认值
        assert config.get_priority(TaskType.STOCK_DAILY) == TaskPriority.HIGH
        assert config.get_priority(TaskType.BALANCESHEET) == TaskPriority.MEDIUM
        # 未自定义的应该保持默认值
        assert config.get_priority(TaskType.STOCK_BASIC) == TaskPriority.HIGH
    
    def test_set_priority(self):
        """测试动态设置优先级"""
        config = TaskTypeConfig()
        
        # 修改优先级
        config.set_priority(TaskType.STOCK_DAILY, TaskPriority.LOW)
        assert config.get_priority(TaskType.STOCK_DAILY) == TaskPriority.LOW
    
    def test_unknown_task_type_default(self):
        """测试未知任务类型返回默认优先级"""
        config = TaskTypeConfig()
        
        # 模拟一个不存在的任务类型
        # 由于我们无法创建新的 TaskType，这里测试现有类型的默认行为
        assert config.get_priority(TaskType.INCOME) == TaskPriority.LOW


class TestTaskScheduler:
    """测试 TaskScheduler"""
    
    @pytest.fixture
    def scheduler(self):
        """创建任务调度器实例"""
        return TaskScheduler()
    
    def test_init_with_default_config(self):
        """测试使用默认配置初始化"""
        scheduler = TaskScheduler()
        assert scheduler.task_type_config is not None
        assert scheduler.is_empty()
        assert scheduler.size() == 0
    
    def test_init_with_custom_config(self):
        """测试使用自定义配置初始化"""
        custom_config = TaskTypeConfig({TaskType.STOCK_DAILY: TaskPriority.HIGH})
        scheduler = TaskScheduler(custom_config)
        assert scheduler.task_type_config == custom_config
    
    def test_add_single_task(self, scheduler):
        """测试添加单个任务"""
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        scheduler.add_task(config)
        
        assert not scheduler.is_empty()
        assert scheduler.size() == 1
    
    def test_add_multiple_tasks(self, scheduler):
        """测试添加多个任务"""
        configs = [
            DownloadTaskConfig("600519", TaskType.STOCK_DAILY),
            DownloadTaskConfig("000001", TaskType.STOCK_DAILY),
            DownloadTaskConfig("", TaskType.STOCK_BASIC)
        ]
        scheduler.add_tasks(configs)
        
        assert scheduler.size() == 3
    
    def test_priority_ordering(self, scheduler):
        """测试优先级排序"""
        # 添加不同优先级的任务
        low_config = DownloadTaskConfig("600519", TaskType.BALANCESHEET)  # 默认 LOW
        medium_config = DownloadTaskConfig("000001", TaskType.STOCK_DAILY)  # 默认 MEDIUM
        high_config = DownloadTaskConfig("", TaskType.STOCK_BASIC)  # 默认 HIGH
        
        # 按相反顺序添加
        scheduler.add_task(low_config)
        scheduler.add_task(medium_config)
        scheduler.add_task(high_config)
        
        # 获取任务应该按优先级排序
        first_task = scheduler.get_next_task()
        second_task = scheduler.get_next_task()
        third_task = scheduler.get_next_task()
        
        assert first_task.task_type == TaskType.STOCK_BASIC  # HIGH
        assert second_task.task_type == TaskType.STOCK_DAILY  # MEDIUM
        assert third_task.task_type == TaskType.BALANCESHEET  # LOW
    
    def test_fifo_same_priority(self, scheduler):
        """测试相同优先级时的 FIFO 顺序"""
        config1 = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        config2 = DownloadTaskConfig("000001", TaskType.STOCK_DAILY)
        config3 = DownloadTaskConfig("000002", TaskType.STOCK_DAILY)
        
        scheduler.add_task(config1)
        scheduler.add_task(config2)
        scheduler.add_task(config3)
        
        # 应该按添加顺序获取
        first_task = scheduler.get_next_task()
        second_task = scheduler.get_next_task()
        third_task = scheduler.get_next_task()
        
        assert first_task.symbol == "600519"
        assert second_task.symbol == "000001"
        assert third_task.symbol == "000002"
    
    def test_explicit_priority_override(self, scheduler):
        """测试显式优先级覆盖默认优先级"""
        # 创建一个低优先级任务类型，但设置为高优先级
        config = DownloadTaskConfig(
            "600519", 
            TaskType.BALANCESHEET,  # 默认 LOW
            priority=TaskPriority.HIGH  # 显式设置为 HIGH
        )
        normal_config = DownloadTaskConfig("", TaskType.STOCK_BASIC)  # 默认 HIGH
        
        scheduler.add_task(normal_config)
        scheduler.add_task(config)
        
        # 两个都是 HIGH 优先级，应该按 FIFO 顺序
        first_task = scheduler.get_next_task()
        second_task = scheduler.get_next_task()
        
        assert first_task.task_type == TaskType.STOCK_BASIC
        assert second_task.task_type == TaskType.BALANCESHEET
    
    def test_get_next_task_empty_queue(self, scheduler):
        """测试从空队列获取任务"""
        # 非阻塞获取应该返回 None
        task = scheduler.get_next_task(timeout=0.1)
        assert task is None
    
    def test_get_all_tasks(self, scheduler):
        """测试获取所有任务"""
        configs = [
            DownloadTaskConfig("600519", TaskType.STOCK_DAILY),
            DownloadTaskConfig("000001", TaskType.STOCK_DAILY),
            DownloadTaskConfig("", TaskType.STOCK_BASIC)
        ]
        scheduler.add_tasks(configs)
        
        all_tasks = scheduler.get_all_tasks()
        
        assert len(all_tasks) == 3
        assert scheduler.is_empty()
        assert scheduler.size() == 0
        
        # 任务应该按优先级排序
        assert all_tasks[0].task_type == TaskType.STOCK_BASIC  # HIGH
        assert all_tasks[1].task_type == TaskType.STOCK_DAILY  # MEDIUM
        assert all_tasks[2].task_type == TaskType.STOCK_DAILY  # MEDIUM
    
    def test_clear(self, scheduler):
        """测试清空队列"""
        configs = [
            DownloadTaskConfig("600519", TaskType.STOCK_DAILY),
            DownloadTaskConfig("000001", TaskType.STOCK_DAILY)
        ]
        scheduler.add_tasks(configs)
        
        assert scheduler.size() == 2
        
        scheduler.clear()
        
        assert scheduler.is_empty()
        assert scheduler.size() == 0
    
    def test_thread_safety(self, scheduler):
        """测试线程安全性"""
        def add_tasks():
            for i in range(100):
                config = DownloadTaskConfig(f"60051{i % 10}", TaskType.STOCK_DAILY)
                scheduler.add_task(config)
        
        def get_tasks():
            tasks = []
            for _ in range(50):
                task = scheduler.get_next_task(timeout=1.0)
                if task:
                    tasks.append(task)
            return tasks
        
        # 启动多个线程同时添加和获取任务
        add_threads = [threading.Thread(target=add_tasks) for _ in range(2)]
        get_threads = [threading.Thread(target=get_tasks) for _ in range(2)]
        
        for thread in add_threads + get_threads:
            thread.start()
        
        for thread in add_threads + get_threads:
            thread.join()
        
        # 验证没有发生死锁或数据竞争
        # 剩余任务数应该是合理的
        remaining_tasks = scheduler.size()
        assert 0 <= remaining_tasks <= 200


class TestCreateTaskConfigs:
    """测试 create_task_configs 函数"""
    
    def test_create_normal_task_configs(self):
        """测试创建普通任务配置"""
        symbols = ["600519", "000001", "000002"]
        configs = create_task_configs(symbols, TaskType.STOCK_DAILY)
        
        assert len(configs) == 3
        for i, config in enumerate(configs):
            assert config.symbol == symbols[i]
            assert config.task_type == TaskType.STOCK_DAILY
            assert config.priority == TaskPriority.MEDIUM
            assert config.max_retries == 2
    
    def test_create_stock_basic_task_config(self):
        """测试创建 STOCK_BASIC 任务配置"""
        symbols = ["600519", "000001"]  # 对于 STOCK_BASIC，symbols 会被忽略
        configs = create_task_configs(symbols, TaskType.STOCK_BASIC)
        
        assert len(configs) == 1  # 只创建一个任务
        assert configs[0].symbol == ""  # symbol 为空
        assert configs[0].task_type == TaskType.STOCK_BASIC
    
    def test_create_with_custom_priority(self):
        """测试使用自定义优先级创建任务配置"""
        symbols = ["600519"]
        configs = create_task_configs(
            symbols, 
            TaskType.STOCK_DAILY, 
            priority=TaskPriority.HIGH,
            max_retries=5
        )
        
        assert len(configs) == 1
        assert configs[0].priority == TaskPriority.HIGH
        assert configs[0].max_retries == 5
    
    def test_create_empty_symbols(self):
        """测试空 symbols 列表"""
        configs = create_task_configs([], TaskType.STOCK_DAILY)
        assert len(configs) == 0
        
        # STOCK_BASIC 即使 symbols 为空也会创建一个任务
        configs = create_task_configs([], TaskType.STOCK_BASIC)
        assert len(configs) == 1
        assert configs[0].symbol == ""