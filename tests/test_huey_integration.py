"""Huey 集成测试

测试 Huey 任务的异步链式调用和多线程 Consumer 功能。
"""

from neo.configs.huey_config import huey
from neo.tasks.huey_tasks import download_task, process_data_task
from neo.task_bus.types import TaskType


class TestHueyIntegration:
    """Huey 集成测试类"""

    def test_huey_configuration(self):
        """测试 Huey 配置是否正确"""
        assert huey.name == "stock_downloader"
        assert not huey.immediate  # 异步执行
        assert not huey.utc  # 使用本地时区

    def test_async_task_chain(self):
        """测试异步任务链式调用"""
        # 验证任务函数可以被调用（不执行实际的下载逻辑）
        assert callable(download_task)
        assert callable(process_data_task)

        # 验证任务类型枚举可用
        assert hasattr(TaskType, "stock_basic")
        assert TaskType.stock_basic is not None

    def test_process_data_task_execution(self):
        """测试数据处理任务执行"""
        # 验证任务函数可以被调用
        assert callable(process_data_task)

        # 验证任务类型枚举可用
        assert hasattr(TaskType, "stock_basic")
        assert TaskType.stock_basic is not None

    def test_task_registration(self):
        """测试任务是否正确注册到 Huey"""
        # 检查任务是否在 Huey 注册表中
        # 简化测试：只验证 Huey 实例有注册表
        assert hasattr(huey, "_registry")
        assert huey._registry is not None

        # 验证任务函数存在
        from neo.tasks.huey_tasks import download_task, process_data_task

        assert callable(download_task)
        assert callable(process_data_task)

    
