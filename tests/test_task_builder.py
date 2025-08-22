from neo.helpers.task_builder import TaskBuilder
from neo.task_bus.types import TaskType, DownloadTaskConfig, TaskPriority


class TestTaskBuilder:
    """TaskBuilder 类的测试用例"""

    def setup_method(self):
        """每个测试方法执行前的设置"""
        self.task_builder = TaskBuilder()

    def test_build_tasks_with_single_symbol_and_task_type(self):
        """测试单个股票代码和任务类型的任务构建"""
        symbols = ["000001.SZ"]
        task_types = [TaskType.STOCK_BASIC]
        priority = TaskPriority.HIGH

        tasks = self.task_builder.build_tasks(symbols, task_types, priority)

        assert len(tasks) == 1
        task = tasks[0]
        assert isinstance(task, DownloadTaskConfig)
        assert task.symbol == "000001.SZ"
        assert task.task_type == TaskType.STOCK_BASIC
        assert task.priority == priority

    def test_build_tasks_with_multiple_symbols_and_task_types(self):
        """测试多个股票代码和任务类型的任务构建"""
        symbols = ["000001.SZ", "000002.SZ"]
        task_types = [TaskType.STOCK_BASIC, TaskType.STOCK_DAILY]
        priority = TaskPriority.MEDIUM

        tasks = self.task_builder.build_tasks(symbols, task_types, priority)

        # 应该生成 2 * 2 = 4 个任务
        assert len(tasks) == 4

        # 验证任务组合
        expected_combinations = [
            ("000001.SZ", TaskType.STOCK_BASIC),
            ("000001.SZ", TaskType.STOCK_DAILY),
            ("000002.SZ", TaskType.STOCK_BASIC),
            ("000002.SZ", TaskType.STOCK_DAILY),
        ]

        actual_combinations = [(task.symbol, task.task_type) for task in tasks]
        assert set(actual_combinations) == set(expected_combinations)

        # 验证所有任务的优先级
        for task in tasks:
            assert task.priority == priority

    def test_build_tasks_with_empty_symbols(self):
        """测试空股票代码列表（stock_basic组的情况）"""
        symbols = []
        task_types = [TaskType.STOCK_BASIC]
        priority = TaskPriority.LOW

        tasks = self.task_builder.build_tasks(symbols, task_types, priority)

        # stock_basic组即使symbols为空也应该创建任务
        assert len(tasks) == 1
        assert tasks[0].symbol == ""
        assert tasks[0].task_type == TaskType.STOCK_BASIC
        assert tasks[0].priority == TaskPriority.LOW

    def test_build_tasks_with_empty_task_types(self):
        """测试空任务类型列表"""
        symbols = ["000001.SZ"]
        task_types = []
        priority = TaskPriority.LOW

        tasks = self.task_builder.build_tasks(symbols, task_types, priority)

        assert len(tasks) == 0

    def test_build_tasks_with_default_priority(self):
        """测试默认优先级"""
        symbols = ["000001.SZ"]
        task_types = [TaskType.STOCK_BASIC]
        priority = TaskPriority.MEDIUM  # 显式指定优先级

        tasks = self.task_builder.build_tasks(symbols, task_types, priority)

        assert len(tasks) == 1
        assert tasks[0].priority == TaskPriority.MEDIUM

    def test_build_tasks_returns_download_task_config_instances(self):
        """测试返回的任务都是 DownloadTaskConfig 实例"""
        symbols = ["000001.SZ", "000002.SZ"]
        task_types = [TaskType.STOCK_BASIC]
        priority = TaskPriority.LOW

        tasks = self.task_builder.build_tasks(symbols, task_types, priority)

        for task in tasks:
            assert isinstance(task, DownloadTaskConfig)
            assert hasattr(task, "symbol")
            assert hasattr(task, "task_type")
            assert hasattr(task, "priority")
