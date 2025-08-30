"""TaskBuilder 类的测试用例"""

from neo.helpers.task_builder import TaskBuilder, DownloadTaskConfig


class TestTaskBuilder:
    """TaskBuilder 类的测试"""

    def test_create_default(self):
        """测试创建默认的 TaskBuilder 实例"""
        builder = TaskBuilder.create_default()

        assert isinstance(builder, TaskBuilder)
        # Protocol 接口检查在运行时不可用，只检查方法存在
        assert hasattr(builder, "build_tasks")
        assert callable(getattr(builder, "build_tasks"))

    def test_build_tasks_with_symbols_and_task_types(self):
        """测试有股票代码和任务类型的情况"""
        builder = TaskBuilder()
        symbols = ["000001.SZ", "000002.SZ"]
        task_types = ["stock_daily", "daily_basic"]

        tasks = builder.build_tasks(symbols, task_types)

        # 验证任务数量：2个股票 × 2个任务类型 = 4个任务
        assert len(tasks) == 4

        # 验证任务内容
        expected_tasks = [
            ("000001.SZ", "stock_daily"),
            ("000001.SZ", "daily_basic"),
            ("000002.SZ", "stock_daily"),
            ("000002.SZ", "daily_basic"),
        ]

        for i, (expected_symbol, expected_task_type) in enumerate(expected_tasks):
            assert tasks[i].symbol == expected_symbol
            assert tasks[i].task_type == expected_task_type
            assert isinstance(tasks[i], DownloadTaskConfig)

    def test_build_tasks_without_symbols(self):
        """测试无股票代码的情况（如 stock_basic 组）"""
        builder = TaskBuilder()
        symbols = []
        task_types = ["stock_basic", "income"]

        tasks = builder.build_tasks(symbols, task_types)

        # 验证任务数量：2个任务类型
        assert len(tasks) == 2

        # 验证任务内容
        assert tasks[0].symbol == ""
        assert tasks[0].task_type == "stock_basic"
        assert tasks[1].symbol == ""
        assert tasks[1].task_type == "income"

        for task in tasks:
            assert isinstance(task, DownloadTaskConfig)

    def test_build_tasks_with_none_symbols(self):
        """测试 symbols 为 None 的情况"""
        builder = TaskBuilder()
        symbols = None
        task_types = ["stock_basic"]

        # symbols 为 None 时，应该按照无股票代码的逻辑处理
        tasks = builder.build_tasks(symbols, task_types)

        assert len(tasks) == 1
        assert tasks[0].symbol == ""
        assert tasks[0].task_type == "stock_basic"

    def test_build_tasks_empty_task_types(self):
        """测试空任务类型列表"""
        builder = TaskBuilder()
        symbols = ["000001.SZ"]
        task_types = []

        tasks = builder.build_tasks(symbols, task_types)

        # 空任务类型列表应该返回空任务列表
        assert len(tasks) == 0
        assert tasks == []

    def test_build_tasks_none_task_types(self):
        """测试 task_types 为 None 的情况"""
        builder = TaskBuilder()
        symbols = ["000001.SZ"]
        task_types = None

        tasks = builder.build_tasks(symbols, task_types)

        # None 任务类型列表应该返回空任务列表
        assert len(tasks) == 0
        assert tasks == []

    def test_build_tasks_empty_symbols_and_task_types(self):
        """测试空股票代码和空任务类型"""
        builder = TaskBuilder()
        symbols = []
        task_types = []

        tasks = builder.build_tasks(symbols, task_types)

        assert len(tasks) == 0
        assert tasks == []

    def test_build_tasks_single_symbol_single_task_type(self):
        """测试单个股票代码和单个任务类型"""
        builder = TaskBuilder()
        symbols = ["600000.SH"]
        task_types = ["stock_daily"]

        tasks = builder.build_tasks(symbols, task_types)

        assert len(tasks) == 1
        assert tasks[0].symbol == "600000.SH"
        assert tasks[0].task_type == "stock_daily"
        assert isinstance(tasks[0], DownloadTaskConfig)

    def test_build_tasks_multiple_symbols_single_task_type(self):
        """测试多个股票代码和单个任务类型"""
        builder = TaskBuilder()
        symbols = ["000001.SZ", "000002.SZ", "600000.SH"]
        task_types = ["stock_daily"]

        tasks = builder.build_tasks(symbols, task_types)

        assert len(tasks) == 3
        expected_symbols = ["000001.SZ", "000002.SZ", "600000.SH"]

        for i, expected_symbol in enumerate(expected_symbols):
            assert tasks[i].symbol == expected_symbol
            assert tasks[i].task_type == "stock_daily"

    def test_build_tasks_single_symbol_multiple_task_types(self):
        """测试单个股票代码和多个任务类型"""
        builder = TaskBuilder()
        symbols = ["000001.SZ"]
        task_types = [
            "stock_daily",
            "daily_basic",
            "stock_adj_qfq",
        ]

        tasks = builder.build_tasks(symbols, task_types)

        assert len(tasks) == 3
        expected_task_types = [
            "stock_daily",
            "daily_basic",
            "stock_adj_qfq",
        ]

        for i, expected_task_type in enumerate(expected_task_types):
            assert tasks[i].symbol == "000001.SZ"
            assert tasks[i].task_type == expected_task_type

    def test_build_tasks_mixed_scenarios(self):
        """测试混合场景：包含需要股票代码和不需要股票代码的任务类型"""
        builder = TaskBuilder()

        # 测试有股票代码的场景
        symbols_with_codes = ["000001.SZ"]
        task_types_with_codes = ["stock_daily"]
        tasks_with_codes = builder.build_tasks(
            symbols_with_codes, task_types_with_codes
        )

        # 测试无股票代码的场景
        symbols_without_codes = []
        task_types_without_codes = ["stock_basic"]
        tasks_without_codes = builder.build_tasks(
            symbols_without_codes, task_types_without_codes
        )

        # 验证结果
        assert len(tasks_with_codes) == 1
        assert tasks_with_codes[0].symbol == "000001.SZ"
        assert tasks_with_codes[0].task_type == "stock_daily"

        assert len(tasks_without_codes) == 1
        assert tasks_without_codes[0].symbol == ""
        assert tasks_without_codes[0].task_type == "stock_basic"

    def test_task_builder_implements_interface(self):
        """测试 TaskBuilder 实现了 ITaskBuilder 接口"""
        builder = TaskBuilder()

        # Protocol 接口检查在运行时不可用，只验证接口方法可调用
        assert hasattr(builder, "build_tasks")
        assert callable(getattr(builder, "build_tasks"))

    def test_build_tasks_return_type(self):
        """测试 build_tasks 方法的返回类型"""
        builder = TaskBuilder()
        symbols = ["000001.SZ"]
        task_types = ["stock_daily"]

        tasks = builder.build_tasks(symbols, task_types)

        # 验证返回类型
        assert isinstance(tasks, list)
        assert all(isinstance(task, DownloadTaskConfig) for task in tasks)

        # 验证列表中的元素类型
        if tasks:
            assert hasattr(tasks[0], "symbol")
            assert hasattr(tasks[0], "task_type")
