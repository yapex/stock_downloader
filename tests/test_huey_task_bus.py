import pandas as pd
from unittest.mock import Mock, patch
from huey import MemoryHuey
from neo.task_bus.huey_task_bus import HueyTaskBus, get_huey, process_task_result
from neo.task_bus.types import TaskResult, DownloadTaskConfig, TaskType, TaskPriority
from neo.data_processor.interfaces import IDataProcessor


class MockDataProcessor(IDataProcessor):
    """模拟数据处理器"""

    def __init__(self):
        self.processed_results = []
        self.should_succeed = True

    def process(self, task_result: TaskResult) -> bool:
        """模拟处理任务结果"""
        self.processed_results.append(task_result)
        return self.should_succeed


class TestHueyTaskBus:
    """HueyTaskBus 类的测试用例"""

    def setup_method(self):
        """每个测试方法执行前的设置"""
        # 创建内存模式的 Huey 实例
        self.test_huey = MemoryHuey(immediate=True)
        self.mock_data_processor = MockDataProcessor()

        # 使用 patch 替换全局 Huey 实例和 huey 变量
        self.huey_patcher = patch(
            "neo.task_bus.huey_task_bus._huey_instance", self.test_huey
        )
        self.huey_var_patcher = patch("neo.task_bus.huey_task_bus.huey", self.test_huey)
        self.huey_patcher.start()
        self.huey_var_patcher.start()

        # 重新注册任务到测试 Huey 实例
        import neo.task_bus.huey_task_bus as huey_module

        # 获取原始函数（未装饰的版本）
        original_func = (
            huey_module.process_task_result.__wrapped__
            if hasattr(huey_module.process_task_result, "__wrapped__")
            else huey_module.process_task_result
        )

        # 用测试 Huey 实例重新装饰
        self.test_task = self.test_huey.task(
            name="process_task_result", retries=2, retry_delay=5
        )(original_func)

        # 替换模块中的任务函数
        self.task_func_patcher = patch.object(
            huey_module, "process_task_result", self.test_task
        )
        self.task_func_patcher.start()

        # 创建 HueyTaskBus 实例
        self.task_bus = HueyTaskBus()

    def teardown_method(self):
        """每个测试方法执行后的清理"""
        self.huey_patcher.stop()
        self.huey_var_patcher.stop()
        self.task_func_patcher.stop()
        # 重置全局实例
        import neo.task_bus.huey_task_bus

        neo.task_bus.huey_task_bus._huey_instance = None

    def test_init(self):
        """测试 HueyTaskBus 初始化"""
        assert self.task_bus.huey == self.test_huey
        assert hasattr(self.task_bus, "huey")

    def test_get_huey_singleton(self):
        """测试 get_huey 单例模式"""
        with patch("neo.task_bus.huey_task_bus.get_config") as mock_config:
            mock_config.return_value.huey.db_file = ":memory:"
            mock_config.return_value.huey.immediate = True

            # 重置全局实例
            import neo.task_bus.huey_task_bus

            neo.task_bus.huey_task_bus._huey_instance = None

            huey1 = get_huey()
            huey2 = get_huey()

            assert huey1 is huey2

    def test_serialize_task_result_success(self):
        """测试成功任务结果的序列化"""
        # 创建测试数据
        test_data = pd.DataFrame(
            {"ts_code": ["000001.SZ", "000002.SZ"], "name": ["平安银行", "万科A"]}
        )

        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.STOCK_BASIC,
            priority=TaskPriority.HIGH,
            max_retries=3,
        )

        task_result = TaskResult(
            config=config, success=True, data=test_data, retry_count=1
        )

        serialized = self.task_bus._serialize_task_result(task_result)

        assert serialized["config"]["symbol"] == "000001.SZ"
        assert serialized["config"]["task_type"] == TaskType.STOCK_BASIC.value
        assert serialized["config"]["priority"] == TaskPriority.HIGH.value
        assert serialized["config"]["max_retries"] == 3
        assert serialized["success"] is True
        assert serialized["data"] is not None
        assert serialized["error"] is None
        assert serialized["retry_count"] == 1

    def test_serialize_task_result_failure(self):
        """测试失败任务结果的序列化"""
        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.STOCK_BASIC,
            priority=TaskPriority.MEDIUM,
        )

        error = Exception("Connection failed")
        task_result = TaskResult(config=config, success=False, error=error)

        serialized = self.task_bus._serialize_task_result(task_result)

        assert serialized["success"] is False
        assert serialized["data"] is None
        assert serialized["error"] == "Connection failed"
        assert serialized["retry_count"] == 0

    def test_deserialize_task_result_success(self):
        """测试成功任务结果的反序列化"""
        serialized_data = {
            "config": {
                "symbol": "000001.SZ",
                "task_type": TaskType.STOCK_BASIC.value,
                "priority": TaskPriority.HIGH.value,
                "max_retries": 3,
            },
            "success": True,
            "data": {
                "ts_code": {0: "000001.SZ", 1: "000002.SZ"},
                "name": {0: "平安银行", 1: "万科A"},
            },
            "error": None,
            "retry_count": 1,
        }

        task_result = self.task_bus._deserialize_task_result(serialized_data)

        assert task_result.config.symbol == "000001.SZ"
        assert task_result.config.task_type == TaskType.STOCK_BASIC
        assert task_result.config.priority == TaskPriority.HIGH
        assert task_result.config.max_retries == 3
        assert task_result.success is True
        assert task_result.data is not None
        assert len(task_result.data) == 2
        assert task_result.error is None
        assert task_result.retry_count == 1

    def test_deserialize_task_result_failure(self):
        """测试失败任务结果的反序列化"""
        serialized_data = {
            "config": {
                "symbol": "000001.SZ",
                "task_type": TaskType.STOCK_BASIC.value,
                "priority": TaskPriority.MEDIUM.value,
                "max_retries": 3,
            },
            "success": False,
            "data": None,
            "error": "Connection failed",
            "retry_count": 2,
        }

        task_result = self.task_bus._deserialize_task_result(serialized_data)

        assert task_result.success is False
        assert task_result.data is None
        assert str(task_result.error) == "Connection failed"
        assert task_result.retry_count == 2

    @patch("neo.task_bus.huey_task_bus.process_task_result")
    def test_submit_task(self, mock_process_task):
        """测试任务提交到队列"""
        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.STOCK_BASIC,
            priority=TaskPriority.HIGH,
        )

        test_data = pd.DataFrame({"ts_code": ["000001.SZ"], "name": ["平安银行"]})

        task_result = TaskResult(config=config, success=True, data=test_data)

        # 提交任务
        self.task_bus.submit_task(task_result)

        # 验证任务被调用
        mock_process_task.assert_called_once()

        # 获取调用参数
        call_args = mock_process_task.call_args[0][0]
        assert call_args["config"]["symbol"] == "000001.SZ"
        assert call_args["config"]["task_type"] == TaskType.STOCK_BASIC.value
        assert call_args["success"] is True


class TestProcessTaskResult:
    """测试 process_task_result 函数的基本功能"""

    def test_process_task_result_function_exists(self):
        """测试 process_task_result 函数存在且可导入"""
        assert callable(process_task_result)

    def test_process_task_result_is_huey_task(self):
        """测试 process_task_result 是 Huey 任务"""
        # 检查是否有 Huey 任务的属性
        assert hasattr(process_task_result, "__wrapped__") or hasattr(
            process_task_result, "task_class"
        )


class TestHueyTaskBusIntegration:
    """HueyTaskBus 集成测试"""

    def setup_method(self):
        """每个测试方法执行前的设置"""
        # 创建内存模式的 Huey 实例
        self.test_huey = MemoryHuey(immediate=True)
        self.mock_data_processor = MockDataProcessor()

        # 使用 patch 替换全局 Huey 实例和 huey 变量
        self.huey_patcher = patch(
            "neo.task_bus.huey_task_bus._huey_instance", self.test_huey
        )
        self.huey_var_patcher = patch("neo.task_bus.huey_task_bus.huey", self.test_huey)
        self.huey_patcher.start()
        self.huey_var_patcher.start()

        # 重新注册任务到测试 Huey 实例
        # 需要重新导入并重新装饰 process_task_result 函数
        import neo.task_bus.huey_task_bus as huey_module

        # 获取原始函数（未装饰的版本）
        original_func = (
            huey_module.process_task_result.__wrapped__
            if hasattr(huey_module.process_task_result, "__wrapped__")
            else huey_module.process_task_result
        )

        # 用测试 Huey 实例重新装饰
        self.test_task = self.test_huey.task(
            name="process_task_result", retries=2, retry_delay=5
        )(original_func)

        # 替换模块中的任务函数
        self.task_func_patcher = patch.object(
            huey_module, "process_task_result", self.test_task
        )
        self.task_func_patcher.start()

        # 创建 HueyTaskBus 实例
        self.task_bus = HueyTaskBus()

    def teardown_method(self):
        """每个测试方法执行后的清理"""
        self.huey_patcher.stop()
        # 重置全局实例
        import neo.task_bus.huey_task_bus

        neo.task_bus.huey_task_bus._huey_instance = None

    @patch("neo.data_processor.simple_data_processor.SimpleDataProcessor")
    def test_end_to_end_task_processing(self, mock_processor_class, capfd):
        """测试端到端的任务处理流程"""
        # 设置模拟数据处理器
        mock_processor = Mock()
        mock_processor.process.return_value = True
        mock_processor_class.return_value = mock_processor

        # 创建任务配置和结果
        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.STOCK_BASIC,
            priority=TaskPriority.HIGH,
        )

        test_data = pd.DataFrame({"ts_code": ["000001.SZ"], "name": ["平安银行"]})

        task_result = TaskResult(config=config, success=True, data=test_data)

        # 提交任务（由于使用 immediate=True，任务会立即执行）
        self.task_bus.submit_task(task_result)

        # 验证数据处理器被调用
        mock_processor.process.assert_called_once()

        # 验证调用参数
        processed_task_result = mock_processor.process.call_args[0][0]
        assert processed_task_result.config.symbol == "000001.SZ"
        assert processed_task_result.config.task_type == TaskType.STOCK_BASIC
        assert processed_task_result.success is True
        assert processed_task_result.data is not None

        # 不再检查日志输出内容
