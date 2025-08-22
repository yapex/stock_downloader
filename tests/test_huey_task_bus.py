import pandas as pd
import pytest
from unittest.mock import Mock
from neo.task_bus.huey_task_bus import HueyTaskBus
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


@pytest.fixture
def mock_data_processor():
    """创建模拟数据处理器"""
    return MockDataProcessor()


@pytest.fixture
def mock_config():
    """创建模拟配置对象"""
    config = Mock()
    config.huey = Mock()
    config.huey.db_file = ":memory:"
    config.huey.get = Mock(return_value=True)  # immediate=True for testing
    return config


# 移除异步配置，专注于同步测试


@pytest.fixture
def task_bus_with_processor(mock_data_processor, mock_config):
    """创建带数据处理器的任务总线"""
    return HueyTaskBus(data_processor=mock_data_processor, config=mock_config)


@pytest.fixture
def task_bus_without_processor(mock_config):
    """创建不带数据处理器的任务总线"""
    return HueyTaskBus(data_processor=None, config=mock_config)


class TestHueyTaskBus:
    """HueyTaskBus 类的测试用例"""

    def test_init(self, task_bus_with_processor):
        """测试 HueyTaskBus 初始化"""
        assert hasattr(task_bus_with_processor, "huey")
        assert hasattr(task_bus_with_processor, "data_processor")
        assert hasattr(task_bus_with_processor, "config")

    def test_data_processor_injection(self, mock_config):
        """测试数据处理器注入"""
        mock_processor = MockDataProcessor()
        task_bus = HueyTaskBus(data_processor=mock_processor, config=mock_config)

        assert task_bus.data_processor is mock_processor

    def test_serialize_task_result_success(self, task_bus_with_processor):
        """测试成功任务结果的序列化"""
        # 创建测试数据
        test_data = pd.DataFrame(
            {"ts_code": ["000001.SZ", "000002.SZ"], "name": ["平安银行", "万科A"]}
        )

        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.stock_basic,
            priority=TaskPriority.HIGH,
            max_retries=3,
        )

        task_result = TaskResult(
            config=config, success=True, data=test_data, retry_count=1
        )

        serialized = task_bus_with_processor._serialize_task_result(task_result)

        assert serialized["config"]["symbol"] == "000001.SZ"
        assert serialized["config"]["task_type"] == TaskType.stock_basic.value
        assert serialized["config"]["priority"] == TaskPriority.HIGH.value
        assert serialized["config"]["max_retries"] == 3
        assert serialized["success"] is True
        assert serialized["data"] is not None
        assert serialized["error"] is None
        assert serialized["retry_count"] == 1

    def test_serialize_task_result_failure(self, task_bus_with_processor):
        """测试失败任务结果的序列化"""
        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.stock_basic,
            priority=TaskPriority.MEDIUM,
        )

        error = Exception("Connection failed")
        task_result = TaskResult(config=config, success=False, error=error)

        serialized = task_bus_with_processor._serialize_task_result(task_result)

        assert serialized["success"] is False
        assert serialized["data"] is None
        assert serialized["error"] == "Connection failed"
        assert serialized["retry_count"] == 0

    def test_deserialize_task_result_success(self, task_bus_with_processor):
        """测试成功任务结果的反序列化"""
        serialized_data = {
            "config": {
                "symbol": "000001.SZ",
                "task_type": TaskType.stock_basic.value,
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

        task_result = task_bus_with_processor._deserialize_task_result(serialized_data)

        assert task_result.config.symbol == "000001.SZ"
        assert task_result.config.task_type == TaskType.stock_basic
        assert task_result.config.priority == TaskPriority.HIGH
        assert task_result.config.max_retries == 3
        assert task_result.success is True
        assert task_result.data is not None
        assert len(task_result.data) == 2
        assert task_result.error is None
        assert task_result.retry_count == 1

    def test_deserialize_task_result_failure(self, task_bus_with_processor):
        """测试失败任务结果的反序列化"""
        serialized_data = {
            "config": {
                "symbol": "000001.SZ",
                "task_type": TaskType.stock_basic.value,
                "priority": TaskPriority.MEDIUM.value,
                "max_retries": 3,
            },
            "success": False,
            "data": None,
            "error": "Connection failed",
            "retry_count": 2,
        }

        task_result = task_bus_with_processor._deserialize_task_result(serialized_data)

        assert task_result.success is False
        assert task_result.data is None
        assert str(task_result.error) == "Connection failed"
        assert task_result.retry_count == 2

    def test_submit_task(self, task_bus_with_processor, mock_data_processor):
        """测试任务提交到队列"""
        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.stock_basic,
            priority=TaskPriority.HIGH,
        )

        test_data = pd.DataFrame({"ts_code": ["000001.SZ"], "name": ["平安银行"]})
        task_result = TaskResult(config=config, success=True, data=test_data)

        # 提交任务
        task_bus_with_processor.submit_task(task_result)

        # 验证任务被正确序列化和提交（通过检查测试 huey 实例中的任务）
        # 由于使用 immediate=True，任务会立即执行，所以我们检查数据处理器是否被调用
        assert len(mock_data_processor.processed_results) == 1
        processed_result = mock_data_processor.processed_results[0]
        assert processed_result.config.symbol == "000001.SZ"
        assert processed_result.success is True


class TestHueyTaskBusIntegration:
    """HueyTaskBus 集成测试"""

    def test_end_to_end_task_processing(
        self, task_bus_with_processor, mock_data_processor
    ):
        """测试端到端的任务处理流程"""
        # 创建任务配置和结果
        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.stock_basic,
            priority=TaskPriority.HIGH,
        )

        test_data = pd.DataFrame({"ts_code": ["000001.SZ"], "name": ["平安银行"]})

        task_result = TaskResult(config=config, success=True, data=test_data)

        # 提交任务（由于使用 immediate=True，任务会立即执行）
        task_bus_with_processor.submit_task(task_result)

        # 验证数据处理器被调用
        assert len(mock_data_processor.processed_results) == 1

        # 验证调用参数
        processed_task_result = mock_data_processor.processed_results[0]
        assert processed_task_result.config.symbol == "000001.SZ"
        assert processed_task_result.config.task_type == TaskType.stock_basic
        assert processed_task_result.success is True
        assert processed_task_result.data is not None

        # 不再检查日志输出内容

    # 移除异步测试，专注于同步测试和核心逻辑验证
