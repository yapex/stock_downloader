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
def task_bus_with_processor(mock_data_processor):
    """创建带有数据处理器的任务总线实例"""
    from huey import SqliteHuey
    # 使用SqliteHuey但配置为内存模式，并设置immediate=True确保任务立即执行
    immediate_huey = SqliteHuey(filename=':memory:', immediate=True)
    return HueyTaskBus(data_processor=mock_data_processor, huey_instance=immediate_huey)


@pytest.fixture
def task_bus_without_processor():
    """创建不带数据处理器的任务总线实例"""
    from huey import MemoryHuey
    # 使用immediate模式的MemoryHuey确保任务立即执行
    immediate_huey = MemoryHuey(immediate=True)
    return HueyTaskBus(data_processor=None, huey_instance=immediate_huey)


class TestHueyTaskBus:
    """HueyTaskBus 类的测试用例"""

    def test_init(self, task_bus_with_processor):
        """测试 HueyTaskBus 初始化"""
        assert hasattr(task_bus_with_processor, "huey")
        assert hasattr(task_bus_with_processor, "data_processor")

    def test_data_processor_injection(self):
        """测试数据处理器注入"""
        mock_processor = MockDataProcessor()
        task_bus = HueyTaskBus(data_processor=mock_processor)

        assert task_bus.data_processor is mock_processor

    def test_init_without_data_processor(self):
        """测试不注入数据处理器的初始化"""
        task_bus = HueyTaskBus(data_processor=None)
        
        assert task_bus.data_processor is None
        assert hasattr(task_bus, 'huey')
        assert hasattr(task_bus, 'process_task_result')

    def test_init_sets_global_data_processor(self):
        """测试初始化时设置全局数据处理器"""
        mock_processor = MockDataProcessor()
        
        # Mock tasks模块以验证set_data_processor被调用
        import unittest.mock
        with unittest.mock.patch('neo.task_bus.tasks.set_data_processor') as mock_set_processor:
            task_bus = HueyTaskBus(data_processor=mock_processor)
            
            # 验证set_data_processor被调用
            mock_set_processor.assert_called_once_with(mock_processor)
            
        assert task_bus.data_processor is mock_processor

    def test_init_skips_global_data_processor_when_none(self):
        """测试当数据处理器为None时跳过设置全局数据处理器"""
        # Mock tasks模块
        import unittest.mock
        with unittest.mock.patch('neo.task_bus.tasks.set_data_processor') as mock_set_processor:
            task_bus = HueyTaskBus(data_processor=None)
            
            # 验证set_data_processor没有被调用
            mock_set_processor.assert_not_called()
            assert task_bus.data_processor is None
    
    def test_huey_instance_injection(self):
        """测试Huey实例的依赖注入"""
        from huey import MemoryHuey
        custom_huey = MemoryHuey()
        
        task_bus = HueyTaskBus(data_processor=None, huey_instance=custom_huey)
        
        # 验证注入的huey实例被正确设置
        assert task_bus.huey is custom_huey
        assert isinstance(task_bus.huey, MemoryHuey)
    
    def test_huey_instance_defaults_to_global(self):
        """测试未提供huey_instance时使用全局实例"""
        from neo.task_bus.huey_task_bus import huey
        
        task_bus = HueyTaskBus(data_processor=None)
        
        assert task_bus.huey is huey

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

    # NOTE: 反序列化逻辑已移至 tasks.py，不再在 HueyTaskBus 类中测试
    # 在实际使用中，反序列化由 Huey 任务函数处理

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

    def test_submit_task_exception_handling(self, task_bus_with_processor):
        """测试提交任务时的异常处理"""
        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.stock_basic,
            priority=TaskPriority.HIGH,
        )
        
        # 创建一个会导致序列化失败的任务结果
        task_result = TaskResult(config=config, success=True, data=None)
        
        # Mock _serialize_task_result 方法抛出异常
        import unittest.mock
        with unittest.mock.patch.object(
            task_bus_with_processor, '_serialize_task_result', 
            side_effect=Exception("Serialization failed")
        ):
            with pytest.raises(Exception, match="Serialization failed"):
                task_bus_with_processor.submit_task(task_result)

    def test_get_data_processor_with_injected_processor(self, task_bus_with_processor):
        """测试获取注入的数据处理器"""
        processor = task_bus_with_processor._get_data_processor()
        assert processor is task_bus_with_processor.data_processor
        assert isinstance(processor, MockDataProcessor)

    def test_get_data_processor_without_injected_processor(self, task_bus_without_processor):
        """测试获取默认数据处理器"""
        processor = task_bus_without_processor._get_data_processor()
        # 验证返回的是SimpleDataProcessor实例
        from neo.data_processor.simple_data_processor import SimpleDataProcessor
        assert isinstance(processor, SimpleDataProcessor)
        # 验证不是注入的处理器
        assert processor is not task_bus_without_processor.data_processor

    def test_start_consumer_immediate_mode(self, task_bus_with_processor):
        """测试在immediate模式下启动消费者"""
        # 确保huey处于immediate模式
        task_bus_with_processor.huey.immediate = True
        
        # 调用start_consumer应该直接返回，不启动消费者
        # 这个测试主要验证方法不会抛出异常
        task_bus_with_processor.start_consumer()
        
        # 验证immediate模式被正确检查
        assert task_bus_with_processor.huey.immediate is True

    def test_start_consumer_production_mode(self, task_bus_with_processor):
        """测试在生产模式下启动消费者"""
        # 设置huey为非immediate模式
        task_bus_with_processor.huey.immediate = False
        
        # Mock Consumer类以避免实际启动消费者进程
        import unittest.mock
        with unittest.mock.patch('huey.consumer.Consumer') as mock_consumer_class:
            mock_consumer = Mock()
            mock_consumer_class.return_value = mock_consumer
            
            task_bus_with_processor.start_consumer()
            
            # 验证Consumer被正确创建和启动
            mock_consumer_class.assert_called_once_with(task_bus_with_processor.huey)
            mock_consumer.run.assert_called_once()
