"""DataProcessorFactory 单元测试"""

from unittest.mock import Mock, patch, PropertyMock

import pytest

from src.neo.data_processor.data_processor_factory import DataProcessorFactory
from src.neo.data_processor.full_replace_data_processor import FullReplaceDataProcessor
from src.neo.data_processor.simple_data_processor import SimpleDataProcessor


class TestDataProcessorFactory:
    """DataProcessorFactory 测试类"""

    @pytest.fixture
    def mock_container(self):
        """模拟依赖注入容器"""
        container = Mock()
        container.full_replace_data_processor.return_value = Mock(spec=FullReplaceDataProcessor)
        container.data_processor.return_value = Mock(spec=SimpleDataProcessor)
        return container

    @pytest.fixture
    def mock_config(self):
        """模拟配置对象"""
        config = Mock()
        config.download_tasks = Mock()
        
        # 设置 stock_basic 为全量替换
        stock_basic_config = Mock()
        stock_basic_config.update_strategy = "full_replace"
        config.download_tasks.stock_basic = stock_basic_config
        
        # 其他任务类型没有特殊配置，使用默认策略
        return config

    @pytest.fixture
    def factory(self, mock_container, mock_config):
        """创建工厂实例"""
        with patch('src.neo.data_processor.data_processor_factory.get_config', return_value=mock_config):
            return DataProcessorFactory(mock_container)

    def test_create_full_replace_processor_for_stock_basic(self, factory, mock_container):
        """测试为 stock_basic 创建全量替换处理器"""
        processor = factory.create_processor('stock_basic')
        
        # 验证调用了正确的容器方法
        mock_container.full_replace_data_processor.assert_called_once()
        mock_container.data_processor.assert_not_called()
        
        # 验证返回的是全量替换处理器
        assert processor is mock_container.full_replace_data_processor.return_value

    def test_create_simple_processor_for_stock_daily(self, factory, mock_container):
        """测试为 stock_daily 创建简单处理器"""
        processor = factory.create_processor('stock_daily')
        
        # 验证调用了正确的容器方法
        mock_container.data_processor.assert_called_once()
        mock_container.full_replace_data_processor.assert_not_called()
        
        # 验证返回的是简单处理器
        assert processor is mock_container.data_processor.return_value

    def test_create_simple_processor_for_unknown_table(self, factory, mock_container):
        """测试为未知表类型创建简单处理器（默认行为）"""
        processor = factory.create_processor('unknown_table')
        
        # 验证调用了正确的容器方法
        mock_container.data_processor.assert_called_once()
        mock_container.full_replace_data_processor.assert_not_called()
        
        # 验证返回的是简单处理器
        assert processor is mock_container.data_processor.return_value

    def test_get_update_strategy_from_config(self, mock_container):
        """测试从配置中获取更新策略"""
        # 创建配置，包含多种更新策略
        config = Mock()
        config.download_tasks = Mock()
        
        # stock_basic 使用全量替换
        stock_basic_config = Mock()
        stock_basic_config.update_strategy = "full_replace"
        config.download_tasks.stock_basic = stock_basic_config
        
        # stock_daily 使用增量更新
        stock_daily_config = Mock()
        stock_daily_config.update_strategy = "incremental"
        config.download_tasks.stock_daily = stock_daily_config
        
        with patch('src.neo.data_processor.data_processor_factory.get_config', return_value=config):
            factory = DataProcessorFactory(mock_container)
            
            # 测试获取配置的策略
            assert factory._get_update_strategy('stock_basic') == "full_replace"
            assert factory._get_update_strategy('stock_daily') == "incremental"

    def test_get_update_strategy_default(self, mock_container):
        """测试获取默认更新策略"""
        config = Mock()
        config.download_tasks = Mock(spec=[])
        
        with patch('src.neo.data_processor.data_processor_factory.get_config', return_value=config):
            factory = DataProcessorFactory(mock_container)
            
            # 测试未配置的任务类型返回默认策略
            assert factory._get_update_strategy('unknown_table') == "incremental"

    def test_get_update_strategy_missing_attribute(self, mock_container):
        """测试任务配置存在但缺少 update_strategy 属性"""
        config = Mock()
        config.download_tasks = Mock()
        
        # 任务配置存在但没有 update_strategy 属性
        task_config = Mock(spec=[])
        config.download_tasks.stock_basic = task_config
        
        with patch('src.neo.data_processor.data_processor_factory.get_config', return_value=config):
            factory = DataProcessorFactory(mock_container)
            
            # 应该返回默认策略
            assert factory._get_update_strategy('stock_basic') == "incremental"

    def test_get_update_strategy_exception_handling(self, mock_container):
        """测试获取更新策略时的异常处理"""
        config = Mock()
        
        # 模拟 download_tasks 属性访问时抛出异常
        type(config).download_tasks = PropertyMock(side_effect=AttributeError("Config access error"))
        
        with patch('src.neo.data_processor.data_processor_factory.get_config', return_value=config):
            factory = DataProcessorFactory(mock_container)
            
            # 异常情况下应该返回默认策略
            assert factory._get_update_strategy('stock_basic') == "incremental"

    def test_multiple_processor_creation(self, factory, mock_container):
        """测试多次创建处理器"""
        # 重置 mock 调用计数
        mock_container.reset_mock()
        
        # 创建多个不同类型的处理器
        processor1 = factory.create_processor('stock_basic')
        processor2 = factory.create_processor('stock_daily')
        processor3 = factory.create_processor('stock_basic')
        
        # 验证调用次数
        assert mock_container.full_replace_data_processor.call_count == 2
        assert mock_container.data_processor.call_count == 1
        
        # 验证返回的处理器
        assert processor1 is mock_container.full_replace_data_processor.return_value
        assert processor2 is mock_container.data_processor.return_value
        assert processor3 is mock_container.full_replace_data_processor.return_value

    @pytest.mark.parametrize("task_type,expected_strategy,expected_method", [
        ('stock_basic', 'full_replace', 'full_replace_data_processor'),
        ('stock_daily', 'incremental', 'data_processor'),
        ('daily_basic', 'incremental', 'data_processor'),
        ('trade_cal', 'incremental', 'data_processor'),
        ('unknown_table', 'incremental', 'data_processor'),
    ])
    def test_processor_type_mapping(self, mock_container, task_type, expected_strategy, expected_method):
        """参数化测试不同任务类型对应的处理器选择"""
        config = Mock()
        config.download_tasks = Mock()
        
        if expected_strategy == 'full_replace':
            # 为需要全量替换的任务类型设置配置
            task_config = Mock()
            task_config.update_strategy = "full_replace"
            setattr(config.download_tasks, task_type, task_config)
        
        with patch('src.neo.data_processor.data_processor_factory.get_config', return_value=config):
            factory = DataProcessorFactory(mock_container)
            
            # 重置 mock 调用计数
            mock_container.reset_mock()
            
            # 创建处理器
            processor = factory.create_processor(task_type)
            
            # 验证调用了正确的容器方法
            expected_mock = getattr(mock_container, expected_method)
            expected_mock.assert_called_once()
            
            # 验证返回的处理器
            assert processor is expected_mock.return_value

    def test_factory_initialization(self, mock_container, mock_config):
        """测试工厂初始化"""
        with patch('src.neo.data_processor.data_processor_factory.get_config', return_value=mock_config):
            factory = DataProcessorFactory(mock_container)
            
            # 验证依赖被正确存储
            assert factory.container is mock_container
            assert factory.config is mock_config

    def test_config_access_during_initialization(self, mock_container):
        """测试初始化时配置访问"""
        mock_config = Mock()
        
        with patch('src.neo.data_processor.data_processor_factory.get_config', return_value=mock_config) as mock_get_config:
            DataProcessorFactory(mock_container)
            
            # 验证配置被正确获取
            mock_get_config.assert_called_once()