"""DataProcessorFactory 单元测试"""

from unittest.mock import Mock

import pytest

from src.neo.data_processor.data_processor_factory import DataProcessorFactory
from src.neo.data_processor.full_replace_data_processor import FullReplaceDataProcessor
from src.neo.data_processor.simple_data_processor import SimpleDataProcessor
from src.neo.database.interfaces import IDBQueryer
from src.neo.database.schema_loader import SchemaLoader
from src.neo.writers.interfaces import IParquetWriter


class TestDataProcessorFactory:
    """DataProcessorFactory 测试类"""

    @pytest.fixture
    def mock_parquet_writer(self):
        """模拟 ParquetWriter"""
        return Mock(spec=IParquetWriter)

    @pytest.fixture
    def mock_db_queryer(self):
        """模拟数据库查询器"""
        return Mock(spec=IDBQueryer)

    @pytest.fixture
    def mock_schema_loader(self):
        """模拟 Schema 加载器"""
        return Mock(spec=SchemaLoader)

    @pytest.fixture
    def factory(self, mock_parquet_writer, mock_db_queryer, mock_schema_loader):
        """创建工厂实例"""
        return DataProcessorFactory(
            parquet_writer=mock_parquet_writer,
            db_queryer=mock_db_queryer,
            schema_loader=mock_schema_loader,
        )

    def test_create_full_replace_processor_for_stock_basic(self, factory):
        """测试为 stock_basic 创建全量替换处理器"""
        processor = factory.create_processor('stock_basic')
        
        assert isinstance(processor, FullReplaceDataProcessor)

    def test_create_simple_processor_for_stock_daily(self, factory):
        """测试为 stock_daily 创建简单处理器"""
        processor = factory.create_processor('stock_daily')
        
        assert isinstance(processor, SimpleDataProcessor)

    def test_create_simple_processor_for_daily_basic(self, factory):
        """测试为 daily_basic 创建简单处理器"""
        processor = factory.create_processor('daily_basic')
        
        assert isinstance(processor, SimpleDataProcessor)

    def test_create_simple_processor_for_trade_cal(self, factory):
        """测试为 trade_cal 创建简单处理器"""
        processor = factory.create_processor('trade_cal')
        
        assert isinstance(processor, SimpleDataProcessor)

    def test_create_simple_processor_for_unknown_table(self, factory):
        """测试为未知表类型创建简单处理器（默认行为）"""
        processor = factory.create_processor('unknown_table')
        
        assert isinstance(processor, SimpleDataProcessor)

    def test_create_processor_with_different_cases(self, factory):
        """测试不同大小写的表名"""
        # 测试大写
        processor_upper = factory.create_processor('STOCK_BASIC')
        assert isinstance(processor_upper, SimpleDataProcessor)  # 大小写敏感，应该返回默认处理器
        
        # 测试小写（正确的）
        processor_lower = factory.create_processor('stock_basic')
        assert isinstance(processor_lower, FullReplaceDataProcessor)

    def test_factory_dependencies_injection(self, mock_parquet_writer, mock_db_queryer, mock_schema_loader):
        """测试工厂正确注入依赖"""
        factory = DataProcessorFactory(
            parquet_writer=mock_parquet_writer,
            db_queryer=mock_db_queryer,
            schema_loader=mock_schema_loader,
        )
        
        # 验证依赖被正确存储
        assert factory.parquet_writer is mock_parquet_writer
        assert factory.db_queryer is mock_db_queryer
        assert factory.schema_loader is mock_schema_loader

    def test_multiple_processor_creation(self, factory):
        """测试多次创建处理器"""
        # 创建多个不同类型的处理器
        processor1 = factory.create_processor('stock_basic')
        processor2 = factory.create_processor('stock_daily')
        processor3 = factory.create_processor('stock_basic')
        
        # 验证类型正确
        assert isinstance(processor1, FullReplaceDataProcessor)
        assert isinstance(processor2, SimpleDataProcessor)
        assert isinstance(processor3, FullReplaceDataProcessor)
        
        # 验证每次都创建新实例
        assert processor1 is not processor3

    def test_processor_has_correct_dependencies(self, factory, mock_parquet_writer, mock_db_queryer, mock_schema_loader):
        """测试创建的处理器具有正确的依赖"""
        processor = factory.create_processor('stock_basic')
        
        # 验证依赖注入正确
        assert processor.parquet_writer is mock_parquet_writer
        assert processor.db_queryer is mock_db_queryer
        assert processor.schema_loader is mock_schema_loader

    @pytest.mark.parametrize("table_name,expected_type", [
        ('stock_basic', FullReplaceDataProcessor),
        ('stock_daily', SimpleDataProcessor),
        ('daily_basic', SimpleDataProcessor),
        ('trade_cal', SimpleDataProcessor),
        ('stock_adj_qfq', SimpleDataProcessor),
        ('income', SimpleDataProcessor),
        ('balancesheet', SimpleDataProcessor),
        ('cashflow', SimpleDataProcessor),
        ('random_table', SimpleDataProcessor),
    ])
    def test_processor_type_mapping(self, factory, table_name, expected_type):
        """参数化测试不同表名对应的处理器类型"""
        processor = factory.create_processor(table_name)
        assert isinstance(processor, expected_type)