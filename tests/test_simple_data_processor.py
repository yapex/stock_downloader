"""SimpleDataProcessor 测试

测试同步数据处理器的核心功能
"""

import pandas as pd
import pytest
from unittest.mock import Mock

from src.neo.data_processor.simple_data_processor import SimpleDataProcessor


class TestSimpleDataProcessor:
    """SimpleDataProcessor 测试类"""

    @pytest.fixture
    def mock_db_operator(self):
        """模拟数据库操作器"""
        mock = Mock()
        mock.upsert.return_value = True
        return mock

    @pytest.fixture
    def mock_schema_loader(self):
        """模拟 Schema 加载器"""
        mock = Mock()
        mock_schema = Mock()
        mock_schema.table_name = "test_table"
        mock.load_schema.return_value = mock_schema
        return mock

    @pytest.fixture
    def sample_data(self):
        """示例数据"""
        return pd.DataFrame(
            {
                "symbol": ["600519", "000001"],
                "value": [100.0, 200.0],
                "date": ["2023-01-01", "2023-01-02"],
            }
        )

    def test_create_default(self):
        """测试创建默认配置的同步数据处理器"""
        processor = SimpleDataProcessor.create_default()
        assert processor is not None
        assert processor.db_operator is not None
        assert processor.schema_loader is not None

    def test_process_success(
        self, mock_db_operator, mock_schema_loader, sample_data
    ):
        """测试成功处理数据"""
        processor = SimpleDataProcessor(
            db_operator=mock_db_operator,
            schema_loader=mock_schema_loader,
        )
        result = processor.process("test_task", sample_data)
        assert result is True
        mock_db_operator.upsert.assert_called_once()

    def test_process_empty_data(
        self, mock_db_operator, mock_schema_loader
    ):
        """测试处理空数据"""
        processor = SimpleDataProcessor(
            db_operator=mock_db_operator,
            schema_loader=mock_schema_loader,
        )
        empty_data = pd.DataFrame()
        result = processor.process("test_task", empty_data)
        assert result is False
        mock_db_operator.upsert.assert_not_called()

    def test_process_none_data(
        self, mock_db_operator, mock_schema_loader
    ):
        """测试处理 None 数据"""
        processor = SimpleDataProcessor(
            db_operator=mock_db_operator,
            schema_loader=mock_schema_loader,
        )
        result = processor.process("test_task", None)
        assert result is False
        mock_db_operator.upsert.assert_not_called()

    def test_shutdown(
        self, mock_db_operator, mock_schema_loader
    ):
        """测试关闭功能"""
        processor = SimpleDataProcessor(
            db_operator=mock_db_operator,
            schema_loader=mock_schema_loader,
        )
        # 应该不会抛出异常
        processor.shutdown()