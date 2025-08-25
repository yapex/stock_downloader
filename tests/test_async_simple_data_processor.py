"""AsyncSimpleDataProcessor 测试

测试异步数据处理器的核心功能
"""

import pandas as pd
import pytest
from unittest.mock import Mock, patch, AsyncMock

from src.neo.data_processor.simple_data_processor import AsyncSimpleDataProcessor


class TestAsyncSimpleDataProcessor:
    """AsyncSimpleDataProcessor 测试类"""

    @pytest.fixture
    def mock_db_operator(self):
        """模拟数据库操作器"""
        mock = Mock()
        mock.save_data.return_value = True
        return mock

    @pytest.fixture
    def mock_schema_loader(self):
        """模拟 Schema 加载器"""
        mock = Mock()
        mock_schema = Mock()
        mock_schema.table_name = "test_table"
        mock_schema.columns = [
            {"name": "symbol", "type": "TEXT"},
            {"name": "value", "type": "REAL"},
            {"name": "date", "type": "TEXT"},
        ]
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

    @pytest.mark.asyncio
    async def test_create_default(self):
        """测试创建默认配置的异步数据处理器"""
        processor = AsyncSimpleDataProcessor.create_default()

        assert processor is not None
        assert processor.enable_batch is True
        assert processor.db_operator is not None
        assert processor.schema_loader is not None

    @pytest.mark.asyncio
    async def test_process_with_batch_mode(
        self, mock_db_operator, mock_schema_loader, sample_data
    ):
        """测试批量处理模式（已移除缓冲机制）"""
        processor = AsyncSimpleDataProcessor(
            db_operator=mock_db_operator,
            enable_batch=True,
            schema_loader=mock_schema_loader,
        )

        result = await processor.process("test_task", sample_data)

        assert result is True
        mock_db_operator.upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_without_batch_mode(
        self, mock_db_operator, mock_schema_loader, sample_data
    ):
        """测试非批量处理模式（已移除缓冲机制）"""
        processor = AsyncSimpleDataProcessor(
            db_operator=mock_db_operator,
            enable_batch=False,
            schema_loader=mock_schema_loader,
        )

        result = await processor.process("test_task", sample_data)

        assert result is True
        mock_db_operator.upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_empty_data(
        self, mock_db_operator, mock_schema_loader
    ):
        """测试处理空数据"""
        processor = AsyncSimpleDataProcessor(
            db_operator=mock_db_operator,
            enable_batch=True,
            schema_loader=mock_schema_loader,
        )

        empty_data = pd.DataFrame()
        result = await processor.process("test_task", empty_data)

        assert result is False
        mock_db_operator.upsert.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_none_data(
        self, mock_db_operator, mock_schema_loader
    ):
        """测试处理 None 数据"""
        processor = AsyncSimpleDataProcessor(
            db_operator=mock_db_operator,
            enable_batch=True,
            schema_loader=mock_schema_loader,
        )

        result = await processor.process("test_task", None)

        assert result is False
        mock_db_operator.upsert.assert_not_called()

    @pytest.mark.asyncio
    async def test_shutdown(
        self, mock_db_operator, mock_schema_loader
    ):
        """测试关闭功能（已移除缓冲机制）"""
        processor = AsyncSimpleDataProcessor(
            db_operator=mock_db_operator,
            enable_batch=True,
            schema_loader=mock_schema_loader,
        )

        # 应该不会抛出异常
        await processor.shutdown()



    @pytest.mark.asyncio
    async def test_buffer_status(
        self, mock_db_operator, mock_schema_loader
    ):
        """测试缓冲区状态（已移除缓冲机制）"""
        processor = AsyncSimpleDataProcessor(
            db_operator=mock_db_operator,
            enable_batch=True,
            schema_loader=mock_schema_loader,
        )

        # 应该返回空字典
        status = processor.get_buffer_status()
        assert status == {}

    @pytest.mark.asyncio
    async def test_flush_all(
        self, mock_db_operator, mock_schema_loader
    ):
        """测试刷新功能（已移除缓冲机制）"""
        processor = AsyncSimpleDataProcessor(
            db_operator=mock_db_operator,
            enable_batch=True,
            schema_loader=mock_schema_loader,
        )

        # 应该始终返回 True
        result = await processor.flush_all()
        assert result is True
