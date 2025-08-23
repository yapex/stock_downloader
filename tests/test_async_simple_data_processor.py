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
            {"name": "date", "type": "TEXT"}
        ]
        mock.load_schema.return_value = mock_schema
        return mock

    @pytest.fixture
    def mock_data_buffer(self):
        """模拟异步数据缓冲器"""
        mock = AsyncMock()
        mock.register_type = Mock()      # 同步方法
        mock.add = AsyncMock()           # 异步方法
        mock.shutdown = AsyncMock()      # 异步方法
        return mock

    @pytest.fixture
    def sample_data(self):
        """示例数据"""
        return pd.DataFrame({
            'symbol': ['600519', '000001'],
            'value': [100.0, 200.0],
            'date': ['2023-01-01', '2023-01-02']
        })

    @pytest.mark.asyncio
    async def test_create_default(self):
        """测试创建默认配置的异步数据处理器"""
        processor = AsyncSimpleDataProcessor.create_default()
        
        assert processor is not None
        assert processor.enable_batch is True
        assert processor.db_operator is not None
        assert processor.schema_loader is not None

    @pytest.mark.asyncio
    async def test_process_with_batch_mode(self, mock_db_operator, mock_schema_loader, mock_data_buffer, sample_data):
        """测试批量处理模式"""
        processor = AsyncSimpleDataProcessor(
            db_operator=mock_db_operator,
            enable_batch=True,
            schema_loader=mock_schema_loader,
            data_buffer=mock_data_buffer
        )
        
        result = await processor.process("test_task", sample_data)
        
        assert result is True
        mock_data_buffer.register_type.assert_called_once()
        mock_data_buffer.add.assert_called_once_with("test_task", sample_data)

    @pytest.mark.asyncio
    async def test_process_without_batch_mode(self, mock_db_operator, mock_schema_loader, mock_data_buffer, sample_data):
        """测试非批量处理模式"""
        processor = AsyncSimpleDataProcessor(
            db_operator=mock_db_operator,
            enable_batch=False,
            schema_loader=mock_schema_loader,
            data_buffer=mock_data_buffer
        )
        
        result = await processor.process("test_task", sample_data)

        assert result is True
        mock_db_operator.upsert.assert_called_once()
        # 非批量模式不应该调用数据缓冲器
        mock_data_buffer.register_type.assert_not_called()
        mock_data_buffer.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_empty_data(self, mock_db_operator, mock_schema_loader, mock_data_buffer):
        """测试处理空数据"""
        processor = AsyncSimpleDataProcessor(
            db_operator=mock_db_operator,
            enable_batch=True,
            schema_loader=mock_schema_loader,
            data_buffer=mock_data_buffer
        )
        
        empty_data = pd.DataFrame()
        result = await processor.process("test_task", empty_data)
        
        assert result is False
        mock_data_buffer.register_type.assert_not_called()
        mock_data_buffer.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_none_data(self, mock_db_operator, mock_schema_loader, mock_data_buffer):
        """测试处理 None 数据"""
        processor = AsyncSimpleDataProcessor(
            db_operator=mock_db_operator,
            enable_batch=True,
            schema_loader=mock_schema_loader,
            data_buffer=mock_data_buffer
        )
        
        result = await processor.process("test_task", None)
        
        assert result is False
        mock_data_buffer.register_type.assert_not_called()
        mock_data_buffer.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_shutdown(self, mock_db_operator, mock_schema_loader, mock_data_buffer):
        """测试关闭功能"""
        processor = AsyncSimpleDataProcessor(
            db_operator=mock_db_operator,
            enable_batch=True,
            schema_loader=mock_schema_loader,
            data_buffer=mock_data_buffer
        )
        
        await processor.shutdown()
        
        mock_data_buffer.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_without_data_buffer(self, mock_db_operator, mock_schema_loader):
        """测试没有数据缓冲器时的关闭功能"""
        processor = AsyncSimpleDataProcessor(
            db_operator=mock_db_operator,
            enable_batch=True,
            schema_loader=mock_schema_loader,
            data_buffer=None
        )
        
        # 应该不会抛出异常
        await processor.shutdown()

    @pytest.mark.asyncio
    async def test_data_buffer_initialization(self, mock_db_operator, mock_schema_loader, sample_data):
        """测试数据缓冲器的延迟初始化"""
        processor = AsyncSimpleDataProcessor(
            db_operator=mock_db_operator,
            enable_batch=True,
            schema_loader=mock_schema_loader,
            data_buffer=None  # 不提供数据缓冲器，测试延迟初始化
        )
        
        # 初始状态下数据缓冲器应该是 None
        assert processor.data_buffer is None
        assert processor._data_buffer_initialized is False
        
        # 调用 process 应该触发数据缓冲器的初始化
        with patch('src.neo.data_processor.data_buffer.get_async_data_buffer') as mock_get_buffer:
            mock_buffer = AsyncMock()
            mock_buffer.register_type = Mock()  # 同步方法
            mock_buffer.add = AsyncMock()
            mock_get_buffer.return_value = mock_buffer
            
            result = await processor.process("test_task", sample_data)
            
            assert result is True
            assert processor.data_buffer is not None
            assert processor._data_buffer_initialized is True
            mock_get_buffer.assert_called_once()
            mock_buffer.register_type.assert_called_once()
            mock_buffer.add.assert_called_once()