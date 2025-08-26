"""SimpleDataProcessor 测试

测试同步数据处理器的核心功能
"""

import pandas as pd
import pytest
from unittest.mock import Mock, MagicMock

from src.neo.data_processor.simple_data_processor import SimpleDataProcessor
from src.neo.writers.interfaces import IParquetWriter


@pytest.fixture
def mock_parquet_writer() -> MagicMock:
    """模拟 Parquet 写入器"""
    mock = MagicMock(spec=IParquetWriter)
    mock.write.return_value = None
    return mock

@pytest.fixture
def sample_data() -> pd.DataFrame:
    """示例数据"""
    return pd.DataFrame(
        {
            "trade_date": ["2023-01-01", "2023-01-02"],
            "symbol": ["600519", "000001"],
            "value": [100.0, 200.0],
        }
    )

def test_create_default():
    """测试创建默认配置的同步数据处理器"""
    # 这个测试验证在不提供 writer 的情况下，它能否自行使用配置创建
    processor = SimpleDataProcessor.create_default()
    assert processor is not None
    assert processor.parquet_writer is not None

def test_process_success(mock_parquet_writer: MagicMock, sample_data: pd.DataFrame):
    """测试成功处理数据"""
    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    result = processor.process("stock_daily", sample_data)

    assert result is True
    # 验证 write 方法被以正确的参数调用
    # 我们需要验证传递给 write 的 DataFrame 是否包含了新增的 'year' 列
    from unittest.mock import ANY
    mock_parquet_writer.write.assert_called_once_with(
        ANY, # 使用 ANY 来匹配包含了新 'year' 列的 DataFrame
        "stock_daily", 
        ["year"]
    )

def test_process_empty_data(mock_parquet_writer: MagicMock):
    """测试处理空数据"""
    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    empty_data = pd.DataFrame()
    result = processor.process("test_task", empty_data)

    assert result is False
    mock_parquet_writer.write.assert_not_called()

def test_process_none_data(mock_parquet_writer: MagicMock):
    """测试处理 None 数据"""
    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    result = processor.process("test_task", None)

    assert result is False
    mock_parquet_writer.write.assert_not_called()

def test_shutdown(mock_parquet_writer: MagicMock):
    """测试关闭功能"""
    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    # 应该不会抛出异常
    processor.shutdown()
