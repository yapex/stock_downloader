"""SimpleDataProcessor 测试

测试同步数据处理器的核心功能
"""

import pandas as pd
import pytest
from unittest.mock import Mock, MagicMock, patch, ANY

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


@pytest.fixture
def sample_data_with_end_date() -> pd.DataFrame:
    """示例数据，包含 end_date 列"""
    return pd.DataFrame(
        {
            "end_date": ["20221231", "20230331"],
            "symbol": ["600000", "000001"],
            "value": [100.0, 200.0],
        }
    )


def test_process_with_end_date(mock_parquet_writer: MagicMock, sample_data_with_end_date: pd.DataFrame):
    """测试处理包含 end_date 列的数据"""
    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    result = processor.process("finance_report", sample_data_with_end_date)

    assert result is True
    mock_parquet_writer.write.assert_called_once_with(
        ANY,
        "finance_report",
        ["year"]
    )


@pytest.fixture
def sample_data_no_date_cols() -> pd.DataFrame:
    """示例数据，不包含日期列"""
    return pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["A", "B"],
        }
    )


def test_process_no_date_columns(mock_parquet_writer: MagicMock, sample_data_no_date_cols: pd.DataFrame):
    """测试处理不包含日期列的数据"""
    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    result = processor.process("some_other_data", sample_data_no_date_cols)

    assert result is True
    mock_parquet_writer.write.assert_called_once_with(
        sample_data_no_date_cols,
        "some_other_data",
        []
    )


def test_process_exception_handling(mock_parquet_writer: MagicMock, sample_data: pd.DataFrame):
    """测试 process 方法中的异常处理"""
    mock_parquet_writer.write.side_effect = Exception("Test write error")
    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)

    with patch('src.neo.data_processor.simple_data_processor.logger') as mock_logger:
        result = processor.process("stock_daily", sample_data)

        assert result is False
        mock_logger.error.assert_called_once()
        assert "Test write error" in mock_logger.error.call_args[0][0]
