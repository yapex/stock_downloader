"""SimpleDataProcessor 集成测试

本测试文件旨在验证 SimpleDataProcessor 与真实的 schema 配置文件 (stock_schema.toml)
能否正确集成，并根据 real schema 做出正确的分区决策。
"""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, ANY

from src.neo.data_processor.simple_data_processor import SimpleDataProcessor
from src.neo.writers.interfaces import IParquetWriter
from src.neo.database.schema_loader import SchemaLoader


@pytest.fixture
def mock_parquet_writer() -> MagicMock:
    """模拟 Parquet 写入器，用于捕获调用参数"""
    return MagicMock(spec=IParquetWriter)


@pytest.fixture
def real_schema_loader() -> SchemaLoader:
    """提供一个真实的 SchemaLoader 实例"""
    return SchemaLoader()


@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._get_update_strategy",
    return_value="incremental",
)
def test_with_real_schema_for_stock_daily_should_partition(
    mock_get_strategy,  # patch arugment
    mock_parquet_writer: MagicMock,
    real_schema_loader: SchemaLoader,
):
    """测试使用真实的 stock_daily schema，应按 year 分区"""
    # GIVEN: 一个真实的 schema loader 和一个待处理的 task_type
    task_type = "stock_daily"
    # 根据真实的 schema，我们知道需要 trade_date 列
    sample_data = pd.DataFrame(
        {
            "trade_date": ["2023-01-01", "2024-01-02"],
            "ts_code": ["600519.SH", "000001.SZ"],
        }
    )

    # WHEN: 使用真实的 schema loader 初始化 processor 并处理数据
    processor = SimpleDataProcessor(
        parquet_writer=mock_parquet_writer, schema_loader=real_schema_loader
    )
    result = processor.process(task_type, "any_symbol", sample_data)

    # THEN: writer 应被告知按 ['year'] 分区
    assert result is True
    mock_parquet_writer.write.assert_called_once_with(
        ANY, task_type, ["year"], "any_symbol"
    )


@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._get_update_strategy",
    return_value="incremental",
)
def test_with_real_schema_for_stock_basic_should_not_partition(
    mock_get_strategy,  # patch arugment
    mock_parquet_writer: MagicMock,
    real_schema_loader: SchemaLoader,
):
    """测试使用真实的 stock_basic schema，不应分区"""
    # GIVEN
    task_type = "stock_basic"
    # stock_basic 不需要日期列
    sample_data = pd.DataFrame(
        {"ts_code": ["600519.SH", "000001.SZ"], "name": ["贵州茅台", "平安银行"]}
    )

    # WHEN
    processor = SimpleDataProcessor(
        parquet_writer=mock_parquet_writer, schema_loader=real_schema_loader
    )
    result = processor.process(task_type, "any_symbol", sample_data)

    # THEN: writer 应被告知不进行分区
    assert result is True
    mock_parquet_writer.write.assert_called_once_with(ANY, task_type, [], "any_symbol")


@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._get_update_strategy",
    return_value="incremental",
)
def test_with_real_schema_for_trade_cal_should_partition(
    mock_get_strategy,  # patch arugment
    mock_parquet_writer: MagicMock,
    real_schema_loader: SchemaLoader,
):
    """测试使用真实的 trade_cal schema，应按 year 分区"""
    # GIVEN
    task_type = "trade_cal"
    # 根据 schema，trade_cal 需要 cal_date 列
    sample_data = pd.DataFrame(
        {
            "cal_date": ["20230101", "20240102"],
            "exchange": ["SSE", "SSE"],
            "is_open": [1, 1],
        }
    )

    # WHEN
    processor = SimpleDataProcessor(
        parquet_writer=mock_parquet_writer, schema_loader=real_schema_loader
    )
    result = processor.process(task_type, "any_symbol", sample_data)

    # THEN: writer 应被告知按 ['year'] 分区
    assert result is True
    mock_parquet_writer.write.assert_called_once_with(
        ANY, task_type, ["year"], "any_symbol"
    )
