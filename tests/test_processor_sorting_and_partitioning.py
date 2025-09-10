"""验证 SimpleDataProcessor 在增量模式下的排序和分区逻辑"""

import pandas as pd
import pytest
from unittest.mock import MagicMock

from src.neo.data_processor.simple_data_processor import SimpleDataProcessor
from src.neo.writers.interfaces import IParquetWriter
from src.neo.database.schema_loader import SchemaLoader


@pytest.fixture
def mock_parquet_writer() -> MagicMock:
    """模拟 Parquet 写入器"""
    return MagicMock(spec=IParquetWriter)


@pytest.fixture
def real_schema_loader() -> SchemaLoader:
    """提供真实的 SchemaLoader 实例"""
    return SchemaLoader()


def test_processor_sorts_and_partitions_correctly_for_incremental(
    mock_parquet_writer: MagicMock,
    real_schema_loader: SchemaLoader,
):
    """
    TDD: 验证 SimpleDataProcessor 在增量模式下：
    1. 只按 ['year'] 分区
    2. 在写入前按 schema 定义的主键对数据进行排序
    """
    # GIVEN: income 任务，其 schema 定义了 date_col 和 primary_key
    task_type = "income"
    symbol = "600519.SH"
    # 准备一个乱序的 DataFrame
    unsorted_data = pd.DataFrame(
        {
            "ts_code": [symbol, symbol],
            "ann_date": ["2023-04-28", "2023-01-30"],  # 乱序
            "f_ann_date": ["20230428", "20230130"],  # 乱序
            "value": [200, 100],
        }
    )
    # 先创建期望的排序后结果，因为 processor 会原地修改 DataFrame
    expected_sorted_data = unsorted_data.sort_values(
        by=["ts_code", "ann_date"], ignore_index=True
    )

    # WHEN: 调用 process 方法
    # 注意：我们依赖于刚刚修改过的 config.toml，其中 income 的策略是 incremental
    processor = SimpleDataProcessor.create_default(
        parquet_writer=mock_parquet_writer, schema_loader=real_schema_loader
    )
    processor.process(task_type, symbol, unsorted_data)

    # THEN: 验证 write 方法被调用，且参数正确
    mock_parquet_writer.write.assert_called_once()

    # 1. 验证分区列是否正确
    call_args = mock_parquet_writer.write.call_args
    passed_partition_cols = call_args[0][2]
    assert passed_partition_cols == ["year"], (
        f"分区列不正确，期望 ['year']，实际 {passed_partition_cols}"
    )

    # 2. 验证数据是否已排序
    passed_data = call_args[0][0]
    passed_data_for_compare = passed_data.drop(columns=["year"])

    pd.testing.assert_frame_equal(passed_data_for_compare, expected_sorted_data)
