"""ParquetWriter 单元测试"""

import pytest
import pandas as pd
from pathlib import Path

from src.neo.writers.parquet_writer import ParquetWriter

def test_writer_preserves_nulls_in_numeric_columns(tmp_path: Path):
    """
    TDD: 验证 ParquetWriter 在写入时，能将数字列中的 None 正确保存为 Parquet 的 NULL，
    而不是字符串 'None'。
    """
    # GIVEN: 一个包含 None 的、dtype 为 object 的 Series
    # 需要显式指定 dtype=object 才能避免 pandas 自动类型推断
    source_df = pd.DataFrame({
        'value': pd.Series([10.5, None, 20.0, 15.1], dtype=object),
        'description': ['a', 'b', 'c', 'd']
    })
    assert source_df['value'].dtype == 'object', "前提条件失败：测试数据的 value 列应为 object 类型"

    table_name = "test_nulls"
    writer = ParquetWriter(base_path=str(tmp_path))

    # WHEN: 调用 write 方法
    writer.write(source_df, table_name, partition_cols=[])

    # THEN: 读回数据，验证数据类型和 null 值
    output_dir = tmp_path / table_name
    written_files = list(output_dir.glob("*.parquet"))
    assert len(written_files) == 1, "未能找到写入的 Parquet 文件"

    read_back_df = pd.read_parquet(written_files[0])

    # 1. 验证数据类型应为浮点数，而不是 object/string
    assert pd.api.types.is_float_dtype(read_back_df['value']), (
        f"数据类型校验失败，期望 float，实际 {read_back_df['value'].dtype}"
    )

    # 2. 验证 null 值的数量是否正确
    assert read_back_df['value'].isnull().sum() == 1, "Null 值数量不正确"

    # 3. 验证非空值是否正确
    assert read_back_df['value'][0] == 10.5
