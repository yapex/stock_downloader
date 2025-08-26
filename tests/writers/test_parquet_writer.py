"""测试 ParquetWriter"""

import pandas as pd
import pytest
from pathlib import Path

from src.neo.writers.parquet_writer import ParquetWriter


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """创建一个用于测试的样本 DataFrame"""
    data = {
        'trade_date': ['20250826', '20250826', '20250827'],
        'ts_code': ['000001.SZ', '600519.SH', '000001.SZ'],
        'close': [13.5, 1700.0, 13.6]
    }
    return pd.DataFrame(data)


def test_write_creates_partitioned_parquet_file(tmp_path: Path, sample_dataframe: pd.DataFrame):
    """测试 write 方法是否能正确创建分区的 Parquet 文件"""
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_daily"
    partition_cols = ['trade_date']

    # 执行
    writer.write(sample_dataframe, task_type, partition_cols)

    # 验证
    expected_partition_1 = base_path / task_type / "trade_date=20250826"
    expected_partition_2 = base_path / task_type / "trade_date=20250827"

    assert expected_partition_1.is_dir()
    assert expected_partition_2.is_dir()

    # 检查每个分区下是否有名为 .parquet 的文件
    assert any(expected_partition_1.glob("*.parquet"))
    assert any(expected_partition_2.glob("*.parquet"))

    # 验证数据完整性 (可选但推荐)
    read_df = pd.read_parquet(base_path / task_type)

    # --- 标准化两个DataFrame以进行比较 ---
    # 1. 确保列顺序一致
    ordered_read_df = read_df[sample_dataframe.columns]
    
    # 2. 确保有问题的列类型一致 (分区列经常被读为 Categorical)
    comparison_df = sample_dataframe.copy()
    comparison_df['trade_date'] = comparison_df['trade_date'].astype(str)
    ordered_read_df['trade_date'] = ordered_read_df['trade_date'].astype(str)

    # 3. 按确定性顺序排序
    comparison_df = comparison_df.sort_values('ts_code').reset_index(drop=True)
    ordered_read_df = ordered_read_df.sort_values('ts_code').reset_index(drop=True)

    pd.testing.assert_frame_equal(ordered_read_df, comparison_df)
