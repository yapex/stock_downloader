"""测试 ParquetWriter"""

import pandas as pd
import pytest
from pathlib import Path

from src.neo.writers.parquet_writer import ParquetWriter


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """创建一个用于测试的样本 DataFrame"""
    data = {
        "trade_date": ["20250826", "20250826", "20250827"],
        "ts_code": ["000001.SZ", "600519.SH", "000001.SZ"],
        "close": [13.5, 1700.0, 13.6],
    }
    return pd.DataFrame(data)


def test_write_creates_partitioned_parquet_file(
    tmp_path: Path, sample_dataframe: pd.DataFrame
):
    """测试 write 方法是否能正确创建分区的 Parquet 文件"""
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_daily"
    partition_cols = ["trade_date"]

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
    comparison_df["trade_date"] = comparison_df["trade_date"].astype(str)
    ordered_read_df["trade_date"] = ordered_read_df["trade_date"].astype(str)

    # 3. 按确定性顺序排序
    comparison_df = comparison_df.sort_values("ts_code").reset_index(drop=True)
    ordered_read_df = ordered_read_df.sort_values("ts_code").reset_index(drop=True)

    pd.testing.assert_frame_equal(ordered_read_df, comparison_df)


def test_write_full_replace_creates_temp_directory(tmp_path: Path, sample_dataframe: pd.DataFrame):
    """测试 write_full_replace 方法创建临时目录"""
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_basic"
    partition_cols = ["year"]
    
    # 添加年份分区列
    df_with_year = sample_dataframe.copy()
    df_with_year["year"] = 2024
    
    # 执行
    writer.write_full_replace(df=df_with_year, task_type=task_type, partition_cols=partition_cols)
    
    # 验证临时目录被创建
    temp_dir = base_path / f"{task_type}_temp"
    assert temp_dir.is_dir()
    
    # 验证数据被写入临时目录
    assert any(temp_dir.glob("**/*.parquet"))


def test_write_full_replace_without_partition_cols(tmp_path: Path, sample_dataframe: pd.DataFrame):
    """测试 write_full_replace 方法不使用分区列"""
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_basic"
    
    # 执行
    writer.write_full_replace(df=sample_dataframe, task_type=task_type, partition_cols=[])
    
    # 验证临时目录被创建
    temp_dir = base_path / f"{task_type}_temp"
    assert temp_dir.is_dir()
    
    # 验证数据被写入（无分区）
    parquet_files = list(temp_dir.glob("*.parquet"))
    assert len(parquet_files) > 0
    
    # 验证数据完整性
    read_df = pd.read_parquet(temp_dir)
    assert len(read_df) == len(sample_dataframe)


def test_write_full_replace_with_partition_cols(tmp_path: Path, sample_dataframe: pd.DataFrame):
    """测试 write_full_replace 方法使用分区列"""
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_daily"
    partition_cols = ["trade_date"]
    
    # 执行
    writer.write_full_replace(df=sample_dataframe, task_type=task_type, partition_cols=partition_cols)
    
    # 验证临时目录被创建
    temp_dir = base_path / f"{task_type}_temp"
    assert temp_dir.is_dir()
    
    # 验证分区目录被创建
    partition_dirs = list(temp_dir.glob("trade_date=*"))
    assert len(partition_dirs) == 2  # 应该有两个不同的交易日期分区
    
    # 验证每个分区都有数据文件
    for partition_dir in partition_dirs:
        assert any(partition_dir.glob("*.parquet"))


def test_write_full_replace_empty_dataframe(tmp_path: Path):
    """测试 write_full_replace 方法处理空数据框"""
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_basic"
    empty_df = pd.DataFrame()
    
    # 执行
    writer.write_full_replace(df=empty_df, task_type=task_type, partition_cols=[])
    
    # 验证临时目录被创建
    temp_dir = base_path / f"{task_type}_temp"
    assert temp_dir.is_dir()


def test_write_full_replace_logging(tmp_path: Path, sample_dataframe: pd.DataFrame, caplog):
    """测试 write_full_replace 方法的日志记录"""
    import logging
    
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_basic"
    
    # 执行
    with caplog.at_level(logging.INFO):
        writer.write_full_replace(df=sample_dataframe, task_type=task_type, partition_cols=[])
    
    # 验证日志
    assert "✅ 全量替换成功写入" in caplog.text
    assert f"到 {base_path / f'{task_type}_temp'}" in caplog.text


def test_write_full_replace_multiple_calls_same_task(tmp_path: Path, sample_dataframe: pd.DataFrame):
    """测试对同一任务类型多次调用 write_full_replace"""
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_basic"
    
    # 第一次调用
    writer.write_full_replace(df=sample_dataframe, task_type=task_type, partition_cols=[])
    temp_dir = base_path / f"{task_type}_temp"
    assert temp_dir.is_dir()
    
    # 第二次调用（应该覆盖之前的临时目录）
    new_df = sample_dataframe.copy()
    new_df["close"] = new_df["close"] * 2  # 修改数据
    
    writer.write_full_replace(df=new_df, task_type=task_type, partition_cols=[])
    
    # 验证临时目录仍然存在
    assert temp_dir.is_dir()
    
    # 验证数据被更新
    read_df = pd.read_parquet(temp_dir)
    # 检查数据是否为新数据（close 值应该是原来的两倍）
    expected_close_values = sample_dataframe["close"] * 2
    actual_close_values = read_df.sort_values("ts_code")["close"].reset_index(drop=True)
    expected_close_values = expected_close_values.sort_values().reset_index(drop=True)
    
    pd.testing.assert_series_equal(actual_close_values, expected_close_values, check_names=False)
