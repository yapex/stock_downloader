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


def test_write_full_replace_creates_temp_directory(
    tmp_path: Path, sample_dataframe: pd.DataFrame
):
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
    writer.write_full_replace(
        data=df_with_year, task_type=task_type, partition_cols=partition_cols
    )

    # 验证目标目录被创建
    target_dir = base_path / task_type
    assert target_dir.is_dir()

    # 验证数据被写入目标目录
    assert any(target_dir.glob("**/*.parquet"))


def test_write_full_replace_without_partition_cols(
    tmp_path: Path, sample_dataframe: pd.DataFrame
):
    """测试 write_full_replace 方法不使用分区列"""
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_basic"

    # 执行
    writer.write_full_replace(
        data=sample_dataframe, task_type=task_type, partition_cols=[]
    )

    # 验证目标目录被创建
    target_dir = base_path / task_type
    assert target_dir.is_dir()

    # 验证数据被写入（无分区）
    parquet_files = list(target_dir.glob("*.parquet"))
    assert len(parquet_files) > 0

    # 验证数据完整性
    read_df = pd.read_parquet(target_dir)
    assert len(read_df) == len(sample_dataframe)


def test_write_full_replace_with_partition_cols(
    tmp_path: Path, sample_dataframe: pd.DataFrame
):
    """测试 write_full_replace 方法使用分区列"""
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_daily"
    partition_cols = ["trade_date"]

    # 执行
    writer.write_full_replace(
        data=sample_dataframe, task_type=task_type, partition_cols=partition_cols
    )

    # 验证目标目录被创建
    target_dir = base_path / task_type
    assert target_dir.is_dir()

    # 验证分区目录被创建
    partition_dirs = list(target_dir.glob("trade_date=*"))
    assert len(partition_dirs) == 2  # 应该有两个不同的交易日期分区

    # 验证每个分区下都有 parquet 文件
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
    writer.write_full_replace(data=empty_df, task_type=task_type, partition_cols=[])

    # 验证空数据时不创建目录（符合实现逻辑）
    target_dir = base_path / task_type
    assert not target_dir.exists()


def test_write_full_replace_success(tmp_path: Path, sample_dataframe: pd.DataFrame):
    """测试 write_full_replace 方法成功执行"""
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_basic"

    # 执行
    writer.write_full_replace(
        data=sample_dataframe, task_type=task_type, partition_cols=[]
    )

    # 验证目标目录被创建
    target_dir = base_path / task_type
    assert target_dir.is_dir()

    # 验证数据被写入
    parquet_files = list(target_dir.glob("*.parquet"))
    assert len(parquet_files) > 0

    # 验证数据完整性
    read_df = pd.read_parquet(target_dir)
    assert len(read_df) == len(sample_dataframe)


def test_write_full_replace_multiple_calls_same_task(
    tmp_path: Path, sample_dataframe: pd.DataFrame
):
    """测试对同一任务类型多次调用 write_full_replace"""
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_basic"

    # 第一次调用
    writer.write_full_replace(
        data=sample_dataframe, task_type=task_type, partition_cols=[]
    )
    target_dir = base_path / task_type
    assert target_dir.is_dir()

    # 第二次调用（应该覆盖之前的目标目录）
    new_df = sample_dataframe.copy()
    new_df["close"] = new_df["close"] * 2  # 修改数据

    writer.write_full_replace(data=new_df, task_type=task_type, partition_cols=[])

    # 验证目标目录仍然存在
    assert target_dir.is_dir()

    # 验证数据被更新
    read_df = pd.read_parquet(target_dir)
    # 检查数据是否为新数据（close 值应该是原来的两倍）
    expected_close_values = sample_dataframe["close"] * 2
    actual_close_values = read_df.sort_values("ts_code")["close"].reset_index(drop=True)
    expected_close_values = expected_close_values.sort_values().reset_index(drop=True)

    pd.testing.assert_series_equal(
        actual_close_values, expected_close_values, check_names=False
    )


def test_write_full_replace_by_symbol_deletes_only_symbol_partition(
    tmp_path: Path,
):
    """测试 write_full_replace_by_symbol 是否只删除指定 symbol 的分区"""
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_daily"
    partition_cols = ["ts_code"]

    # 准备两个不同 symbol 的数据
    df1 = pd.DataFrame(
        {
            "trade_date": ["20250826"],
            "ts_code": ["000001.SZ"],
            "close": [13.5],
        }
    )
    df2 = pd.DataFrame(
        {
            "trade_date": ["20250826"],
            "ts_code": ["600519.SH"],
            "close": [1700.0],
        }
    )

    # 先写入两个 symbol 的数据
    writer.write(df1, task_type, partition_cols)
    writer.write(df2, task_type, partition_cols)

    # 验证两个 symbol 的分区都存在
    partition1_path = base_path / task_type / "ts_code=000001.SZ"
    partition2_path = base_path / task_type / "ts_code=600519.SH"
    assert partition1_path.exists()
    assert partition2_path.exists()

    # 准备新数据，只替换 '000001.SZ'
    new_df1 = pd.DataFrame(
        {
            "trade_date": ["20250827"],
            "ts_code": ["000001.SZ"],
            "close": [14.0],
        }
    )

    # 执行按 symbol 替换
    writer.write_full_replace_by_symbol(
        new_df1, task_type, partition_cols, symbol="000001.SZ"
    )

    # 验证：'000001.SZ' 的数据被更新
    read_df1 = pd.read_parquet(partition1_path)
    assert len(read_df1) == 1
    assert read_df1["trade_date"].iloc[0] == "20250827"
    assert read_df1["close"].iloc[0] == 14.0

    # 验证：'600519.SH' 的分区依然存在且未被改变
    assert partition2_path.exists()
    read_df2 = pd.read_parquet(partition2_path)
    assert len(read_df2) == 1
    assert read_df2["close"].iloc[0] == 1700.0


def test_write_full_replace_by_symbol_empty_dataframe(tmp_path: Path):
    """测试 write_full_replace_by_symbol 方法处理空数据框"""
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_daily"
    symbol = "000001.SZ"
    symbol_partition_path = base_path / task_type / f"ts_code={symbol}"

    # 先创建一些数据
    sample_df = pd.DataFrame(
        {"trade_date": ["20250826"], "ts_code": [symbol], "close": [13.5]}
    )
    writer.write(sample_df, task_type, partition_cols=["ts_code"])
    assert symbol_partition_path.exists()

    # 执行
    empty_df = pd.DataFrame()
    writer.write_full_replace_by_symbol(
        empty_df, task_type, partition_cols=["ts_code"], symbol=symbol
    )

    # 验证：目录应该仍然存在，因为函数会提前返回
    assert symbol_partition_path.exists()


def raise_io_error(*args, **kwargs):
    raise IOError("Mocked error")


def test_write_full_replace_by_symbol_exception_handling(tmp_path: Path, monkeypatch):
    """测试 write_full_replace_by_symbol 方法的异常处理"""
    # 准备
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_daily"
    symbol = "000001.SZ"
    sample_df = pd.DataFrame(
        {"trade_date": ["20250826"], "ts_code": [symbol], "close": [13.5]}
    )
    (base_path / task_type / f"ts_code={symbol}").mkdir(parents=True, exist_ok=True)

    # Mock shutil.rmtree 来触发异常
    monkeypatch.setattr("shutil.rmtree", raise_io_error)

    # 执行并验证异常
    with pytest.raises(IOError, match="Mocked error"):
        writer.write_full_replace_by_symbol(
            sample_df, task_type, partition_cols=["ts_code"], symbol=symbol
        )


def test_write_empty_dataframe(tmp_path: Path):
    """测试 write 方法处理空数据框"""
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_daily"
    target_dir = base_path / task_type

    writer.write(pd.DataFrame(), task_type, partition_cols=["trade_date"])

    assert not target_dir.exists()


def test_write_exception_handling(tmp_path: Path, sample_dataframe: pd.DataFrame, monkeypatch):
    """测试 write 方法的异常处理"""
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_daily"

    def mock_write_to_dataset(*args, **kwargs):
        raise Exception("Test error")

    monkeypatch.setattr("pyarrow.parquet.write_to_dataset", mock_write_to_dataset)

    with pytest.raises(Exception, match="Test error"):
        writer.write(sample_dataframe, task_type, partition_cols=["trade_date"])


def test_write_full_replace_exception_handling(
    tmp_path: Path, sample_dataframe: pd.DataFrame, monkeypatch
):
    """测试 write_full_replace 方法的异常处理"""
    base_path = tmp_path / "parquet_data"
    writer = ParquetWriter(base_path=str(base_path))
    task_type = "stock_basic"
    (base_path / task_type).mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("shutil.rmtree", raise_io_error)

    with pytest.raises(IOError, match="Mocked error"):
        writer.write_full_replace(
            sample_dataframe, task_type, partition_cols=[]
        )