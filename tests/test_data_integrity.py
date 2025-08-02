import pytest
import pandas as pd
from pathlib import Path
import yaml
from tqdm import tqdm

# ===================================================================
#           Fixtures: 准备测试环境和数据
# ===================================================================


@pytest.fixture(scope="module")
def data_path() -> Path:
    """从配置文件中读取数据存储的根目录。"""
    try:
        with open("config.yaml", "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        path = Path(config["storage"]["base_path"])
        if not path.exists():
            pytest.skip("数据目录 'data/' 不存在，跳过数据完整性测试。")
        return path
    except (FileNotFoundError, KeyError):
        pytest.skip("无法加载'config.yaml'或找不到'storage.base_path'，跳过测试。")


@pytest.fixture(scope="module")
def all_parquet_files(data_path: Path) -> list[Path]:
    """扫描并返回所有已下载的 data.parquet 文件列表。"""
    files = list(data_path.rglob("data.parquet"))
    if not files:
        pytest.skip("在 'data/' 目录中未找到任何 'data.parquet' 文件，跳过测试。")
    return files


# ===================================================================
#           数据完整性测试用例 (简化版)
# ===================================================================


@pytest.mark.data_integrity
def test_all_files_schema_and_types(all_parquet_files: list[Path]):
    """
    【数据完整性】
    遍历所有Parquet文件，检查它们的Schema和核心列的数据类型。
    """
    # 使用tqdm来显示测试进度，因为这个测试可能会很慢
    for parquet_file in tqdm(all_parquet_files, desc="校验Schema和类型"):
        df = pd.read_parquet(parquet_file)

        # 基础Schema检查
        assert not df.empty, f"文件为空: {parquet_file}"

        # 假设所有日线类数据都应包含这些列
        required_cols = {"trade_date", "open", "high", "low", "close", "vol"}
        if "daily" in str(parquet_file):  # 只对日线数据进行此检查
            assert required_cols.issubset(df.columns), f"缺少核心列 in {parquet_file}"

            # 数据类型检查
            numeric_cols = ["open", "high", "low", "close", "vol"]
            for col in numeric_cols:
                assert pd.api.types.is_numeric_dtype(
                    df[col]
                ), f"列 '{col}' 的数据类型不是数值型 in {parquet_file}"

            assert pd.api.types.is_string_dtype(
                df["trade_date"]
            ), f"列 'trade_date' 的数据类型不是字符串 in {parquet_file}"


@pytest.mark.data_integrity
def test_all_files_value_sanity(all_parquet_files: list[Path]):
    """
    【数据完整性】
    遍历所有Parquet文件，检查数据值是否符合逻辑常规。
    此版本已优化，能正确处理 NaN 值。
    """
    for parquet_file in tqdm(all_parquet_files, desc="校验数据值逻辑"):
        if "daily" not in str(parquet_file):
            continue

        df = pd.read_parquet(parquet_file)

        # ---> 核心修正：在比较前，先移除 high 或 low 为 NaN 的行 <---
        # 我们只对具有有效高低价的行进行逻辑检查
        valid_price_df = df.dropna(subset=["high", "low", "open", "close"])

        if valid_price_df.empty:
            # 如果所有行都包含NaN，我们只打印一个警告，而不是让测试失败
            print(
                f"警告: 文件 {parquet_file} 中所有行的价格数据都包含NaN，跳过逻辑检查。"
            )
            continue

        # 价格逻辑检查 (现在在过滤后的 DataFrame 上进行)
        assert (
            valid_price_df["high"] >= valid_price_df["low"]
        ).all(), f"发现 high < low 的记录 in {parquet_file}"
        assert (
            valid_price_df["high"] >= valid_price_df["close"]
        ).all(), f"发现 high < close 的记录 in {parquet_file}"
        assert (
            valid_price_df["high"] >= valid_price_df["open"]
        ).all(), f"发现 high < open 的记录 in {parquet_file}"
        assert (
            valid_price_df["low"] <= valid_price_df["close"]
        ).all(), f"发现 low > close 的记录 in {parquet_file}"
        assert (
            valid_price_df["low"] <= valid_price_df["open"]
        ).all(), f"发现 low > open 的记录 in {parquet_file}"

        # 核心数值非负检查 (同样在过滤后的 DataFrame 上进行)
        assert (
            (valid_price_df[["open", "high", "low", "close", "vol"]] >= 0).all().all()
        ), f"发现负数价格或成交量 in {parquet_file}"

        # 日期唯一性和顺序检查 (这个检查应该在原始 DataFrame 上进行)
        assert (
            not df["trade_date"].duplicated().any()
        ), f"发现重复的日期 in {parquet_file}"
        assert df[
            "trade_date"
        ].is_monotonic_increasing, f"日期未按升序排列 in {parquet_file}"
