"""测试数据压缩和排序脚本"""

import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from unittest.mock import patch, MagicMock

# 假定脚本位于项目根目录下的 scripts/ 文件夹中
# 为了能正确导入，我们需要将 src 目录添加到 sys.path
import sys
PROJECT_ROOT = Path(__file__).parent.parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

# 现在可以导入脚本中的函数了
from scripts.compact_and_sort import (
    optimize_table,
    validate_source_directory_structure,
    get_sort_columns,
)


@pytest.fixture
def partitioned_table_config():
    """模拟一个按年分区的表的 schema 配置"""
    return {
        "table_name": "income",
        "date_col": "f_ann_date",
        "primary_key": ["ts_code", "ann_date"],
    }


@pytest.fixture
def non_partitioned_table_config():
    """模拟一个非分区表 (如 stock_basic) 的 schema 配置"""
    return {
        "table_name": "stock_basic",
        "primary_key": ["ts_code"],
    }


# --- 测试目录结构校验 --- 

def test_validation_success_for_correct_partitioned_dir(tmp_path: Path):
    """测试：对于结构正确的已分区目录，校验应通过"""
    (tmp_path / "year=2023").mkdir()
    (tmp_path / "year=2024").mkdir()
    # 校验函数没有返回值，不抛出异常即为通过
    validate_source_directory_structure(tmp_path, is_partitioned=True)


def test_validation_success_for_correct_non_partitioned_dir(tmp_path: Path):
    """测试：对于结构正确的非分区目录，校验应通过"""
    (tmp_path / "data1.parquet").touch()
    (tmp_path / "data2.parquet").touch()
    validate_source_directory_structure(tmp_path, is_partitioned=False)


def test_validation_fails_for_partitioned_dir_with_files(tmp_path: Path):
    """测试：对于已分区目录，如果根下有文件，校验应失败"""
    (tmp_path / "year=2023").mkdir()
    (tmp_path / "rogue_file.txt").touch()
    with pytest.raises(ValueError, match="分区表.*不应包含任何文件"):
        validate_source_directory_structure(tmp_path, is_partitioned=True)


def test_validation_fails_for_partitioned_dir_with_other_dirs(tmp_path: Path):
    """测试：对于已分区目录，如果根下有非 year= 的目录，校验应失败"""
    (tmp_path / "year=2023").mkdir()
    (tmp_path / "ts_code=123").mkdir()
    with pytest.raises(ValueError, match="分区表.*的子目录必须以 'year=' 开头"):
        validate_source_directory_structure(tmp_path, is_partitioned=True)


def test_validation_fails_for_non_partitioned_dir_with_subdir(tmp_path: Path):
    """测试：对于非分区目录，如果根下有子目录，校验应失败"""
    (tmp_path / "data.parquet").touch()
    (tmp_path / "some_dir").mkdir()
    with pytest.raises(ValueError, match="非分区表.*不应包含任何子目录"):
        validate_source_directory_structure(tmp_path, is_partitioned=False)


# --- 测试系统文件白名单机制 ---

def test_validation_ignores_system_files_in_partitioned_dir(tmp_path: Path):
    """测试：分区表目录中的系统文件应该被忽略"""
    # 创建正常的分区目录
    (tmp_path / "year=2023").mkdir()
    (tmp_path / "year=2024").mkdir()
    
    # 添加系统文件
    (tmp_path / ".DS_Store").touch()      # macOS 系统文件
    (tmp_path / "Thumbs.db").touch()      # Windows 系统文件
    (tmp_path / ".gitignore").touch()     # Git 配置文件
    
    # 校验应该通过，系统文件会被忽略
    validate_source_directory_structure(tmp_path, is_partitioned=True)


def test_validation_ignores_system_files_in_non_partitioned_dir(tmp_path: Path):
    """测试：非分区表目录中的系统文件应该被忽略"""
    # 创建正常的 parquet 文件
    (tmp_path / "data1.parquet").touch()
    (tmp_path / "data2.parquet").touch()
    
    # 添加系统文件
    (tmp_path / ".DS_Store").touch()      # macOS 系统文件
    (tmp_path / "desktop.ini").touch()    # Windows 系统文件  
    (tmp_path / ".localized").touch()     # macOS 本地化文件
    
    # 校验应该通过，系统文件会被忽略
    validate_source_directory_structure(tmp_path, is_partitioned=False)


def test_validation_still_fails_for_non_system_files(tmp_path: Path):
    """测试：非系统文件仍然会导致校验失败"""
    # 创建正常的分区目录
    (tmp_path / "year=2023").mkdir()
    
    # 添加系统文件（应该被忽略）
    (tmp_path / ".DS_Store").touch()
    
    # 添加非系统文件（应该导致失败）
    (tmp_path / "bad_file.txt").touch()
    
    # 校验应该失败，因为存在非系统的违规文件
    with pytest.raises(ValueError, match="分区表.*不应包含任何文件.*bad_file.txt"):
        validate_source_directory_structure(tmp_path, is_partitioned=True)


# --- 测试排序键生成 ---

def test_get_sort_columns_for_partitioned_table(partitioned_table_config):
    """测试：为分区表生成的排序列不应包含 year"""
    sort_cols_str = get_sort_columns(partitioned_table_config)
    assert "year" not in sort_cols_str
    assert "ts_code" in sort_cols_str
    assert "ann_date" in sort_cols_str
    assert sort_cols_str == "ts_code, ann_date"


def test_get_sort_columns_for_non_partitioned_table(non_partitioned_table_config):
    """测试：为非分区表生成的排序列"""
    sort_cols_str = get_sort_columns(non_partitioned_table_config)
    assert sort_cols_str == "ts_code"


# --- 测试核心优化逻辑 ---

@patch("scripts.compact_and_sort.ask_user_confirmation", return_value=True)
@patch("scripts.compact_and_sort.validate_data_consistency", return_value=(True, False))
@patch("scripts.compact_and_sort.shutil.rmtree")
@patch("pathlib.Path.rename")
def test_optimize_partitioned_table_uses_correct_sql(
    mock_rename, mock_rmtree, mock_validate, mock_ask,
    tmp_path: Path, 
    partitioned_table_config: dict
):
    """TDD: 验证为分区表生成的 DuckDB SQL 是正确的"""
    # GIVEN
    table_name = partitioned_table_config["table_name"]
    source_root = tmp_path / "data" / "parquet"
    temp_root = tmp_path / "temp"

    # 创建脚本期望的源目录结构
    source_table_path = source_root / table_name
    source_table_path.mkdir(parents=True)
    (source_table_path / "year=2023").mkdir()
    (source_table_path / "year=2023" / "data.parquet").touch()

    mock_con = MagicMock()

    # WHEN
    with patch("scripts.compact_and_sort.DATA_DIR", source_root):
        with patch("scripts.compact_and_sort.TEMP_DIR", temp_root):
            optimize_table(mock_con, table_name, partitioned_table_config)

    # THEN
    # 1. 验证执行的 SQL 命令
    executed_sql = mock_con.execute.call_args[0][0]
    
    # 1a. 验证读取路径是正确的
    assert f"FROM read_parquet('{source_table_path}/**/*.parquet'" in executed_sql
    
    # 1b. 验证 ORDER BY 子句是正确的（不含 year）
    assert "ORDER BY ts_code, ann_date" in executed_sql
    
    # 1c. 验证 PARTITION BY 子句是正确的
    assert "PARTITION_BY (year)" in executed_sql


@patch("scripts.compact_and_sort.ask_user_confirmation", return_value=True)
@patch("scripts.compact_and_sort.validate_data_consistency", return_value=(True, False))
@patch("scripts.compact_and_sort.shutil.rmtree")
@patch("pathlib.Path.rename")
def test_optimize_non_partitioned_table_preserves_directory_structure(
    mock_rename, mock_rmtree, mock_validate, mock_ask,
    tmp_path: Path, 
    non_partitioned_table_config: dict
):
    """TDD: 验证非分区表优化后保持目录结构"""
    # GIVEN
    table_name = non_partitioned_table_config["table_name"]
    source_root = tmp_path / "data" / "parquet"
    temp_root = tmp_path / "temp"

    # 创建脚本期望的源目录结构（非分区表）
    source_table_path = source_root / table_name
    source_table_path.mkdir(parents=True)
    (source_table_path / "data1.parquet").touch()
    (source_table_path / "data2.parquet").touch()

    mock_con = MagicMock()

    # WHEN
    with patch("scripts.compact_and_sort.DATA_DIR", source_root):
        with patch("scripts.compact_and_sort.TEMP_DIR", temp_root):
            optimize_table(mock_con, table_name, non_partitioned_table_config)

    # THEN
    # 1. 验证执行的 SQL 命令
    executed_sql = mock_con.execute.call_args[0][0]
    
    # 1a. 验证读取路径是正确的
    assert f"FROM read_parquet('{source_table_path}/**/*.parquet'" in executed_sql
    
    # 1b. 验证 ORDER BY 子句是正确的（对于 stock_basic 只有 ts_code）
    assert "ORDER BY ts_code" in executed_sql
    
    # 1c. 验证目标路径指向的是具体文件，不是目录
    # 由于使用了UUID，文件名会包含uuid，所以只验证表名部分
    assert f"/{table_name}/{table_name}-" in executed_sql
    assert ".parquet'" in executed_sql
    
    # 1d. 验证没有 PARTITION_BY 子句
    assert "PARTITION_BY" not in executed_sql
    
    # 1e. 验证不使用 hive_partitioning
    assert "hive_partitioning=1" not in executed_sql
