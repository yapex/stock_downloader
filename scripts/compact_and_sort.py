#!/usr/bin/env python3
"""
数据压缩和排序脚本 (V2)

本脚本为新版数据架构服务，其核心职责是：
1. 校验数据目录结构是否符合规范。
2. 将分区内零散的 Parquet 文件合并、排序、重写为单个大文件，以提升查询性能。

使用方法:
    uv run python scripts/compact_and_sort.py --help
"""

import os
import sys
import shutil
import tomllib
from pathlib import Path
from typing import List, Dict, Any
import typer
from typing_extensions import Annotated

# --- 动态路径设置 ---
PROJECT_ROOT = Path(__file__).parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

try:
    import duckdb
except ImportError:
    print("错误: 无法导入 duckdb。请运行 'uv pip install duckdb'")
    sys.exit(1)

import pandas as pd

# --- 核心路径配置 ---
DATA_DIR = PROJECT_ROOT / "data" / "parquet"
TEMP_DIR = PROJECT_ROOT / "data" / "parquet_temp"
SCHEMA_FILE = PROJECT_ROOT / "stock_schema.toml"

# --- Typer 应用实例 ---
app = typer.Typer(
    help="数据压缩和排序工具 (V2) - 为新架构下的 Parquet 数据进行合并和排序。",
    add_completion=False,
    no_args_is_help=True,
)


def ask_user_confirmation(message: str, default: bool = False) -> bool:
    """简单的用户确认函数"""
    default_text = "[Y/n]" if default else "[y/N]"
    try:
        response = input(f"⚠️ {message} {default_text}: ").strip().lower()
        if not response:
            return default
        return response in ['y', 'yes']
    except (KeyboardInterrupt, EOFError):
        print("\n用户中断操作。")
        return False

def safe_remove_backup(backup_path: Path) -> bool:
    """安全删除备份目录，处理权限问题"""
    try:
        if backup_path.exists():
            # 先修复权限
            os.system(f"chmod -R u+w '{backup_path}'")
            shutil.rmtree(backup_path)
            print(f"  ✅ 备份已删除: {backup_path}")
            return True
    except Exception as e:
        print(f"  ❌ 删除备份失败 {backup_path}: {e}")
        return False
    return False

def load_schema_config() -> Dict[str, Any]:
    """加载 stock_schema.toml 配置文件"""
    if not SCHEMA_FILE.exists():
        raise FileNotFoundError(f"配置文件不存在: {SCHEMA_FILE}")
    with open(SCHEMA_FILE, "rb") as f:
        return tomllib.load(f)

def validate_source_directory_structure(source_path: Path, is_partitioned: bool):
    """前置校验：检查源目录结构是否符合新架构规范"""
    print(f"  校验目录结构: {source_path}")
    if not source_path.exists() or not source_path.is_dir():
        raise FileNotFoundError(f"源目录不存在或不是一个目录: {source_path}")

    if is_partitioned:
        # 分区表：目录下只允许存在 year=... 的子目录
        for item in source_path.iterdir():
            if item.is_file():
                raise ValueError(f"校验失败！分区表 {source_path.name} 的根目录不应包含任何文件，发现: {item.name}")
            if not item.name.startswith("year="):
                raise ValueError(f"校验失败！分区表 {source_path.name} 的子目录必须以 'year=' 开头，发现: {item.name}")
    else:
        # 非分区表：目录下只允许存在 .parquet 文件
        for item in source_path.iterdir():
            if item.is_dir():
                raise ValueError(f"校验失败！非分区表 {source_path.name} 的根目录不应包含任何子目录，发现: {item.name}")
            if item.suffix != ".parquet":
                raise ValueError(f"校验失败！非分区表 {source_path.name} 的根目录只应包含 .parquet 文件，发现: {item.name}")
    print("  ✅ 目录结构校验通过。")

def get_sort_columns(table_config: Dict[str, Any]) -> str:
    """根据表配置获取排序列 (不再包含 year)"""
    primary_key = table_config.get("primary_key", [])
    if not primary_key:
        raise ValueError(f"表 {table_config.get('table_name')} 没有定义主键")
    return ", ".join(primary_key)

def validate_data_consistency(con: duckdb.DuckDBPyConnection, table_name: str, source_path: Path, target_path: Path) -> bool:
    """验证源数据和优化后数据的一致性（简化版）"""
    print("  开始数据一致性验证...")
    try:
        source_pattern = f"'{source_path}/**/*.parquet'"
        target_pattern = f"'{target_path}/**/*.parquet'"

        source_count = con.execute(f"SELECT COUNT(*) FROM read_parquet({source_pattern}, hive_partitioning=1)").fetchone()[0]
        target_count = con.execute(f"SELECT COUNT(*) FROM read_parquet({target_pattern}, hive_partitioning=1)").fetchone()[0]

        print(f"    记录数验证: 源={source_count:,}, 优化后={target_count:,}")
        if source_count != target_count:
            if not ask_user_confirmation(f"表 {table_name} 记录数不一致 (源: {source_count}, 目标: {target_count})。这通常是由于去重导致。是否继续？", default=True):
                return False
        print("    ✅ 记录数校验通过 (或用户确认)。")
        return True
    except Exception as e:
        print(f"  ❌ 验证过程中发生错误: {e}")
        if not ask_user_confirmation("验证失败，是否强行继续？", default=False):
            return False
    return True

def optimize_table(con: duckdb.DuckDBPyConnection, table_name: str, table_config: Dict[str, Any]):
    """对单个表的数据进行排序、分区和重写"""
    source_path = DATA_DIR / table_name
    target_path = TEMP_DIR / table_name
    
    is_partitioned = "date_col" in table_config

    # 1. 前置校验
    validate_source_directory_structure(source_path, is_partitioned)

    # 2. 准备 SQL
    sort_columns = get_sort_columns(table_config)
    source_pattern = f"'{source_path}/**/*.parquet'"
    
    copy_statement = f"""
    COPY (
        SELECT * FROM read_parquet({source_pattern}, hive_partitioning=1)
        ORDER BY {sort_columns}
    )
    TO '{target_path}'
    (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_IGNORE 1); 
    """ if is_partitioned else f"""
    COPY (
        SELECT * FROM read_parquet({source_pattern})
        ORDER BY {sort_columns}
    )
    TO '{target_path}'
    (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1);
    """

    print(f"  准备执行优化 SQL...")
    print(copy_statement)

    # 3. 执行优化
    if target_path.exists():
        shutil.rmtree(target_path)
    con.execute(copy_statement)
    print(f"  ✅ 表 {table_name} 已成功优化到临时目录: {target_path}")

    # 4. 数据一致性校验
    if not validate_data_consistency(con, table_name, source_path, target_path):
        print(f"❌ {table_name} 数据验证失败，优化中断。临时数据保留在 {target_path}")
        raise typer.Exit(1)

    # 5. 替换旧数据
    backup_path = source_path.with_suffix(f".backup_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}")
    print(f"  正在用优化后的数据替换旧数据...备份至: {backup_path}")
    source_path.rename(backup_path)
    target_path.rename(source_path)
    print("  ✅ 数据替换成功。 সন")

    # 6. 清理备份
    if ask_user_confirmation(f"是否删除备份目录 {backup_path}？", default=True):
        safe_remove_backup(backup_path)

@app.command(help="优化指定的表")
def optimize(
    table: Annotated[str, typer.Option("--table", "-t", help="要优化的表名")],
):
    """优化指定的表"""
    try:
        schema_config = load_schema_config()
        if table not in schema_config:
            raise ValueError(f"表 '{table}' 在配置文件中不存在")
        
        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(database=':memory:') as con:
            optimize_table(con, table, schema_config[table])
        print(f"\n🎉 表 {table} 优化完成！")

    except (ValueError, FileNotFoundError) as e:
        print(f"\n❌ 错误: {e}")
        raise typer.Exit(1)
    finally:
        if TEMP_DIR.exists() and not any(TEMP_DIR.iterdir()):
            TEMP_DIR.rmdir()

@app.command(help="优化所有表")
def optimize_all(
    exclude: Annotated[List[str], typer.Option("--exclude", "-e", help="要排除的表名")] = None,
):
    """优化所有表"""
    exclude = exclude or []
    schema_config = load_schema_config()
    tables_to_optimize = [name for name in schema_config.keys() if name not in exclude]
    
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    
    with duckdb.connect(database=':memory:') as con:
        for i, table_name in enumerate(tables_to_optimize):
            print(f"\n---\n处理表 {i+1}/{len(tables_to_optimize)}: {table_name}")
            try:
                optimize_table(con, table_name, schema_config[table_name])
            except (ValueError, FileNotFoundError) as e:
                print(f"❌ 跳过表 {table_name}，原因: {e}")
                continue
    
    print("\n🎉 所有表优化完成！")
    if TEMP_DIR.exists() and not any(TEMP_DIR.iterdir()):
        TEMP_DIR.rmdir()

@app.command(help="清理所有旧的备份目录")
def clean_backups(
    force: Annotated[bool, typer.Option("--force", "-f", help="强制删除，不询问用户")] = False,
):
    """清理所有旧的备份目录"""
    if not DATA_DIR.exists():
        print(f"❌ 数据目录不存在: {DATA_DIR}")
        raise typer.Exit(1)
    
    # 查找所有备份目录
    backup_dirs = []
    for item in DATA_DIR.iterdir():
        if item.is_dir() and (".backup" in item.name or item.name.endswith(".backup")):
            backup_dirs.append(item)
    
    if not backup_dirs:
        print("✅ 没有发现需要清理的备份目录。")
        return
    
    print(f"发现 {len(backup_dirs)} 个备份目录:")
    for backup_dir in backup_dirs:
        print(f"  - {backup_dir.name}")
    
    if not force:
        if not ask_user_confirmation(f"是否删除所有 {len(backup_dirs)} 个备份目录？", default=False):
            print("用户取消操作。")
            return
    
    # 删除备份
    success_count = 0
    for backup_dir in backup_dirs:
        print(f"\n正在删除: {backup_dir}")
        if safe_remove_backup(backup_dir):
            success_count += 1
    
    print(f"\n🎉 清理完成！成功删除 {success_count}/{len(backup_dirs)} 个备份目录。")

if __name__ == "__main__":
    app()
