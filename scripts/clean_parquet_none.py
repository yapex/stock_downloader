#!/usr/bin/env python3
"""
清理 Parquet 文件中的 None 字符串脚本

由于 metadata.db 中存储的是视图而不是基础表，无法直接更新。
此脚本直接处理 Parquet 文件，将字符串 'None' 替换为 NULL 值。

使用方法:
    uv run python scripts/clean_parquet_none.py --help
    uv run python scripts/clean_parquet_none.py --table balance_sheet
    uv run python scripts/clean_parquet_none.py --all
"""

import os
import sys
import shutil
import tomllib
from pathlib import Path
from typing import List, Dict, Any, Optional
import typer
from typing_extensions import Annotated

# 动态添加项目根目录到 Python 路径
PROJECT_ROOT = Path(__file__).parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

try:
    import duckdb
    import pandas as pd
except ImportError as e:
    print(f"错误: 无法导入必要的库: {e}")
    print("请确保已安装 duckdb 和 pandas")
    sys.exit(1)

# 配置路径
DATA_DIR = PROJECT_ROOT / "data" / "parquet"
TEMP_DIR = PROJECT_ROOT / "data" / "parquet_clean_temp"
SCHEMA_FILE = PROJECT_ROOT / "stock_schema.toml"
METADATA_DB = PROJECT_ROOT / "data" / "metadata.db"

# Typer 应用实例
app = typer.Typer(
    help="清理 Parquet 文件中的 None 字符串工具",
    add_completion=False,
    no_args_is_help=True,
)


def load_schema_config() -> Dict[str, Any]:
    """加载 stock_schema.toml 配置文件"""
    if not SCHEMA_FILE.exists():
        raise FileNotFoundError(f"配置文件不存在: {SCHEMA_FILE}")
    
    with open(SCHEMA_FILE, "rb") as f:
        return tomllib.load(f)


def get_text_columns(table_config: Dict[str, Any]) -> List[str]:
    """获取表中的文本列"""
    columns = table_config.get("columns", [])
    text_columns = []
    
    for col in columns:
        if col.get("type") == "TEXT":
            text_columns.append(col["name"])
    
    return text_columns


def clean_table_parquet(table_name: str, table_config: Dict[str, Any], dry_run: bool = False) -> bool:
    """清理单个表的 Parquet 文件中的 None 字符串"""
    source_path = DATA_DIR / table_name
    target_path = TEMP_DIR / table_name
    
    if not source_path.exists():
        print(f"源目录不存在，跳过表: {table_name}")
        return False
    
    print(f"开始清理表: {table_name}...")
    
    try:
        # 获取需要清理的文本列
        text_columns = get_text_columns(table_config)
        
        if not text_columns:
            print(f" -> 表 {table_name} 没有文本列，跳过")
            return True
        
        print(f" -> 需要清理的文本列: {', '.join(text_columns)}")
        
        if dry_run:
            print(f" -> [DRY RUN] 将清理表 {table_name} 中的 None 字符串")
            return True
        
        # 使用 DuckDB 读取和清理数据
        with duckdb.connect(database=':memory:') as con:
            # 首先检查实际的列类型
            print(f" -> 检查实际列类型...")
            
            # 读取一小部分数据来检查实际类型，启用 union_by_name 来处理不同 schema
            sample_query = f"SELECT * FROM read_parquet('{source_path}/**/*.parquet', hive_partitioning=0, union_by_name=true) LIMIT 1"
            sample_df = con.execute(sample_query).fetchdf()
            
            # 获取实际的字符串类型列
            actual_text_columns = []
            for col_name in sample_df.columns:
                if col_name in text_columns and sample_df[col_name].dtype == 'object':
                    actual_text_columns.append(col_name)
            
            print(f" -> 实际需要清理的文本列: {', '.join(actual_text_columns) if actual_text_columns else '无'}")
            
            if not actual_text_columns:
                print(f" -> 表 {table_name} 没有实际的文本列需要清理，跳过")
                return True
            
            # 构建清理 SQL
            # 只为实际的文本列创建 CASE 语句来替换 'None' 为 NULL
            select_clauses = []
            
            # 获取所有列信息
            all_columns = [col["name"] for col in table_config.get("columns", [])]
            
            for col_name in all_columns:
                if col_name in actual_text_columns:
                    # 对实际文本列进行 None 清理，使用引号包围列名以避免关键字冲突
                    select_clauses.append(
                        f"CASE WHEN \"{col_name}\" = 'None' THEN NULL ELSE \"{col_name}\" END AS \"{col_name}\""
                    )
                else:
                    # 非文本列直接选择，使用引号包围列名
                    select_clauses.append(f"\"{col_name}\"")
            
            select_clause = ",\n                ".join(select_clauses)
            
            # 检查是否有日期列用于分区
            date_col = table_config.get("date_col")
            
            if date_col:
                # 有日期列，按年份分区
                sql_command = f"""
                COPY (
                    SELECT {select_clause},
                           CAST(SUBSTR(\"{date_col}\", 1, 4) AS INTEGER) AS year
                    FROM read_parquet('{source_path}/**/*.parquet', hive_partitioning=0, union_by_name=true)
                )
                TO '{target_path}'
                (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_IGNORE 1);
                """
            else:
                # 无日期列，不分区
                sql_command = f"""
                COPY (
                    SELECT {select_clause}
                    FROM read_parquet('{source_path}/**/*.parquet', hive_partitioning=0, union_by_name=true)
                )
                TO '{target_path}'
                (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1);
                """
            
            print(f" -> 执行清理操作...")
            con.execute(sql_command)
            print(f" -> 表 {table_name} 清理完成")
        
        # 替换原文件
        backup_path = source_path.with_suffix(".backup")
        
        print(f" -> 正在替换原数据...")
        if backup_path.exists():
            shutil.rmtree(backup_path)
        
        # 原子操作：备份 -> 移动新数据
        source_path.rename(backup_path)
        target_path.rename(source_path)
        
        print(f" -> 数据替换完成。备份保存在: {backup_path}")
        return True
        
    except Exception as e:
        print(f" -> 清理表 {table_name} 时发生错误: {e}")
        # 清理临时目录
        if target_path.exists():
            shutil.rmtree(target_path)
        return False


@app.command(help="清理指定表的 None 字符串")
def clean(
    table: Annotated[
        str,
        typer.Option("--table", "-t", help="要清理的表名")
    ],
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="仅显示将要执行的操作，不实际执行")
    ] = False,
):
    """清理指定表的 None 字符串"""
    try:
        schema_config = load_schema_config()
        
        if table not in schema_config:
            print(f"错误: 表 '{table}' 在配置文件中不存在")
            available_tables = list(schema_config.keys())
            print(f"可用的表: {', '.join(available_tables)}")
            raise typer.Exit(1)
        
        table_config = schema_config[table]
        
        if not dry_run:
            # 创建临时目录
            TEMP_DIR.mkdir(parents=True, exist_ok=True)
        
        success = clean_table_parquet(table, table_config, dry_run)
        
        if success and not dry_run:
            print(f"\n表 {table} 清理完成！")
            print("请运行以下命令更新元数据:")
            print(f"uv run python scripts/manual_sync_metadata.py sync")
        elif not success:
            print(f"\n表 {table} 清理失败！")
            raise typer.Exit(1)
            
    except Exception as e:
        print(f"执行过程中发生错误: {e}")
        raise typer.Exit(1)


@app.command(help="清理所有表的 None 字符串")
def clean_all(
    exclude: Annotated[
        List[str],
        typer.Option("--exclude", "-e", help="要排除的表名（可多次指定）")
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="仅显示将要执行的操作，不实际执行")
    ] = False,
):
    """清理所有表的 None 字符串"""
    exclude = exclude or []
    
    try:
        schema_config = load_schema_config()
        tables_to_clean = [name for name in schema_config.keys() if name not in exclude]
        
        if dry_run:
            print("将要清理的表:")
            for table_name in tables_to_clean:
                table_config = schema_config[table_name]
                text_columns = get_text_columns(table_config)
                if text_columns:
                    print(f"  {table_name}: {', '.join(text_columns)}")
                else:
                    print(f"  {table_name}: 无文本列")
            return
        
        # 创建临时目录
        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        
        success_count = 0
        failed_tables = []
        
        for table_name in tables_to_clean:
            table_config = schema_config[table_name]
            print(f"\n{'='*50}")
            
            if clean_table_parquet(table_name, table_config, dry_run):
                success_count += 1
            else:
                failed_tables.append(table_name)
        
        print(f"\n{'='*50}")
        print(f"清理完成! 成功: {success_count}, 失败: {len(failed_tables)}")
        
        if failed_tables:
            print(f"失败的表: {', '.join(failed_tables)}")
        
        if success_count > 0:
            print("\n请运行以下命令更新元数据:")
            print(f"uv run python scripts/manual_sync_metadata.py sync")
            
    except Exception as e:
        print(f"执行过程中发生错误: {e}")
        raise typer.Exit(1)


@app.command(help="列出所有表及其文本列")
def list_tables():
    """列出所有表及其文本列"""
    try:
        schema_config = load_schema_config()
        
        print("所有表及其文本列:")
        for table_name, table_config in schema_config.items():
            text_columns = get_text_columns(table_config)
            description = table_config.get("description", "")
            
            print(f"\n  {table_name}:")
            print(f"    描述: {description}")
            if text_columns:
                print(f"    文本列: {', '.join(text_columns)}")
            else:
                print(f"    文本列: 无")
            
    except Exception as e:
        print(f"加载配置时发生错误: {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()