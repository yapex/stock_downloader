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
import uuid
from pathlib import Path
from typing import List, Dict, Any
import typer
from typing_extensions import Annotated

# --- 动态路径设置 ---
PROJECT_ROOT = Path(__file__).parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

# --- 导入新的compact_and_sort模块 ---
try:
    from neo.helpers.compact_and_sort import (
        CompactAndSortServiceImpl,
        HybridDeduplicationStrategy,
        UUIDFilenameGenerator
    )
    NEW_MODULE_AVAILABLE = True
except ImportError:
    NEW_MODULE_AVAILABLE = False
    # 定义假的类以避免NameError
    class CompactAndSortServiceImpl:
        def __init__(self, *args, **kwargs):
            pass
        def compact_and_sort_table(self, *args, **kwargs):
            pass
    class HybridDeduplicationStrategy:
        pass
    class UUIDFilenameGenerator:
        pass

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

# --- 系统文件白名单 ---
SYSTEM_FILES_WHITELIST = {
    ".DS_Store",        # macOS 系统文件
    "Thumbs.db",        # Windows 系统文件
    ".gitignore",       # Git 配置文件
    "desktop.ini",      # Windows 系统文件
    ".localized",       # macOS 本地化文件
    "._.DS_Store",      # macOS 网络卷上的双点文件
    "Icon\r",           # macOS 自定义图标文件
}

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

# 旧函数已移至compact_and_sort模块，不再需要

def optimize_table(con: duckdb.DuckDBPyConnection, table_name: str, table_config: Dict[str, Any]):
    """对单个表的数据进行排序、分区和重写"""
    
    # 使用新模块进行优化
    service = CompactAndSortServiceImpl(
        data_dir=DATA_DIR,
        temp_dir=TEMP_DIR,
        deduplication_strategy=HybridDeduplicationStrategy(),
        filename_generator=UUIDFilenameGenerator()
    )
    service.compact_and_sort_table(table_name, table_config)
    print(f"  ✅ 表 {table_name} 优化完成")

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
