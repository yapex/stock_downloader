import os
import sys
import typer
from typing_extensions import Annotated

# --- 动态添加项目根目录到 Python 路径 ---
# 假设 scripts 目录位于项目根目录下
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

try:
    from neo.helpers.metadata_sync import MetadataSyncManager
except ImportError:
    print("错误: 无法从 'neo.helpers.metadata_sync' 导入 MetadataSyncManager。")
    print("请确保您的项目结构和 PYTHONPATH 正确。")
    sys.exit(1)

# --- Typer 应用实例 ---
app = typer.Typer(
    help="一个用于管理 DuckDB 元数据与 Parquet 数据湖同步的 CLI 工具。",
    add_completion=False,
    no_args_is_help=True,
)


# --- Typer 命令函数 ---
@app.command(help="执行智能的、增量式的元数据同步。")
def sync(
    force_full_scan: Annotated[
        bool,
        typer.Option(
            "--force-full-scan",
            "-f",
            help="强制执行完整的状态比较扫描，忽略文件修改时间的快速检查。用于修复因文件删除等操作导致的状态不一致。",
        ),
    ] = False,
    mtime_check_minutes: Annotated[
        int,
        typer.Option(
            "--mtime-minutes",
            "-m",
            help="文件修改时间的检查窗口（分钟）。默认只检查最近变动的文件。设置为 0 表示检查所有文件。",
        ),
    ] = 30,
    db_file: Annotated[
        str, typer.Option(help="元数据数据库文件的路径。")
    ] = os.path.join(PROJECT_ROOT, "data/metadata.db"),
    parquet_dir: Annotated[
        str, typer.Option(help="Parquet 数据湖的根目录路径。")
    ] = os.path.join(PROJECT_ROOT, "data/parquet"),
):
    """
    主同步函数，由 Typer 装饰，提供丰富的命令行选项。
    """
    print("--- 启动元数据同步 ---")
    if force_full_scan:
        print("模式: 强制全量扫描（忽略文件修改时间）")
    elif mtime_check_minutes > 0:
        print(f"模式: 快速扫描")
    else:
        print("模式: 检查所有文件（不限制时间）")

    try:
        manager = MetadataSyncManager(
            metadata_db_path=db_file, parquet_base_path=parquet_dir
        )

        manager.sync(
            force_full_scan=force_full_scan, mtime_check_minutes=mtime_check_minutes
        )
    except Exception as e:
        print(f"\n脚本执行过程中发生严重错误: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
