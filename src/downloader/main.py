# -*- coding: utf-8 -*-
"""
命令行接口 (CLI) 入口点。
"""
import logging
import sys
import warnings
from datetime import datetime
from typing import List, Optional

import typer
from dotenv import load_dotenv

from .app import DownloaderApp
from .config import load_config
from .logging_setup import setup_logging

# --- 忽略来自 tushare 的 FutureWarning ---
warnings.filterwarnings("ignore", category=FutureWarning, module="tushare")

load_dotenv()

# --- Typer 应用定义 ---
app = typer.Typer(
    name="downloader",
    help="一个基于 Tushare Pro 的、可插拔的个人量化数据下载器。",
    add_completion=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    symbols: Optional[List[str]] = typer.Option(
        None,
        "--symbols",
        "-s",
        help=(
            "【可选】指定一个或多个股票代码 (例如 --symbols 600519.SH --symbols 000001.SZ)。"
            "如果第一个是 'all'，则下载所有A股。"
            "如果未提供，则使用配置文件中的设置。"
        ),
    ),
    group: str = typer.Option(
        "default",
        "--group",
        "-g",
        help="指定要执行的任务组。",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="强制执行所有启用的任务，无视冷却期。",
        show_default=False,
    ),
    config_file: str = typer.Option(
        "config.yaml",
        "--config",
        "-c",
        help="指定配置文件的路径。",
    ),
):
    """
    程序的主执行函数。
    """
    if ctx.invoked_subcommand is not None:
        return

    # 创建临时的启动处理器，确保"正在启动..."消息能够即时输出
    root_logger = logging.getLogger()
    startup_handler = logging.StreamHandler(sys.stdout)
    startup_handler.setFormatter(logging.Formatter("%(message)s"))
    root_logger.addHandler(startup_handler)
    root_logger.setLevel(logging.INFO)
    
    logging.info("正在启动...")

    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("初始化组件...")

    separator = "=" * 30
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.debug(f"\n\n{separator} 程序开始运行: {timestamp} {separator}\n")

    downloader_app = DownloaderApp(logger)

    try:
        downloader_app.run_download(config_path=config_file, group_name=group, symbols=symbols, force=force)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.debug(f"\n{separator} 程序运行结束: {timestamp} {separator}\n")

    except (ValueError, FileNotFoundError) as e:
        logger.critical(f"程序启动失败: {e}")
    except Exception as e:
        logger.critical(f"程序主流程发生严重错误: {e}", exc_info=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.fatal(f"\n{separator} 程序异常终止: {timestamp} {separator}\n")


@app.command()
def list_groups(
    config_file: str = typer.Option(
        "config.yaml",
        "--config",
        "-c",
        help="指定配置文件的路径。",
    ),
):
    """
    列出配置文件中所有可用的任务组。
    """
    try:
        config = load_config(config_file)
        if "groups" not in config or not config["groups"]:
            print("配置文件中没有定义任何组。")
            return

        print("可用的任务组:")
        for name, group_info in config["groups"].items():
            desc = group_info.get("description", "无描述")
            print(f"  - {name}: {desc}")

    except FileNotFoundError:
        print(f"错误: 配置文件 '{config_file}' 未找到。")
    except Exception as e:
        print(f"读取配置时出错: {e}")


@app.command()
def summary(
    config_file: str = typer.Option(
        "config.yaml",
        "--config",
        "-c",
        help="指定配置文件的路径。",
    ),
):
    """
    显示数据库中所有表的记录数摘要。
    """
    try:
        from tabulate import tabulate
        from .storage import DuckDBStorage

        config = load_config(config_file)
        storage_config = config.get("storage", {})
        db_path = storage_config.get("path", "data/stock_data.db")

        if storage_config.get("type", "duckdb") != "duckdb":
            print("错误: 'summary' 命令仅支持 'duckdb' 存储类型。")
            raise typer.Exit(code=1)

        storage = DuckDBStorage(db_path)
        summary_data = storage.get_summary()

        if not summary_data:
            print("数据库中没有找到任何表。")
            return

        # 排序数据
        summary_data.sort(key=lambda x: x["table_name"])
        
        # 准备表格数据
        headers = ["表名", "记录数"]
        table_data = [[item["table_name"], item["record_count"]] for item in summary_data]

        print("数据库内容摘要:")
        print(tabulate(table_data, headers=headers, tablefmt="grid"))

    except FileNotFoundError:
        print(f"错误: 配置文件 '{config_file}' 未找到。")
    except ImportError:
        print("错误: 'tabulate' 未安装。请运行 'pip install tabulate'。")
    except Exception as e:
        print(f"执行摘要时出错: {e}")


if __name__ == "__main__":
    app()
