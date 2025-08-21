#!/usr/bin/env python3
"""基于 Typer 的下载器命令行应用程序"""

import typer
from typing import Optional
from .manager.downloader_manager import DownloaderManager
from .producer.tushare_downloader import TushareDownloader
from .utils import setup_logging
import logging

logger = logging.getLogger(__name__)
setup_logging(logger)

app = typer.Typer(help="股票数据下载器命令行工具")


@app.command()
def download_command(
    group: str = typer.Option(..., "--group", "-g", help="任务组名称，例如: all, test"),
    symbols: Optional[str] = typer.Option(
        None,
        "--symbols",
        "-s",
        help="股票代码列表，用引号包裹，逗号分隔，例如: '000001,600519' ",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="仅验证参数，不实际启动下载器"
    ),
):
    """启动股票数据下载任务"""
    try:
        # 解析 symbols 参数
        symbol_list = None
        if symbols:
            # 去除空格并分割
            symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]

        # 如果是 dry-run 模式，仅验证参数不启动下载器
        if dry_run:
            typer.echo("参数验证成功，dry-run 模式不启动下载器")
            return

        # 创建下载管理器，使用 TushareDownloader 作为任务执行器
        tushare_executor = TushareDownloader()
        manager = DownloaderManager.create_from_config(
            task_executor=tushare_executor,
            max_workers=4,
            enable_progress_bar=True
        )

        # 执行下载任务组
        stats = manager.download_group(
            group=group,
            symbols=symbol_list,
            max_retries=3  # 默认重试次数
        )
        
        # 输出执行统计信息
        typer.echo(f"任务执行完成:")
        typer.echo(f"  总任务数: {stats.total_tasks}")
        typer.echo(f"  成功任务数: {stats.successful_tasks}")
        typer.echo(f"  失败任务数: {stats.failed_tasks}")
        typer.echo(f"  成功率: {stats.success_rate:.2%}")

    except ValueError as e:
        typer.echo(f"错误: {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"意外错误: {e}", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
