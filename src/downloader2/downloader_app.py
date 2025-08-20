#!/usr/bin/env python3
"""基于 Typer 的下载器命令行应用程序"""

import typer
from typing import Optional
from .producer.downloader_manager import DownloaderManager
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
        help="股票代码列表，用引号包裹，逗号分隔，例如: ‘000001,600519‘ ",
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
            typer.echo(f"使用指定的股票代码: {symbol_list}")

        typer.echo(f"启动下载任务，任务组: {group}")

        # 创建下载管理器
        manager = DownloaderManager(task_group=group, symbols=symbol_list)

        # 如果是 dry-run 模式，仅验证参数不启动下载器
        if dry_run:
            typer.echo("参数验证成功，dry-run 模式不启动下载器")
            return

        # 启动下载器
        downloaders = manager.start()

        typer.echo(f"成功启动 {len(downloaders)} 个下载器")

        # 使用 DownloaderManager 的信号处理功能等待用户中断
        typer.echo("下载器正在运行中... 按 Ctrl+C 停止")

        # 等待用户中断信号
        manager.wait_for_stop()

        # 如果收到停止信号，优雅地停止下载器
        typer.echo("\n正在停止下载器...")
        manager.stop(downloaders)
        typer.echo("下载器已停止")

    except ValueError as e:
        typer.echo(f"错误: {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"意外错误: {e}", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
