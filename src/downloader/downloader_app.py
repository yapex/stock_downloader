#!/usr/bin/env python3
"""基于 Typer 的下载器命令行应用程序"""

import typer
from typing import Optional, List
from .manager.downloader_manager import DownloaderManager
from .producer.fetcher_builder import TaskType
from .task.types import TaskPriority
from .config import get_config
from .utils import setup_logging
import logging

logger = logging.getLogger(__name__)
setup_logging(logger)

# 任务名称到 TaskType 的映射
TASK_NAME_TO_TYPE = {
    "stock_basic": TaskType.STOCK_BASIC,
    "stock_daily": TaskType.STOCK_DAILY,
    "stock_adj_qfq": TaskType.DAILY_BAR_QFQ,
    "balance_sheet": TaskType.BALANCESHEET,
    "income_statement": TaskType.INCOME,
    "cash_flow": TaskType.CASHFLOW,
}

def get_task_types_from_group(group: str) -> List[TaskType]:
    """根据任务组名称获取任务类型列表"""
    config = get_config()
    
    if group not in config.task_groups:
        raise ValueError(f"未找到任务组: {group}")
    
    task_names = config.task_groups[group]
    task_types = []
    
    for task_name in task_names:
        if task_name not in TASK_NAME_TO_TYPE:
            raise ValueError(f"未知的任务类型: {task_name}")
        task_types.append(TASK_NAME_TO_TYPE[task_name])
    
    return task_types

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

        # 获取任务类型列表
        task_types = get_task_types_from_group(group)

        # 如果是 dry-run 模式，仅验证参数不启动下载器
        if dry_run:
            typer.echo("参数验证成功，dry-run 模式不启动下载器")
            return

        # 创建下载管理器
        manager = DownloaderManager(
            max_workers=4,
            task_executor=None,  # 使用默认的 TaskExecutor
            task_type_config=None,  # 使用默认的任务类型配置
            enable_progress_bar=True
        )

        # 添加下载任务
        for task_type in task_types:
            manager.add_download_tasks(
                symbols=symbol_list or ["all"],  # 如果没有指定股票代码，使用 "all"
                task_type=task_type,
                priority=TaskPriority.MEDIUM,  # 默认优先级
                max_retries=3  # 默认重试次数
            )

        # 启动下载任务
        manager.start()
        stats = manager.run()
        manager.stop()
        
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
