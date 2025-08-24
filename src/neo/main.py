"""Neo包主程序

基于四层架构的命令行应用程序。
"""

import typer
from typing import List, Optional


from neo.helpers.app_service import AppService
from neo.container import AppContainer
from dependency_injector.wiring import Provide

app = typer.Typer(help="Neo 股票数据处理系统命令行工具")


@app.command()
def dl(
    stock_codes: Optional[List[str]] = typer.Option(
        None, "--symbols", "-s", help="股票代码列表"
    ),
    group: Optional[str] = typer.Option(None, "--group", "-g", help="任务组名称"),
    task_type: Optional[str] = typer.Option(None, "--type", "-t", help="任务类型"),
    log_level: str = typer.Option(
        "info",
        "--log-level",
        "-l",
        help="日志级别 (debug, info, warning, error, critical)",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="仅显示将要执行的任务，不实际执行"
    ),
):
    """下载股票数据"""
    from neo.helpers.utils import setup_logging

    # 初始化下载日志配置
    setup_logging("download", log_level)

    container = AppContainer()
    task_builder = container.task_builder()
    group_handler = container.group_handler()
    app_service = container.app_service()

    # 处理组配置，获取股票代码和任务类型
    if not group:
        raise ValueError("必须指定任务组 (-g/--group) 参数")

    if stock_codes:
        symbols = stock_codes
    else:
        symbols = group_handler.get_symbols_for_group(group)

    if task_type:
        # 将字符串转换为TaskType枚举（转换为大写）
        from neo.task_bus.types import TaskType

        task_types = [getattr(TaskType, task_type)]
    else:
        task_types = group_handler.get_task_types_for_group(group)

    # 构建任务列表
    tasks = task_builder.build_tasks(symbols=symbols, task_types=task_types)

    # 运行下载器
    app_service.run_downloader(tasks, dry_run=dry_run)


def main(app_service: AppService = Provide[AppContainer.app_service]):
    """主函数"""
    app()


if __name__ == "__main__":
    main()
