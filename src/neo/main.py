"""Neo包主程序

基于四层架构的命令行应用程序。
"""

import typer
from typing import List, Optional


from neo.helpers.app_service import AppService
from neo.app import container
from dependency_injector.wiring import Provide

app = typer.Typer(help="Neo 股票数据处理系统命令行工具")


@app.command()
def dl(
    group: Optional[str] = typer.Option(None, "--group", "-g", help="任务组名称"),
    stock_codes: Optional[List[str]] = typer.Option(
        None, "--symbols", "-s", help="股票代码列表"
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="启用调试模式，输出详细日志",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="仅显示将要执行的任务，不实际执行"
    ),
):
    """下载股票数据"""
    from neo.helpers.utils import setup_logging

    # 初始化下载日志配置
    log_level = "debug" if debug else "info"
    setup_logging("download", log_level)

    # 使用共享的容器实例
    container.task_builder()
    container.group_handler()
    """下载指定分组的股票数据

    根据配置的更新策略，提交相应的下载构建任务。
    """
    from neo.tasks.huey_tasks import build_and_enqueue_downloads_task
    from neo.tasks.download_tasks import detect_task_group_strategy

    # 检测任务组的更新策略类型
    strategy = detect_task_group_strategy(group)
    strategy_text = {
        "full_replace": "全量替换",
        "incremental": "增量更新",
        "mixed": "混合策略",
        "unknown": "未知策略",
    }.get(strategy, "未知策略")

    typer.echo(f"正在提交任务组 '{group}' 的{strategy_text}下载构建任务...")

    # 使用 AppService 构建任务映射
    app_service = container.app_service()
    task_stock_mapping = app_service.build_task_stock_mapping_from_group(
        group, stock_codes
    )

    if not task_stock_mapping:
        typer.echo(f"⚠️ 任务组 '{group}' 没有找到任何任务或股票代码")
        return

    # 将任务提交到 Huey 慢速队列
    task_result = build_and_enqueue_downloads_task(task_stock_mapping)
    typer.echo(
        f"✅ 任务已成功提交到后台处理，任务ID: {task_result.id}。请启动消费者来执行任务。"
    )


@app.command()
def dp(
    queue_name: str = typer.Argument(
        ..., help="要启动的队列名称 ('fast', 'slow', 或 'maint')"
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="启用调试模式，输出详细日志",
    ),
):
    """启动指定队列的数据处理器消费者"""
    from neo.helpers.utils import setup_logging

    # 初始化消费者日志配置
    log_level = "debug" if debug else "info"
    setup_logging(f"consumer_{queue_name}", log_level)  # 为不同队列使用不同日志

    # 使用共享的容器实例
    app_service = container.app_service()

    # 启动指定队列的消费者
    app_service.run_data_processor(queue_name)


def main(app_service: AppService = Provide["AppContainer.app_service"]):
    """主函数"""
    app()


if __name__ == "__main__":
    main()
