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
    task_builder = container.task_builder()
    group_handler = container.group_handler()
    """下载指定分组的股票数据

    通过向 Huey 慢速队列提交一个构建任务来启动整个增量下载流程。
    """
    from neo.tasks.huey_tasks import build_and_enqueue_downloads_task
    
    typer.echo(f"正在提交任务组 '{group}' 的增量下载构建任务...")
    # 将任务提交到 Huey 慢速队列，而不是直接执行函数
    # 注意：这里调用的是 Huey 任务对象，会异步执行
    # 问题修复：之前直接调用导致函数执行两次（同步+异步），现在确保只异步执行
    task_result = build_and_enqueue_downloads_task(group_name=group, stock_codes=stock_codes)
    typer.echo(f"✅ 任务已成功提交到后台处理，任务ID: {task_result.id}。请启动消费者来执行任务。")


@app.command()
def dp(
    queue_name: str = typer.Argument(..., help="要启动的队列名称 ('fast', 'slow', 或 'maint')"),
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
    setup_logging(f"consumer_{queue_name}", log_level) # 为不同队列使用不同日志

    # 使用共享的容器实例
    app_service = container.app_service()

    # 启动指定队列的消费者
    app_service.run_data_processor(queue_name)


def main(app_service: AppService = Provide["AppContainer.app_service"]):
    """主函数"""
    app()


if __name__ == "__main__":
    main()
