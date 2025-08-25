import time
from rich.live import Live
from rich.table import Table
from rich.console import Console

from neo.configs.huey_config import huey

console = Console()


def generate_table() -> Table:
    """生成并返回一个包含 Huey 状态的 Rich Table"""
    table = Table(title="Huey 任务队列实时监控")

    table.add_column("指标", justify="right", style="cyan", no_wrap=True)
    table.add_column("数值", style="magenta")

    # 获取 Huey 的核心指标
    pending_tasks = len(huey)
    scheduled_tasks = huey.scheduled_count()

    # 判断状态
    if pending_tasks > 0:
        status = "[bold yellow]忙碌 (Busy)[/bold yellow]"
    else:
        status = "[bold green]空闲 (Idle)[/bold green]"

    table.add_row("当前状态", status)
    table.add_row("等待执行的任务数 (Queue Size)", str(pending_tasks))
    table.add_row("计划中的任务数 (Scheduled)", str(scheduled_tasks))

    return table


if __name__ == "__main__":
    try:
        # 使用 Live display，它会自动处理屏幕刷新
        with Live(generate_table(), screen=True, redirect_stderr=False) as live:
            while True:
                time.sleep(1)  # 每秒刷新一次
                live.update(generate_table())
    except KeyboardInterrupt:
        print("\n监控已停止。")
    except Exception as e:
        console.print_exception()
