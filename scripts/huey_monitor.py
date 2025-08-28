import time
from rich.live import Live
from rich.table import Table
from rich.console import Console
from collections import deque

# 导入在 huey_config.py 中配置好的、全局唯一的 Huey 实例
from neo.configs.huey_config import huey_fast, huey_slow, huey_maint

console = Console()


class TaskMonitor:
    """为单个Huey队列提供监控和速率计算功能"""

    def __init__(self, window_size=10):
        self.window_size = window_size
        self.task_counts = deque(maxlen=window_size)
        self.timestamps = deque(maxlen=window_size)

    def update(self, current_task_count):
        """更新当前任务数并记录时间戳"""
        now = time.time()
        self.task_counts.append(current_task_count)
        self.timestamps.append(now)

    def get_processing_rate(self):
        """计算处理速率 (任务/秒)"""
        if len(self.task_counts) < 2:
            return 0

        tasks_processed = self.task_counts[0] - self.task_counts[-1]
        time_elapsed = self.timestamps[-1] - self.timestamps[0]

        if time_elapsed > 0:
            rate = tasks_processed / time_elapsed
            return max(0, rate)  # 确保速率不为负
        return 0

    def get_eta(self, pending_tasks):
        """估算剩余时间"""
        rate = self.get_processing_rate()
        if rate > 0 and pending_tasks > 0:
            return pending_tasks / rate
        return None


def format_time(seconds):
    """格式化时间显示"""
    if seconds is None or seconds < 0:
        return "未知"
    if seconds < 60:
        return f"{seconds:.0f}秒"
    if seconds < 3600:
        return f"{seconds / 60:.1f}分钟"
    return f"{seconds / 3600:.1f}小时"


def generate_table(
    fast_monitor: TaskMonitor,
    slow_monitor: TaskMonitor,
    maint_monitor: TaskMonitor,
    start_time: float,
) -> Table:
    """生成并返回一个包含三个Huey队列状态的Rich Table"""
    table = Table(title="Huey 三队列实时监控")

    table.add_column("指标", justify="right", style="cyan", no_wrap=True)
    table.add_column("快速队列 (Fast)", style="magenta")
    table.add_column("慢速队列 (Slow)", style="yellow")
    table.add_column("维护队列 (Maint)", style="green")

    # 核心修复：在每次查询前，关闭旧的数据库连接，以强制 Huey 重新连接并获取最新状态。
    # 这可以避免因连接缓存导致的数据陈旧问题，且不会触发重量级的实例重建和数据库锁。
    huey_fast.storage.close()
    huey_slow.storage.close()
    huey_maint.storage.close()

    # 获取三个队列的核心指标
    pending_fast = len(huey_fast)
    pending_slow = len(huey_slow)
    pending_maint = len(huey_maint)
    scheduled_fast = len(huey_fast.scheduled())
    scheduled_slow = len(huey_slow.scheduled())
    scheduled_maint = len(huey_maint.scheduled())

    # 更新监控器
    fast_monitor.update(pending_fast)
    slow_monitor.update(pending_slow)
    maint_monitor.update(pending_maint)

    # 获取处理速率和ETA
    rate_fast = fast_monitor.get_processing_rate()
    rate_slow = slow_monitor.get_processing_rate()
    rate_maint = maint_monitor.get_processing_rate()
    eta_fast = fast_monitor.get_eta(pending_fast)
    eta_slow = slow_monitor.get_eta(pending_slow)
    eta_maint = maint_monitor.get_eta(pending_maint)

    # 添加表格行
    table.add_row(
        "等待中的任务数", str(pending_fast), str(pending_slow), str(pending_maint)
    )
    table.add_row(
        "计划中的任务数", str(scheduled_fast), str(scheduled_slow), str(scheduled_maint)
    )
    table.add_row(
        "处理速率 (任务/秒)",
        f"{rate_fast:.2f}",
        f"{rate_slow:.2f}",
        f"{rate_maint:.2f}",
    )
    table.add_row(
        "预计剩余时间",
        format_time(eta_fast),
        format_time(eta_slow),
        format_time(eta_maint),
    )

    # 添加总计信息和运行时间
    total_pending = pending_fast + pending_slow + pending_maint
    total_rate = rate_fast + rate_slow + rate_maint
    elapsed_time = time.time() - start_time
    table.add_section()
    table.add_row("[bold]总计[/bold]", "", "")
    table.add_row("总等待任务数", f"[bold green]{total_pending}[/bold green]")
    table.add_row("总处理速率", f"{total_rate:.2f} 任务/秒")
    table.add_row("已运行时间", format_time(elapsed_time))

    return table


if __name__ == "__main__":
    # 为每个队列创建一个监控器实例
    fast_monitor = TaskMonitor()
    slow_monitor = TaskMonitor()
    maint_monitor = TaskMonitor()
    start_time = time.time()

    try:
        with Live(
            generate_table(fast_monitor, slow_monitor, maint_monitor, start_time),
            screen=True,
            redirect_stderr=False,
        ) as live:
            while True:
                time.sleep(1)  # 每秒刷新一次
                live.update(
                    generate_table(
                        fast_monitor, slow_monitor, maint_monitor, start_time
                    )
                )
    except KeyboardInterrupt:
        print("\n监控已停止。")
    except Exception:
        console.print_exception()
