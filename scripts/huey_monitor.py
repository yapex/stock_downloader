import time
from rich.live import Live
from rich.table import Table
from rich.console import Console
from collections import deque

# 导入新的双队列实例
from neo.configs.huey_config import huey_fast, huey_slow

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

        # 计算窗口期内的任务增减
        tasks_processed = self.task_counts[0] - self.task_counts[-1]
        time_elapsed = self.timestamps[-1] - self.timestamps[0]

        if time_elapsed > 0:
            return tasks_processed / time_elapsed
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


def generate_table(fast_monitor: TaskMonitor, slow_monitor: TaskMonitor, start_time: float) -> Table:
    """生成并返回一个包含两个Huey队列状态的Rich Table"""
    table = Table(title="Huey 双队列实时监控")

    table.add_column("指标", justify="right", style="cyan", no_wrap=True)
    table.add_column("快速队列 (Fast)", style="magenta")
    table.add_column("慢速队列 (Slow)", style="yellow")

    # 获取两个队列的核心指标
    pending_fast = len(huey_fast)
    pending_slow = len(huey_slow)
    scheduled_fast = huey_fast.scheduled_count()
    scheduled_slow = huey_slow.scheduled_count()

    # 更新监控器
    fast_monitor.update(pending_fast)
    slow_monitor.update(pending_slow)

    # 获取处理速率和ETA
    rate_fast = fast_monitor.get_processing_rate()
    rate_slow = slow_monitor.get_processing_rate()
    eta_fast = fast_monitor.get_eta(pending_fast)
    eta_slow = slow_monitor.get_eta(pending_slow)

    # 添加表格行
    table.add_row("等待中的任务数", str(pending_fast), str(pending_slow))
    table.add_row("计划中的任务数", str(scheduled_fast), str(scheduled_slow))
    table.add_row("处理速率 (任务/秒)", f"{rate_fast:.2f}", f"{rate_slow:.2f}")
    table.add_row("预计剩余时间", format_time(eta_fast), format_time(eta_slow))

    # 添加总计信息和运行时间
    total_pending = pending_fast + pending_slow
    total_rate = rate_fast + rate_slow
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
    start_time = time.time()

    try:
        with Live(generate_table(fast_monitor, slow_monitor, start_time), screen=True, redirect_stderr=False) as live:
            while True:
                time.sleep(1)  # 每秒刷新一次
                live.update(generate_table(fast_monitor, slow_monitor, start_time))
    except KeyboardInterrupt:
        print("\n监控已停止。")
    except Exception:
        console.print_exception()
