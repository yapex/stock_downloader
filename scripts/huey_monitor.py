import time
from rich.live import Live
from rich.table import Table
from rich.console import Console
from collections import deque

from neo.configs.huey_config import huey

console = Console()


class TaskMonitor:
    def __init__(self, window_size=10):
        self.window_size = window_size
        self.task_counts = deque(maxlen=window_size)
        self.timestamps = deque(maxlen=window_size)
        self.start_time = time.time()
        self.initial_task_count = None

    def update(self, current_task_count):
        now = time.time()
        self.task_counts.append(current_task_count)
        self.timestamps.append(now)
        
        # 记录初始任务数
        if self.initial_task_count is None:
            self.initial_task_count = current_task_count

    def get_processing_rate(self):
        """计算任务处理速率（任务/秒）"""
        if len(self.task_counts) < 2:
            return 0
            
        # 计算窗口期内的任务处理数量
        tasks_processed = self.task_counts[0] - self.task_counts[-1]
        time_elapsed = self.timestamps[-1] - self.timestamps[0]
        
        if time_elapsed > 0:
            return tasks_processed / time_elapsed
        return 0

    def get_eta(self):
        """估算剩余时间"""
        if self.initial_task_count is None or len(self.task_counts) < 2:
            return None
            
        current_count = self.task_counts[-1]
        rate = self.get_processing_rate()
        
        if rate > 0 and current_count > 0:
            remaining_seconds = current_count / rate
            return remaining_seconds
        return None


def format_time(seconds):
    """格式化时间显示"""
    if seconds is None:
        return "未知"
        
    if seconds < 60:
        return f"{seconds:.0f}秒"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}分钟"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}小时"


def generate_table(monitor: TaskMonitor) -> Table:
    """生成并返回一个包含 Huey 状态的 Rich Table"""
    table = Table(title="Huey 任务队列实时监控")

    table.add_column("指标", justify="right", style="cyan", no_wrap=True)
    table.add_column("数值", style="magenta")

    # 获取 Huey 的核心指标
    pending_tasks = len(huey)
    scheduled_tasks = huey.scheduled_count()

    # 更新监控器
    monitor.update(pending_tasks)

    # 判断状态
    if pending_tasks > 0:
        status = "[bold yellow]忙碌 (Busy)[/bold yellow]"
    else:
        status = "[bold green]空闲 (Idle)[/bold green]"

    # 获取处理速率和剩余时间
    processing_rate = monitor.get_processing_rate()
    eta = monitor.get_eta()

    table.add_row("当前状态", status)
    table.add_row("等待执行的任务数 (Queue Size)", str(pending_tasks))
    table.add_row("计划中的任务数 (Scheduled)", str(scheduled_tasks))
    table.add_row("任务处理速率", f"{processing_rate:.2f} 任务/秒")
    table.add_row("预计剩余时间", format_time(eta))
    
    # 显示运行时间
    elapsed = time.time() - monitor.start_time
    table.add_row("运行时间", format_time(elapsed))

    return table


if __name__ == "__main__":
    monitor = TaskMonitor()
    
    try:
        # 使用 Live display，它会自动处理屏幕刷新
        with Live(generate_table(monitor), screen=True, redirect_stderr=False) as live:
            while True:
                time.sleep(1)  # 每秒刷新一次
                live.update(generate_table(monitor))
    except KeyboardInterrupt:
        print("\n监控已停止。")
    except Exception as e:
        console.print_exception()