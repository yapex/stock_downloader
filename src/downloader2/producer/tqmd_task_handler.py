from downloader2.interfaces.task_handler import ITaskHandler
from downloader2.interfaces.task_handler import TaskEventType
from typing import Any, Optional
from tqdm import tqdm
import logging
from downloader2.interfaces.event_bus import IEventBus

logger = logging.getLogger(__name__)


class TqdmTaskHandler(ITaskHandler):
    """
    一个实现了 ITaskHandler 协议的事件处理器，
    使用 tqdm 来在控制台显示任务进度。
    """

    def __init__(self, event_bus: IEventBus):
        # 将 tqdm 实例作为成员变量，以便在不同方法中访问
        self.pbar: Optional[tqdm] = None
        event_bus.subscribe(TaskEventType.TASK_STARTED, self.on_started)
        event_bus.subscribe(TaskEventType.TASK_SUCCEEDED, self.on_progress)
        event_bus.subscribe(TaskEventType.TASK_FAILED, self.on_failed)
        event_bus.subscribe(TaskEventType.TASK_FINISHED, self.on_finished)

    def on_started(self, sender: Any, **kwargs) -> None:
        """
        响应 TASK_STARTED 事件：创建并初始化进度条。
        """
        total = kwargs.get("total_task_count", 0)
        task_type = kwargs.get("task_type", "Tasks")
        if total > 0:
            self.pbar = tqdm(
                total=total,
                desc=f"Processing {task_type}",
                unit="task",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]",
            )
            # 使用 tqdm 的 write 方法输出启动信息，避免干扰进度条
            self.pbar.write(f"🚀 Starting downloader simulation for {total} tasks...")

    def on_progress(self, sender: Any, **kwargs) -> None:
        """
        响应 TASK_SUCCEEDED 事件：进度条前进并显示最后成功的符号。
        """
        if not self.pbar:
            logger.warning("进度条尚未初始化，但收到了 on_progress 事件。")
            return

        symbol = kwargs.get("symbol", "N/A")
        self.pbar.set_postfix_str(f"Success: {symbol}", refresh=True)
        self.pbar.update(1)

    def on_failed(self, sender: Any, **kwargs) -> None:
        """
        响应 TASK_FAILED 事件：进度条前进并在独立行显示失败信息。
        """
        if not self.pbar:
            logger.warning("进度条尚未初始化，但收到了 on_failed 事件。")
            return

        symbol = kwargs.get("symbol", "N/A")
        error = kwargs.get("error", "Unknown error")

        # 在独立行显示错误信息，避免被进度条刷屏
        self.pbar.write(f"❌ 失败: {symbol} - {str(error)[:100]}")

        # 更新进度条但不在postfix显示错误信息
        self.pbar.set_postfix_str(f"Last: {symbol} (Failed)", refresh=True)
        self.pbar.update(1)

    def on_finished(self, sender: Any, **kwargs) -> None:
        """
        响应 TASK_FINISHED 事件：关闭进度条并打印最终总结。
        """
        if not self.pbar:
            return

        successful = kwargs.get("successful_task_count", 0)
        failed = kwargs.get("failed_task_count", 0)
        total = kwargs.get("total_task_count", successful + failed)

        # 确保进度条在关闭前是100%
        # 这处理了任务提前终止的情况
        self.pbar.n = successful + failed
        self.pbar.refresh()

        self.pbar.set_postfix_str("Completed!", refresh=True)
        self.pbar.close()
        # 在新的一行打印最终总结
        self.pbar.write(
            f"🏁 Task finished. Total: {total}, Success: {successful}, Failed: {failed}."
        )
