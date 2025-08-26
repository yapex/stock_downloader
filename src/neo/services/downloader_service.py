"""
下载器服务
"""
import logging
from typing import List

from ..task_bus.types import DownloadTaskConfig
from ..tasks.huey_tasks import download_task

class DownloaderService:
    """下载器服务实现"""

    def run(
        self, tasks: List[DownloadTaskConfig], dry_run: bool = False
    ) -> None:
        """运行下载器 (同步阻塞版本)

        Args:
            tasks: 下载任务列表
            dry_run: 是否为试运行模式
        """
        if dry_run:
            self._print_dry_run_info(tasks)
            return

        print("🚀 开始提交下载任务到快速队列...")
        task_results = []
        for task in tasks:
            result = self._execute_download_task_with_submission(task)
            if result is not None:
                task_results.append(result)
        print(f"✅ 已成功提交 {len(task_results)} 个下载任务。")
        return task_results

    def _print_dry_run_info(self, tasks: List[DownloadTaskConfig]) -> None:
        """打印试运行信息"""
        print(f"[DRY RUN] 将要执行 {len(tasks)} 个下载任务:")
        for task in tasks:
            task_type_str = (
                task.task_type.name
                if hasattr(task.task_type, "name")
                else str(task.task_type)
            )
            task_name = (
                f"{task.symbol}_{task_type_str}" if task.symbol else task_type_str
            )
            print(f" running task - {task_name}")

    def _execute_download_task_with_submission(self, task: DownloadTaskConfig):
        """执行单个下载任务并提交到 Huey 快速队列"""
        logger = logging.getLogger(__name__)

        task_type_str = (
            task.task_type.name
            if hasattr(task.task_type, "name")
            else str(task.task_type)
        )
        task_name = f"{task.symbol}_{task_type_str}" if task.symbol else task_type_str
        try:
            # 直接调用第一个任务，Huey会将其放入队列
            result = download_task(task.task_type, task.symbol)
            logger.debug(f"成功提交任务: {task_name}")
            return result
        except Exception as e:
            logger.error(f"提交任务失败 {task_name}: {e}")
            return None
