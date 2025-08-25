"""应用服务

负责应用的初始化、配置和运行逻辑。
"""

from typing import List, Optional

from neo.task_bus.types import DownloadTaskConfig
from .huey_consumer_manager import HueyConsumerManager


class DataProcessorRunner:
    """数据处理器运行工具类"""

    @staticmethod
    def run_data_processor() -> None:
        """运行独立的数据处理器"""
        HueyConsumerManager.run_consumer_standalone()


class AppService:
    """应用服务实现"""

    def __init__(self):
        pass

    def run_data_processor(self) -> None:
        """运行数据处理器"""
        DataProcessorRunner.run_data_processor()

    def run_downloader(
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

        print("🚀 开始执行下载任务...")
        task_results = []
        for task in tasks:
            result = self._execute_download_task_with_submission(task)
            if result is not None:
                task_results.append(result)
        print(f"⏳ 已成功提交 {len(task_results)} 个任务链，等待执行完成...")
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
        """执行单个下载任务并提交到 Huey 队列（使用 pipeline 链接下载和处理）"""
        import logging

        logger = logging.getLogger(__name__)

        task_type_str = (
            task.task_type.name
            if hasattr(task.task_type, "name")
            else str(task.task_type)
        )
        task_name = f"{task.symbol}_{task_type_str}" if task.symbol else task_type_str
        try:
            from neo.tasks.huey_tasks import (
                download_task,
                process_data_task,
            )
            from neo.configs.huey_config import huey

            logger.info(f"PRODUCER: Submitting pipeline with Huey instance: {huey}")
            logger.info(f"PRODUCER: Huey backend is: {huey.storage}")

            # 创建 pipeline：下载任务 -> 数据处理任务
            # 参考原型的成功模式，Huey 会自动将 download_task 返回的字典解包给 process_data_task
            pipeline = download_task.s(task.task_type, task.symbol).then(
                process_data_task
            )
            pipeline_result = huey.enqueue(pipeline)
            logger.debug(f"成功提交任务链 (pipeline): {task_name}")
            return pipeline_result
        except Exception as e:
            logger.error(f"提交任务链失败 {task_name}: {e}")
            return None
