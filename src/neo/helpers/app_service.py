"""应用服务

负责应用的初始化、配置和运行逻辑。
"""

from typing import List

from neo.task_bus.types import DownloadTaskConfig
from neo.configs import get_config
import sys


class DataProcessorRunner:
    """数据处理器运行工具类"""

    @staticmethod
    def run_data_processor(queue_name: str) -> None:
        """独立运行 Huey 消费者

        在主线程中启动多线程 Consumer，适用于独立的消费者进程。
        """
        from huey.consumer import Consumer
        from neo.configs import get_config

        # 根据名字动态选择要启动的huey实例
        if queue_name == 'fast':
            from neo.configs.huey_config import huey_fast as huey
            max_workers = get_config().huey_fast.max_workers
            print(f"🚀 正在启动快速队列消费者 (fast_queue) ，配置 {max_workers} 个 workers...")
        elif queue_name == 'slow':
            from neo.configs.huey_config import huey_slow as huey
            max_workers = get_config().huey_slow.max_workers
            print(f"🐌 正在启动慢速队列消费者 (slow_queue)，配置 {max_workers} 个 workers...")
        else:
            print(f"❌ 错误：无效的队列名称 '{queue_name}'。请使用 'fast' 或 'slow'。", file=sys.stderr)
            sys.exit(1)


        # 重要：导入任务模块，让 Consumer 能够识别和执行任务
        import neo.tasks.huey_tasks  # noqa: F401

        try:
            # 创建 Consumer 实例，配置多线程
            consumer = Consumer(
                huey,
                workers=max_workers,
                worker_type="thread",
            )
            print("数据处理器已启动，按 Ctrl+C 停止...")
            consumer.run()
        except KeyboardInterrupt:
            print(f"\n数据处理器 ({queue_name}) 已停止")
        except Exception as e:
            print(f"Consumer ({queue_name}) 运行异常: {e}")
            sys.exit(1)


class AppService:
    """应用服务实现"""

    def __init__(self):
        pass

    def run_data_processor(self, queue_name: str) -> None:
        """运行数据处理器
        
        Args:
            queue_name: 要运行的队列名称 ('fast' or 'slow')
        """
        DataProcessorRunner.run_data_processor(queue_name)

    def run_downloader(
        self,
        tasks: List[DownloadTaskConfig],
        dry_run: bool = False
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
        print(f"✅ 已成功提交 {len(task_results)} 个下载任务ảng。")
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
        import logging

        logger = logging.getLogger(__name__)

        task_type_str = (
            task.task_type.name
            if hasattr(task.task_type, "name")
            else str(task.task_type)
        )
        task_name = f"{task.symbol}_{task_type_str}" if task.symbol else task_type_str
        try:
            # 只需要导入第一个任务，后续的链接在任务内部完成
            from neo.tasks.huey_tasks import download_task

            # 直接调用第一个任务，Huey会将其放入队列
            result = download_task(task.task_type, task.symbol)
            logger.debug(f"成功提交任务: {task_name}")
            return result
        except Exception as e:
            logger.error(f"提交任务失败 {task_name}: {e}")
            return None
