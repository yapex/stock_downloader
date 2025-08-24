"""应用服务

负责应用的初始化、配置和运行逻辑。
"""

from typing import List, Optional

from neo.tqmd.interfaces import ITasksProgressTracker
from neo.task_bus.types import DownloadTaskConfig
# 延迟导入以避免循环导入
# from neo.tasks.huey_tasks import download_task
from .huey_consumer_manager import HueyConsumerManager


class DataProcessorRunner:
    """数据处理器运行工具类
    
    简化版，主要负责独立数据处理器的启动。
    """

    @staticmethod
    def run_data_processor() -> None:
        """运行独立的数据处理器"""
        HueyConsumerManager.setup_signal_handlers()
        HueyConsumerManager.setup_huey_logging()
        HueyConsumerManager.run_consumer_standalone()


class AppService:
    """应用服务实现"""

    def __init__(
        self,
        tasks_progress_tracker: Optional[ITasksProgressTracker] = None,
    ):
        """初始化应用服务

        Args:
            tasks_progress_tracker: 任务进度跟踪器，可选
        """
        self.tasks_progress_tracker = tasks_progress_tracker

    def __del__(self):
        """析构函数：清理资源"""
        self.cleanup()

    def cleanup(self):
        """清理应用服务资源"""
        pass

    @classmethod
    def create_default(cls, with_progress: bool = True) -> "AppService":
        """创建默认的 AppService 实例

        Args:
            with_progress: 是否启用进度管理器

        Returns:
            AppService: 配置好的应用服务实例
        """
        # 创建进度管理器（如果需要）
        tasks_progress_tracker = None
        if with_progress:
            from neo.tqmd import TasksProgressTracker, ProgressTrackerFactory

            factory = ProgressTrackerFactory()
            tasks_progress_tracker = TasksProgressTracker(factory)

        return cls(
            tasks_progress_tracker=tasks_progress_tracker,
        )

    def run_data_processor(self) -> None:
        """运行数据处理器"""
        DataProcessorRunner.run_data_processor()

    def run_downloader(
        self, tasks: List[DownloadTaskConfig], dry_run: bool = False
    ) -> None:
        """运行下载器

        Args:
            tasks: 下载任务列表
            dry_run: 是否为试运行模式
        """
        if dry_run:
            self._print_dry_run_info(tasks)
            return

        # 使用 asyncio 在主线程中启动 Consumer 并执行任务
        import asyncio

        asyncio.run(self._run_downloader_async(tasks))

    async def _run_downloader_async(self, tasks: List[DownloadTaskConfig]) -> None:
        """异步运行下载器

        Args:
            tasks: 下载任务列表
        """
        # 启动 Consumer
        consumer_task = await HueyConsumerManager.start_consumer_async()

        try:
            # 判断是否为组任务（多个任务）
            is_group_task = len(tasks) > 1

            if self.tasks_progress_tracker:
                # 重置进度条位置计数器
                from neo.tqmd import TqdmProgressTracker

                TqdmProgressTracker.reset_positions()

                # 按任务类型分组任务
                task_groups = self._group_tasks_by_type(tasks)

                # 启动母进度条（单任务和多任务都使用）
                self.tasks_progress_tracker.start_group_progress(
                    len(tasks), "处理下载任务"
                )

                # 为每个任务类型启动子进度条
                for task_type, type_tasks in task_groups.items():
                    self.tasks_progress_tracker.start_task_type_progress(
                        task_type, len(type_tasks)
                    )

                # 提交所有任务并收集任务结果
                task_results = []
                task_info_list = []  # 存储任务信息用于后续进度更新

                for task in tasks:
                    result = self._execute_download_task_with_submission(task)
                    if result:
                        task_results.append(result)
                        task_info_list.append(task)

                # 初始化完成计数器
                completed_by_type = {task_type: 0 for task_type in task_groups.keys()}
            else:
                print("🚀 开始执行下载任务...")
                task_results = []
                for task in tasks:
                    result = self._execute_download_task_with_submission(task)
                    if result:
                        task_results.append(result)

            if not self.tasks_progress_tracker:
                print("⏳ 等待任务执行完成...")

            # 异步等待所有任务完成并实时更新进度条
            import asyncio
            from huey.contrib.asyncio import aget_result

            try:
                if task_results:  # 只要有任务结果就等待
                    if self.tasks_progress_tracker:
                        # 有进度管理器：逐个等待任务完成并更新进度条（单任务和多任务）
                        for i, (result, task) in enumerate(
                            zip(task_results, task_info_list)
                        ):
                            await aget_result(result)  # 等待单个任务完成

                            # 更新对应任务类型的进度条
                            # 检查task_type的类型并正确访问
                            task_type_name = task.task_type.name if hasattr(task.task_type, 'name') else str(task.task_type)
                            completed_by_type[task_type_name] += 1
                            total_for_type = len(task_groups[task_type_name])

                            self.tasks_progress_tracker.update_task_type_progress(
                                task_type_name,
                                increment=1,
                                completed=completed_by_type[task_type_name],
                                total=total_for_type,
                            )

                            # 更新母进度条
                            self.tasks_progress_tracker.update_group_progress(
                                1, f"已完成 {i + 1}/{len(task_results)} 个任务"
                            )
                    else:
                        # 没有进度管理器：直接等待所有任务完成
                        await asyncio.gather(
                            *[aget_result(result) for result in task_results]
                        )
            except Exception as e:
                if self.tasks_progress_tracker:
                    self.tasks_progress_tracker.finish_all()
                print(f"任务执行失败: {e}")
                raise

            if self.tasks_progress_tracker:
                self.tasks_progress_tracker.finish_all()
            else:
                print("✅ 所有任务执行完成!")
        finally:
            # 等待所有任务（包括数据处理任务）完成后再停止 Consumer
            await HueyConsumerManager.wait_for_all_tasks_completion()
            await HueyConsumerManager.stop_consumer_async(consumer_task)


    def _group_tasks_by_type(
        self, tasks: List[DownloadTaskConfig]
    ) -> dict[str, List[DownloadTaskConfig]]:
        """按任务类型分组任务

        Args:
            tasks: 任务列表

        Returns:
            dict: 按任务类型分组的任务字典
        """
        task_groups = {}
        for task in tasks:
            # 检查task_type的类型并正确访问
            task_type_name = task.task_type.name if hasattr(task.task_type, 'name') else str(task.task_type)
            if task_type_name not in task_groups:
                task_groups[task_type_name] = []
            task_groups[task_type_name].append(task)
        return task_groups

    def _get_task_name(self, task: DownloadTaskConfig) -> str:
        """获取任务名称

        Args:
            task: 下载任务配置

        Returns:
            str: 任务名称
        """
        # 检查task_type的类型并正确访问
        task_type_str = task.task_type.name if hasattr(task.task_type, 'name') else str(task.task_type)
        return f"{task.symbol}_{task_type_str}" if task.symbol else task_type_str

    def _print_dry_run_info(self, tasks: List[DownloadTaskConfig]) -> None:
        """打印试运行信息

        Args:
            tasks: 任务列表
        """
        print(f"[DRY RUN] 将要执行 {len(tasks)} 个下载任务:")
        for task in tasks:
            task_name = self._get_task_name(task)

            print(f" running task - {task_name}")

    def _execute_download_task_with_submission(self, task: DownloadTaskConfig):
        """执行单个下载任务并提交到 Huey 队列

        Args:
            task: 下载任务配置

        Returns:
            任务结果对象，可用于等待任务完成
        """
        import logging

        logger = logging.getLogger(__name__)

        task_name = self._get_task_name(task)
        try:
            # 延迟导入以避免循环导入
            from neo.tasks.huey_tasks import download_task
            # 提交任务到 Huey 队列进行异步处理
            result = download_task(task.task_type, task.symbol)

            # 当启用进度管理器时使用logging，否则使用print
            if self.tasks_progress_tracker:
                logger.debug(f"成功提交下载任务: {task_name}")
            else:
                print(f"成功提交下载任务: {task_name}")
            return result
        except Exception as e:
            # 当启用进度管理器时使用logging，否则使用print
            if self.tasks_progress_tracker:
                logger.error(f"提交下载任务失败 {task_name}: {e}")
            else:
                print(f"提交下载任务失败 {task_name}: {e}")
            return None


class ServiceFactory:
    """服务工厂类

    提供创建各种服务实例的工厂方法。
    """

    @staticmethod
    def create_app_service(
        with_progress: bool = True,
    ) -> AppService:
        """创建 AppService 实例

        Args:
            with_progress: 是否启用进度管理器

        Returns:
            AppService: 配置好的应用服务实例
        """
        return AppService.create_default(with_progress=with_progress)
