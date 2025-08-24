"""应用服务

负责应用的初始化、配置和运行逻辑。
"""

import signal
import sys
from typing import List, Optional

from neo.tqmd.interfaces import ITasksProgressTracker
from neo.configs import get_config


# 延迟导入 SimpleDownloader 以避免循环导入
from neo.task_bus.types import DownloadTaskConfig
from neo.tasks.huey_tasks import download_task


class DataProcessorRunner:
    """数据处理器运行工具类"""

    @staticmethod
    def setup_signal_handlers():
        """设置信号处理器"""

        def signal_handler(signum, frame):
            print("\n数据处理器已停止")
            import logging

            logger = logging.getLogger(__name__)
            logger.info("数据处理器收到停止信号，正在优雅关闭...")
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    @staticmethod
    def setup_huey_logging():
        """配置 Huey 日志"""
        import logging

        # 配置日志 - 保持简洁
        logging.basicConfig(
            level=logging.WARNING,  # 只显示警告和错误
            format="%(message)s",
        )

        # 设置 Huey 日志级别
        huey_logger = logging.getLogger("huey")
        huey_logger.setLevel(logging.ERROR)

    @staticmethod
    def run_consumer():
        """运行 Huey 消费者

        在主线程中启动多线程 Consumer，避免 signal 相关问题。
        """
        import asyncio
        import concurrent.futures
        from huey.consumer import Consumer
        from neo.configs import huey

        # 导入任务以确保它们被注册到 huey 实例

        def start_consumer():
            """启动 Consumer 的同步函数"""
            try:
                # 从配置文件读取工作线程数
                config = get_config()
                max_workers = config.huey.max_workers

                # 创建 Consumer 实例，配置多线程
                consumer = Consumer(
                    huey,
                    workers=max_workers,  # 从配置文件读取工作线程数
                    worker_type="thread",  # 使用线程而不是进程
                )
                print("数据处理器已启动（多线程模式），按 Ctrl+C 停止...")
                consumer.run()
            except Exception as e:
                print(f"Consumer 运行异常: {e}")
                raise

        def stop_consumer():
            """停止 Consumer 的同步函数"""
            print("正在停止数据处理器...")

        try:
            # 在主线程的 executor 中运行 Consumer
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                # 在 executor 中启动 Consumer
                future = executor.submit(start_consumer)

                try:
                    # 等待 Consumer 完成
                    future.result()
                except KeyboardInterrupt:
                    print("\n收到停止信号，正在优雅关闭...")
                    stop_consumer()
                    future.cancel()

        except KeyboardInterrupt:
            print("\n数据处理器已停止")
        except Exception as e:
            print(f"启动失败: {e}")
            sys.exit(1)
        finally:
            if "loop" in locals():
                loop.close()


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
        DataProcessorRunner.setup_signal_handlers()
        DataProcessorRunner.setup_huey_logging()
        DataProcessorRunner.run_consumer()

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
        await self._start_consumer()

        try:
            # 判断是否为组任务（多个任务）
            is_group_task = len(tasks) > 1

            if self.tasks_progress_tracker and is_group_task:
                # 重置进度条位置计数器
                from neo.tqmd import TqdmProgressTracker

                TqdmProgressTracker.reset_positions()

                # 按任务类型分组任务
                task_groups = self._group_tasks_by_type(tasks)

                # 启动母进度条
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

            elif self.tasks_progress_tracker:
                # 单任务：直接启动任务进度条
                self.tasks_progress_tracker.start_task_progress(1, "执行下载任务")

                result = self._execute_download_task_with_submission(tasks[0])
                task_results = [result] if result else []

                self.tasks_progress_tracker.update_task_progress(1)
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
                if self.tasks_progress_tracker and is_group_task and task_results:
                    # 逐个等待任务完成并更新进度条
                    for i, (result, task) in enumerate(
                        zip(task_results, task_info_list)
                    ):
                        await aget_result(result)  # 等待单个任务完成

                        # 更新对应任务类型的进度条
                        task_type_name = task.task_type
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
                    # 没有进度管理器或单任务，直接等待所有任务完成
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
            # 停止 Consumer
            await self._stop_consumer()

    async def _start_consumer(self) -> None:
        """在主线程中启动 Huey Consumer"""
        import asyncio
        from huey.consumer import Consumer
        from neo.configs import huey

        # 导入任务以确保它们被注册到 huey 实例

        def run_consumer_sync():
            """同步运行 consumer"""
            # 启动多线程 Consumer，支持真正的并发执行
            consumer = Consumer(huey, workers=4, worker_type="thread")
            consumer.run()

        # 在 executor 中运行 consumer，避免阻塞主线程
        loop = asyncio.get_event_loop()
        self._consumer_task = loop.run_in_executor(None, run_consumer_sync)

        print("🚀 Huey Consumer 已启动 (4个工作线程)")
        # 给 consumer 一点时间启动
        await asyncio.sleep(0.5)

    async def _stop_consumer(self) -> None:
        """停止 Huey Consumer"""
        import asyncio

        if hasattr(self, "_consumer_task") and self._consumer_task:
            try:
                self._consumer_task.cancel()
                await asyncio.sleep(0.1)  # 给一点时间让任务清理
            except asyncio.CancelledError:
                pass
            print("🛑 Huey Consumer 已停止")

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
            task_type_name = task.task_type
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
        return f"{task.symbol}_{task.task_type}" if task.symbol else task.task_type

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
