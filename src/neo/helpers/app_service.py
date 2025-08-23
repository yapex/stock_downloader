"""应用服务

负责应用的初始化、配置和运行逻辑。
"""

import signal
import sys
from typing import List
from neo.configs import get_config
from neo.database.interfaces import IDBOperator
from neo.database.operator import DBOperator
from neo.downloader.interfaces import IDownloader

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
        db_operator: IDBOperator,
        downloader: IDownloader,
    ):
        """初始化应用服务

        Args:
            db_operator: 数据库操作器
            downloader: 下载器
        """
        self.db_operator = db_operator
        self.downloader = downloader

    @classmethod
    def create_default(cls) -> "AppService":
        """创建默认的 AppService 实例

        Returns:
            AppService: 配置好的应用服务实例
        """
        # 延迟导入以避免循环导入
        from neo.downloader.simple_downloader import SimpleDownloader

        get_config()

        # 创建默认的数据库操作器
        db_operator = DBOperator.create_default()

        downloader = SimpleDownloader.create_default()

        return cls(db_operator=db_operator, downloader=downloader)

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
            print("🚀 开始执行下载任务...")

            # 提交所有任务并收集任务结果
            task_results = []
            for task in tasks:
                result = self._execute_download_task_with_submission(task)
                if result:
                    task_results.append(result)

            print("⏳ 等待任务执行完成...")
            # 异步等待所有任务完成
            import asyncio
            from huey.contrib.asyncio import aget_result

            try:
                await asyncio.gather(*[aget_result(result) for result in task_results])
            except Exception as e:
                print(f"任务执行失败: {e}")

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

    def _get_task_name(self, task: DownloadTaskConfig) -> str:
        """获取任务名称

        Args:
            task: 下载任务配置

        Returns:
            str: 任务名称
        """
        return (
            f"{task.symbol}_{task.task_type.name}"
            if task.symbol
            else task.task_type.name
        )

    def _print_dry_run_info(self, tasks: List[DownloadTaskConfig]) -> None:
        """打印试运行信息

        Args:
            tasks: 任务列表
        """
        print(f"[DRY RUN] 将要执行 {len(tasks)} 个下载任务:")
        for task in tasks:
            task_name = self._get_task_name(task)
            print(f"  - {task_name}: {task.task_type.value.api_method}")

    def _execute_download_task_with_submission(self, task: DownloadTaskConfig):
        """执行单个下载任务并提交到 Huey 队列

        Args:
            task: 下载任务配置

        Returns:
            任务结果对象，可用于等待任务完成
        """
        task_name = self._get_task_name(task)
        try:
            # 提交任务到 Huey 队列进行异步处理
            result = download_task(task.task_type, task.symbol)
            print(f"成功提交下载任务: {task_name}")
            return result
        except Exception as e:
            print(f"提交下载任务失败 {task_name}: {e}")
            return None


class ServiceFactory:
    """服务工厂类

    提供创建各种服务实例的工厂方法。
    """

    @staticmethod
    def create_app_service(
        db_operator: IDBOperator = None, downloader: IDownloader = None
    ) -> AppService:
        """创建 AppService 实例

        Args:
            db_operator: 数据库操作器，如果为 None 则使用默认实现
            downloader: 下载器，如果为 None 则使用默认实现

        Returns:
            AppService: 配置好的应用服务实例
        """
        if db_operator is None or downloader is None:
            # 使用默认实现
            return AppService.create_default()
        else:
            # 使用提供的实现
            return AppService(db_operator=db_operator, downloader=downloader)
