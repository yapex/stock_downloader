"""应用服务

负责应用的初始化、配置和运行逻辑。
"""

import os
import signal
import sys
from typing import List
from neo.config import get_config
from neo.database.interfaces import IDBOperator
from neo.database.operator import DBOperator
from neo.downloader.interfaces import IDownloader
# 延迟导入 SimpleDownloader 以避免循环导入
from neo.task_bus.types import DownloadTaskConfig
from neo.tasks.huey_tasks import download_task
from neo.helpers.utils import setup_logging


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
        """运行 Huey 消费者"""
        from huey.consumer import Consumer
        from neo.huey_config import huey

        # 导入任务以确保它们被注册到 huey 实例
        from neo.tasks import huey_tasks, data_processing_task

        try:
            # 使用全局的 MiniHuey 实例创建消费者
            consumer = Consumer(huey)
            print("数据处理器已启动，按 Ctrl+C 停止...")
            consumer.run()
        except KeyboardInterrupt:
            print("\n数据处理器已停止")
        except Exception as e:
            print(f"启动失败: {e}")
            sys.exit(1)


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
        
        config = get_config()

        # 创建默认的数据库操作器
        db_operator = DBOperator()

        downloader = SimpleDownloader()

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

        # 启动 MiniHuey 调度器
        from neo.huey_config import huey
        print("🚀 启动 MiniHuey 调度器...")
        huey.start()
        
        try:
            # 提交所有任务并收集任务结果
            task_results = []
            for task in tasks:
                result = self._execute_download_task_with_submission(task)
                if result:
                    task_results.append(result)
            
            print("⏳ 等待任务执行完成...")
            # 等待所有任务完成
            for result in task_results:
                try:
                    result()  # 阻塞等待任务完成
                except Exception as e:
                    print(f"任务执行失败: {e}")
            
            print("✅ 所有任务执行完成!")
        finally:
            # 停止调度器
            print("🛑 停止 MiniHuey 调度器...")
            huey.stop()

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
