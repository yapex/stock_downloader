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
from neo.data_processor.interfaces import IDataProcessor
from neo.data_processor import SimpleDataProcessor
from neo.task_bus.interfaces import ITaskBus
from neo.task_bus import HueyTaskBus
from neo.task_bus.types import DownloadTaskConfig
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
        from neo.task_bus.huey_config import huey
        from neo.task_bus import tasks  # 确保任务被注册
        
        try:
            # 直接创建和启动 Huey 消费者
            consumer = Consumer(huey)
            print("数据处理器已启动，按 Ctrl+C 停止...")
            consumer.run()
        except KeyboardInterrupt:
            print("\n数据处理器已停止")
        except Exception as e:
            print(f"启动失败: {e}")
            sys.exit(1)


class ServiceFactory:
    """服务工厂类，负责创建和配置各种服务实例"""
    
    @staticmethod
    def create_app_service(
        db_operator: IDBOperator = None,
        downloader: IDownloader = None,
        data_processor: IDataProcessor = None,
        task_bus: ITaskBus = None,
    ) -> 'AppService':
        """创建 AppService 实例
        
        Args:
            db_operator: 数据库操作器
            downloader: 下载器
            data_processor: 数据处理器
            task_bus: 任务总线
            
        Returns:
            AppService: 配置好的应用服务实例
        """
        config = get_config()
        
        # 创建数据库操作器
        db_operator = db_operator or DBOperator()
        
        # 创建数据处理器
        data_processor = data_processor or SimpleDataProcessor(db_operator)
        
        # 创建任务总线
        if task_bus is None:
            task_bus = HueyTaskBus(data_processor=data_processor)
        
        # 创建下载器
        if downloader is None:
            from neo.downloader import SimpleDownloader
            downloader = SimpleDownloader(task_bus=task_bus, db_operator=db_operator)
        
        return AppService(
            db_operator=db_operator,
            downloader=downloader,
            data_processor=data_processor,
            task_bus=task_bus
        )



class AppService:
    """应用服务实现"""

    def __init__(
        self,
        db_operator: IDBOperator,
        downloader: IDownloader,
        data_processor: IDataProcessor,
        task_bus: ITaskBus,
    ):
        """初始化应用服务
        
        Args:
            db_operator: 数据库操作器
            downloader: 下载器
            data_processor: 数据处理器
            task_bus: 任务总线
        """
        self._db_operator = db_operator
        self._downloader = downloader
        self._data_processor = data_processor
        self._task_bus = task_bus
        
        # 配置日志
        setup_logging()

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

        for task in tasks:
            self._execute_download_task_with_submission(task)
    
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
    
    def _execute_download_task_with_submission(self, task: DownloadTaskConfig) -> None:
        """执行单个下载任务并提交到任务总线
        
        Args:
            task: 下载任务配置
        """
        task_name = self._get_task_name(task)
        try:
            # 执行下载任务
            task_result = self._downloader.download(task)
            # 将结果提交到任务总线进行异步处理
            self._task_bus.submit_task(task_result)
            print(f"成功下载并提交: {task_name}")
        except Exception as e:
            print(f"下载失败 {task_name}: {e}")
