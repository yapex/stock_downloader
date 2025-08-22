"""应用服务

负责应用的初始化、配置和运行逻辑。
"""

import signal
import subprocess
import sys
from typing import List, Protocol
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


class IAppService(Protocol):
    """应用服务接口"""

    def run_producer_consumer(self, tasks: List[DownloadTaskConfig]) -> None:
        """运行生产者-消费者模式

        Args:
            tasks: 任务列表
        """
        ...

    def run_data_processor(self) -> None:
        """运行数据处理器"""
        ...

    def run_demo(self) -> None:
        """运行演示程序"""
        ...

    def run_downloader(
        self, tasks: List[DownloadTaskConfig], dry_run: bool = False
    ) -> None:
        """运行下载器

        Args:
            tasks: 下载任务列表
            dry_run: 是否为试运行模式
        """
        if dry_run:
            print(f"[DRY RUN] 将要执行 {len(tasks)} 个下载任务:")
            for task in tasks:
                task_name = (
                    f"{task.symbol}_{task.task_type.name}"
                    if task.symbol
                    else task.task_type.name
                )
                print(f"  - {task_name}: {task.task_type.value.api_method}")
            return

        for task in tasks:
            try:
                self._downloader.download(task)
                task_name = (
                    f"{task.symbol}_{task.task_type.name}"
                    if task.symbol
                    else task.task_type.name
                )
                print(f"成功下载: {task_name}")
            except Exception as e:
                task_name = (
                    f"{task.symbol}_{task.task_type.name}"
                    if task.symbol
                    else task.task_type.name
                )
                print(f"下载失败 {task_name}: {e}")


class AppService:
    """应用服务实现"""

    def __init__(
        self,
        db_operator: IDBOperator = None,
        downloader: IDownloader = None,
        data_processor: IDataProcessor = None,
        task_bus: ITaskBus = None,
    ):
        # 使用依赖注入，如果没有提供则使用默认实现
        self._config = get_config()
        self._db_operator = db_operator or DBOperator()

        # 延迟导入避免循环导入
        if downloader is None:
            from neo.downloader import SimpleDownloader

            self._downloader = SimpleDownloader()
        else:
            self._downloader = downloader

        self._data_processor = data_processor or SimpleDataProcessor(self._db_operator)
        self._task_bus = task_bus or HueyTaskBus()

        # 配置日志
        setup_logging()

    def run_producer_consumer(self, tasks: List[DownloadTaskConfig]) -> None:
        """运行生产者-消费者模式

        Args:
            tasks: 任务列表
        """
        # 处理每个任务
        for task in tasks:
            # 执行下载任务
            task_result = self._downloader.download(task)
            # 将结果提交到任务总线进行异步处理
            self._task_bus.submit_task(task_result)

    def run_data_processor(self) -> None:
        """运行数据处理器"""
        import logging

        logger = logging.getLogger(__name__)

        def signal_handler(signum, frame):
            print("\n接收到中断信号，正在停止数据处理器...")
            logger.info("数据处理器收到停止信号，正在优雅关闭...")
            sys.exit(0)

        # 注册信号处理器
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # 输出启动信息
        print("=== Neo 数据处理器启动 ===")
        print(f"数据库路径: {self._config.database.path}")
        print(f"Huey 数据库: {self._config.huey.db_file}")
        print(f"即时模式: {'开启' if self._config.huey.immediate else '关闭'}")
        print("支持的任务类型:")

        # 显示支持的任务类型
        from neo.task_bus.types import TaskType

        for task_type in TaskType:
            print(f"  - {task_type.name}: {task_type.value.api_method}")

        print("\n正在启动 Huey 消费者进程...")
        logger.info("开始启动数据处理器")

        try:
            # 启动 Huey 消费者进程
            cmd = ["uv", "run", "huey_consumer", "neo.task_bus.huey_task_bus.huey"]
            process = subprocess.Popen(cmd)

            print("数据处理器已启动，按 Ctrl+C 停止...")
            logger.info("数据处理器启动成功，开始监听任务队列")
            process.wait()

        except KeyboardInterrupt:
            print("\n正在停止数据处理器...")
            if "process" in locals():
                process.terminate()
                process.wait()
        except Exception as e:
            print(f"启动数据处理器时出错: {e}")
            sys.exit(1)

    def run_demo(self) -> None:
        """运行演示程序"""
        from neo.helpers.utils import normalize_stock_code

        print("=== Neo 股票数据下载器演示 ===")
        print()

        # 演示股票代码标准化
        print("1. 股票代码标准化演示:")
        test_codes = ["600519", "SH600519", "000001", "sz000001", "300001"]
        for code in test_codes:
            try:
                normalized = normalize_stock_code(code)
                print(f"  {code} -> {normalized}")
            except Exception as e:
                print(f"  {code} -> 错误: {e}")

        print()
        print("2. 配置信息:")
        print(f"  数据库路径: {self._config.database.path}")
        print(
            f"  Tushare Token: {'已配置' if self._config.tushare.token else '未配置'}"
        )

        print()
        print("3. 可用的任务组:")
        groups = self._config.task_groups
        for group_name, group_config in groups.items():
            task_types = [t.value for t in group_config.task_types]
            print(f"  {group_name}: {', '.join(task_types)}")

        print()
        print("演示完成！")

    def run_downloader(
        self, tasks: List[DownloadTaskConfig], dry_run: bool = False
    ) -> None:
        """运行下载器

        Args:
            tasks: 下载任务列表
            dry_run: 是否为试运行模式
        """
        if dry_run:
            print(f"[DRY RUN] 将要执行 {len(tasks)} 个下载任务:")
            for task in tasks:
                task_name = (
                    f"{task.symbol}_{task.task_type.name}"
                    if task.symbol
                    else task.task_type.name
                )
                print(f"  - {task_name}: {task.task_type.value.api_method}")
            return

        for task in tasks:
            try:
                self._downloader.download(task)
                task_name = (
                    f"{task.symbol}_{task.task_type.name}"
                    if task.symbol
                    else task.task_type.name
                )
                print(f"成功下载: {task_name}")
            except Exception as e:
                task_name = (
                    f"{task.symbol}_{task.task_type.name}"
                    if task.symbol
                    else task.task_type.name
                )
                print(f"下载失败 {task_name}: {e}")
