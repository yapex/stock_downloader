from concurrent.futures import ThreadPoolExecutor
import signal
import threading

from downloader2.producer.tqdm_task_handler import TqdmTaskHandler
from downloader2.producer.event_bus import BlinkerEventBus
from downloader2.producer.fetcher_builder import FetcherBuilder, TaskType
from downloader2.producer.tushare_downloader import TushareDownloader
from downloader2.config import get_config
import logging

logger = logging.getLogger(__name__)

config = get_config()
task_groups = config.task_groups


class DownloaderManager:
    def __init__(self, task_group: str, symbols: list = None):
        self.downloaders = []
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.event_bus = BlinkerEventBus()
        self.task_handler = TqdmTaskHandler(self.event_bus)
        self.symbols = symbols
        self._stop_event = threading.Event()
        self._signal_handlers_registered = False
        self._finished_downloaders = set()  # 跟踪已完成的下载器
        self._completion_lock = threading.Lock()  # 保护完成状态的锁
        
        # 监听下载器完成事件
        from downloader2.interfaces.task_handler import TaskEventType
        self.event_bus.subscribe(TaskEventType.TASK_FINISHED.value, self._on_downloader_finished)

        # 验证任务组是否存在于配置中
        if task_group not in task_groups:
            available_groups = list(task_groups.keys())
            raise ValueError(
                f"任务组 '{task_group}' 不存在。可用的任务组: {available_groups}"
            )

        self.task_group = task_groups[task_group]

    def _on_downloader_finished(self, sender, **kwargs):
        """处理下载器完成事件"""
        with self._completion_lock:
            # 记录已完成的下载器
            self._finished_downloaders.add(sender)
            logger.info(f"下载器 {sender.task_type.value} 已完成，已完成: {len(self._finished_downloaders)}/{len(self.downloaders)}")
            
            # 检查是否所有下载器都已完成
            if len(self._finished_downloaders) >= len(self.downloaders):
                logger.info("所有下载器任务已完成，自动停止程序")
                # 只设置停止标志，不调用 stop() 方法，避免重复调用下载器的 stop()
                self._stop_event.set()

    def _signal_handler(self, signum, frame):
        """处理用户中断信号"""
        logger.info(f"接收到信号 {signum}，正在停止下载器...")
        self._stop_event.set()

    def _register_signal_handlers(self):
        """注册信号处理器"""
        if not self._signal_handlers_registered:
            signal.signal(signal.SIGINT, self._signal_handler)  # Ctrl+C
            signal.signal(signal.SIGTERM, self._signal_handler)  # 终止信号
            self._signal_handlers_registered = True
            logger.debug("信号处理器已注册")

    def is_stop_requested(self):
        """检查是否收到停止请求"""
        return self._stop_event.is_set()

    def _build_downloader(self):
        # 如果没有提供 symbols，则先执行 stock_basic_fetcher 获取全部 symbols
        if self.symbols is None:
            stock_basic_fetcher = FetcherBuilder().build_by_task(TaskType.STOCK_BASIC)
            try:
                df = stock_basic_fetcher()
                if df is None or df.empty:
                    raise ValueError("获取股票基本信息表失败")
                symbols = df["ts_code"].tolist()
            except Exception as e:
                logger.error(f"获取股票列表失败: {str(e)}")
                raise
        else:
            symbols = self.symbols

        downloaders = []

        # 任务名到 TaskType 的映射
        task_mapping = {
            "stock_basic": TaskType.STOCK_BASIC,
            "stock_daily": TaskType.STOCK_DAILY,
            "stock_adj_qfq": TaskType.DAILY_BAR_QFQ,
            "balance_sheet": TaskType.BALANCESHEET,
            "income_statement": TaskType.INCOME,
            "cash_flow": TaskType.CASHFLOW,
        }

        # 根据任务组中的任务列表创建对应的下载器
        for task_name in self.task_group:
            if task_name in task_mapping:
                task_type = task_mapping[task_name]

                # 创建对应的 TushareDownloader
                downloader = TushareDownloader(
                    symbols=symbols,
                    task_type=task_type,
                    executor=self.executor,
                    event_bus=self.event_bus,
                )
                downloaders.append(downloader)
            else:
                # 如果任务名不存在于映射中，记录警告并跳过
                logger.warning(f"Warning: Unknown task type '{task_name}', skipping...")
                continue

        return downloaders

    def start(self):
        # 注册信号处理器
        self._register_signal_handlers()
        
        # 构建下载器
        downloaders = self._build_downloader()
        self.downloaders = downloaders

        # 启动所有下载器
        for downloader in downloaders:
            downloader.start()

        return downloaders

    def stop(self, downloaders: list = None):
        """停止所有下载器
        
        Args:
            downloaders: 要停止的下载器列表，如果为 None，则使用 self.downloaders
        """
        # 设置停止标志
        self._stop_event.set()
        
        # 如果没有提供下载器列表，则使用实例变量
        if downloaders is None:
            downloaders = self.downloaders
            
        # 停止所有下载器
        for downloader in downloaders:
            downloader.stop()

        # 关闭线程池
        self.executor.shutdown(wait=True)
        
        logger.info("所有下载器已停止")
        
    def wait_for_stop(self):
        """等待用户中断信号
        
        返回:
            bool: 如果收到停止信号则返回 True
        """
        try:
            while not self._stop_event.is_set():
                # 每秒检查一次停止标志
                self._stop_event.wait(1)
            return True
        except KeyboardInterrupt:
            # 如果在等待过程中收到键盘中断，也设置停止标志
            self._stop_event.set()
            return True
