from concurrent.futures import ThreadPoolExecutor
import signal
import threading
import time
from typing import List, Optional

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
    """下载器管理器，负责管理多个下载器的生命周期和线程安全"""
    
    def __init__(self, task_group: str, symbols: Optional[List[str]] = None):
        # 验证任务组是否存在于配置中
        if task_group not in task_groups:
            available_groups = list(task_groups.keys())
            raise ValueError(
                f"任务组 '{task_group}' 不存在。可用的任务组: {available_groups}"
            )
        
        self.task_group = task_groups[task_group]
        self.symbols = symbols
        
        # 线程池和事件总线
        self.executor: Optional[ThreadPoolExecutor] = None
        self.event_bus = BlinkerEventBus()
        self.task_handler = TqdmTaskHandler(self.event_bus)
        
        # 下载器管理
        self.downloaders: List[TushareDownloader] = []
        
        # 线程安全控制
        self._stop_event = threading.Event()
        self._started = threading.Event()
        self._shutdown_complete = threading.Event()
        self._state_lock = threading.RLock()  # 使用可重入锁
        
        # 完成状态跟踪
        self._finished_downloaders = set()
        self._completion_lock = threading.Lock()
        
        # 信号处理
        self._signal_handlers_registered = False
        self._original_sigint_handler = None
        self._original_sigterm_handler = None
        
        # 监听下载器完成事件
        from downloader2.interfaces.task_handler import TaskEventType
        self.event_bus.subscribe(TaskEventType.TASK_FINISHED.value, self._on_downloader_finished)

    def _on_downloader_finished(self, sender, **kwargs):
        """处理下载器完成事件，线程安全"""
        if self._stop_event.is_set():
            return  # 如果已经停止，忽略后续事件
            
        with self._completion_lock:
            # 防止重复处理同一个下载器
            if sender in self._finished_downloaders:
                return
                
            # 记录已完成的下载器
            self._finished_downloaders.add(sender)
            finished_count = len(self._finished_downloaders)
            total_count = len(self.downloaders)
            
            logger.info(f"下载器 {sender.task_type.value} 已完成，已完成: {finished_count}/{total_count}")
            
            # 检查是否所有下载器都已完成
            if finished_count >= total_count and total_count > 0:
                logger.info("所有下载器任务已完成，自动停止程序")
                self._stop_event.set()

    def _signal_handler(self, signum, frame):
        """处理用户中断信号"""
        logger.info(f"接收到信号 {signum}，正在停止下载器...")
        self._stop_event.set()

    def _register_signal_handlers(self):
        """注册信号处理器，保存原始处理器以便恢复"""
        with self._state_lock:
            if not self._signal_handlers_registered:
                try:
                    self._original_sigint_handler = signal.signal(signal.SIGINT, self._signal_handler)
                    self._original_sigterm_handler = signal.signal(signal.SIGTERM, self._signal_handler)
                    self._signal_handlers_registered = True
                    logger.debug("信号处理器已注册")
                except (OSError, ValueError) as e:
                    logger.warning(f"注册信号处理器失败: {e}")
                    
    def _restore_signal_handlers(self):
        """恢复原始信号处理器"""
        with self._state_lock:
            if self._signal_handlers_registered:
                try:
                    if self._original_sigint_handler is not None:
                        signal.signal(signal.SIGINT, self._original_sigint_handler)
                    if self._original_sigterm_handler is not None:
                        signal.signal(signal.SIGTERM, self._original_sigterm_handler)
                    self._signal_handlers_registered = False
                    logger.debug("信号处理器已恢复")
                except (OSError, ValueError) as e:
                    logger.warning(f"恢复信号处理器失败: {e}")

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
        """启动下载器管理器和所有下载器"""
        with self._state_lock:
            if self._started.is_set():
                logger.warning("DownloaderManager 已经启动")
                return self.downloaders
                
            if self._stop_event.is_set():
                raise RuntimeError("DownloaderManager 已经停止，无法重新启动")
        
        try:
            # 创建线程池
            self.executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="DownloaderManager")
            
            # 注册信号处理器
            self._register_signal_handlers()
            
            # 构建下载器
            self.downloaders = self._build_downloader()
            
            if not self.downloaders:
                logger.warning("没有创建任何下载器")
                return self.downloaders
            
            # 启动所有下载器
            started_downloaders = []
            for downloader in self.downloaders:
                try:
                    downloader.start()
                    started_downloaders.append(downloader)
                    logger.debug(f"下载器 {downloader.task_type.value} 启动成功")
                except Exception as e:
                    logger.error(f"启动下载器 {downloader.task_type.value} 失败: {e}")
                    # 停止已启动的下载器
                    for started in started_downloaders:
                        try:
                            started.stop()
                        except Exception as stop_error:
                            logger.error(f"停止下载器失败: {stop_error}")
                    raise
            
            self._started.set()
            logger.info(f"DownloaderManager 启动成功，共启动 {len(started_downloaders)} 个下载器")
            return self.downloaders
            
        except Exception as e:
            logger.error(f"启动 DownloaderManager 失败: {e}")
            # 清理资源
            self._cleanup_resources()
            raise

    def stop(self, downloaders: Optional[List[TushareDownloader]] = None, timeout: float = 30.0):
        """停止所有下载器
        
        Args:
            downloaders: 要停止的下载器列表，如果为 None，则使用 self.downloaders
            timeout: 等待停止的超时时间（秒）
        """
        with self._state_lock:
            if self._shutdown_complete.is_set():
                logger.debug("DownloaderManager 已经停止")
                return
            self._stop_event.set()
        
        try:
            downloaders = downloaders or self.downloaders
            
            if downloaders:
                logger.info(f"正在停止 {len(downloaders)} 个下载器...")
                for downloader in downloaders:
                    try:
                        downloader.stop()
                        logger.debug(f"下载器 {downloader.task_type.value} 停止信号已发送")
                    except Exception as e:
                        logger.error(f"停止下载器 {downloader.task_type.value} 时发生错误: {e}")
            
            self._cleanup_resources(timeout)
            
            with self._state_lock:
                self._shutdown_complete.set()
                
            logger.info("DownloaderManager 已完全停止")
            
        except Exception as e:
            logger.error(f"停止 DownloaderManager 时发生错误: {e}")
            try:
                self._cleanup_resources(timeout)
            except Exception as cleanup_error:
                logger.error(f"清理资源时发生错误: {cleanup_error}")
            raise
            
    def _cleanup_resources(self, timeout: float = 30.0):
        """清理资源，包括线程池和信号处理器"""
        # 恢复信号处理器
        self._restore_signal_handlers()
        
        # 关闭线程池
        if self.executor is not None:
            try:
                logger.debug("正在关闭线程池...")
                self.executor.shutdown(wait=True)
                logger.debug("线程池已关闭")
            except Exception as e:
                logger.error(f"关闭线程池时发生错误: {e}")
                # 强制关闭
                try:
                    self.executor.shutdown(wait=False)
                except Exception:
                    pass
            finally:
                self.executor = None
        
    def wait_for_stop(self, timeout: Optional[float] = None):
        """等待停止信号
        
        Args:
            timeout: 等待超时时间（秒），None 表示无限等待
            
        返回:
            bool: 如果收到停止信号则返回 True，超时返回 False
        """
        try:
            if timeout is None:
                # 无限等待
                while not self._stop_event.is_set():
                    self._stop_event.wait(1.0)
                return True
            else:
                # 有超时的等待
                return self._stop_event.wait(timeout)
        except KeyboardInterrupt:
            logger.info("收到键盘中断信号")
            self._stop_event.set()
            return True
        except Exception as e:
            logger.error(f"等待停止信号时发生错误: {e}")
            self._stop_event.set()
            return True
            
    def __enter__(self):
        """上下文管理器入口"""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口，确保资源被正确清理"""
        try:
            self.stop()
        except Exception as e:
            logger.error(f"上下文管理器清理时发生错误: {e}")
        return False  # 不抑制异常
        
    @property
    def is_started(self) -> bool:
        """检查是否已启动"""
        return self._started.is_set()
        
    @property
    def is_stopped(self) -> bool:
        """检查是否已停止"""
        return self._stop_event.is_set()
        
    @property
    def is_shutdown_complete(self) -> bool:
        """检查是否完全关闭"""
        return self._shutdown_complete.is_set()
