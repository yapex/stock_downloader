import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import sys

# 导入核心组件以进行类型提示，增强代码可读性和健壮性
from ..fetcher import TushareFetcher
from ..storage import DuckDBStorage
# rate_limit装饰器已移至各个fetcher方法中使用ratelimit库

# 从新的 utils 模块导入工具函数
from ..utils import record_failed_task
from ..error_handler import is_test_task, classify_error


class BaseTaskHandler(ABC):
    """所有任务处理器的抽象基类。"""

    def __init__(
        self,
        task_config: dict,
        fetcher: TushareFetcher,
        storage: DuckDBStorage,
        force_run: bool = False,
    ):
        self.task_config = task_config
        self.fetcher = fetcher
        self.storage = storage
        self.force_run = force_run
        self.logger = logging.getLogger(self.__class__.__name__)
        self._current_progress_bar = None

    def _safe_log(self, level: str, message: str, *args, **kwargs):
        """安全的日志输出方法，与进度条兼容"""
        if self._current_progress_bar is not None:
            # 如果当前有活跃的进度条，使用 tqdm.write 输出
            log_msg = f"{self.logger.name} - {level.upper()} - {message}"
            if args:
                log_msg = log_msg % args
            tqdm.write(log_msg)
        else:
            # 否则使用常规的 logger
            getattr(self.logger, level.lower())(message, *args, **kwargs)

    def _log_debug(self, message: str, *args, **kwargs):
        self._safe_log("DEBUG", message, *args, **kwargs)

    def _log_info(self, message: str, *args, **kwargs):
        """信息级别日志"""
        self._safe_log("INFO", message, *args, **kwargs)

    def _log_warning(self, message: str, *args, **kwargs):
        """警告级别日志"""
        self._safe_log("WARNING", message, *args, **kwargs)

    def _log_error(self, message: str, *args, **kwargs):
        """错误级别日志"""
        self._safe_log("ERROR", message, *args, **kwargs)

    @abstractmethod
    def execute(self, **kwargs):
        """执行任务的主方法。"""
        raise NotImplementedError


class IncrementalTaskHandler(BaseTaskHandler):
    """
    模板方法模式：为所有基于时间戳的增量任务提供一个固定的算法骨架。
    子类只需要实现几个特定的“可变点”即可。
    """

    def execute(self, **kwargs):
        target_symbols = kwargs.get("target_symbols")
        if not target_symbols:
            self._log_warning(
                f"任务 '{self.task_config['name']}' 未收到目标股票列表，跳过。"
            )
            return

        task_name = self.task_config["name"]
        self._log_debug(
            f"--- 开始为 {len(target_symbols)} 只股票执行增量任务: '{task_name}' ---"
        )

        try:
            progress_bar = tqdm(
                target_symbols,
                desc=f"执行: {task_name}",
                ncols=100,
                leave=True,
                file=sys.stdout,
            )
            self._current_progress_bar = progress_bar
        except (BrokenPipeError, OSError) as e:
            self._log_warning(f"进度条初始化失败，切换为静默模式: {e}")
            # 使用静默模式，禁用进度条
            self._current_progress_bar = None
            # 直接使用迭代器而不是进度条
            progress_bar = target_symbols

        # 简化执行逻辑：直接处理每只股票，不再收集网络错误进行二次重试
        is_progress_bar_active = hasattr(progress_bar, 'close')  # 判断是否为真正的进度条
        try:
            for ts_code in progress_bar:
                self._process_single_symbol(ts_code)
        finally:
            self._current_progress_bar = None
            if is_progress_bar_active:
                progress_bar.close()

    def _process_single_symbol(self, ts_code: str) -> bool:
        """
        处理单个股票的下载、保存逻辑。
        重试逻辑已经移至 fetcher 层的 @enhanced_retry 装饰器中。

        Args:
            ts_code (str): 股票代码。

        Returns:
            bool: 是否成功处理
        """
        task_name = self.task_config["name"]
        data_type = self.get_data_type()
        date_col = self.get_date_col()
        
        desc_prefix = f"处理: {data_type}_{ts_code}"
        if self._current_progress_bar:
            try:
                self._current_progress_bar.set_description(desc_prefix)
            except (BrokenPipeError, OSError):
                # 进度条更新失败，切换为静默模式
                self._log_debug(f"进度条更新失败，禁用进度条: {desc_prefix}")
                self._current_progress_bar = None

        try:
            # 步骤1: 获取最新日期
            # 将data_type映射到内部数据类型
            if data_type == "daily_basic" or data_type.startswith("fundamental"):
                internal_type = "fundamental"
            elif data_type.startswith("daily"):
                internal_type = "daily"
            elif data_type.startswith("financials"):
                internal_type = "financial"
            else:
                internal_type = "daily"  # 默认
            
            latest_date = self.storage.get_latest_date_by_stock(ts_code, internal_type)
            
            # 步骤2: 根据 latest_date 确定 start_date
            start_date = "19901219"  # 默认起始日期
            if latest_date:
                try:
                    # 首先尝试标准的 YYYYMMDD 格式
                    start_date = (
                        pd.to_datetime(latest_date, format="%Y%m%d") + timedelta(days=1)
                    ).strftime("%Y%m%d")
                except ValueError:
                    try:
                        # 如果标准格式失败，尝试自动解析
                        start_date = (
                            pd.to_datetime(latest_date) + timedelta(days=1)
                        ).strftime("%Y%m%d")
                    except Exception as e:
                        self._log_warning(f"无法解析日期格式 {latest_date}，使用默认起始日期: {e}")
                        start_date = "19901219"

            end_date = datetime.now().strftime("%Y%m%d")
            if start_date > end_date:
                return True  # 没有新数据需要下载

            # 步骤3: 获取数据（包含重试逻辑）
            rate_limit_config = self.task_config.get("rate_limit", {})
            # 限流现在由fetcher方法中的ratelimit库处理
            # fetch_data 方法已经包含重试机制
            df = self.fetch_data(ts_code, start_date, end_date)

            # 步骤4: 保存数据
            if df is not None and not df.empty:
                # 直接写入数据库 - 使用新的直接方法
                # 确保ts_code列存在
                if "ts_code" not in df.columns:
                    df = df.copy()
                    df["ts_code"] = ts_code
                
                # 根据data_type调用对应的保存方法
                if data_type.startswith("stock_list") or data_type == "system":
                    self.storage.save_stock_list(df)
                elif data_type == "daily_basic" or data_type.startswith("fundamental"):
                    self.storage.save_fundamental_data(df)
                elif data_type.startswith("daily"):
                    self.storage.save_daily_data(df)
                elif data_type.startswith("financials"):
                    self.storage.save_financial_data(df)
                else:
                    self.storage.save_daily_data(df)  # 默认
                return True
            elif df is not None:  # 空 DataFrame，表示没有新数据
                return True
            else:  # None，表示获取失败（已在 fetcher 层记录和重试）
                return False

        except Exception as e:
            # Task层的异常（通常是非网络相关的逻辑错误）
            error_category = classify_error(e)
            task_category = "test" if is_test_task(task_name) else error_category.value
            
            self._log_error(f"❌ 处理股票 {ts_code} 时发生错误: {e}")
            record_failed_task(
                task_name, 
                f"{data_type}_{ts_code}", 
                str(e),
                task_category
            )
            return False


    @abstractmethod
    def get_data_type(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_date_col(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def fetch_data(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        raise NotImplementedError

