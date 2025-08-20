"""数据获取器构建器模块

基于函数构建器模式，提供灵活的数据获取器构建功能。
支持配置驱动的任务模板和运行时参数覆盖。
"""

import tushare as ts
from typing import Callable, Dict, Any, Optional
import pandas as pd
import logging
from dataclasses import dataclass
from threading import Lock

from downloader2.config import get_config
from downloader2.utils import normalize_stock_code
from enum import Enum

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TaskTemplate:
    """任务模板配置"""

    api_method: str
    base_object: str = "pro"
    default_params: Dict[str, Any] = None

    def __post_init__(self):
        if self.default_params is None:
            object.__setattr__(self, "default_params", {})


class TaskType(Enum):
    """任务类型常量定义，包含模板配置"""

    STOCK_BASIC = TaskTemplate(api_method="stock_basic")
    STOCK_DAILY = TaskTemplate(api_method="daily")
    DAILY_BAR_QFQ = TaskTemplate(
        api_method="pro_bar", base_object="ts", default_params={"adj": "qfq"}
    )
    DAILY_BAR_NONE = TaskTemplate(
        api_method="pro_bar", base_object="ts", default_params={"adj": None}
    )
    BALANCESHEET = TaskTemplate(api_method="balancesheet")
    INCOME = TaskTemplate(api_method="income")
    CASHFLOW = TaskTemplate(api_method="cashflow")

    @property
    def template(self) -> "TaskTemplate":
        """获取任务模板"""
        return self.value


@dataclass(frozen=True)
class TaskTemplate:
    """任务模板配置"""

    api_method: str
    base_object: str = "pro"
    default_params: Dict[str, Any] = None

    def __post_init__(self):
        if self.default_params is None:
            object.__setattr__(self, "default_params", {})


class TushareApiManager:
    """Tushare API 管理器（单例模式）"""

    _instance: Optional["TushareApiManager"] = None
    _lock = Lock()

    def __init__(self):
        if TushareApiManager._instance is not None:
            raise RuntimeError("TushareApiManager 是单例类，请使用 get_instance() 方法")

        config = get_config()
        logger.info("初始化 Tushare API...")
        ts.set_token(config.tushare.token)
        self.pro = ts.pro_api()
        self.ts = ts
        self.api_objects = {"pro": self.pro, "ts": self.ts}

    @classmethod
    def get_instance(cls) -> "TushareApiManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_api_function(self, base_object: str, method_name: str):
        """获取 API 函数"""
        if base_object not in self.api_objects:
            raise ValueError(f"不支持的 API 对象: {base_object}")

        api_obj = self.api_objects[base_object]
        if not hasattr(api_obj, method_name):
            raise ValueError(f"API 对象 {base_object} 没有方法 {method_name}")

        return getattr(api_obj, method_name)


class FetcherBuilder:
    """数据获取器构建器"""

    def __init__(self):
        self.api_manager = TushareApiManager.get_instance()

    def build_by_task(
        self, task_type: TaskType, symbol: str = "", **overrides: Any
    ) -> Callable[[], pd.DataFrame]:
        """构建指定股票代码的数据获取器

        Args:
            task_type: 任务类型
            symbol: 股票代码
            **overrides: 运行时参数覆盖

        Returns:
            可执行的数据获取函数
        """
        # 检查任务类型是否有效
        if not isinstance(task_type, TaskType):
            raise ValueError(f"不支持的任务类型: {task_type}")

        # 直接从枚举获取模板
        template = task_type.template

        # 标准化股票代码
        if symbol:
            overrides["ts_code"] = normalize_stock_code(symbol)

        # 合并参数：默认参数 + 运行时覆盖
        merged_params = (
            template.default_params.copy() if template.default_params else {}
        )
        merged_params.update(overrides)

        # 获取 API 函数
        api_func = self.api_manager.get_api_function(
            template.base_object, template.api_method
        )

        def execute() -> pd.DataFrame:
            """执行数据获取"""
            logger.debug(f"调用 {template.api_method}，参数: {merged_params}")
            try:
                result = api_func(**merged_params)
                logger.debug(f"获取 {len(result)} 条记录")
                return result
            except Exception as e:
                logger.error(f"任务执行失败: {e}")
                raise

        return execute


if __name__ == "__main__":
    # 最精简的用法示例
    task_type = TaskType.STOCK_BASIC
    # print(task_type)
    fetcher = FetcherBuilder().build_by_task(task_type, "600519")
    df = fetcher()
    print(df.head(1))
    fetcher = FetcherBuilder().build_by_task(task_type)
    df = fetcher()
    print(df["ts_code"].tail(3))
