"""数据获取器构建器模块

基于函数构建器模式，提供灵活的数据获取器构建功能。
支持配置驱动的任务模板和运行时参数覆盖。
"""

import tushare as ts
from typing import Callable, Dict, Any, Optional
import pandas as pd
from pathlib import Path
import logging
from dataclasses import dataclass
from threading import Lock

from downloader2.config import get as get_config
from enum import Enum, auto

logger = logging.getLogger(__name__)


class TaskType(str, Enum):
    """任务类型常量定义"""

    STOCK_BASIC = "stock_basic"
    STOCK_DAILY = "stock_daily"
    DAILY_BAR_QFQ = "daily_bar_qfq"
    DAILY_BAR_NONE = "daily_bar_none"
    BALANCESHEET = "balancesheet"
    INCOME = "income"
    CASHFLOW = "cashflow"


@dataclass(frozen=True)
class TaskTemplate:
    """任务模板配置"""

    name: str
    description: str
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

    # 预定义的任务模板
    TASK_TEMPLATES = {
        TaskType.STOCK_BASIC: TaskTemplate(
            name=TaskType.STOCK_BASIC,
            description="股票基本信息",
            api_method="stock_basic",
            base_object="pro",
            default_params={},
        ),
        TaskType.STOCK_DAILY: TaskTemplate(
            name=TaskType.STOCK_DAILY,
            description="股票日线行情",
            api_method="daily",
            base_object="pro",
            default_params={},
        ),
        TaskType.DAILY_BAR_QFQ: TaskTemplate(
            name=TaskType.DAILY_BAR_QFQ,
            description="股票前复权日线行情",
            api_method="pro_bar",
            base_object="ts",
            default_params={"adj": "qfq"},
        ),
        TaskType.DAILY_BAR_NONE: TaskTemplate(
            name=TaskType.DAILY_BAR_NONE,
            description="股票不复权日线行情",
            api_method="pro_bar",
            base_object="ts",
            default_params={"adj": None},
        ),
        TaskType.BALANCESHEET: TaskTemplate(
            name=TaskType.BALANCESHEET,
            description="资产负债表",
            api_method="balancesheet",
            base_object="pro",
            default_params={},
        ),
        TaskType.INCOME: TaskTemplate(
            name=TaskType.INCOME,
            description="利润表",
            api_method="income",
            base_object="pro",
            default_params={},
        ),
        TaskType.CASHFLOW: TaskTemplate(
            name=TaskType.CASHFLOW,
            description="现金流量表",
            api_method="cashflow",
            base_object="pro",
            default_params={},
        ),
    }

    def __init__(self):
        self.api_manager = TushareApiManager.get_instance()

    def build_fetcher(
        self, task_name: str, **overrides: Any
    ) -> Callable[[], pd.DataFrame]:
        """构建数据获取器函数

        Args:
            task_name: 任务名称
            **overrides: 运行时参数覆盖

        Returns:
            可执行的数据获取函数
        """
        if task_name not in self.TASK_TEMPLATES:
            raise ValueError(f"不支持的任务类型: {task_name}")

        template = self.TASK_TEMPLATES[task_name]

        # 合并参数：默认参数 + 运行时覆盖
        merged_params = template.default_params.copy()
        merged_params.update(overrides)

        # 获取 API 函数
        api_func = self.api_manager.get_api_function(
            template.base_object, template.api_method
        )

        def execute() -> pd.DataFrame:
            """执行数据获取"""
            logger.info(f"开始执行任务: {template.name} - {template.description}")
            logger.debug(f"调用参数: {merged_params}")

            try:
                result = api_func(**merged_params)
                logger.info(f"任务执行成功，获取 {len(result)} 条记录")
                return result
            except Exception as e:
                logger.error(f"任务执行失败: {e}")
                raise

        return execute

    def build_by_task(
        self, task_name: str, symbol: str, **overrides: Any
    ) -> Callable[[], pd.DataFrame]:
        """构建指定股票代码的数据获取器

        Args:
            task_name: 任务名称
            symbol: 股票代码
            **overrides: 运行时参数覆盖

        Returns:
            可执行的数据获取函数
        """
        # 将 symbol 添加到参数中
        overrides["ts_code"] = symbol
        return self.build_fetcher(task_name, **overrides)


# 便利函数
def build_stock_basic(**overrides) -> Callable[[], pd.DataFrame]:
    """构建股票基本信息获取器"""
    builder = FetcherBuilder()
    return builder.build_fetcher(TaskType.STOCK_BASIC, **overrides)


def build_stock_daily(**overrides) -> Callable[[], pd.DataFrame]:
    """构建股票日线行情获取器"""
    builder = FetcherBuilder()
    return builder.build_fetcher(TaskType.STOCK_DAILY, **overrides)


def build_daily_bar_qfq(**overrides) -> Callable[[], pd.DataFrame]:
    """构建股票前复权日线行情获取器"""
    builder = FetcherBuilder()
    return builder.build_fetcher(TaskType.DAILY_BAR_QFQ, **overrides)


def build_daily_bar_none(**overrides) -> Callable[[], pd.DataFrame]:
    """构建股票不复权日线行情获取器"""
    builder = FetcherBuilder()
    return builder.build_fetcher(TaskType.DAILY_BAR_NONE, **overrides)


def build_balancesheet(**overrides) -> Callable[[], pd.DataFrame]:
    """构建资产负债表获取器"""
    builder = FetcherBuilder()
    return builder.build_fetcher(TaskType.BALANCESHEET, **overrides)


def build_income(**overrides) -> Callable[[], pd.DataFrame]:
    """构建利润表获取器"""
    builder = FetcherBuilder()
    return builder.build_fetcher(TaskType.INCOME, **overrides)


def build_cashflow(**overrides) -> Callable[[], pd.DataFrame]:
    """构建现金流量表获取器"""
    builder = FetcherBuilder()
    return builder.build_fetcher(TaskType.CASHFLOW, **overrides)


if __name__ == "__main__":
    # 最精简的用法示例
    task_type = TaskType.STOCK_BASIC
    # print(task_type)
    fetcher = FetcherBuilder().build_fetcher(task_type)
    df = fetcher()
    print(df.head(1))
