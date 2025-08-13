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

logger = logging.getLogger(__name__)


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
        "stock_basic": TaskTemplate(
            name="stock_basic",
            description="股票基本信息",
            api_method="stock_basic",
            base_object="pro",
            default_params={},
        ),
        "stock_daily": TaskTemplate(
            name="stock_daily",
            description="股票日线行情",
            api_method="daily",
            base_object="pro",
            default_params={},
        ),
        "daily_bar_qfq": TaskTemplate(
            name="daily_bar_qfq",
            description="股票前复权日线行情",
            api_method="pro_bar",
            base_object="ts",
            default_params={"adj": "qfq"},
        ),
        "daily_bar_none": TaskTemplate(
            name="daily_bar_none",
            description="股票不复权日线行情",
            api_method="pro_bar",
            base_object="ts",
            default_params={"adj": None},
        ),
        "balancesheet": TaskTemplate(
            name="balancesheet",
            description="资产负债表",
            api_method="balancesheet",
            base_object="pro",
            default_params={},
        ),
        "income": TaskTemplate(
            name="income",
            description="利润表",
            api_method="income",
            base_object="pro",
            default_params={},
        ),
        "cashflow": TaskTemplate(
            name="cashflow",
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
    return builder.build_fetcher("stock_basic", **overrides)


def build_stock_daily(**overrides) -> Callable[[], pd.DataFrame]:
    """构建股票日线行情获取器"""
    builder = FetcherBuilder()
    return builder.build_fetcher("stock_daily", **overrides)


def build_daily_bar_qfq(**overrides) -> Callable[[], pd.DataFrame]:
    """构建股票前复权日线行情获取器"""
    builder = FetcherBuilder()
    return builder.build_fetcher("daily_bar_qfq", **overrides)


def build_daily_bar_none(**overrides) -> Callable[[], pd.DataFrame]:
    """构建股票不复权日线行情获取器"""
    builder = FetcherBuilder()
    return builder.build_fetcher("daily_bar_none", **overrides)


def build_balancesheet(**overrides) -> Callable[[], pd.DataFrame]:
    """构建资产负债表获取器"""
    builder = FetcherBuilder()
    return builder.build_fetcher("balancesheet", **overrides)


def build_income(**overrides) -> Callable[[], pd.DataFrame]:
    """构建利润表获取器"""
    builder = FetcherBuilder()
    return builder.build_fetcher("income", **overrides)


def build_cashflow(**overrides) -> Callable[[], pd.DataFrame]:
    """构建现金流量表获取器"""
    builder = FetcherBuilder()
    return builder.build_fetcher("cashflow", **overrides)


if __name__ == "__main__":
    # 最精简的用法示例
    ts_code = "600519.SH"
    basic_fetcher = build_stock_basic()
    daily_fetcher = build_stock_daily(ts_code=ts_code)

    # 执行获取数据
    basic_data = basic_fetcher()
    daily_data = daily_fetcher()

    print("\n==== 股票基本信息 ====")
    if not basic_data.empty:
        print(basic_data.head(1))

    print("\n==== 日线行情数据 ====")
    if not daily_data.empty:
        print(daily_data.head(1))

    # 构建获取前复权日线行情的函数
    qfq_fetcher = build_daily_bar_qfq(
        ts_code=ts_code, start_date="20240101", end_date="20240131"
    )
    qfq_data = qfq_fetcher()
    print(f"\n==== 前复权日线行情 ({ts_code}) ====")
    print(f"数据形状: {qfq_data.shape}")
    print(f"列名: {list(qfq_data.columns)}")
    if not qfq_data.empty:
        print(qfq_data.head(1))

    # 构建获取不复权日线行情的函数
    none_fetcher = build_daily_bar_none(
        ts_code=ts_code, start_date="20240101", end_date="20240131"
    )
    none_data = none_fetcher()
    print(f"\n==== 不复权日线行情 ({ts_code}) ====")
    print(f"数据形状: {none_data.shape}")
    print(f"列名: {list(none_data.columns)}")
    if not none_data.empty:
        print(none_data.head(1))

    # 构建获取财务数据的函数
    period = "20231231"  # 2023年年报

    # 资产负债表
    balance_fetcher = build_balancesheet(ts_code=ts_code, period=period)
    balance_data = balance_fetcher()
    print(f"\n==== 资产负债表 ({ts_code}, {period}) ====")
    print(f"数据形状: {balance_data.shape}")
    if not balance_data.empty:
        print(f"列数: {len(balance_data.columns)}")
        # 显示关键财务指标
        key_cols = [
            "ts_code",
            "ann_date",
            "f_ann_date",
            "end_date",
            "total_assets",
            "total_liab",
            "total_hldr_eqy_exc_min_int",
        ]
        available_cols = [col for col in key_cols if col in balance_data.columns]
        if available_cols:
            print(f"关键指标: {available_cols}")
            print(balance_data[available_cols].head(1))

    # 利润表
    income_fetcher = build_income(ts_code=ts_code, period=period)
    income_data = income_fetcher()
    print(f"\n==== 利润表 ({ts_code}, {period}) ====")
    print(f"数据形状: {income_data.shape}")
    if not income_data.empty:
        print(f"列数: {len(income_data.columns)}")
        # 显示关键财务指标
        key_cols = [
            "ts_code",
            "ann_date",
            "f_ann_date",
            "end_date",
            "total_revenue",
            "revenue",
            "n_income",
            "n_income_attr_p",
        ]
        available_cols = [col for col in key_cols if col in income_data.columns]
        if available_cols:
            print(f"关键指标: {available_cols}")
            print(income_data[available_cols].head(1))

    # 现金流量表
    cashflow_fetcher = build_cashflow(ts_code=ts_code, period=period)
    cashflow_data = cashflow_fetcher()
    print(f"\n==== 现金流量表 ({ts_code}, {period}) ====")
    print(f"数据形状: {cashflow_data.shape}")
    if not cashflow_data.empty:
        print(f"列数: {len(cashflow_data.columns)}")
        # 显示关键财务指标
        key_cols = [
            "ts_code",
            "ann_date",
            "f_ann_date",
            "end_date",
            "n_cashflow_act",
            "n_cashflow_inv_act",
            "n_cashflow_fin_act",
        ]
        available_cols = [col for col in key_cols if col in cashflow_data.columns]
        if available_cols:
            print(f"关键指标: {available_cols}")
            print(cashflow_data[available_cols].head(1))
