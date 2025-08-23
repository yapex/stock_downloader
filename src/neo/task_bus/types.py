"""任务总线相关类型定义

定义 producer/consumer 共享的核心数据类型和枚举。"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any
import pandas as pd


@dataclass(frozen=True)
class TaskTemplate:
    """任务模板配置"""

    api_method: str
    base_object: str = "pro"
    default_params: Dict[str, Any] = None
    required_params: Dict[str, Any] = None

    def __post_init__(self):
        if self.default_params is None:
            object.__setattr__(self, "default_params", {})
        if self.required_params is None:
            object.__setattr__(self, "required_params", {})


class TaskType(Enum):
    """任务类型枚举"""
    
    # 股票基本信息
    stock_basic = TaskTemplate(
        api_method="stock_basic",
        base_object="pro"
    )
    
    # 股票日线数据
    stock_daily = TaskTemplate(
        api_method="daily",
        base_object="pro"
    )
    
    # 复权行情数据
    stock_adj_qfq = TaskTemplate(
        api_method="pro_bar",
        base_object="ts",
        required_params={"adj": "qfq"}
    )
    
    # 每日基本面指标
    daily_basic = TaskTemplate(
        api_method="daily_basic",
        base_object="pro"
    )
    
    # 利润表
    income_statement = TaskTemplate(
        api_method="income",
        base_object="pro"
    )
    
    # 资产负债表
    balance_sheet = TaskTemplate(
        api_method="balancesheet",
        base_object="pro"
    )
    
    # 现金流量表
    cash_flow = TaskTemplate(
        api_method="cashflow",
        base_object="pro"
    )
    
    @property
    def template(self) -> TaskTemplate:
        """获取任务模板"""
        return self.value
    
    @classmethod
    def is_valid_task_type(cls, task_type_name: str) -> bool:
        """验证任务类型是否有效"""
        try:
            cls[task_type_name]
            return True
        except KeyError:
            return False
    
    @classmethod
    def get_all_task_types(cls) -> list[str]:
        """获取所有任务类型名称"""
        return [task_type.name for task_type in cls]


@dataclass
class DownloadTaskConfig:
    """下载任务配置"""

    symbol: str
    task_type: TaskType
    max_retries: int = 3


__all__ = [
    "DownloadTaskConfig",
    "TaskType",
    "TaskTemplate",
]
