"""任务总线相关类型定义

定义 producer/consumer 共享的核心数据类型和枚举。"""

from dataclasses import dataclass

# 移除 Enum 导入，改用常量类
from typing import Dict, Any


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


class TaskType:
    """任务类型常量

    纯粹的任务类型标识，不包含具体的配置信息。
    配置信息由 TaskTemplateRegistry 管理。
    """

    # 股票基本信息
    stock_basic = "stock_basic"

    # 交易日历
    trade_cal = "trade_cal"

    # 股票日线数据
    stock_daily = "stock_daily"

    # 复权行情数据
    stock_adj_qfq = "stock_adj_qfq"

    # 每日基本面指标
    daily_basic = "daily_basic"

    # 利润表
    income_statement = "income_statement"

    # 资产负债表
    balance_sheet = "balance_sheet"

    # 现金流量表
    cash_flow = "cash_flow"

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


class TaskTemplateRegistry:
    """任务模板配置注册表

    管理任务类型与其对应的 TaskTemplate 配置的映射关系。
    """

    _templates = {
        TaskType.stock_basic: TaskTemplate(api_method="stock_basic", base_object="pro"),
        TaskType.stock_daily: TaskTemplate(api_method="daily", base_object="pro"),
        TaskType.stock_adj_qfq: TaskTemplate(
            api_method="pro_bar", base_object="ts", required_params={"adj": "qfq"}
        ),
        TaskType.daily_basic: TaskTemplate(api_method="daily_basic", base_object="pro"),
        TaskType.income_statement: TaskTemplate(api_method="income", base_object="pro"),
        TaskType.balance_sheet: TaskTemplate(
            api_method="balancesheet", base_object="pro"
        ),
        TaskType.cash_flow: TaskTemplate(api_method="cashflow", base_object="pro"),
        TaskType.trade_cal: TaskTemplate(api_method="trade_cal", base_object="pro"),
    }

    @classmethod
    def get_template(cls, task_type: TaskType) -> TaskTemplate:
        """获取指定任务类型的模板配置

        Args:
            task_type: 任务类型枚举

        Returns:
            TaskTemplate: 对应的任务模板配置

        Raises:
            KeyError: 当任务类型不存在时
        """
        if task_type not in cls._templates:
            raise KeyError(f"不支持的任务类型: {task_type}")
        return cls._templates[task_type]

    @classmethod
    def register_template(cls, task_type: TaskType, template: TaskTemplate) -> None:
        """注册新的任务模板配置

        Args:
            task_type: 任务类型枚举
            template: 任务模板配置
        """
        cls._templates[task_type] = template

    @classmethod
    def get_all_templates(cls) -> Dict[TaskType, TaskTemplate]:
        """获取所有任务模板配置

        Returns:
            Dict[TaskType, TaskTemplate]: 所有任务类型与模板的映射
        """
        return cls._templates.copy()


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
    "TaskTemplateRegistry",
]
