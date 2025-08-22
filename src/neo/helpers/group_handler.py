"""组处理器

负责处理任务组相关的逻辑，包括解析组配置、获取股票代码等。
"""

from typing import List, Optional, Protocol
from neo.config import get_config
from neo.database.interfaces import IDBOperator
from neo.task_bus.types import TaskType


class IGroupHandler(Protocol):
    """组处理器接口"""

    def get_symbols_for_group(self, group_name: str) -> List[str]:
        """获取指定组的股票代码列表

        Args:
            group_name: 组名

        Returns:
            股票代码列表
        """
        ...

    def get_task_types_for_group(self, group_name: str) -> List[TaskType]:
        """获取指定组的任务类型列表

        Args:
            group_name: 组名

        Returns:
            任务类型列表
        """
        ...


class GroupHandler:
    """组处理器实现"""

    def __init__(self, db_operator: Optional[IDBOperator] = None):
        self._db_operator = db_operator

    def get_symbols_for_group(self, group_name: str) -> List[str]:
        """获取指定组的股票代码列表

        Args:
            group_name: 组名

        Returns:
            股票代码列表
        """
        config = get_config()

        if group_name not in config.task_groups:
            raise ValueError(f"未找到组配置: {group_name}")

        # 获取组的任务类型
        task_type_names = config.task_groups[group_name]
        
        # 如果组包含 stock_basic 任务，不需要具体的股票代码
        if "stock_basic" in task_type_names:
            return []

        # 其他组需要从数据库获取所有股票代码
        if self._db_operator:
            return self._get_all_symbols_from_db()
        else:
            # 如果没有数据库操作器，返回测试用的股票代码
            return ["000001.SZ", "600519.SH"]

    def get_task_types_for_group(self, group_name: str) -> List[TaskType]:
        """获取指定组的任务类型列表

        Args:
            group_name: 组名

        Returns:
            任务类型列表
        """
        config = get_config()

        if group_name not in config.task_groups:
            raise ValueError(f"未找到组配置: {group_name}")

        # 获取任务类型字符串列表
        task_type_names = config.task_groups[group_name]

        # 转换为 TaskType 枚举
        task_types = []
        for name in task_type_names:
            try:
                # 直接使用原始名称访问枚举
                task_type = getattr(TaskType, name)
                task_types.append(task_type)
            except AttributeError:
                raise ValueError(f"未知的任务类型: {name}")

        return task_types

    def _get_all_symbols_from_db(self) -> List[str]:
        """从数据库获取所有股票代码

        Returns:
            股票代码列表
        """
        try:
            symbols = self._db_operator.get_all_symbols()
            if not symbols:
                raise ValueError("数据库中没有找到股票代码，请先运行 stock_basic 任务")
            return symbols
        except Exception as e:
            raise ValueError(f"从数据库获取股票代码失败: {e}")
