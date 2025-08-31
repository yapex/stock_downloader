"""组处理器

负责处理任务组相关的逻辑，包括解析组配置、获取股票代码等。
"""

from typing import List, Optional

from neo.configs import get_config
from neo.database.interfaces import IDBOperator
from neo.helpers.task_filter import TaskFilter


class GroupHandler:
    """组处理器实现"""

    def __init__(self, db_operator: Optional[IDBOperator] = None, task_filter: Optional[TaskFilter] = None):
        """初始化任务组处理器"""
        self.config = get_config()
        self.db_operator = db_operator
        self.task_filter = task_filter
        self._all_symbols: Optional[List[str]] = None

    @classmethod
    def create_default(cls) -> "GroupHandler":
        """创建默认的 GroupHandler 实例"""
        from neo.database.operator import ParquetDBQueryer
        from pathlib import Path

        db_operator = ParquetDBQueryer.create_default()
        task_filter = TaskFilter(Path("config.toml"))
        return cls(db_operator=db_operator, task_filter=task_filter)

    def get_symbols_for_group(self, group_name: str) -> List[str]:
        """获取指定组的股票代码列表"""
        if group_name not in self.config.task_groups:
            raise ValueError(f"未找到组配置: {group_name}")

        task_type_names = self.config.task_groups[group_name]

        # 如果组包含不需要股票列表的任务，返回空列表
        if any(task in ["stock_basic", "trade_cal"] for task in task_type_names):
            return []

        # 其他组需要从数据库获取所有股票代码
        if self._all_symbols is None:
            all_symbols = self._get_all_symbols_from_db()
            # 使用任务过滤器过滤股票代码
            if self.task_filter:
                self._all_symbols = self.task_filter.filter_symbols(all_symbols)
            else:
                self._all_symbols = all_symbols

        return self._all_symbols

    def get_task_types_for_group(self, group_name: str) -> List[str]:
        """获取指定组的任务类型列表"""
        if group_name not in self.config.task_groups:
            raise ValueError(f"未找到组配置: {group_name}")

        return self.config.task_groups[group_name]

    def _get_all_symbols_from_db(self) -> List[str]:
        """从数据库获取所有股票代码"""
        if self.db_operator is None:
            from neo.database.operator import ParquetDBQueryer

            self.db_operator = ParquetDBQueryer.create_default()

        try:
            symbols = self.db_operator.get_all_symbols()
            if not symbols:
                raise ValueError("数据库中没有找到股票代码，请先运行 stock_basic 任务")
            return symbols
        except Exception as e:
            raise ValueError(f"从数据库获取股票代码失败: {e}")

