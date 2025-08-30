"""组处理器

负责处理任务组相关的逻辑，包括解析组配置、获取股票代码等。
"""

from typing import List, Optional, Protocol
from neo.configs import get_config
from neo.database.interfaces import IDBOperator, ISchemaLoader
from neo.database.schema_loader import SchemaLoader


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

    def get_task_types_for_group(self, group_name: str) -> List[str]:
        """获取指定组的任务类型列表

        Args:
            group_name: 组名

        Returns:
            任务类型字符串列表
        """
        ...


class GroupHandler:
    """组处理器实现"""

    def __init__(
        self, db_operator: Optional[IDBOperator] = None, schema_loader: Optional[ISchemaLoader] = None
    ):
        self._db_operator = db_operator
        self._schema_loader = schema_loader or SchemaLoader()

    @classmethod
    def create_default(cls) -> "GroupHandler":
        """创建默认的 GroupHandler 实例

        Returns:
            GroupHandler: 带有默认 DBOperator 的 GroupHandler 实例
        """
        from neo.database.operator import ParquetDBQueryer

        schema_loader = SchemaLoader()
        db_operator = ParquetDBQueryer(schema_loader=schema_loader)
        return cls(db_operator=db_operator, schema_loader=schema_loader)

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
        if not self._db_operator:
            from neo.database.operator import ParquetDBQueryer

            self._db_operator = ParquetDBQueryer(schema_loader=self._schema_loader)

        return self._get_all_symbols_from_db()

    def get_task_types_for_group(self, group_name: str) -> List[str]:
        """获取指定组的任务类型列表

        Args:
            group_name: 组名

        Returns:
            任务类型字符串列表
        """
        config = get_config()

        if group_name not in config.task_groups:
            raise ValueError(f"未找到组配置: {group_name}")

        # 获取任务类型字符串列表
        task_type_names = config.task_groups[group_name]
        valid_task_types = self._schema_loader.get_table_names()

        # 验证任务类型
        for name in task_type_names:
            if name not in valid_task_types:
                raise ValueError(f"未知的任务类型: {name}")

        return task_type_names

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
