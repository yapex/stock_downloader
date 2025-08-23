"""Database 包的接口定义"""

from typing import Protocol, List, Dict
import pandas as pd
from .types import TableSchema


class IDBOperator(Protocol):
    """数据库操作器接口"""

    def upsert(self, table_key: str, data: pd.DataFrame) -> None:
        """插入或更新数据

        Args:
            table_key: 表键名
            data: 要插入或更新的数据
        """
        ...

    def get_all_symbols(self) -> List[str]:
        """获取所有股票代码

        Returns:
            股票代码列表
        """
        ...

    def create_table(self, table_key: str) -> None:
        """创建表

        Args:
            table_key: 表键名
        """
        ...

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在

        Args:
            table_name: 表名

        Returns:
            表是否存在
        """
        ...


class ISchemaTableCreator(Protocol):
    """Schema表创建器接口"""

    def create_table(self, table_name: str) -> bool:
        """创建表

        Args:
            table_name: 表名

        Returns:
            创建是否成功
        """
        ...

    def drop_table(self, table_name: str) -> bool:
        """删除表

        Args:
            table_name: 表名

        Returns:
            删除是否成功
        """
        ...

    def drop_all_tables(self) -> Dict[str, bool]:
        """删除所有表

        Returns:
            每个表的删除结果
        """
        ...

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在

        Args:
            table_name: 表名

        Returns:
            表是否存在
        """
        ...

    def create_all_tables(self) -> Dict[str, bool]:
        """创建所有表

        Returns:
            每个表的创建结果
        """
        ...


class ISchemaLoader(Protocol):
    """Schema配置加载器接口"""

    def load_schema(self, table_name: str) -> TableSchema:
        """加载指定表的Schema配置

        Args:
            table_name: 表名

        Returns:
            表的Schema配置
        """
        ...

    def load_all_schemas(self) -> Dict[str, TableSchema]:
        """加载所有表的Schema配置

        Returns:
            所有表的Schema配置字典
        """
        ...

    def get_table_names(self) -> List[str]:
        """获取所有表名

        Returns:
            表名列表
        """
        ...


class IBatchSaver(Protocol):
    """批量数据保存器接口"""

    def save_batch(self, table_name: str, data: pd.DataFrame) -> bool:
        """批量保存数据

        Args:
            table_name: 表名
            data: 要保存的数据

        Returns:
            保存是否成功
        """
        ...

    def get_batch_size(self) -> int:
        """获取批量大小

        Returns:
            批量大小
        """
        ...
