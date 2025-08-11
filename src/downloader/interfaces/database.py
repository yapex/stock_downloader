"""数据库接口定义

定义数据库相关的Protocol接口。
"""

from typing import Any, Optional, Protocol, ContextManager
import pandas as pd


class IDatabase(Protocol):
    """数据库连接协议接口"""

    def execute(self, query: str, params: Optional[list] = None) -> Any:
        """执行SQL查询"""
        ...

    def register(self, name: str, df: pd.DataFrame) -> None:
        """注册DataFrame到数据库"""
        ...

    def unregister(self, name: str) -> None:
        """取消注册DataFrame"""
        ...

    def close(self) -> None:
        """关闭连接"""
        ...


class IDatabaseFactory(Protocol):
    """数据库连接工厂协议接口"""

    def create_connection(
        self, database_path: str, read_only: bool = False
    ) -> IDatabase:
        """创建数据库连接"""
        ...

    def create_for_reading(self, database_path: str) -> IDatabase:
        """创建只读连接"""
        ...

    def create_for_writing(self, database_path: str) -> IDatabase:
        """创建写入连接（单例模式）"""
        ...

    def get_write_connection(self, database_path: str) -> ContextManager[IDatabase]:
        """获取写连接的上下文管理器"""
        ...

    def get_read_connection(self, database_path: str) -> ContextManager[IDatabase]:
        """获取读连接的上下文管理器"""
        ...