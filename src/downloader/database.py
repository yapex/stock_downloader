"""数据库连接抽象接口

注意：with_* 装饰器已弃用，内部仍可用但不再在本项目中使用。
推荐使用极简的 connect_db() 函数或上下文管理器版本。
"""

from typing import Any, Optional, Protocol, Callable, TypeVar, ContextManager
import threading
import pandas as pd
from functools import wraps
from contextlib import contextmanager

T = TypeVar("T")


def connect_db(database_path: str, read_only: bool = False):
    """创建数据库连接的极薄别名函数
    
    直接返回DuckDB连接，无封装，无管理
    
    Args:
        database_path: 数据库路径
        read_only: 是否为只读连接
        
    Returns:
        DuckDB连接对象
    """
    import duckdb
    return duckdb.connect(database=database_path, read_only=read_only)


class DatabaseConnection(Protocol):
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


class DatabaseConnectionFactory(Protocol):
    """数据库连接工厂协议接口"""

    def create_connection(
        self, database_path: str, read_only: bool = False
    ) -> DatabaseConnection:
        """创建数据库连接"""
        ...

    def create_for_reading(self, database_path: str) -> DatabaseConnection:
        """创建只读连接"""
        ...

    def create_for_writing(self, database_path: str) -> DatabaseConnection:
        """创建写入连接（单例模式）"""
        ...

    def get_write_connection(self, database_path: str) -> ContextManager[DatabaseConnection]:
        """获取写连接的上下文管理器"""
        ...

    def get_read_connection(self, database_path: str) -> ContextManager[DatabaseConnection]:
        """获取读连接的上下文管理器"""
        ...


class DuckDBConnection(DatabaseConnection):
    """DuckDB连接实现"""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, query: str, params: Optional[list] = None) -> Any:
        """执行SQL查询"""
        if params:
            return self._conn.execute(query, params)
        return self._conn.execute(query)

    def register(self, name: str, df: pd.DataFrame) -> None:
        """注册DataFrame到数据库"""
        self._conn.register(name, df)

    def unregister(self, name: str) -> None:
        """取消注册DataFrame"""
        self._conn.unregister(name)

    def close(self) -> None:
        """关闭连接"""
        self._conn.close()


class DuckDBConnectionFactory:
    """DuckDB连接工厂实现

    基于DuckDB最佳实践的连接管理：
    1. 即用即创，用完即弃 (Create on demand, dispose after use)
    2. 每个线程一个连接 (One connection per thread)
    3. 拥抱短生命周期连接原则 (Embrace short-lived connections)
    4. 多读单写并发模型 (Multiple readers, single writer)

    注意：此工厂不维护连接池，每次调用都创建新连接
    """

    def __init__(self):
        # 不维护任何连接状态，完全遵循"即用即创，用完即弃"原则
        pass

    def create_connection(
        self, database_path: str, read_only: bool = False
    ) -> DatabaseConnection:
        """创建DuckDB连接

        每次调用都创建全新连接，符合短生命周期原则
        """
        import duckdb

        conn = duckdb.connect(database=database_path, read_only=read_only)
        return DuckDBConnection(conn)

    def create_for_reading(self, database_path: str) -> DatabaseConnection:
        """创建读连接

        策略：
        1. 每次调用都创建新连接
        2. 使用者负责在使用后关闭连接
        3. 由于DuckDB限制，所有连接都使用读写模式
        4. 完全遵循"即用即创，用完即弃"原则

        注意：DuckDB不允许对同一数据库文件使用不同配置的连接
        """
        return self.create_connection(database_path, read_only=False)

    def create_for_writing(self, database_path: str) -> DatabaseConnection:
        """创建写连接

        策略：
        1. 每次调用都创建新连接
        2. 使用者负责在使用后关闭连接
        3. 应用层面需要确保写操作的串行化
        4. 完全遵循"即用即创，用完即弃"原则
        """
        return self.create_connection(database_path, read_only=False)

    def close_connection(self, database_path: str) -> None:
        """关闭指定数据库的连接

        注意：在新的"即用即创，用完即弃"模式下，
        连接的关闭由使用者负责，此方法保留用于向后兼容
        """
        # 在新模式下，连接不由工厂管理，此方法为空实现
        pass

    @contextmanager
    def get_write_connection(self, database_path: str):
        """获取写连接的上下文管理器"""
        conn = self.create_for_writing(database_path)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def get_read_connection(self, database_path: str):
        """获取读连接的上下文管理器"""
        conn = self.create_for_reading(database_path)
        try:
            yield conn
        finally:
            conn.close()


def with_db_connection(
    database_path: str,
    read_only: bool = False,
    factory: Optional[DatabaseConnectionFactory] = None,
):
    """数据库连接装饰器

    实现DuckDB "即用即创，用完即弃" 的最佳实践

    Args:
        database_path: 数据库路径
        read_only: 是否为只读连接
        factory: 连接工厂实例，默认使用DuckDBConnectionFactory

    Usage:
        @with_db_connection(":memory:")
        def my_function(conn: DatabaseConnection):
            return conn.execute("SELECT 1").fetchone()
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # 创建连接工厂
            conn_factory = factory or DuckDBConnectionFactory()

            # 创建连接
            if read_only:
                conn = conn_factory.create_for_reading(database_path)
            else:
                conn = conn_factory.create_for_writing(database_path)

            try:
                # 将连接作为第一个参数传递给函数
                return func(conn, *args, **kwargs)
            finally:
                # 确保连接被关闭
                conn.close()

        return wrapper

    return decorator


def with_read_connection(
    database_path: str, factory: Optional[DatabaseConnectionFactory] = None
):
    """只读连接装饰器

    Args:
        database_path: 数据库路径
        factory: 连接工厂实例，默认使用DuckDBConnectionFactory
    """
    return with_db_connection(database_path, read_only=True, factory=factory)


def with_write_connection(
    database_path: str, factory: Optional[DatabaseConnectionFactory] = None
):
    """写连接装饰器

    Args:
        database_path: 数据库路径
        factory: 连接工厂实例，默认使用DuckDBConnectionFactory
    """
    return with_db_connection(database_path, read_only=False, factory=factory)


from contextlib import contextmanager


@contextmanager
def connect_db_ctx(
    database_path: str,
    read_only: bool = False,
    factory: Optional[DatabaseConnectionFactory] = None,
):
    """数据库连接上下文管理器
    
    实现DuckDB "即用即创，用完即弃" 的最佳实践
    
    Args:
        database_path: 数据库路径
        read_only: 是否为只读连接
        factory: 连接工厂实例，默认使用DuckDBConnectionFactory
        
    Usage:
        with connect_db_ctx(":memory:") as conn:
            result = conn.execute("SELECT 1")
    """
    # 创建连接工厂
    conn_factory = factory or DuckDBConnectionFactory()
    
    # 创建连接
    if read_only:
        conn = conn_factory.create_for_reading(database_path)
    else:
        conn = conn_factory.create_for_writing(database_path)
    
    try:
        yield conn
    finally:
        # 确保连接被关闭
        conn.close()
