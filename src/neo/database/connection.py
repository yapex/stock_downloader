#!/usr/bin/env python3
"""
DuckDB 数据库连接管理器

提供简单的上下文管理器接口来管理 DuckDB 连接，确保连接在使用完毕后自动关闭。
"""

from contextlib import contextmanager
from typing import Generator
import duckdb
import logging
import threading
from ..config import get_config

logger = logging.getLogger(__name__)

# Thread-local 存储用于内存数据库连接
_thread_local = threading.local()


class DatabaseConnectionManager:
    """数据库连接管理器"""

    def __init__(self, db_path: str = None):
        """初始化连接管理器

        Args:
            db_path: 数据库路径，如果为 None 则从配置中获取
        """
        config = get_config()
        self.db_path = db_path or config.database.path

    @contextmanager
    def get_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """获取数据库连接的上下文管理器

        使用方式:
            with db_manager.get_connection() as conn:
                conn.execute("SELECT * FROM table")

        Yields:
            DuckDB 连接对象
        """
        conn = None
        try:
            conn = duckdb.connect(self.db_path)
            logger.debug(f"连接到数据库 {self.db_path} 成功")
            yield conn
        except Exception as e:
            logger.error(f"数据库连接错误: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("数据库连接已关闭")


# 全局实例，提供便捷的函数接口
_default_manager = None


def get_conn(db_path: str = None) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """获取数据库连接的便捷函数

    使用方式:
        with get_conn() as conn:
            conn.execute("SELECT * FROM table")

    Args:
        db_path: 数据库路径，如果为 None 则从配置中获取

    Yields:
        DuckDB 连接对象
    """
    global _default_manager
    if _default_manager is None or (
        _default_manager.db_path != db_path and db_path is not None
    ):
        _default_manager = DatabaseConnectionManager(db_path)

    return _default_manager.get_connection()


@contextmanager
def get_memory_conn() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """获取内存数据库连接的便捷函数
    
    使用 thread-local 存储确保同一线程中返回相同的内存数据库连接。

    使用方式:
        with get_memory_conn() as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")

    Yields:
        DuckDB 内存连接对象
    """
    if not hasattr(_thread_local, 'memory_conn'):
        _thread_local.memory_conn = duckdb.connect(":memory:")
    
    try:
        yield _thread_local.memory_conn
    except Exception as e:
        logger.error(f"内存数据库连接错误: {e}")
        raise
    finally:
        # 内存连接不需要关闭，保持在 thread-local 中
        pass