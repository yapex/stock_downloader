#!/usr/bin/env python3
"""
Storage工厂和单例实现

解决问题：
1. 统一存储实例的创建和管理
2. 支持单例模式，避免重复创建连接
3. 提供统一的storage创建入口
4. 支持不同类型的存储后端扩展
"""

import threading
from typing import Optional, Dict, Any
from pathlib import Path

from .storage import PartitionedStorage
from .database import DuckDBConnectionFactory, DatabaseConnectionFactory
from .logger_interface import LoggerFactory, LoggerInterface


class StorageSingleton:
    """
    Storage单例类
    
    确保整个应用只有一个Storage实例，避免重复创建数据库连接。
    线程安全的单例实现。
    """
    
    _instance: Optional[PartitionedStorage] = None
    _lock = threading.Lock()
    _db_path: Optional[str] = None
    _db_factory: Optional[DatabaseConnectionFactory] = None
    _logger: Optional[LoggerInterface] = None
    
    @classmethod
    def get_instance(
        cls, 
        db_path: str = "data/stock.db", 
        db_factory: Optional[DatabaseConnectionFactory] = None,
        logger: Optional[LoggerInterface] = None,
        **kwargs
    ) -> PartitionedStorage:
        """
        获取Storage单例实例
        
        Args:
            db_path: 数据库文件路径
            db_factory: 数据库连接工厂
            logger: 日志接口
            **kwargs: 传递给PartitionedStorage构造函数的其他参数
            
        Returns:
            PartitionedStorage: storage实例
        """
        # 使用默认工厂和日志器
        if db_factory is None:
            db_factory = DuckDBConnectionFactory()
        if logger is None:
            logger = LoggerFactory.create_logger(__name__)
            
        if cls._instance is None or cls._db_path != db_path:
            with cls._lock:
                # 双重检查锁定模式
                if cls._instance is None or cls._db_path != db_path:
                    if cls._instance is not None:
                        logger.info(f"数据库路径变更，重新创建Storage实例: {cls._db_path} -> {db_path}")
                    
                    logger.debug(f"创建Storage单例实例: {db_path}")
                    cls._instance = PartitionedStorage(db_path, db_factory, logger)
                    cls._db_path = db_path
                    cls._db_factory = db_factory
                    cls._logger = logger
                    
                    logger.info(f"Storage单例实例已创建: {db_path}")
        
        return cls._instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """
        重置单例实例（主要用于测试）
        """
        with cls._lock:
            if cls._instance is not None:
                if cls._logger:
                    cls._logger.debug("重置Storage单例实例")
                cls._instance = None
                cls._db_path = None
                cls._db_factory = None
                cls._logger = None
    
    @classmethod
    def get_instance_info(cls) -> Dict[str, Any]:
        """
        获取当前单例实例的信息
        
        Returns:
            Dict: 包含实例信息的字典
        """
        with cls._lock:
            return {
                "has_instance": cls._instance is not None,
                "db_path": cls._db_path,
                "instance_id": id(cls._instance) if cls._instance else None,
                "thread_id": threading.current_thread().ident
            }


def get_storage(
    use_singleton: bool = True, 
    db_path: str = "data/stock.db", 
    db_factory: Optional[DatabaseConnectionFactory] = None,
    logger: Optional[LoggerInterface] = None,
    **kwargs
) -> PartitionedStorage:
    """
    获取Storage实例的便捷函数
    
    Args:
        use_singleton: 是否使用单例模式，默认True
        db_path: 数据库文件路径
        db_factory: 数据库连接工厂
        logger: 日志接口
        **kwargs: 传递给PartitionedStorage构造函数的其他参数
        
    Returns:
        PartitionedStorage: storage实例
    """
    # 使用默认工厂和日志器
    if db_factory is None:
        db_factory = DuckDBConnectionFactory()
    if logger is None:
        logger = LoggerFactory.create_logger(__name__)
        
    if use_singleton:
        return StorageSingleton.get_instance(db_path=db_path, db_factory=db_factory, logger=logger, **kwargs)
    else:
        logger.debug(f"创建新的Storage实例: {db_path}")
        return PartitionedStorage(db_path, db_factory, logger)


def get_storage_instance_info() -> Dict[str, Any]:
    """
    获取当前Storage单例实例的信息
    
    Returns:
        Dict: 包含实例信息的字典
    """
    return StorageSingleton.get_instance_info()


class StorageFactory:
    """
    Storage工厂类
    
    提供统一的storage创建接口，支持单例和非单例模式。
    """
    
    @staticmethod
    def create_storage(
        use_singleton: bool = True, 
        db_path: str = "data/stock.db", 
        db_factory: Optional[DatabaseConnectionFactory] = None,
        logger: Optional[LoggerInterface] = None,
        **kwargs
    ) -> PartitionedStorage:
        """
        创建Storage实例
        
        Args:
            use_singleton: 是否使用单例模式，默认True
            db_path: 数据库文件路径
            db_factory: 数据库连接工厂
            logger: 日志接口
            **kwargs: 传递给PartitionedStorage构造函数的其他参数
            
        Returns:
            PartitionedStorage: storage实例
        """
        return get_storage(use_singleton=use_singleton, db_path=db_path, db_factory=db_factory, logger=logger, **kwargs)
    
    @staticmethod
    def create_partitioned_storage(
        db_path: str = "data/stock.db", 
        db_factory: Optional[DatabaseConnectionFactory] = None,
        logger: Optional[LoggerInterface] = None,
        **kwargs
    ) -> PartitionedStorage:
        """
        直接创建分区表存储实例（非单例）
        
        Args:
            db_path: 数据库文件路径
            db_factory: 数据库连接工厂
            logger: 日志接口
            **kwargs: 传递给PartitionedStorage构造函数的其他参数
            
        Returns:
            PartitionedStorage: storage实例
        """
        # 使用默认工厂和日志器
        if db_factory is None:
            db_factory = DuckDBConnectionFactory()
        if logger is None:
            logger = LoggerFactory.create_logger(__name__)
        return PartitionedStorage(db_path, db_factory, logger)
    
    @staticmethod
    def get_default_storage(config: Optional[Dict[str, Any]] = None) -> PartitionedStorage:
        """
        根据配置获取默认的Storage实例
        
        Args:
            config: 配置字典，包含数据库路径等信息
            
        Returns:
            PartitionedStorage: storage实例
        """
        if config is None:
            config = {}
        
        # 从配置中获取数据库路径
        storage_config = config.get("storage", {})
        db_path = (
            storage_config.get("db_path") or 
            config.get("database", {}).get("path", "data/stock.db")
        )
        
        # 确保路径是字符串
        if isinstance(db_path, Path):
            db_path = str(db_path)
        
        return get_storage(use_singleton=True, db_path=db_path)


# 向后兼容的别名
DuckDBStorageFactory = StorageFactory