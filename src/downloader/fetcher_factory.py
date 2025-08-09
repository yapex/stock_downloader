#!/usr/bin/env python3
"""
TushareFetcher工厂和单例实现

解决问题：
1. 确保整个应用只有一个TushareFetcher实例
2. 避免多个实例导致的ratelimit计数器分散
3. 提供统一的fetcher创建入口
"""

import threading
import logging
from typing import Optional
from .fetcher import TushareFetcher

logger = logging.getLogger(__name__)


class TushareFetcherSingleton:
    """
    TushareFetcher单例实现
    
    确保整个应用程序只有一个TushareFetcher实例，
    避免多实例导致的ratelimit计数器分散问题。
    """
    
    _instance: Optional[TushareFetcher] = None
    _lock = threading.Lock()
    _initialized = False
    
    @classmethod
    def get_instance(cls, **kwargs) -> TushareFetcher:
        """
        获取TushareFetcher单例实例
        
        Args:
            **kwargs: 传递给TushareFetcher构造函数的参数
            
        Returns:
            TushareFetcher: 单例实例
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    logger.info("创建TushareFetcher单例实例")
                    cls._instance = TushareFetcher(**kwargs)
                    cls._initialized = True
                    logger.info(f"TushareFetcher单例实例创建完成，实例ID: {id(cls._instance)}")
                else:
                    logger.debug(f"返回已存在的TushareFetcher单例实例，实例ID: {id(cls._instance)}")
        else:
            logger.debug(f"返回已存在的TushareFetcher单例实例，实例ID: {id(cls._instance)}")
        
        return cls._instance
    
    @classmethod
    def reset(cls) -> None:
        """
        重置单例实例（主要用于测试）
        """
        with cls._lock:
            if cls._instance is not None:
                logger.info(f"重置TushareFetcher单例实例，原实例ID: {id(cls._instance)}")
            cls._instance = None
            cls._initialized = False
    
    @classmethod
    def is_initialized(cls) -> bool:
        """
        检查单例是否已初始化
        
        Returns:
            bool: 是否已初始化
        """
        return cls._initialized
    
    @classmethod
    def get_instance_id(cls) -> Optional[int]:
        """
        获取当前实例的ID（用于调试）
        
        Returns:
            Optional[int]: 实例ID，如果未初始化则返回None
        """
        return id(cls._instance) if cls._instance else None


class TushareFetcherFactory:
    """
    TushareFetcher工厂类
    
    提供统一的fetcher创建接口，支持单例和非单例模式。
    """
    
    @staticmethod
    def create_fetcher(use_singleton: bool = True, **kwargs) -> TushareFetcher:
        """
        创建TushareFetcher实例
        
        Args:
            use_singleton: 是否使用单例模式，默认True
            **kwargs: 传递给TushareFetcher构造函数的参数
            
        Returns:
            TushareFetcher: fetcher实例
        """
        if use_singleton:
            logger.debug("使用单例模式创建TushareFetcher")
            return TushareFetcherSingleton.get_instance(**kwargs)
        else:
            logger.debug("创建新的TushareFetcher实例")
            return TushareFetcher(**kwargs)
    
    @staticmethod
    def get_singleton() -> Optional[TushareFetcher]:
        """
        获取单例实例（如果存在）
        
        Returns:
            Optional[TushareFetcher]: 单例实例，如果不存在则返回None
        """
        return TushareFetcherSingleton._instance
    
    @staticmethod
    def reset_singleton() -> None:
        """
        重置单例实例
        """
        TushareFetcherSingleton.reset()
    
    @staticmethod
    def is_singleton_initialized() -> bool:
        """
        检查单例是否已初始化
        
        Returns:
            bool: 是否已初始化
        """
        return TushareFetcherSingleton.is_initialized()


# 便捷函数
def get_fetcher(use_singleton: bool = True, **kwargs) -> TushareFetcher:
    """
    便捷函数：获取TushareFetcher实例
    
    Args:
        use_singleton: 是否使用单例模式，默认True
        **kwargs: 传递给TushareFetcher构造函数的参数
        
    Returns:
        TushareFetcher: fetcher实例
    """
    return TushareFetcherFactory.create_fetcher(use_singleton=use_singleton, **kwargs)


def reset_fetcher_singleton() -> None:
    """
    便捷函数：重置fetcher单例
    """
    TushareFetcherFactory.reset_singleton()


def get_fetcher_instance_info() -> dict:
    """
    获取fetcher实例信息（用于调试）
    
    Returns:
        dict: 包含实例信息的字典
    """
    return {
        "is_singleton_initialized": TushareFetcherSingleton.is_initialized(),
        "singleton_instance_id": TushareFetcherSingleton.get_instance_id(),
        "singleton_exists": TushareFetcherSingleton._instance is not None
    }