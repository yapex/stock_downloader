#!/usr/bin/env python3
"""
TushareFetcher工厂实现

解决问题：
1. 确保整个应用只有一个TushareFetcher实例
2. 避免多个实例导致的ratelimit计数器分散
3. 提供统一的fetcher创建入口
"""

import os
import threading
import logging
from typing import Optional
from .fetcher import TushareFetcher

logger = logging.getLogger(__name__)


class TushareFetcherFactory:
    """
    TushareFetcher工厂类
    
    提供统一的fetcher创建接口，使用单例模式确保全局唯一实例。
    """
    
    _instance: Optional[TushareFetcher] = None
    _lock = threading.Lock()
    _initialized = False
    
    @staticmethod
    def get_instance() -> TushareFetcher:
        """
        获取TushareFetcher单例实例
        
        从环境变量读取TUSHARE_TOKEN并创建实例
            
        Returns:
            TushareFetcher: 单例实例
            
        Raises:
            ValueError: 当未设置TUSHARE_TOKEN环境变量时
        """
        if TushareFetcherFactory._instance is None:
            with TushareFetcherFactory._lock:
                if TushareFetcherFactory._instance is None:
                    token = os.getenv("TUSHARE_TOKEN")
                    if not token:
                        raise ValueError("错误：未设置 TUSHARE_TOKEN 环境变量")
                    
                    logger.info("创建TushareFetcher单例实例")
                    TushareFetcherFactory._instance = TushareFetcher(token)
                    TushareFetcherFactory._initialized = True
                    logger.info(f"TushareFetcher单例实例创建完成，实例ID: {id(TushareFetcherFactory._instance)}")
                else:
                    logger.debug(f"返回已存在的TushareFetcher单例实例，实例ID: {id(TushareFetcherFactory._instance)}")
        else:
            logger.debug(f"返回已存在的TushareFetcher单例实例，实例ID: {id(TushareFetcherFactory._instance)}")
        
        return TushareFetcherFactory._instance
    
    @staticmethod
    def reset() -> None:
        """
        重置单例实例（主要用于测试）
        """
        with TushareFetcherFactory._lock:
            if TushareFetcherFactory._instance is not None:
                logger.info(f"重置TushareFetcher单例实例，原实例ID: {id(TushareFetcherFactory._instance)}")
            TushareFetcherFactory._instance = None
            TushareFetcherFactory._initialized = False
    
    @staticmethod
    def is_initialized() -> bool:
        """
        检查单例是否已初始化
        
        Returns:
            bool: 是否已初始化
        """
        return TushareFetcherFactory._initialized
    
    @staticmethod
    def get_instance_id() -> Optional[int]:
        """
        获取当前实例的ID（用于调试）
        
        Returns:
            Optional[int]: 实例ID，如果未初始化则返回None
        """
        return id(TushareFetcherFactory._instance) if TushareFetcherFactory._instance else None


# 对外接口
def get_singleton() -> TushareFetcher:
    """
    获取TushareFetcher单例实例
        
    Returns:
        TushareFetcher: 单例实例
    """
    return TushareFetcherFactory.get_instance()


# 向后兼容接口
def get_fetcher(use_singleton: bool = True) -> TushareFetcher:
    """
    向后兼容接口：获取TushareFetcher实例
    
    Args:
        use_singleton: 是否使用单例模式，默认True（非单例模式已废弃）
        
    Returns:
        TushareFetcher: fetcher实例
    """
    if not use_singleton:
        logger.warning("非单例模式已废弃，将使用单例模式")
    return get_singleton()


# 内部使用的调试和测试接口
def _reset_singleton() -> None:
    """
    重置fetcher单例（仅供测试使用）
    """
    TushareFetcherFactory.reset()


def _get_instance_info() -> dict:
    """
    获取fetcher实例信息（仅供调试使用）
    
    Returns:
        dict: 包含实例信息的字典
    """
    return {
        "is_singleton_initialized": TushareFetcherFactory.is_initialized(),
        "singleton_instance_id": TushareFetcherFactory.get_instance_id(),
        "singleton_exists": TushareFetcherFactory._instance is not None
    }