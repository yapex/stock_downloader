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
    
    提供统一的fetcher创建接口，支持全局单例和线程本地两种模式。
    """
    
    # 全局单例相关
    _instance: Optional[TushareFetcher] = None
    _lock = threading.Lock()
    _initialized = False
    
    # 线程本地存储相关
    _thread_local = threading.local()
    
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
    
    @staticmethod
    def get_thread_local_instance() -> TushareFetcher:
        """
        获取线程本地的TushareFetcher实例
        
        每个线程都有自己独立的fetcher实例，但在同一线程内保持唯一。
        这样可以避免多线程环境下的速率限制器冲突，同时保证线程内的一致性。
        
        Returns:
            TushareFetcher: 线程本地实例
            
        Raises:
            ValueError: 当未设置TUSHARE_TOKEN环境变量时
        """
        # 检查当前线程是否已有实例
        if not hasattr(TushareFetcherFactory._thread_local, 'instance'):
            token = os.getenv("TUSHARE_TOKEN")
            if not token:
                raise ValueError("错误：未设置 TUSHARE_TOKEN 环境变量")
            
            thread_id = threading.get_ident()
            logger.info(f"为线程 {thread_id} 创建TushareFetcher实例")
            TushareFetcherFactory._thread_local.instance = TushareFetcher(token)
            logger.info(f"线程 {thread_id} 的TushareFetcher实例创建完成，实例ID: {id(TushareFetcherFactory._thread_local.instance)}")
        else:
            thread_id = threading.get_ident()
            logger.debug(f"返回线程 {thread_id} 已存在的TushareFetcher实例，实例ID: {id(TushareFetcherFactory._thread_local.instance)}")
        
        return TushareFetcherFactory._thread_local.instance
    
    @staticmethod
    def reset_thread_local() -> None:
        """
        重置当前线程的fetcher实例（主要用于测试）
        """
        if hasattr(TushareFetcherFactory._thread_local, 'instance'):
            thread_id = threading.get_ident()
            logger.info(f"重置线程 {thread_id} 的TushareFetcher实例，原实例ID: {id(TushareFetcherFactory._thread_local.instance)}")
            delattr(TushareFetcherFactory._thread_local, 'instance')
    
    @staticmethod
    def has_thread_local_instance() -> bool:
        """
        检查当前线程是否已有fetcher实例
        
        Returns:
            bool: 当前线程是否已有实例
        """
        return hasattr(TushareFetcherFactory._thread_local, 'instance')
    
    @staticmethod
    def get_thread_local_instance_id() -> Optional[int]:
        """
        获取当前线程fetcher实例的ID（用于调试）
        
        Returns:
            Optional[int]: 实例ID，如果当前线程未初始化则返回None
        """
        if hasattr(TushareFetcherFactory._thread_local, 'instance'):
            return id(TushareFetcherFactory._thread_local.instance)
        return None


# 对外接口
def get_singleton() -> TushareFetcher:
    """
    获取TushareFetcher单例实例
        
    Returns:
        TushareFetcher: 单例实例
    """
    return TushareFetcherFactory.get_instance()


def get_thread_local_fetcher() -> TushareFetcher:
    """
    获取线程本地的TushareFetcher实例
    
    每个线程都有自己独立的fetcher实例，但在同一线程内保持唯一。
    适用于多线程环境，避免速率限制器冲突。
        
    Returns:
        TushareFetcher: 线程本地实例
    """
    return TushareFetcherFactory.get_thread_local_instance()


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