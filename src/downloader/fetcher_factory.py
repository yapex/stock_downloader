#!/usr/bin/env python3
"""
TushareFetcher工厂实现

解决问题：
1. 通过策略模式提供不同的实例获取策略
2. 避免多个实例导致的ratelimit计数器分散
3. 提供统一的fetcher创建入口
"""

import os
import threading
from typing import Optional, Callable
from .fetcher import TushareFetcher
from .interfaces.config import IConfig as ConfigInterface
from .utils import get_logger

logger = get_logger(__name__)


class TushareFetcherFactory:
    """
    TushareFetcher工厂类

    通过策略模式提供不同的实例获取策略。
    """

    def __init__(
        self,
        config: ConfigInterface,
        instance_strategy: Optional[Callable[[str], TushareFetcher]] = None,
    ):
        """
        初始化工厂

        Args:
            config: 配置接口
            instance_strategy: 获取实例的策略函数，接受token参数
        """
        self._config = config
        self._instance_strategy = instance_strategy or (
            lambda token: TushareFetcher(token)
        )

    def get_instance(self) -> TushareFetcher:
        """
        获取TushareFetcher实例

        Returns:
            TushareFetcher: fetcher实例
        """
        token = self._config.get_runtime_token()
        return self._instance_strategy(token)


# 全局单例相关
_instance: Optional[TushareFetcher] = None
_config_instance: Optional[ConfigInterface] = None
_lock = threading.Lock()
_initialized = False

# 线程本地存储相关
_thread_local = threading.local()


class TushareFetcherTestHelper:
    """
    TushareFetcher测试辅助类

    提供测试所需的内部状态查询和重置功能。
    """

    @staticmethod
    def is_singleton_initialized() -> bool:
        """检查单例是否已初始化"""
        return _initialized

    @staticmethod
    def get_singleton_instance_id() -> Optional[int]:
        """获取单例实例ID"""
        return id(_instance) if _instance else None

    @staticmethod
    def reset_singleton() -> None:
        """重置单例实例"""
        global _instance, _initialized
        with _lock:
            _instance = None
            _initialized = False

    @staticmethod
    def has_thread_local_instance() -> bool:
        """检查当前线程是否有实例"""
        return hasattr(_thread_local, "instance")

    @staticmethod
    def get_thread_local_instance_id() -> Optional[int]:
        """获取线程本地实例ID"""
        if hasattr(_thread_local, "instance"):
            return id(_thread_local.instance)
        return None

    @staticmethod
    def reset_thread_local() -> None:
        """重置线程本地实例"""
        if hasattr(_thread_local, "instance"):
            delattr(_thread_local, "instance")


# 对外接口
def get_singleton(config: Optional[ConfigInterface] = None) -> TushareFetcher:
    """
    获取TushareFetcher单例实例

    Args:
        config: 配置接口，如果为None则尝试从环境变量获取token（向后兼容）

    Returns:
        TushareFetcher: 单例实例
    """
    global _instance, _config_instance, _initialized
    if _instance is None or (_config_instance != config and config is not None):
        with _lock:
            if _instance is None or (_config_instance != config and config is not None):
                if config is not None:
                    token = config.get_runtime_token()
                    if not token:
                        raise ValueError("错误：未配置 TUSHARE_TOKEN")
                    _config_instance = config
                    logger.info("使用配置接口创建TushareFetcher单例实例")
                else:
                    # 向后兼容：从环境变量获取
                    token = os.getenv("TUSHARE_TOKEN")
                    if not token:
                        raise ValueError("错误：未设置 TUSHARE_TOKEN 环境变量")
                    logger.info("从环境变量创建TushareFetcher单例实例")

                _instance = TushareFetcher(token)
                _initialized = True
                logger.info(f"TushareFetcher单例实例创建完成，实例ID: {id(_instance)}")

    return _instance


def get_thread_local_fetcher(
    config: Optional[ConfigInterface] = None,
) -> TushareFetcher:
    """
    获取线程本地的TushareFetcher实例

    每个线程都有自己独立的fetcher实例，但在同一线程内保持唯一。
    适用于多线程环境，避免速率限制器冲突。

    Args:
        config: 配置接口，如果为None则尝试从环境变量获取token（向后兼容）

    Returns:
        TushareFetcher: 线程本地实例
    """
    # 检查当前线程是否已有实例
    if not hasattr(_thread_local, "instance"):
        if config is not None:
            token = config.get_runtime_token()
            if not token:
                raise ValueError("错误：未配置 TUSHARE_TOKEN")
        else:
            # 向后兼容：从环境变量获取
            token = os.getenv("TUSHARE_TOKEN")
            if not token:
                raise ValueError("错误：未设置 TUSHARE_TOKEN 环境变量")

        thread_id = threading.get_ident()
        logger.info(f"为线程 {thread_id} 创建TushareFetcher实例")
        _thread_local.instance = TushareFetcher(token)
        logger.info(
            f"线程 {thread_id} 的TushareFetcher实例创建完成，实例ID: {id(_thread_local.instance)}"
        )

    return _thread_local.instance


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
    TushareFetcherTestHelper.reset_singleton()


def _get_instance_info() -> dict:
    """
    获取实例信息（内部使用）

    Returns:
        dict: 包含实例信息的字典
    """
    return {
        "singleton_instance_exists": TushareFetcherTestHelper.is_singleton_initialized(),
        "singleton_instance_id": TushareFetcherTestHelper.get_singleton_instance_id(),
        "thread_local_instance_exists": TushareFetcherTestHelper.has_thread_local_instance(),
        "thread_local_instance_id": TushareFetcherTestHelper.get_thread_local_instance_id(),
    }


def create_singleton_factory(config: ConfigInterface) -> TushareFetcherFactory:
    """
    创建单例策略的工厂

    Args:
        config: 配置接口

    Returns:
        TushareFetcherFactory: 使用单例策略的工厂实例
    """
    return TushareFetcherFactory(config, lambda token: get_singleton(config))


def create_thread_local_factory(config: ConfigInterface) -> TushareFetcherFactory:
    """
    创建线程本地策略的工厂

    Args:
        config: 配置接口

    Returns:
        TushareFetcherFactory: 使用线程本地策略的工厂实例
    """
    return TushareFetcherFactory(config, lambda token: get_thread_local_fetcher(config))
