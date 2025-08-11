#!/usr/bin/env python3
"""
基于 pyrate-limiter 原生装饰器的速率限制器实现
使用 limiter.as_decorator() 方法，避免重复造轮子
"""

from functools import wraps
from typing import Any, Callable

from pyrate_limiter import Duration, Rate, Limiter, BucketFullException, InMemoryBucket

# 需要导入 downloader 模块的 get_logger
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent / 'downloader'))
from utils import get_logger

# 配置日志
logger = get_logger(__name__)

# Tushare API 限制：每个方法每分钟最多 190 次调用
TUSHARE_RATE_LIMIT = Rate(190, Duration.MINUTE)

# 全局单例 Limiter 实例
# 使用阻塞模式：当超限时等待而不是抛出异常
_limiter = Limiter(
    InMemoryBucket([TUSHARE_RATE_LIMIT]),
    raise_when_fail=False,  # 不抛出异常，而是等待
    max_delay=Duration.MINUTE * 2  # 最大等待时间 2 分钟
)

# 获取原生装饰器
_native_decorator = _limiter.as_decorator()


def rate_limit(method_name: str) -> Callable:
    """
    基于 pyrate-limiter 原生装饰器的速率限制装饰器
    
    Args:
        method_name: 方法名称，用作速率限制的标识符
        
    Returns:
        装饰器函数
    """
    
    def mapping(*args, **kwargs):
        """映射函数：将被装饰函数的参数映射到 limiter.try_acquire 的参数"""
        # 返回 (identity, weight) 元组
        # identity 使用方法名作为独立的速率限制标识符
        # weight 设为 1，表示每次调用消耗 1 个配额
        return method_name, 1
    
    def decorator(func: Callable) -> Callable:
        # 使用原生装饰器
        rate_limited_func = _native_decorator(mapping)(func)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.debug(f"Rate limiting check for method: {method_name}")
            try:
                return rate_limited_func(*args, **kwargs)
            except BucketFullException as e:
                # 理论上不应该到达这里，因为我们设置了 raise_when_fail=False
                logger.warning(f"Rate limit exceeded for {method_name}: {e}")
                raise
        
        return wrapper
    
    return decorator


def get_limiter() -> Limiter:
    """
    获取全局 Limiter 实例
    
    Returns:
        Limiter 实例
    """
    return _limiter


def reset_limiter() -> None:
    """
    重置限制器状态（主要用于测试）
    """
    global _limiter
    _limiter = Limiter(
        InMemoryBucket([TUSHARE_RATE_LIMIT]),
        raise_when_fail=False,
        max_delay=Duration.MINUTE * 2
    )
    global _native_decorator
    _native_decorator = _limiter.as_decorator()
    logger.info("Rate limiter has been reset")