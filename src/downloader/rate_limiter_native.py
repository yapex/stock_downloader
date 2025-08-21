#!/usr/bin/env python3
"""
基于 pyrate-limiter 原生装饰器的速率限制器实现
使用 limiter.as_decorator() 方法，避免重复造轮子
"""

from functools import wraps
from typing import Any, Callable, Optional

from pyrate_limiter import Duration, Rate, Limiter, BucketFullException, InMemoryBucket

# 配置日志 - 使用简单的 logging 避免导入问题
import logging

logger = logging.getLogger(__name__)

# Tushare API 默认限制：每个方法每分钟最多 190 次调用
DEFAULT_TUSHARE_RATE_LIMIT = Rate(190, Duration.MINUTE)

# 全局默认 Limiter 实例
# 使用阻塞模式：当超限时等待而不是抛出异常
_default_limiter = Limiter(
    InMemoryBucket([DEFAULT_TUSHARE_RATE_LIMIT]),
    raise_when_fail=False,  # 不抛出异常，而是等待
    max_delay=Duration.MINUTE * 2,  # 最大等待时间 2 分钟
)

# 存储自定义速率的 Limiter 实例
_custom_limiters = {}


def rate_limit(method_name: str, rate: Optional[Rate] = None) -> Callable:
    """
    基于 pyrate-limiter 原生装饰器的速率限制装饰器

    Args:
        method_name: 方法名称，用作速率限制的标识符
        rate: 可选的自定义速率限制，如果不提供则使用默认的 Tushare 限制

    Returns:
        装饰器函数
    """
    # 选择使用的限制器
    if rate is None:
        # 使用默认限制器
        limiter = _default_limiter
        rate_key = "default"
    else:
        # 为自定义速率创建或获取限制器
        rate_key = f"{rate.limit}_{rate.interval}"
        if rate_key not in _custom_limiters:
            _custom_limiters[rate_key] = Limiter(
                InMemoryBucket([rate]),
                raise_when_fail=False,
                max_delay=Duration.MINUTE * 2,
            )
        limiter = _custom_limiters[rate_key]

    # 获取对应的装饰器
    native_decorator = limiter.as_decorator()

    def mapping(*args, **kwargs):
        """映射函数：将被装饰函数的参数映射到 limiter.try_acquire 的参数"""
        # 返回 (identity, weight) 元组
        # identity 使用方法名作为独立的速率限制标识符
        # weight 设为 1，表示每次调用消耗 1 个配额
        return method_name, 1

    def decorator(func: Callable) -> Callable:
        # 使用对应的原生装饰器
        rate_limited_func = native_decorator(mapping)(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.debug(
                f"Rate limiting check for method: {method_name} (rate_key: {rate_key})"
            )
            try:
                return rate_limited_func(*args, **kwargs)
            except BucketFullException as e:
                # 理论上不应该到达这里，因为我们设置了 raise_when_fail=False
                logger.warning(f"Rate limit exceeded for {method_name}: {e}")
                raise

        return wrapper

    return decorator


def get_limiter(rate: Optional[Rate] = None) -> Limiter:
    """
    获取 Limiter 实例

    Args:
        rate: 可选的自定义速率限制，如果不提供则返回默认限制器

    Returns:
        Limiter 实例
    """
    if rate is None:
        return _default_limiter

    rate_key = f"{rate.limit}_{rate.interval}"
    if rate_key not in _custom_limiters:
        _custom_limiters[rate_key] = Limiter(
            InMemoryBucket([rate]), raise_when_fail=False, max_delay=Duration.MINUTE * 2
        )
    return _custom_limiters[rate_key]


def reset_limiter() -> None:
    """
    重置限制器状态（主要用于测试）
    """
    global _default_limiter, _custom_limiters
    _default_limiter = Limiter(
        InMemoryBucket([DEFAULT_TUSHARE_RATE_LIMIT]),
        raise_when_fail=False,
        max_delay=Duration.MINUTE * 2,
    )
    _custom_limiters.clear()
    logger.info("Rate limiter has been reset")
