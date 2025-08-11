"""基于 pyrate-limiter 的速率限制器

使用简单的异常捕获-阻塞等待策略来实现速率限制。
"""

import time
import logging
from functools import wraps
from typing import Callable, Any

try:
    from pyrate_limiter import Limiter, Rate, Duration, BucketFullException
    from pyrate_limiter.clocks import TimeClock
except ImportError:
    raise ImportError("请安装 pyrate-limiter: uv add pyrate-limiter")

logger = logging.getLogger(__name__)

# 全局速率限制器实例
_global_limiter = None

def get_global_limiter() -> Limiter:
    """获取全局速率限制器实例
    
    Returns:
        Limiter: 全局速率限制器实例
    """
    global _global_limiter
    if _global_limiter is None:
        # 创建速率限制规则：190次/60秒
        rates = [Rate(190, 60 * Duration.SECOND)]
        _global_limiter = Limiter(rates, clock=TimeClock())
    return _global_limiter

def rate_limit(method_name: str):
    """速率限制装饰器
    
    使用简单的异常捕获-阻塞等待策略。当速率超限时，会阻塞当前线程直到可以继续执行。
    每个方法有独立的速率限制计数器，确保每个方法每分钟不超过190次调用。
    
    Args:
        method_name: 方法名称，用于创建独立的速率限制标识符，必须提供且在全局唯一
    """
    if not method_name:
        raise ValueError("method_name 参数不能为空，每个方法必须有唯一的标识符")
        
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            limiter = get_global_limiter()
            # 使用方法名作为唯一标识符，确保每个方法有独立的速率限制
            method_identifier = f"tushare_method_{method_name}"
            
            # 实现阻塞等待策略
            while True:
                try:
                    # 每个方法使用自己的标识符
                    limiter.try_acquire(method_identifier)
                    break  # 成功获取，跳出循环
                except BucketFullException:
                    # 速率超限，等待一小段时间后重试
                    logger.debug(f"速率限制触发，等待重试: {method_name}")
                    time.sleep(0.1)  # 等待100ms后重试
            
            # 成功获取令牌后执行原函数
            return func(*args, **kwargs)
            
        return wrapper
    return decorator

def reset_global_limiter():
    """重置全局速率限制器（主要用于测试）"""
    global _global_limiter
    _global_limiter = None