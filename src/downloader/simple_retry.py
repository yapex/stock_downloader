"""简化的重试机制

基于KISS原则，只处理两种核心场景：
1. RateLimitException: 使用period_remaining等待
2. 其他错误: 按配置重试次数处理
"""

import time
import logging
from functools import wraps
from typing import Callable, Any, Optional

try:
    from ratelimit import RateLimitException as _RateLimitException
    RateLimitException = _RateLimitException
except ImportError:
    class RateLimitException(Exception):
        def __init__(self, message, period_remaining=60.0):
            super().__init__(message)
            self.period_remaining = period_remaining

logger = logging.getLogger(__name__)

# 不应该重试的错误模式
NON_RETRYABLE_PATTERNS = [
    'invalid parameter',
    '参数无效', 
    '参数错误',
    '无法识别',
    'authentication failed',
    'permission denied',
    'unauthorized',
    '401',
    '403'
]

def simple_retry(max_retries: int = 3, task_name: str = "Unknown Task"):
    """简化的重试装饰器
    
    Args:
        max_retries: 最大重试次数
        task_name: 任务名称，用于日志
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_error = None
            # 提取实体ID用于日志
            entity_id = 'unknown'
            if 'ts_code' in kwargs:
                entity_id = kwargs['ts_code']
            elif args:
                entity_id = args[0]
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                    
                except RateLimitException as rate_error:
                    last_error = rate_error
                    
                    if attempt < max_retries:
                        # 使用API返回的等待时间
                        period = getattr(rate_error, 'period_remaining', 60.0)
                        logger.warning(
                            f"[限流等待] {task_name} - 实体: {entity_id}, 等待 {period} 秒"
                        )
                        time.sleep(period)
                        continue
                    else:
                        logger.error(
                            f"[限流重试] {task_name} 超过最大重试次数 - 实体: {entity_id}"
                        )
                        break
                        
                except Exception as error:
                    last_error = error
                    error_str = str(error).lower()
                    
                    # 检查是否为不可重试的错误
                    if any(pattern in error_str for pattern in NON_RETRYABLE_PATTERNS):
                        logger.warning(
                            f"[不可重试] {task_name} 参数错误，不重试 - 实体: {entity_id}, 错误: {error}"
                        )
                        raise error
                    
                    if attempt < max_retries:
                        logger.warning(
                            f"[重试] {task_name} 第{attempt + 1}次失败，准备重试 - 实体: {entity_id}, 错误: {error}"
                        )
                        # 简单的固定延迟
                        time.sleep(1.0)
                        continue
                    else:
                        logger.error(
                            f"[重试失败] {task_name} 超过最大重试次数 - 实体: {entity_id}, 最后错误: {error}"
                        )
                        break
            
            # 所有重试都失败了，返回None表示失败但可以继续
            return None
            
        return wrapper
    return decorator