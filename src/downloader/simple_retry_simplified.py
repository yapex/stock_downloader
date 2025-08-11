"""简化的重试机制

基于KISS原则，专注于处理网络错误和API错误的重试。
速率限制已由 pyrate-limiter 内置阻塞机制处理，无需在此重复处理。
"""

import time
from functools import wraps
from typing import Callable, Any
from .utils import get_logger

logger = get_logger(__name__)

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
    
    专注于处理网络错误和临时性API错误的重试。
    速率限制由 pyrate-limiter 的内置阻塞机制处理。
    
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