import time
import functools
from collections import deque
from typing import Optional, Dict


class RateLimiter:
    """速率限制器，基于令牌桶算法的简化版本"""
    
    def __init__(self, calls_per_minute: int = 150):
        """
        初始化速率限制器
        
        Args:
            calls_per_minute: 每分钟允许的最大调用次数
        """
        self.calls_per_minute = calls_per_minute
        # 使用deque存储调用时间戳，自动维护固定大小
        self.call_times = deque(maxlen=calls_per_minute)
    
    def wait_if_needed(self):
        """
        检查是否需要等待以满足速率限制，并执行必要的等待
        """
        now = time.time()
        
        # 清除一分钟前的调用记录
        while self.call_times and now - self.call_times[0] > 60:
            self.call_times.popleft()
        
        # 如果达到限制，计算需要等待的时间
        if len(self.call_times) >= self.calls_per_minute:
            # 计算需要等待的时间（直到最早的一次调用超过1分钟）
            sleep_time = 60 - (now - self.call_times[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
                # 等待后需要重新更新当前时间
                now = time.time()
        
        # 记录当前调用时间
        self.call_times.append(now)


class DynamicRateLimiter:
    """动态速率限制器，为不同任务提供独立的速率限制"""
    
    def __init__(self):
        self.limiters: Dict[str, RateLimiter] = {}
    
    def get_limiter(self, key: str, calls_per_minute: int = 150) -> RateLimiter:
        """
        获取指定键的速率限制器，如果不存在则创建新的
        
        Args:
            key: 限制器的键（通常为任务名或函数名）
            calls_per_minute: 每分钟调用次数限制
            
        Returns:
            RateLimiter实例
        """
        if key not in self.limiters:
            self.limiters[key] = RateLimiter(calls_per_minute)
        return self.limiters[key]


# 全局动态速率限制器实例
dynamic_limiter = DynamicRateLimiter()


def rate_limit(calls_per_minute: Optional[int] = None, task_key: Optional[str] = None):
    """
    装饰器：限制函数调用频率
    
    Args:
        calls_per_minute: 每分钟调用次数限制，如果为None则不应用限制
        task_key: 任务键，用于区分不同任务的速率限制，如果为None则使用函数名
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 如果没有指定速率限制，则不应用限制
            if calls_per_minute is None:
                return func(*args, **kwargs)
            
            # 获取或创建速率限制器
            key = task_key or func.__name__
            limiter = dynamic_limiter.get_limiter(key, calls_per_minute)
            limiter.wait_if_needed()
            
            return func(*args, **kwargs)
        return wrapper
    return decorator