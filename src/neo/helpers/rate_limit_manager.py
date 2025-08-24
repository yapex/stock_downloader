"""速率限制管理器实现

负责管理pyrate-limiter实例的生命周期。"""

import logging
import threading
from typing import Dict, Optional
from pyrate_limiter import Limiter, InMemoryBucket, Rate, Duration

from neo.configs.app_config import get_config
from neo.task_bus.types import TaskType, TaskTemplateRegistry
from .interfaces import IRateLimitManager

logger = logging.getLogger(__name__)


class RateLimitManager(IRateLimitManager):
    """速率限制管理器实现

    负责管理所有速率限制器的生命周期，包括创建、缓存和清理。
    可以通过 singleton() 类方法获取单例实例，也可以直接实例化创建多个实例。
    """

    _singleton_instance: Optional["RateLimitManager"] = None
    _singleton_lock = threading.Lock()

    @classmethod
    def singleton(cls) -> "RateLimitManager":
        """获取单例实例

        Returns:
            RateLimitManager: 单例实例
        """
        if cls._singleton_instance is None:
            with cls._singleton_lock:
                if cls._singleton_instance is None:
                    cls._singleton_instance = cls()
        return cls._singleton_instance

    def __init__(self):
        """初始化速率限制管理器"""
        self.config = get_config()
        self.rate_limiters: Dict[str, Limiter] = {}  # 按需创建的速率限制器缓存

    def get_limiter(self, task_type: TaskType) -> Limiter:
        """获取指定任务类型的速率限制器

        Args:
            task_type: 任务类型枚举

        Returns:
            Limiter: 对应的速率限制器实例
        """
        task_key = str(task_type)  # 转换为字符串以确保可哈希
        if task_key not in self.rate_limiters:
            # 从配置文件中读取该任务类型的速率限制
            rate_limit = self.get_rate_limit_config(task_type)

            # 为每个任务类型创建独立的速率限制器
            self.rate_limiters[task_key] = Limiter(
                InMemoryBucket([Rate(rate_limit, Duration.MINUTE)]),
                raise_when_fail=False,
                max_delay=Duration.MINUTE * 2,
            )
            logger.debug(
                f"Created rate limiter for task {task_key} with {rate_limit} requests/minute"
            )

        return self.rate_limiters[task_key]

    def apply_rate_limiting(self, task_type: TaskType) -> None:
        """对指定任务类型应用速率限制

        Args:
            task_type: 任务类型枚举
        """
        logger.debug(f"Rate limiting check for task: {task_type}")
        rate_limiter = self.get_limiter(task_type)
        rate_limiter.try_acquire(str(task_type), 1)

    def get_rate_limit_config(self, task_type: TaskType) -> int:
        """获取指定任务类型的速率限制配置

        Args:
            task_type: 任务类型枚举

        Returns:
            int: 每分钟允许的请求数
        """
        try:
            # 从配置中获取任务类型的速率限制，使用 api_method 作为键
            template = TaskTemplateRegistry.get_template(task_type)
            api_method = template.api_method
            task_config = self.config.download_tasks.get(api_method, {})
            rate_limit = task_config.get("rate_limit_per_minute", 190)  # 默认值190
            logger.debug(f"Task {api_method} rate limit: {rate_limit} requests/minute")
            return rate_limit
        except Exception as e:
            logger.warning(
                f"Failed to get rate limit for task {task_type}, using default 190: {e}"
            )
            return 190

    def cleanup(self) -> None:
        """清理所有速率限制器资源

        释放所有创建的限制器和相关资源。
        """
        logger.debug(f"Cleaning up {len(self.rate_limiters)} rate limiters")
        for task_key, limiter in self.rate_limiters.items():
            try:
                # 尝试关闭限制器（如果有close方法）
                if hasattr(limiter, "close"):
                    limiter.close()
                logger.debug(f"Cleaned up rate limiter for task {task_key}")
            except Exception as e:
                logger.warning(
                    f"Error cleaning up rate limiter for task {task_key}: {e}"
                )

        # 清空缓存
        self.rate_limiters.clear()
        logger.debug("Rate limiter cleanup completed")

    def __del__(self):
        """析构函数，确保资源被清理"""
        try:
            self.cleanup()
        except Exception as e:
            logger.warning(f"Error in RateLimitManager destructor: {e}")


def get_rate_limit_manager() -> RateLimitManager:
    """获取全局速率限制管理器实例

    Returns:
        RateLimitManager: 全局唯一的速率限制管理器实例
    """
    return RateLimitManager.singleton()
