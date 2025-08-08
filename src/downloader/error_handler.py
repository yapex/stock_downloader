"""
增强的错误处理和重试机制模块

提供智能重试策略、错误分类和恢复机制。
"""

import time
import logging
from enum import Enum
from typing import Callable, Optional, Any, Dict
from functools import wraps
from .utils import record_failed_task

logger = logging.getLogger(__name__)


class ErrorCategory(Enum):
    """错误分类枚举"""
    BUSINESS = "business"      # 正常业务错误
    TEST = "test"              # 测试相关错误  
    NETWORK = "network"        # 网络连接错误
    PARAMETER = "parameter"    # 参数错误
    SYSTEM = "system"          # 系统错误
    API_LIMIT = "api_limit"    # API限制错误
    DATA_UNAVAILABLE = "data_unavailable"  # 数据不可用


class RetryStrategy:
    """重试策略配置"""
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
        retry_on: Optional[list] = None,
        skip_on: Optional[list] = None
    ):
        """
        初始化重试策略
        
        Args:
            max_retries: 最大重试次数
            base_delay: 基础延迟时间（秒）
            max_delay: 最大延迟时间（秒）
            backoff_factor: 指数退避因子
            retry_on: 需要重试的错误类型列表
            skip_on: 跳过重试的错误类型列表
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        
        # 默认的可重试错误
        self.retry_on = retry_on or [
            "Connection",
            "Timeout", 
            "ProxyError",
            "RemoteDisconnected",
            "ConnectionError",
            "HTTPConnectionPool"
        ]
        
        # 默认跳过重试的错误
        self.skip_on = skip_on or [
            "Invalid parameter",
            "无法识别的股票代码",
            "参数无效",
            "Authentication failed",
            "Permission denied"
        ]

    def should_retry(self, error: Exception, attempt: int) -> bool:
        """判断是否应该重试"""
        if attempt >= self.max_retries:
            return False
        
        error_str = str(error)
        
        # 检查跳过重试的错误
        for skip_pattern in self.skip_on:
            if skip_pattern in error_str:
                return False
        
        # 检查可重试的错误
        for retry_pattern in self.retry_on:
            if retry_pattern in error_str:
                return True
        
        return False
    
    def get_delay(self, attempt: int) -> float:
        """计算延迟时间（指数退避）"""
        delay = self.base_delay * (self.backoff_factor ** attempt)
        return min(delay, self.max_delay)


def classify_error(error: Exception) -> ErrorCategory:
    """智能错误分类"""
    error_str = str(error).lower()
    
    # 网络相关错误
    network_keywords = [
        "connection", "proxy", "timeout", "network", "remote", 
        "httperror", "urlerror", "socket", "dns"
    ]
    if any(keyword in error_str for keyword in network_keywords):
        return ErrorCategory.NETWORK
    
    # 参数相关错误
    parameter_keywords = [
        "invalid parameter", "参数无效", "参数错误", "无法识别", 
        "format error", "validation"
    ]
    if any(keyword in error_str for keyword in parameter_keywords):
        return ErrorCategory.PARAMETER
    
    # API限制错误
    api_limit_keywords = [
        "rate limit", "quota exceeded", "too many requests", 
        "too many calls", "api limit", "频次限制"
    ]
    if any(keyword in error_str for keyword in api_limit_keywords):
        return ErrorCategory.API_LIMIT
    
    # 测试相关错误
    if "test" in error_str or "mock" in error_str:
        return ErrorCategory.TEST
    
    # 数据不可用
    if "no data" in error_str or "数据为空" in error_str or "error." == error_str:
        return ErrorCategory.DATA_UNAVAILABLE
    
    # 默认为业务错误
    return ErrorCategory.BUSINESS


def enhanced_retry(
    strategy: Optional[RetryStrategy] = None,
    task_name: str = "Unknown Task"
):
    """
    增强的重试装饰器
    
    Args:
        strategy: 重试策略，如果为None则使用默认策略
        task_name: 任务名称，用于日志记录
    """
    if strategy is None:
        strategy = RetryStrategy()
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_error = None
            
            for attempt in range(strategy.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                
                except Exception as error:
                    last_error = error
                    error_category = classify_error(error)
                    
                    # 记录错误（但不重复记录最后一次失败）
                    entity_id = kwargs.get('ts_code', args[0] if args else 'unknown')
                    
                    if attempt < strategy.max_retries and strategy.should_retry(error, attempt):
                        delay = strategy.get_delay(attempt)
                        logger.warning(
                            f"[{task_name}] 尝试 {attempt + 1} 失败: {error}. "
                            f"将在 {delay:.2f}s 后重试..."
                        )
                        time.sleep(delay)
                        continue
                    else:
                        # 最终失败，记录到失败日志
                        reason = f"retry_failed_{error_category.value}_{error}" if attempt > 0 else str(error)
                        record_failed_task(
                            task_name,
                            str(entity_id),
                            reason,
                            error_category.value
                        )
                        
                        logger.error(
                            f"[{task_name}] 最终失败 (尝试了 {attempt + 1} 次): {error}"
                        )
                        break
            
            # 如果是参数错误或不可重试的错误，抛出原始异常
            if last_error:
                error_category = classify_error(last_error)
                if error_category in [ErrorCategory.PARAMETER, ErrorCategory.SYSTEM]:
                    raise last_error
            
            # 其他情况返回 None，表示获取失败但可以继续
            return None
        
        return wrapper
    return decorator


def is_test_task(task_name: str) -> bool:
    """判断是否为测试任务"""
    test_keywords = [
        "test", "测试", "Test", "TEST", "demo", "演示", 
        "example", "样例", "mock", "临时"
    ]
    return any(keyword in task_name for keyword in test_keywords)


class ErrorAnalyzer:
    """错误分析器"""
    
    def __init__(self):
        self.error_stats: Dict[str, int] = {}
        self.category_stats: Dict[str, int] = {}
    
    def record_error(self, error: Exception, category: ErrorCategory):
        """记录错误用于分析"""
        error_key = type(error).__name__
        self.error_stats[error_key] = self.error_stats.get(error_key, 0) + 1
        self.category_stats[category.value] = self.category_stats.get(category.value, 0) + 1
    
    def get_summary(self) -> dict:
        """获取错误统计摘要"""
        return {
            "error_types": self.error_stats,
            "error_categories": self.category_stats,
            "total_errors": sum(self.error_stats.values())
        }
    
    def suggest_improvements(self) -> list:
        """基于错误统计提出改进建议"""
        suggestions = []
        
        if self.category_stats.get("network", 0) > 5:
            suggestions.append("网络错误频率较高，建议检查网络连接或代理设置")
        
        if self.category_stats.get("api_limit", 0) > 3:
            suggestions.append("API限制错误较多，建议调整请求频率或升级API权限")
        
        if self.category_stats.get("parameter", 0) > 2:
            suggestions.append("参数错误较多，建议检查股票代码格式或API参数")
        
        return suggestions


# 全局错误分析器实例
error_analyzer = ErrorAnalyzer()


# 预定义的重试策略
NETWORK_RETRY_STRATEGY = RetryStrategy(
    max_retries=5,
    base_delay=2.0,
    max_delay=30.0,
    backoff_factor=1.5
)

API_LIMIT_RETRY_STRATEGY = RetryStrategy(
    max_retries=3,
    base_delay=10.0,
    max_delay=120.0,
    backoff_factor=2.0,
    retry_on=["rate limit", "quota exceeded", "too many requests", "too many calls"]
)

CONSERVATIVE_RETRY_STRATEGY = RetryStrategy(
    max_retries=2,
    base_delay=1.0,
    max_delay=10.0,
    backoff_factor=2.0
)
