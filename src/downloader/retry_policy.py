"""
智能重试策略和死信处理机制

实现可配置的重试策略，支持指数退避，以及友好的死信日志格式。
"""

import json
import time
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from enum import Enum

from .models import DownloadTask, TaskType, Priority

logger = logging.getLogger(__name__)


class BackoffStrategy(Enum):
    """退避策略枚举"""
    FIXED = "fixed"           # 固定延迟
    LINEAR = "linear"         # 线性递增
    EXPONENTIAL = "exponential"  # 指数退避


@dataclass
class RetryPolicy:
    """重试策略配置
    
    Args:
        max_attempts: 最大重试次数（包括初始尝试）
        backoff: 退避策略类型
        base_delay: 基础延迟时间（秒）
        max_delay: 最大延迟时间（秒）
        backoff_factor: 退避因子（用于指数退避和线性退避）
        retryable_errors: 可重试的错误类型列表
        non_retryable_errors: 不可重试的错误类型列表
    """
    max_attempts: int = 3
    backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_factor: float = 2.0
    retryable_errors: Optional[List[str]] = None
    non_retryable_errors: Optional[List[str]] = None
    
    def __post_init__(self):
        """后处理：设置默认错误类型"""
        if self.retryable_errors is None:
            self.retryable_errors = [
                "Connection", "Timeout", "ProxyError", "RemoteDisconnected",
                "ConnectionError", "HTTPConnectionPool", "ReadTimeout",
                "ConnectTimeout", "SSLError", "ChunkedEncodingError",
                "rate limit", "quota exceeded", "too many requests"
            ]
        
        if self.non_retryable_errors is None:
            self.non_retryable_errors = [
                "Invalid parameter", "无法识别的股票代码", "参数无效",
                "Authentication failed", "Permission denied", "Unauthorized",
                "400", "401", "403", "404"  # HTTP 4xx 错误通常不应重试
            ]
        
        # 确保退避策略是枚举类型
        if isinstance(self.backoff, str):
            self.backoff = BackoffStrategy(self.backoff)
    
    def should_retry(self, error: Exception, attempt: int) -> bool:
        """判断是否应该重试
        
        Args:
            error: 异常对象
            attempt: 当前尝试次数（从1开始）
            
        Returns:
            是否应该重试
        """
        if attempt >= self.max_attempts:
            return False
        
        error_str = str(error)
        
        # 检查不可重试的错误
        for pattern in self.non_retryable_errors:
            if pattern.lower() in error_str.lower():
                return False
        
        # 检查可重试的错误
        for pattern in self.retryable_errors:
            if pattern.lower() in error_str.lower():
                return True
        
        # 默认不重试未知错误
        return False
    
    def get_delay(self, attempt: int) -> float:
        """计算延迟时间
        
        Args:
            attempt: 当前尝试次数（从1开始）
            
        Returns:
            延迟时间（秒）
        """
        if self.backoff == BackoffStrategy.FIXED:
            delay = self.base_delay
        elif self.backoff == BackoffStrategy.LINEAR:
            delay = self.base_delay * (attempt * self.backoff_factor)
        elif self.backoff == BackoffStrategy.EXPONENTIAL:
            delay = self.base_delay * (self.backoff_factor ** (attempt - 1))
        else:
            delay = self.base_delay
        
        return min(delay, self.max_delay)
    
    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
        data = asdict(self)
        data['backoff'] = self.backoff.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RetryPolicy':
        """从字典反序列化"""
        if 'backoff' in data and isinstance(data['backoff'], str):
            data['backoff'] = BackoffStrategy(data['backoff'])
        return cls(**data)


# 移除复杂的DeadLetterRecord类，简化为只记录股票代码


class RetryLogger:
    """重试日志管理器
    
    简单记录需要重试的股票代码，格式为Python数组
    """
    
    def __init__(self, log_path: str = "logs/retry_symbols.py"):
        """
        初始化重试日志管理器
        
        Args:
            log_path: 重试日志文件路径
        """
        self.log_path = Path(log_path)
        self.logger = logging.getLogger(f"{__name__}.RetryLogger")
        
        # 确保日志目录存在
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
    
    def log_failed_symbol(self, symbol: str) -> None:
        """记录失败的股票代码
        
        Args:
            symbol: 股票代码
        """
        try:
            # 读取现有的股票代码
            existing_symbols = self.read_symbols()
            
            # 添加新的股票代码（去重）
            if symbol not in existing_symbols:
                existing_symbols.append(symbol)
                existing_symbols.sort()
                
                # 写入文件
                self._write_symbols(existing_symbols)
                
                self.logger.info(f"Added symbol to retry list: {symbol}")
            
        except Exception as e:
            self.logger.error(f"Failed to log failed symbol: {e}")
    
    def log_missing_symbols(self, symbols: List[str]) -> None:
        """批量记录缺失的股票代码
        
        Args:
            symbols: 缺失的股票代码列表
        """
        if not symbols:
            return
        
        try:
            # 读取现有的股票代码
            existing_symbols = self.read_symbols()
            
            # 添加新的股票代码（去重）
            new_symbols = [s for s in symbols if s not in existing_symbols]
            if new_symbols:
                existing_symbols.extend(new_symbols)
                existing_symbols.sort()
                
                # 写入文件
                self._write_symbols(existing_symbols)
                
                self.logger.info(f"Added {len(new_symbols)} symbols to retry list")
            
        except Exception as e:
            self.logger.error(f"Failed to log missing symbols: {e}")
    
    def read_symbols(self) -> List[str]:
        """读取需要重试的股票代码列表
        
        Returns:
            股票代码列表
        """
        if not self.log_path.exists():
            return []
        
        try:
            with open(self.log_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                
            if not content:
                return []
            
            # 执行Python代码获取symbols变量
            local_vars = {}
            exec(content, {}, local_vars)
            
            return local_vars.get('symbols', [])
            
        except Exception as e:
            self.logger.error(f"Failed to read retry symbols: {e}")
            return []
    
    def _write_symbols(self, symbols: List[str]) -> None:
        """写入股票代码列表到文件
        
        Args:
            symbols: 股票代码列表
        """
        with open(self.log_path, 'w', encoding='utf-8') as f:
            f.write(f"# 需要重试的股票代码列表\n")
            f.write(f"symbols = {symbols!r}\n")
    
    def clear_symbols(self) -> None:
        """清空重试列表"""
        try:
            if self.log_path.exists():
                self.log_path.unlink()
                self.logger.info("Cleared retry symbols list")
        except Exception as e:
            self.logger.error(f"Failed to clear retry symbols: {e}")
    
    def remove_symbols(self, symbols_to_remove: List[str]) -> None:
        """从重试列表中移除指定的股票代码
        
        Args:
            symbols_to_remove: 要移除的股票代码列表
        """
        try:
            existing_symbols = self.read_symbols()
            remaining_symbols = [s for s in existing_symbols if s not in symbols_to_remove]
            
            if len(remaining_symbols) != len(existing_symbols):
                self._write_symbols(remaining_symbols)
                removed_count = len(existing_symbols) - len(remaining_symbols)
                self.logger.info(f"Removed {removed_count} symbols from retry list")
            
        except Exception as e:
            self.logger.error(f"Failed to remove symbols: {e}")


# 预定义的重试策略
DEFAULT_RETRY_POLICY = RetryPolicy(
    max_attempts=3,
    backoff=BackoffStrategy.EXPONENTIAL,
    base_delay=1.0,
    max_delay=30.0,
    backoff_factor=2.0
)

NETWORK_RETRY_POLICY = RetryPolicy(
    max_attempts=5,
    backoff=BackoffStrategy.EXPONENTIAL,
    base_delay=2.0,
    max_delay=60.0,
    backoff_factor=1.5,
    retryable_errors=[
        "Connection", "Timeout", "ProxyError", "RemoteDisconnected",
        "ConnectionError", "HTTPConnectionPool", "ReadTimeout",
        "ConnectTimeout", "SSLError"
    ]
)

API_LIMIT_RETRY_POLICY = RetryPolicy(
    max_attempts=3,
    backoff=BackoffStrategy.LINEAR,
    base_delay=10.0,
    max_delay=120.0,
    backoff_factor=2.0,
    retryable_errors=[
        "rate limit", "quota exceeded", "too many requests", "频次限制"
    ]
)

CONSERVATIVE_RETRY_POLICY = RetryPolicy(
    max_attempts=2,
    backoff=BackoffStrategy.FIXED,
    base_delay=1.0,
    max_delay=5.0,
    backoff_factor=1.0
)

# 全局重试日志实例
retry_logger = RetryLogger()
