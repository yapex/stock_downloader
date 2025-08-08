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


@dataclass
class DeadLetterRecord:
    """死信记录
    
    记录执行失败的任务信息，格式友好，便于后续重新执行
    """
    task_id: str
    symbol: str
    task_type: str
    params: Dict[str, Any]
    priority: int
    retry_count: int
    max_retries: int
    error_message: str
    error_type: str
    failed_at: datetime
    original_created_at: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
        return {
            'task_id': self.task_id,
            'symbol': self.symbol,
            'task_type': self.task_type,
            'params': self.params,
            'priority': self.priority,
            'retry_count': self.retry_count,
            'max_retries': self.max_retries,
            'error_message': self.error_message,
            'error_type': self.error_type,
            'failed_at': self.failed_at.isoformat(),
            'original_created_at': self.original_created_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DeadLetterRecord':
        """从字典反序列化"""
        data = data.copy()
        data['failed_at'] = datetime.fromisoformat(data['failed_at'])
        data['original_created_at'] = datetime.fromisoformat(data['original_created_at'])
        return cls(**data)
    
    @classmethod
    def from_task(cls, task: DownloadTask, error: Exception) -> 'DeadLetterRecord':
        """从任务和错误创建死信记录"""
        return cls(
            task_id=task.task_id,
            symbol=task.symbol,
            task_type=task.task_type.value,
            params=task.params,
            priority=task.priority.value,
            retry_count=task.retry_count,
            max_retries=task.max_retries,
            error_message=str(error),
            error_type=type(error).__name__,
            failed_at=datetime.now(),
            original_created_at=task.created_at
        )
    
    def to_task(self) -> DownloadTask:
        """转换回任务对象"""
        return DownloadTask(
            symbol=self.symbol,
            task_type=TaskType(self.task_type),
            params=self.params,
            priority=Priority(self.priority),
            retry_count=0,  # 重新开始时重置重试计数
            max_retries=self.max_retries,
            task_id=self.task_id,
            created_at=self.original_created_at
        )


class DeadLetterLogger:
    """死信日志管理器
    
    负责写入和读取死信日志，格式友好，支持后续重新执行
    """
    
    def __init__(self, log_path: str = "logs/dead_letter.jsonl"):
        """
        初始化死信日志管理器
        
        Args:
            log_path: 死信日志文件路径
        """
        self.log_path = Path(log_path)
        self.logger = logging.getLogger(f"{__name__}.DeadLetterLogger")
        
        # 确保日志目录存在
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
    
    def write_dead_letter(self, task: DownloadTask, error: Exception) -> None:
        """写入死信记录
        
        Args:
            task: 失败的任务
            error: 异常信息
        """
        try:
            record = DeadLetterRecord.from_task(task, error)
            
            # 以JSON Lines格式写入文件
            with open(self.log_path, 'a', encoding='utf-8') as f:
                json.dump(record.to_dict(), f, ensure_ascii=False)
                f.write('\n')
            
            self.logger.info(f"Dead letter written: {task.symbol} ({task.task_type.value}) - {error}")
            
        except Exception as e:
            self.logger.error(f"Failed to write dead letter: {e}")
    
    def read_dead_letters(self, 
                         limit: Optional[int] = None,
                         task_type: Optional[str] = None,
                         symbol_pattern: Optional[str] = None) -> List[DeadLetterRecord]:
        """读取死信记录
        
        Args:
            limit: 最大返回记录数
            task_type: 过滤任务类型
            symbol_pattern: 过滤股票代码模式
            
        Returns:
            死信记录列表
        """
        if not self.log_path.exists():
            return []
        
        records = []
        count = 0
        
        try:
            with open(self.log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if limit and count >= limit:
                        break
                    
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line)
                        record = DeadLetterRecord.from_dict(data)
                        
                        # 应用过滤条件
                        if task_type and record.task_type != task_type:
                            continue
                        
                        if symbol_pattern and symbol_pattern not in record.symbol:
                            continue
                        
                        records.append(record)
                        count += 1
                        
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Invalid JSON line in dead letter log: {line[:100]}...")
                        continue
                    
        except Exception as e:
            self.logger.error(f"Failed to read dead letters: {e}")
        
        return records
    
    def convert_to_tasks(self, records: List[DeadLetterRecord]) -> List[DownloadTask]:
        """将死信记录转换为任务列表
        
        Args:
            records: 死信记录列表
            
        Returns:
            任务列表
        """
        return [record.to_task() for record in records]
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取死信统计信息
        
        Returns:
            统计信息字典
        """
        records = self.read_dead_letters()
        
        if not records:
            return {
                'total_count': 0,
                'by_task_type': {},
                'by_error_type': {},
                'recent_failures': []
            }
        
        # 按任务类型统计
        by_task_type = {}
        for record in records:
            by_task_type[record.task_type] = by_task_type.get(record.task_type, 0) + 1
        
        # 按错误类型统计
        by_error_type = {}
        for record in records:
            by_error_type[record.error_type] = by_error_type.get(record.error_type, 0) + 1
        
        # 最近的失败记录
        recent_failures = sorted(records, key=lambda x: x.failed_at, reverse=True)[:10]
        
        return {
            'total_count': len(records),
            'by_task_type': by_task_type,
            'by_error_type': by_error_type,
            'recent_failures': [
                {
                    'symbol': r.symbol,
                    'task_type': r.task_type,
                    'error_type': r.error_type,
                    'error_message': r.error_message[:100] + '...' if len(r.error_message) > 100 else r.error_message,
                    'failed_at': r.failed_at.isoformat()
                }
                for r in recent_failures
            ]
        }
    
    def log_missing_symbols(self, btype: str, symbols: list[str]) -> None:
        """批量写入缺失股票的死信记录
        
        Args:
            btype: 任务类型字符串
            symbols: 缺失的股票代码列表
        """
        if not symbols:
            return
        
        try:
            # 验证任务类型是否有效
            task_type = TaskType(btype)
            
            # 批量写入记录
            with open(self.log_path, 'a', encoding='utf-8') as f:
                for symbol in symbols:
                    # 构造DownloadTask
                    task = DownloadTask(
                        symbol=symbol,
                        task_type=task_type,
                        params={},
                        priority=Priority.NORMAL,
                        retry_count=0,
                        max_retries=0  # 缺失数据不需要重试
                    )
                    
                    # 创建死信记录，error_type统一为"MISSING_DATA"
                    record = DeadLetterRecord(
                        task_id=task.task_id,
                        symbol=task.symbol,
                        task_type=task.task_type.value,
                        params=task.params,
                        priority=task.priority.value,
                        retry_count=task.retry_count,
                        max_retries=task.max_retries,
                        error_message=f"Symbol {symbol} not found in data source",
                        error_type="MISSING_DATA",
                        failed_at=datetime.now(),
                        original_created_at=task.created_at
                    )
                    
                    # 写入JSON Lines格式
                    json.dump(record.to_dict(), f, ensure_ascii=False)
                    f.write('\n')
            
            self.logger.info(f"Logged {len(symbols)} missing symbols for task type {btype}")
            
        except ValueError as e:
            self.logger.error(f"Invalid task type '{btype}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to log missing symbols: {e}")
            raise
    
    def archive_processed(self, processed_task_ids: List[str]) -> None:
        """归档已处理的死信记录
        
        Args:
            processed_task_ids: 已处理的任务ID列表
        """
        if not self.log_path.exists():
            return
        
        try:
            # 读取所有记录
            remaining_records = []
            
            with open(self.log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line)
                        if data.get('task_id') not in processed_task_ids:
                            remaining_records.append(line)
                    except json.JSONDecodeError:
                        remaining_records.append(line)  # 保留无法解析的行
            
            # 重写文件
            with open(self.log_path, 'w', encoding='utf-8') as f:
                for line in remaining_records:
                    f.write(line + '\n')
            
            self.logger.info(f"Archived {len(processed_task_ids)} processed dead letter records")
            
        except Exception as e:
            self.logger.error(f"Failed to archive processed dead letters: {e}")


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
