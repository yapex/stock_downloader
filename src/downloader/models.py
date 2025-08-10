"""
队列消息数据模型
实现DownloadTask和DataBatch数据类，支持优先级和重试计数
"""

import uuid
import pandas as pd
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional
from enum import Enum


class TaskType(Enum):
    """任务类型枚举"""
    DAILY = "daily"
    DAILY_BASIC = "daily_basic"
    FINANCIALS = "financials"
    STOCK_LIST = "stock_list"


class Priority(Enum):
    """任务优先级枚举"""
    LOW = 1
    NORMAL = 5
    HIGH = 10


@dataclass
class DownloadTask:
    """下载任务数据类
    
    Args:
        symbol: 股票代码或其他标识符
        task_type: 任务类型
        params: 任务参数
        priority: 任务优先级，默认为NORMAL
        retry_count: 重试计数，默认为0
        max_retries: 最大重试次数，默认为3
        task_id: 任务唯一标识，自动生成
        created_at: 创建时间，自动生成
    """
    symbol: str
    task_type: TaskType
    params: Dict[str, Any]
    priority: Priority = Priority.NORMAL
    retry_count: int = 0
    max_retries: int = 3
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """后处理：确保类型正确"""
        if isinstance(self.task_type, str):
            self.task_type = TaskType(self.task_type)
        if isinstance(self.priority, int):
            # 确保优先级值在有效范围内
            if self.priority not in [p.value for p in Priority]:
                raise ValueError(f"Invalid priority value: {self.priority}")
            self.priority = Priority(self.priority)
    
    def can_retry(self) -> bool:
        """检查是否可以重试"""
        return self.retry_count < self.max_retries
    
    def increment_retry(self) -> 'DownloadTask':
        """增加重试次数并返回新实例"""
        return DownloadTask(
            symbol=self.symbol,
            task_type=self.task_type,
            params=self.params,
            priority=self.priority,
            retry_count=self.retry_count + 1,
            max_retries=self.max_retries,
            task_id=self.task_id,
            created_at=self.created_at
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
        return {
            'symbol': self.symbol,
            'task_type': self.task_type.value,
            'params': self.params,
            'priority': self.priority.value,
            'retry_count': self.retry_count,
            'max_retries': self.max_retries,
            'task_id': self.task_id,
            'created_at': self.created_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DownloadTask':
        """从字典反序列化"""
        data = data.copy()
        data['created_at'] = datetime.fromisoformat(data['created_at'])
        data['task_type'] = TaskType(data['task_type'])
        data['priority'] = Priority(data['priority'])
        return cls(**data)


@dataclass
class DataBatch:
    """数据批次数据类
    
    Args:
        df: pandas DataFrame包含的数据
        meta: 元数据信息
        batch_id: 批次唯一标识，自动生成
        task_id: 关联的任务ID
        symbol: 关联的股票代码
        created_at: 创建时间，自动生成
        size: 数据条数，自动计算
    """
    df: pd.DataFrame
    meta: Dict[str, Any]
    task_id: str
    symbol: str = ""
    batch_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """后处理：计算数据大小"""
        if self.df is None:
            self.df = pd.DataFrame()
    
    @property
    def size(self) -> int:
        """数据条数"""
        return len(self.df)
    
    @property
    def is_empty(self) -> bool:
        """是否为空数据"""
        return self.size == 0
    
    @property
    def columns(self) -> list:
        """数据列名"""
        return list(self.df.columns) if not self.df.empty else []
    
    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典（不包含DataFrame）"""
        return {
            'batch_id': self.batch_id,
            'task_id': self.task_id,
            'symbol': self.symbol,
            'meta': self.meta,
            'created_at': self.created_at.isoformat(),
            'size': self.size,
            'columns': self.columns,
            'is_empty': self.is_empty
        }
    
    def to_records(self) -> list:
        """将DataFrame转换为记录列表"""
        if self.is_empty:
            return []
        return self.df.to_dict('records')
    
    @classmethod
    def from_records(cls, records: list, meta: Dict[str, Any], 
                    task_id: str, symbol: str = "") -> 'DataBatch':
        """从记录列表创建DataBatch"""
        df = pd.DataFrame(records) if records else pd.DataFrame()
        return cls(
            df=df,
            meta=meta,
            task_id=task_id,
            symbol=symbol
        )
    
    @classmethod
    def empty(cls, task_id: str, symbol: str = "", 
             meta: Optional[Dict[str, Any]] = None) -> 'DataBatch':
        """创建空的DataBatch"""
        return cls(
            df=pd.DataFrame(),
            meta=meta or {},
            task_id=task_id,
            symbol=symbol
        )


@dataclass
class TaskResult:
    """任务结果数据类"""
    task_id: str
    success: bool
    data_batch: Optional[DataBatch] = None
    error: Optional[str] = None
    retry_count: int = 0
    processing_time: float = 0.0
    created_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
        return {
            'task_id': self.task_id,
            'success': self.success,
            'error': self.error,
            'retry_count': self.retry_count,
            'processing_time': self.processing_time,
            'created_at': self.created_at.isoformat(),
            'has_data': self.data_batch is not None,
            'data_size': self.data_batch.size if self.data_batch else 0
        }


# 为了保持向后兼容，创建别名
TaskMessage = DownloadTask
DataMessage = DataBatch
