"""
消息队列和接口契约的具体实现
定义生产者-消费者模式的核心接口和消息类型
"""

import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, asdict
from enum import Enum


class MessageType(Enum):
    """消息类型枚举"""
    TASK = "task"
    DATA = "data" 


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
class TaskMessage:
    """任务消息数据类"""
    task_id: str
    task_type: TaskType
    params: Dict[str, Any]
    retry_count: int = 0
    priority: Priority = Priority.NORMAL
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if isinstance(self.task_type, str):
            self.task_type = TaskType(self.task_type)
        if isinstance(self.priority, int):
            self.priority = Priority(self.priority)
    
    @classmethod
    def create(cls, task_type: TaskType, params: Dict[str, Any], 
              priority: Priority = Priority.NORMAL) -> 'TaskMessage':
        """创建新的任务消息"""
        return cls(
            task_id=str(uuid.uuid4()),
            task_type=task_type,
            params=params,
            priority=priority
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
        data = asdict(self)
        data['task_type'] = self.task_type.value
        data['priority'] = self.priority.value
        data['created_at'] = self.created_at.isoformat() if self.created_at else None
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskMessage':
        """从字典反序列化"""
        if 'created_at' in data and data['created_at']:
            data['created_at'] = datetime.fromisoformat(data['created_at'])
        return cls(**data)


@dataclass  
class DataMessage:
    """数据消息数据类"""
    message_id: str
    task_id: str
    data_type: str
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
    
    @classmethod
    def create(cls, task_id: str, data_type: str, data: List[Dict[str, Any]], 
              metadata: Optional[Dict[str, Any]] = None) -> 'DataMessage':
        """创建新的数据消息"""
        return cls(
            message_id=str(uuid.uuid4()),
            task_id=task_id,
            data_type=data_type,
            data=data,
            metadata=metadata or {}
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
        data = asdict(self)
        data['created_at'] = self.created_at.isoformat() if self.created_at else None
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataMessage':
        """从字典反序列化"""
        if 'created_at' in data and data['created_at']:
            data['created_at'] = datetime.fromisoformat(data['created_at'])
        return cls(**data)
    
    @property
    def size(self) -> int:
        """数据大小（记录数）"""
        return len(self.data)


class IMessage(ABC):
    """消息抽象接口"""
    
    @property
    @abstractmethod
    def message_id(self) -> str:
        """消息唯一标识"""
        pass
    
    @property  
    @abstractmethod
    def created_at(self) -> datetime:
        """消息创建时间"""
        pass
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
        pass
    
    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IMessage':
        """从字典反序列化"""
        pass


class IProducer(ABC):
    """生产者抽象接口"""
    
    @abstractmethod
    async def start(self) -> None:
        """启动生产者"""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """停止生产者"""
        pass
    
    @abstractmethod
    async def produce(self, task: TaskMessage) -> Optional[DataMessage]:
        """处理单个任务，生产数据消息"""
        pass
    
    @property
    @abstractmethod
    def is_running(self) -> bool:
        """生产者是否正在运行"""
        pass


class IConsumer(ABC):
    """消费者抽象接口"""
    
    @abstractmethod
    async def start(self) -> None:
        """启动消费者"""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """停止消费者"""
        pass
    
    @abstractmethod
    async def consume(self, data: DataMessage) -> bool:
        """处理单个数据消息，返回是否成功"""
        pass
    
    @property
    @abstractmethod
    def is_running(self) -> bool:
        """消费者是否正在运行"""
        pass


class IRetryPolicy(ABC):
    """重试策略抽象接口"""
    
    @abstractmethod
    def should_retry(self, error: Exception, attempt: int) -> bool:
        """判断是否应该重试"""
        pass
    
    @abstractmethod
    def get_delay(self, attempt: int) -> float:
        """获取重试延迟时间(秒)"""
        pass
    
    @abstractmethod
    def get_max_attempts(self) -> int:
        """获取最大重试次数"""
        pass


class ExponentialBackoffRetryPolicy(IRetryPolicy):
    """指数退避重试策略实现"""
    
    def __init__(self, max_attempts: int = 3, base_delay: float = 1.0, 
                 max_delay: float = 60.0, backoff_factor: float = 2.0):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
    
    def should_retry(self, error: Exception, attempt: int) -> bool:
        """判断是否应该重试"""
        if attempt >= self.max_attempts:
            return False
        
        # 根据异常类型决定是否重试
        if isinstance(error, (ConnectionError, TimeoutError)):
            return True
        if hasattr(error, 'response') and hasattr(error.response, 'status_code'):
            # HTTP 5xx 错误可重试，4xx 不重试
            return 500 <= error.response.status_code < 600
        
        return False
    
    def get_delay(self, attempt: int) -> float:
        """获取重试延迟时间(秒)"""
        delay = self.base_delay * (self.backoff_factor ** attempt)
        return min(delay, self.max_delay)
    
    def get_max_attempts(self) -> int:
        """获取最大重试次数"""
        return self.max_attempts


@dataclass
class PerformanceMetrics:
    """性能监控指标"""
    tasks_total: int = 0
    tasks_completed: int = 0  
    tasks_failed: int = 0
    data_messages_total: int = 0
    data_messages_processed: int = 0
    queue_sizes: Dict[str, int] = None
    processing_rates: Dict[str, float] = None
    
    def __post_init__(self):
        if self.queue_sizes is None:
            self.queue_sizes = {}
        if self.processing_rates is None:
            self.processing_rates = {}
    
    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.tasks_total == 0:
            return 0.0
        return self.tasks_completed / self.tasks_total
    
    @property
    def failure_rate(self) -> float:
        """失败率"""
        if self.tasks_total == 0:
            return 0.0
        return self.tasks_failed / self.tasks_total
    
    def reset(self) -> None:
        """重置所有指标"""
        self.tasks_total = 0
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.data_messages_total = 0
        self.data_messages_processed = 0
        self.queue_sizes.clear()
        self.processing_rates.clear()


class IQueueManager(ABC):
    """队列管理器抽象接口"""
    
    @abstractmethod
    async def start(self) -> None:
        """启动队列管理器"""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """停止队列管理器"""
        pass
    
    @abstractmethod
    async def put_task(self, task: TaskMessage) -> None:
        """添加任务到任务队列"""
        pass
    
    @abstractmethod
    async def get_task(self, timeout: Optional[float] = None) -> Optional[TaskMessage]:
        """从任务队列获取任务"""
        pass
    
    @abstractmethod
    async def put_data(self, data: DataMessage) -> None:
        """添加数据到数据队列"""
        pass
    
    @abstractmethod
    async def get_data(self, timeout: Optional[float] = None) -> Optional[DataMessage]:
        """从数据队列获取数据"""
        pass
    
    @property
    @abstractmethod
    def task_queue_size(self) -> int:
        """任务队列大小"""
        pass
    
    @property
    @abstractmethod
    def data_queue_size(self) -> int:
        """数据队列大小"""
        pass
    
    @abstractmethod
    def get_metrics(self) -> PerformanceMetrics:
        """获取性能指标"""
        pass
