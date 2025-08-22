"""任务总线相关类型定义

定义 producer/consumer 共享的核心数据类型和枚举。"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any
from functools import lru_cache
from threading import Lock
import pandas as pd

from neo.database.schema_loader import SchemaLoader
from neo.database.interfaces import ISchemaLoader


@dataclass(frozen=True)
class TaskTemplate:
    """任务模板配置"""

    api_method: str
    base_object: str = "pro"
    default_params: Dict[str, Any] = None
    required_params: Dict[str, Any] = None

    def __post_init__(self):
        if self.default_params is None:
            object.__setattr__(self, "default_params", {})
        if self.required_params is None:
            object.__setattr__(self, "required_params", {})


class TaskTypeRegistry:
    """任务类型注册表，动态生成任务类型"""

    _instance: Optional["TaskTypeRegistry"] = None
    _lock = Lock()

    def __init__(self, schema_loader: Optional[ISchemaLoader] = None):
        if TaskTypeRegistry._instance is not None:
            raise RuntimeError("TaskTypeRegistry 是单例类，请使用 get_instance() 方法")

        self.schema_loader = schema_loader or SchemaLoader()

    @classmethod
    def get_instance(
        cls, schema_loader: Optional[ISchemaLoader] = None
    ) -> "TaskTypeRegistry":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(schema_loader)
        return cls._instance

    def _load_task_types(self) -> Dict[str, TaskTemplate]:
        """从 schema 加载任务类型"""
        task_types = {}
        schemas = self.schema_loader.load_all_schemas()

        for table_name, schema in schemas.items():
            # 确定 base_object
            base_object = "ts" if schema.api_method == "pro_bar" else "pro"

            # 创建任务模板
            template = TaskTemplate(
                api_method=schema.api_method,
                base_object=base_object,
                default_params=schema.default_params.copy(),
                required_params=schema.required_params.copy(),
            )

            # 直接使用原始表名作为枚举名，避免大小写转换
            task_types[table_name] = template

        return task_types

    @lru_cache(maxsize=1)
    def get_task_types(self) -> Dict[str, TaskTemplate]:
        """获取所有任务类型"""
        return self._load_task_types()

    @lru_cache(maxsize=1)
    def get_task_type_enum(self) -> type:
        """获取动态生成的 TaskType 枚举类"""
        task_types = self.get_task_types()

        # 动态创建枚举类
        enum_dict = {}
        for name, template in task_types.items():
            enum_dict[name] = template

        # 添加 template 属性方法
        def template_property(self):
            return self.value

        enum_dict["template"] = property(template_property)

        # 创建枚举类
        return Enum("TaskType", enum_dict)

    def reload(self) -> None:
        """重新加载配置"""
        self.get_task_types.cache_clear()
        self.get_task_type_enum.cache_clear()
        self.schema_loader.reload_schemas()


# 创建全局任务类型注册表实例
_task_registry = TaskTypeRegistry.get_instance()

# 动态生成 TaskType 枚举
TaskType = _task_registry.get_task_type_enum()


class TaskPriority(Enum):
    """任务优先级枚举"""

    LOW = 1
    MEDIUM = 2
    HIGH = 3


@dataclass
class DownloadTaskConfig:
    """下载任务配置"""

    symbol: str
    task_type: TaskType
    priority: TaskPriority = TaskPriority.MEDIUM
    max_retries: int = 3


@dataclass
class TaskResult:
    """任务执行结果"""

    config: DownloadTaskConfig
    success: bool
    data: Optional[pd.DataFrame] = None
    error: Optional[Exception] = None
    retry_count: int = 0


__all__ = [
    "TaskResult",
    "DownloadTaskConfig",
    "TaskType",
    "TaskPriority",
    "TaskTemplate",
    "TaskTypeRegistry",
]
