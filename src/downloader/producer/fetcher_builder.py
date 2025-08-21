"""数据获取器构建器模块

基于函数构建器模式，提供灵活的数据获取器构建功能。
支持配置驱动的任务模板和运行时参数覆盖。
现在支持从 stock_schema.toml 动态生成任务类型。
"""

import tushare as ts
from typing import Callable, Dict, Any, Optional
import pandas as pd
import logging
from dataclasses import dataclass
from threading import Lock
from enum import Enum
from functools import lru_cache
import pysnooper
from pathlib import Path

from downloader.config import get_config
from downloader.utils import normalize_stock_code
from downloader.database.schema_loader import SchemaLoader, ISchemaLoader

logger = logging.getLogger(__name__)

# 创建性能分析日志目录
PERF_LOG_DIR = Path("logs/performance")
PERF_LOG_DIR.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class TaskTemplate:
    """任务模板配置"""

    api_method: str
    base_object: str = "pro"
    default_params: Dict[str, Any] = None

    def __post_init__(self):
        if self.default_params is None:
            object.__setattr__(self, "default_params", {})


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

    @lru_cache(maxsize=1)
    # @pysnooper.snoop(
    #     output=PERF_LOG_DIR / "load_task_types.log", watch=("task_types", "schemas")
    # )
    def _load_task_types(self) -> Dict[str, TaskTemplate]:
        """从 schema 加载任务类型"""
        task_types = {}
        schemas = self.schema_loader.get_all_table_schemas()

        for table_name, schema in schemas.items():
            # 确定 base_object
            base_object = "ts" if schema.api_method == "pro_bar" else "pro"

            # 创建任务模板
            template = TaskTemplate(
                api_method=schema.api_method,
                base_object=base_object,
                default_params=schema.default_params.copy(),
            )

            # 直接使用表名的大写形式作为枚举名
            enum_name = table_name.upper()
            task_types[enum_name] = template

        return task_types

    def get_task_types(self) -> Dict[str, TaskTemplate]:
        """获取所有任务类型"""
        return self._load_task_types()

    @lru_cache(maxsize=1)
    # @pysnooper.snoop(output=PERF_LOG_DIR / "enum_generation.log", watch=('task_types', 'enum_dict'))
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
        self._load_task_types.cache_clear()
        self.get_task_type_enum.cache_clear()
        self.schema_loader.reload()


# 创建全局任务类型注册表实例
_task_registry = TaskTypeRegistry.get_instance()

# 动态生成 TaskType 枚举
TaskType = _task_registry.get_task_type_enum()


@dataclass(frozen=True)
class TaskTemplate:
    """任务模板配置"""

    api_method: str
    base_object: str = "pro"
    default_params: Dict[str, Any] = None

    def __post_init__(self):
        if self.default_params is None:
            object.__setattr__(self, "default_params", {})


class TushareApiManager:
    """Tushare API 管理器（单例模式）"""

    _instance: Optional["TushareApiManager"] = None
    _lock = Lock()

    # @pysnooper.snoop(
    #     output=PERF_LOG_DIR / "api_manager_init.log",
    #     watch=("config", "self.pro", "self.ts"),
    # )
    def __init__(self):
        if TushareApiManager._instance is not None:
            raise RuntimeError("TushareApiManager 是单例类，请使用 get_instance() 方法")

        config = get_config()
        logger.info("初始化 Tushare API...")
        ts.set_token(config.tushare.token)
        self.pro = ts.pro_api()
        self.ts = ts
        self.api_objects = {"pro": self.pro, "ts": self.ts}

    @classmethod
    def get_instance(cls) -> "TushareApiManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_api_function(self, base_object: str, method_name: str):
        """获取 API 函数"""
        if base_object not in self.api_objects:
            raise ValueError(f"不支持的 API 对象: {base_object}")

        api_obj = self.api_objects[base_object]
        if not hasattr(api_obj, method_name):
            raise ValueError(f"API 对象 {base_object} 没有方法 {method_name}")

        return getattr(api_obj, method_name)


class FetcherBuilder:
    """数据获取器构建器"""

    def __init__(self):
        self.api_manager = TushareApiManager.get_instance()

    # @pysnooper.snoop(
    #     output=PERF_LOG_DIR / "build_by_task.log",
    #     watch=("task_type", "template", "merged_params", "api_func"),
    # )
    def build_by_task(
        self, task_type: TaskType, symbol: str = "", **overrides: Any
    ) -> Callable[[], pd.DataFrame]:
        """构建指定股票代码的数据获取器

        Args:
            task_type: 任务类型
            symbol: 股票代码
            **overrides: 运行时参数覆盖

        Returns:
            可执行的数据获取函数
        """
        # 检查任务类型是否有效
        if not isinstance(task_type, TaskType):
            raise ValueError(f"不支持的任务类型: {task_type}")

        # 直接从枚举获取模板
        template = task_type.template

        # 标准化股票代码
        if symbol:
            overrides["ts_code"] = normalize_stock_code(symbol)

        # 合并参数：默认参数 + 运行时参数覆盖
        merged_params = (
            template.default_params.copy() if template.default_params else {}
        )
        merged_params.update(overrides)

        # 获取 API 函数
        api_func = self.api_manager.get_api_function(
            template.base_object, template.api_method
        )

        # @pysnooper.snoop(
        #     output=PERF_LOG_DIR / "api_execution.log", watch=("merged_params", "result")
        # )
        def execute() -> pd.DataFrame:
            """执行数据获取"""
            logger.debug(f"调用 {template.api_method}，参数: {merged_params}")
            try:
                result = api_func(**merged_params)
                logger.debug(f"获取 {len(result)} 条记录")
                return result
            except Exception as e:
                logger.error(f"任务执行失败: {e}")
                raise

        return execute


if __name__ == "__main__":
    # 最精简的用法示例
    task_type = TaskType.STOCK_BASIC
    # print(task_type)
    fetcher = FetcherBuilder().build_by_task(task_type, "600519")
    df = fetcher()
    print(df.head(1))
    fetcher = FetcherBuilder().build_by_task(task_type)
    df = fetcher()
    print(df["ts_code"].tail(3))
