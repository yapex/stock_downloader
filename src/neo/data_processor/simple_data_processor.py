"""简单数据处理器实现

专注数据清洗、转换和验证的数据处理层。
"""

import logging
from typing import Optional, Dict
import pandas as pd

from ..configs import get_config
from .interfaces import IDataProcessor
from ..database.operator import DBOperator
from ..database.interfaces import ISchemaLoader
from ..database.schema_loader import SchemaLoader

logger = logging.getLogger(__name__)


class AsyncSimpleDataProcessor:
    """异步数据处理器实现

    专注于数据清洗、转换和验证的异步数据处理器。
    """

    @classmethod
    def create_default(
        cls,
        enable_batch: bool = True,
        db_operator: Optional[DBOperator] = None,
        schema_loader: Optional[ISchemaLoader] = None,
    ) -> "AsyncSimpleDataProcessor":
        """创建默认配置的异步数据处理器

        Args:
            enable_batch: 是否启用批量处理模式
            db_operator: 数据库操作器，如果为None则使用默认配置
            schema_loader: Schema 加载器，如果为None则使用默认配置

        Returns:
            配置好的异步数据处理器实例
        """
        return cls(
            db_operator=db_operator or DBOperator.create_default(),
            enable_batch=enable_batch,
            schema_loader=schema_loader or SchemaLoader(),
        )

    def __init__(
        self,
        db_operator: Optional[DBOperator] = None,
        enable_batch: bool = True,
        schema_loader: Optional[ISchemaLoader] = None,
    ):
        """初始化异步数据处理器

        Args:
            db_operator: 数据库操作器，用于保存数据
            enable_batch: 是否启用批量处理模式
            schema_loader: Schema 加载器，用于获取表名映射
        """
        self.config = get_config()
        self.db_operator = db_operator or DBOperator()
        self.enable_batch = enable_batch
        self.schema_loader = schema_loader or SchemaLoader()

        # 批量处理配置
        self.batch_size = self.config.data_processor.batch_size
        self.flush_interval_seconds = self.config.data_processor.flush_interval_seconds

        # 移除数据缓冲器相关代码

        # 注册已知的数据类型（如果有的话）
        self._register_known_types()

    def _register_known_types(self) -> None:
        """注册已知的数据类型到缓冲器

        这里可以预先注册一些已知的数据类型，避免运行时注册。
        目前为空实现，数据类型会在首次使用时动态注册。
        """
        pass

    def _get_table_name(self, task_type) -> Optional[str]:
        """根据任务类型获取对应的表名

        Args:
            task_type: 任务类型（可以是字符串或枚举）

        Returns:
            对应的表名，如果找不到返回 None
        """
        try:
            # 如果是枚举类型，使用其 name 属性
            type_name = task_type.name if hasattr(task_type, "name") else str(task_type)
            schema = self.schema_loader.load_schema(type_name)
            return schema.table_name
        except KeyError:
            type_name = task_type.name if hasattr(task_type, "name") else str(task_type)
            logger.debug(f"未找到任务类型 '{type_name}' 对应的表配置")
            return None

    async def process(self, task_type: str, data: pd.DataFrame) -> bool:
        """异步处理任务结果

        Args:
            task_type: 任务类型字符串
            data: 要处理的数据

        Returns:
            bool: 处理是否成功
        """

        try:
            # 检查数据是否存在
            if data is None or data.empty:
                logger.debug("数据为空，跳过处理")
                return False

            logger.debug(
                f"{task_type} 数据维度: {len(data)} 行 x {len(data.columns)} 列"
            )

            # 直接保存数据，不使用缓冲机制
            success = self._save_data(data, task_type)
            return success

        except Exception as e:
            logger.error(f"💥 异步处理异常: {task_type} - {str(e)}")
            return False

    def _save_data(self, data: pd.DataFrame, task_type: str) -> bool:
        """数据保存

        将数据保存到数据库。

        Args:
            data: 转换后的数据
            task_type: 任务类型

        Returns:
            保存是否成功
        """
        try:
            # 根据任务类型动态获取表名
            table_name = self._get_table_name(task_type)
            if not table_name:
                logger.debug(f"未知的任务类型: {task_type}")
                return False

            # 保存数据到数据库
            self.db_operator.upsert(table_name, data)
            logger.info(f"✅ {table_name} 成功保存了 {len(data)} 条数据")

            return True

        except Exception as e:
            logger.error(f"{table_name} 异步数据保存失败: {e}")
            return False

    async def flush_all(self, force: bool = True) -> bool:
        """刷新方法（已移除缓冲机制）

        Args:
            force: 是否强制刷新所有数据（忽略批量大小限制）

        Returns:
            bool: 始终返回True，因为不再使用缓冲机制
        """
        return True

    def get_buffer_status(self) -> Dict[str, int]:
        """获取缓冲区状态（已移除缓冲机制）

        Returns:
            空字典，因为不再使用缓冲机制
        """
        return {}

    async def shutdown(self) -> None:
        """异步关闭数据处理器，清理资源

        已移除缓冲机制，无需特殊清理操作。
        """
        pass
