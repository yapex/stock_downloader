"""简单数据处理器实现

专注数据清洗、转换和验证的数据处理层。
"""

import logging
from typing import Optional, Dict
import pandas as pd

from ..configs import get_config
from .interfaces import IDataProcessor, IDataBuffer
from ..database.operator import DBOperator
from ..database.interfaces import ISchemaLoader
from ..database.schema_loader import SchemaLoader

logger = logging.getLogger(__name__)



class SimpleDataProcessor(IDataProcessor):
    """简化的数据处理器实现

    专注于数据清洗、转换和验证。
    """

    @classmethod
    def create_default(
        cls,
        enable_batch: bool = True,
        db_operator: Optional[DBOperator] = None,
        schema_loader: Optional[ISchemaLoader] = None,
        data_buffer: Optional[IDataBuffer] = None,
    ) -> "SimpleDataProcessor":
        """创建默认配置的数据处理器

        Args:
            enable_batch: 是否启用批量处理模式
            db_operator: 数据库操作器，如果为None则使用默认配置
            schema_loader: Schema 加载器，如果为None则使用默认配置
            data_buffer: 数据缓冲器，如果为None则使用默认配置

        Returns:
            配置好的数据处理器实例
        """
        return cls(
            db_operator=db_operator or DBOperator.create_default(),
            enable_batch=enable_batch,
            schema_loader=schema_loader or SchemaLoader(),
            data_buffer=data_buffer,
        )

    def __init__(
        self,
        db_operator: Optional[DBOperator] = None,
        enable_batch: bool = True,
        schema_loader: Optional[ISchemaLoader] = None,
        data_buffer: Optional[IDataBuffer] = None,
    ):
        """初始化数据处理器

        Args:
            db_operator: 数据库操作器，用于保存数据
            enable_batch: 是否启用批量处理模式
            schema_loader: Schema 加载器，用于获取表名映射
            data_buffer: 数据缓冲器，用于批量处理
        """
        self.config = get_config()
        self.db_operator = db_operator or DBOperator()
        self.enable_batch = enable_batch
        self.schema_loader = schema_loader or SchemaLoader()

        # 批量处理配置
        self.batch_size = self.config.data_processor.batch_size
        self.flush_interval_seconds = self.config.data_processor.flush_interval_seconds

        # 数据缓冲器：负责缓冲区管理和异步刷新
        from .data_buffer import get_sync_data_buffer
        self.data_buffer = data_buffer or get_sync_data_buffer(self.flush_interval_seconds)
        
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
            logger.warning(f"未找到任务类型 '{type_name}' 对应的表配置")
            return None

    def process(self, task_type: str, data: pd.DataFrame) -> bool:
        """处理任务结果

        Args:
            task_type: 任务类型字符串
            data: 要处理的数据

        Returns:
            bool: 处理是否成功
        """
        logger.debug(f"处理任务: {task_type}")

        try:
            # 检查数据是否存在
            if data is None or data.empty:
                logger.warning("数据为空，跳过处理")
                return False

            logger.debug(f"数据维度: {len(data)} 行 x {len(data.columns)} 列")

            # 根据模式选择处理方式
            if self.enable_batch:
                # 批量处理模式：使用数据缓冲器
                # 确保数据类型已注册
                self.data_buffer.register_type(task_type, self._save_data_callback, self.batch_size)
                
                # 添加数据到缓冲器
                self.data_buffer.add(task_type, data)
                logger.debug(f"数据已添加到缓冲区: {task_type}, rows: {len(data)}")
                success = True
            else:
                # 单条处理模式：直接保存
                success = self._save_data(data, task_type)

            if success:
                if not self.enable_batch:
                    logger.info(f"✅ 成功保存 {len(data)} 行数据")
            else:
                logger.warning(f"数据处理失败: {task_type}")

            return success

        except Exception as e:
            print(f"💥 处理异常: {task_type} - {str(e)}")
            logger.error(f"处理数据时出错: {e}")
            return False

    def _save_data_callback(self, data_type: str, data: pd.DataFrame) -> bool:
        """数据保存回调函数
        
        供数据缓冲器调用的回调函数，用于保存合并后的数据。
        
        Args:
            data_type: 数据类型标识
            data: 要保存的合并数据
            
        Returns:
            保存是否成功
        """
        return self._save_data(data, data_type)

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
            # 调试信息：打印 task_type 的类型和值
            logger.debug(f"task_type 类型: {type(task_type)}, 值: {task_type}")

            # 根据任务类型动态获取表名
            table_name = self._get_table_name(task_type)
            if not table_name:
                logger.warning(f"未知的任务类型: {task_type}")
                return False

            # 保存数据到数据库
            self.db_operator.upsert(table_name, data)
            logger.info(f"数据保存成功: {table_name}, {len(data)} rows")

            return True

        except Exception as e:
            logger.error(f"数据保存失败: {e}")
            return False

    def flush_all(self, force: bool = True) -> bool:
        """刷新所有缓冲区数据到数据库

        Args:
            force: 是否强制刷新所有数据（忽略批量大小限制）

        Returns:
            bool: 所有刷新是否成功
        """
        try:
            self.data_buffer.flush()
            return True
        except Exception as e:
            logger.error(f"刷新缓冲区失败: {e}")
            return False

    def get_buffer_status(self) -> Dict[str, int]:
        """获取缓冲区状态
        
        Returns:
            各数据类型的缓冲区大小
        """
        if hasattr(self.data_buffer, 'get_buffer_sizes'):
            return self.data_buffer.get_buffer_sizes()
        return {}
        
    def shutdown(self) -> None:
        """关闭数据处理器，清理资源
        
        确保所有缓冲的数据都被刷新到数据库，并停止后台线程。
        """
        if hasattr(self, 'data_buffer'):
            self.data_buffer.shutdown()


class AsyncSimpleDataProcessor:
    """异步数据处理器实现

    基于AsyncCallbackQueueBuffer实现的异步数据处理器，
    专注于数据清洗、转换和验证。
    """

    @classmethod
    def create_default(
        cls,
        enable_batch: bool = True,
        db_operator: Optional[DBOperator] = None,
        schema_loader: Optional[ISchemaLoader] = None,
        data_buffer: Optional[IDataBuffer] = None,
    ) -> "AsyncSimpleDataProcessor":
        """创建默认配置的异步数据处理器

        Args:
            enable_batch: 是否启用批量处理模式
            db_operator: 数据库操作器，如果为None则使用默认配置
            schema_loader: Schema 加载器，如果为None则使用默认配置
            data_buffer: 数据缓冲器，如果为None则使用异步默认配置

        Returns:
            配置好的异步数据处理器实例
        """
        return cls(
            db_operator=db_operator or DBOperator.create_default(),
            enable_batch=enable_batch,
            schema_loader=schema_loader or SchemaLoader(),
            data_buffer=data_buffer,
        )

    def __init__(
        self,
        db_operator: Optional[DBOperator] = None,
        enable_batch: bool = True,
        schema_loader: Optional[ISchemaLoader] = None,
        data_buffer: Optional[IDataBuffer] = None,
    ):
        """初始化异步数据处理器

        Args:
            db_operator: 数据库操作器，用于保存数据
            enable_batch: 是否启用批量处理模式
            schema_loader: Schema 加载器，用于获取表名映射
            data_buffer: 数据缓冲器，用于批量处理
        """
        self.config = get_config()
        self.db_operator = db_operator or DBOperator()
        self.enable_batch = enable_batch
        self.schema_loader = schema_loader or SchemaLoader()

        # 批量处理配置
        self.batch_size = self.config.data_processor.batch_size
        self.flush_interval_seconds = self.config.data_processor.flush_interval_seconds

        # 异步数据缓冲器：负责缓冲区管理和异步刷新
        if data_buffer is None:
            # 如果没有提供 data_buffer，我们需要异步初始化它
            self.data_buffer = None
            self._data_buffer_initialized = False
        else:
            self.data_buffer = data_buffer
            self._data_buffer_initialized = True
        
        # 注册已知的数据类型（如果有的话）
        self._register_known_types()

    def _register_known_types(self) -> None:
        """注册已知的数据类型到缓冲器
        
        这里可以预先注册一些已知的数据类型，避免运行时注册。
        目前为空实现，数据类型会在首次使用时动态注册。
        """
        pass

    async def _ensure_data_buffer_initialized(self):
        """确保数据缓冲器已初始化"""
        if not self._data_buffer_initialized:
            from .data_buffer import get_async_data_buffer
            self.data_buffer = await get_async_data_buffer(self.flush_interval_seconds)
            if self.data_buffer is None:
                raise RuntimeError("Failed to initialize async data buffer")
            self._data_buffer_initialized = True

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
            logger.warning(f"未找到任务类型 '{type_name}' 对应的表配置")
            return None

    async def process(self, task_type: str, data: pd.DataFrame) -> bool:
        """异步处理任务结果

        Args:
            task_type: 任务类型字符串
            data: 要处理的数据

        Returns:
            bool: 处理是否成功
        """
        logger.debug(f"异步处理任务: {task_type}")

        try:
            # 确保数据缓冲器已初始化
            await self._ensure_data_buffer_initialized()
            
            # 再次检查数据缓冲器是否正确初始化
            if self.data_buffer is None:
                raise RuntimeError("Data buffer is still None after initialization")
            
            # 检查数据是否存在
            if data is None or data.empty:
                logger.warning("数据为空，跳过处理")
                return False

            logger.debug(f"数据维度: {len(data)} 行 x {len(data.columns)} 列")

            # 根据模式选择处理方式
            if self.enable_batch:
                # 批量处理模式：使用异步数据缓冲器
                # 确保数据类型已注册
                self.data_buffer.register_type(task_type, self._save_data_callback, self.batch_size)
                
                # 添加数据到缓冲器
                await self.data_buffer.add(task_type, data)
                logger.debug(f"数据已添加到异步缓冲区: {task_type}, rows: {len(data)}")
                success = True
            else:
                # 单条处理模式：直接保存
                success = self._save_data(data, task_type)

            if success:
                if not self.enable_batch:
                    logger.info(f"✅ 成功保存 {len(data)} 行数据")
            else:
                logger.warning(f"异步数据处理失败: {task_type}")

            return success

        except Exception as e:
            print(f"💥 异步处理异常: {task_type} - {str(e)}")
            logger.error(f"异步处理数据时出错: {e}")
            return False

    async def _save_data_callback(self, data_type: str, data: pd.DataFrame) -> bool:
        """异步数据保存回调函数
        
        供异步数据缓冲器调用的回调函数，用于保存合并后的数据。
        
        Args:
            data_type: 数据类型标识
            data: 要保存的合并数据
            
        Returns:
            保存是否成功
        """
        return self._save_data(data, data_type)

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
            # 调试信息：打印 task_type 的类型和值
            logger.debug(f"task_type 类型: {type(task_type)}, 值: {task_type}")

            # 根据任务类型动态获取表名
            table_name = self._get_table_name(task_type)
            if not table_name:
                logger.warning(f"未知的任务类型: {task_type}")
                return False

            # 保存数据到数据库
            self.db_operator.upsert(table_name, data)
            logger.info(f"异步数据保存成功: {table_name}, {len(data)} rows")

            return True

        except Exception as e:
            logger.error(f"异步数据保存失败: {e}")
            return False

    async def flush_all(self, force: bool = True) -> bool:
        """异步刷新所有缓冲区数据到数据库

        Args:
            force: 是否强制刷新所有数据（忽略批量大小限制）

        Returns:
            bool: 所有刷新是否成功
        """
        try:
            await self.data_buffer.flush()
            return True
        except Exception as e:
            logger.error(f"异步刷新缓冲区失败: {e}")
            return False

    def get_buffer_status(self) -> Dict[str, int]:
        """获取缓冲区状态
        
        Returns:
            各数据类型的缓冲区大小
        """
        if hasattr(self.data_buffer, 'get_buffer_sizes'):
            return self.data_buffer.get_buffer_sizes()
        return {}
        
    async def shutdown(self) -> None:
        """异步关闭数据处理器，清理资源
        
        确保所有缓冲的数据都被刷新到数据库，并停止后台线程。
        """
        if hasattr(self, 'data_buffer') and self.data_buffer is not None and self._data_buffer_initialized:
            await self.data_buffer.shutdown()
