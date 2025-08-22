"""简单数据处理器实现

专注数据清洗、转换和验证的数据处理层。
"""

import logging
from typing import Optional, Dict, Any, List
import pandas as pd
import time
from datetime import timedelta
import threading
from collections import defaultdict

from ..config import get_config
from .interfaces import IDataProcessor
from .types import TaskResult
from ..database.operator import DBOperator
from ..database.interfaces import ISchemaLoader
from ..database.schema_loader import SchemaLoader

logger = logging.getLogger(__name__)


class SimpleDataProcessor(IDataProcessor):
    """简化的数据处理器实现

    专注于数据清洗、转换和验证。
    """

    def __init__(
        self,
        db_operator: Optional[DBOperator] = None,
        enable_batch: bool = True,
        schema_loader: Optional[ISchemaLoader] = None,
    ):
        """初始化数据处理器

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

        # 批量处理缓冲区：按任务类型分组存储待处理数据
        self.batch_buffers: Dict[str, List[pd.DataFrame]] = defaultdict(list)
        self.buffer_lock = threading.Lock()  # 线程安全锁
        self.last_flush_time = time.time()

        # 统计信息跟踪
        self.stats = {
            "total_processed": 0,
            "successful_processed": 0,
            "failed_processed": 0,
            "total_rows_processed": 0,
            "start_time": time.time(),
            "last_stats_output": time.time(),
            "task_type_stats": {},  # 按任务类型统计
            "batch_flushes": 0,  # 批量刷新次数
            "buffered_items": 0,  # 当前缓冲区项目数
        }

        # 统计输出间隔（秒）
        self.stats_output_interval = 30

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
        # 确保任务类型统计结构存在
        if task_type not in self.stats["task_type_stats"]:
            self.stats["task_type_stats"][task_type] = {
                "count": 0,
                "success": 0,
                "rows": 0,
            }

        logger.debug(f"处理任务: {task_type}")

        # 检查是否需要输出统计信息
        self._maybe_output_stats()

        try:
            # 检查数据是否存在
            if data is None or data.empty:
                logger.warning("数据为空，跳过处理")
                # 更新统计信息：总处理数和失败数
                self.stats["total_processed"] += 1
                self.stats["failed_processed"] += 1
                self.stats["task_type_stats"][task_type]["count"] += 1
                return False

            logger.debug(
                f"数据维度: {len(data)} 行 x {len(data.columns)} 列"
            )

            # 根据模式选择处理方式
            if self.enable_batch:
                # 批量处理模式：添加到缓冲区
                success = self._add_to_buffer(data, task_type)
                if success:
                    logger.debug(
                        f"数据已添加到缓冲区: {task_type}, rows: {len(data)}"
                    )

                    # 检查是否需要刷新缓冲区
                    individual_flushed = False
                    if self._should_flush(task_type):
                        flush_success = self._flush_buffer(task_type)
                        if not flush_success:
                            success = False
                        else:
                            individual_flushed = True
                            # 单独刷新成功后，更新最后刷新时间，避免定时刷新立即触发
                            self.last_flush_time = time.time()

                    # 只有在没有进行单独刷新时才检查定时刷新
                    if not individual_flushed:
                        self._check_and_flush_all_buffers()
            else:
                # 单条处理模式：直接保存
                success = self._save_data(data, task_type)

            # 更新统计信息：总处理数和任务类型计数
            self.stats["total_processed"] += 1
            self.stats["task_type_stats"][task_type]["count"] += 1

            if success:
                if not self.enable_batch:
                    print(f"✅ 成功保存 {len(data)} 行数据")
                    # 在批量模式下，行数统计在刷新时更新
                    self.stats["total_rows_processed"] += len(data)

                # 更新成功统计
                self.stats["successful_processed"] += 1
                self.stats["task_type_stats"][task_type]["success"] += 1
                if not self.enable_batch:
                    self.stats["task_type_stats"][task_type]["rows"] += len(data)
            else:
                logger.warning(f"数据处理失败: {task_type}")
                self.stats["failed_processed"] += 1

            return success

        except Exception as e:
            print(f"💥 处理异常: {task_type} - {str(e)}")
            logger.error(f"处理数据时出错: {e}")
            # 更新统计信息：总处理数和失败数
            self.stats["total_processed"] += 1
            self.stats["failed_processed"] += 1
            self.stats["task_type_stats"][task_type]["count"] += 1
            return False

    def _clean_data(self, data: pd.DataFrame, task_type: str) -> Optional[pd.DataFrame]:
        """数据清洗

        Args:
            data: 原始数据
            task_type: 任务类型

        Returns:
            清洗后的数据，如果清洗失败返回None
        """
        try:
            cleaned_data = data.copy()

            # 根据任务类型进行特定清洗
            if task_type == "stock_basic":
                # 股票基础信息清洗
                required_columns = ["ts_code", "symbol", "name"]
                if not all(col in cleaned_data.columns for col in required_columns):
                    logger.warning(f"股票基础信息缺少必要字段: {required_columns}")
                    return None
                # 只移除关键字段为空的行
                cleaned_data = cleaned_data.dropna(subset=required_columns)
            elif task_type in ["daily", "weekly", "monthly"]:
                # 行情数据清洗
                required_columns = [
                    "ts_code",
                    "trade_date",
                    "open",
                    "high",
                    "low",
                    "close",
                ]
                if not all(col in cleaned_data.columns for col in required_columns):
                    logger.warning(f"行情数据缺少必要字段: {required_columns}")
                    return None

                # 只移除关键字段为空的行
                cleaned_data = cleaned_data.dropna(subset=required_columns)

                # 确保价格数据为正数
                price_columns = ["open", "high", "low", "close"]
                for col in price_columns:
                    if col in cleaned_data.columns:
                        cleaned_data = cleaned_data[cleaned_data[col] > 0]
            elif task_type in ["income", "balancesheet", "cashflow"]:
                # 财务数据清洗 - 只检查关键字段
                required_columns = ["ts_code", "end_date"]
                if not all(col in cleaned_data.columns for col in required_columns):
                    logger.warning(f"财务数据缺少必要字段: {required_columns}")
                    return None
                # 只移除关键字段为空的行
                cleaned_data = cleaned_data.dropna(subset=required_columns)
            else:
                # 其他类型数据，保持原样，不进行严格的空值清洗
                pass

            logger.debug(f"数据清洗完成: {len(data)} -> {len(cleaned_data)} rows")
            return cleaned_data

        except Exception as e:
            logger.error(f"数据清洗失败: {e}")
            return None

    def _transform_data(
        self, data: pd.DataFrame, task_type: str
    ) -> Optional[pd.DataFrame]:
        """数据转换

        Args:
            data: 清洗后的数据
            task_type: 任务类型

        Returns:
            转换后的数据，如果转换失败返回None
        """
        try:
            transformed_data = data.copy()

            # 根据任务类型进行特定转换
            if task_type in ["daily", "weekly", "monthly"]:
                # 行情数据转换
                if "trade_date" in transformed_data.columns:
                    # 确保交易日期格式正确
                    transformed_data["trade_date"] = pd.to_datetime(
                        transformed_data["trade_date"], format="%Y%m%d"
                    )

                # 计算涨跌幅（如果有前收盘价）
                if "pre_close" in transformed_data.columns:
                    transformed_data["pct_chg"] = (
                        (transformed_data["close"] - transformed_data["pre_close"])
                        / transformed_data["pre_close"]
                        * 100
                    ).round(2)

            logger.debug(f"数据转换完成: {len(transformed_data)} rows")
            return transformed_data

        except Exception as e:
            logger.error(f"数据转换失败: {e}")
            return None

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

    def _add_to_buffer(self, data: pd.DataFrame, task_type) -> bool:
        """将数据添加到批量处理缓冲区

        Args:
            data: 要添加的数据
            task_type: 任务类型（可以是字符串或枚举）

        Returns:
            bool: 添加是否成功
        """
        try:
            # 转换任务类型为字符串键
            type_key = task_type.name if hasattr(task_type, "name") else str(task_type)

            with self.buffer_lock:
                self.batch_buffers[type_key].append(data.copy())
                self.stats["buffered_items"] += len(data)

            logger.debug(
                f"数据已添加到缓冲区: {type_key}, {len(data)} 行, 缓冲区大小: {len(self.batch_buffers[type_key])}"
            )
            return True

        except Exception as e:
            type_key = task_type.name if hasattr(task_type, "name") else str(task_type)
            logger.error(f"添加数据到缓冲区失败: {type_key} - {e}")
            return False

    def _flush_buffer(self, task_type: str, force: bool = False) -> bool:
        """刷新指定任务类型的缓冲区数据到数据库

        Args:
            task_type: 任务类型
            force: 是否强制刷新（忽略批量大小限制）

        Returns:
            bool: 刷新是否成功
        """
        with self.buffer_lock:
            if task_type not in self.batch_buffers or not self.batch_buffers[task_type]:
                return True  # 没有数据需要刷新，静默返回

            buffer_data = self.batch_buffers[task_type]

            # 检查是否需要刷新（按数据行数计算）
            if not force:
                total_rows = sum(len(df) for df in buffer_data)
                if total_rows < self.batch_size:
                    return True  # 不需要刷新

            try:
                # 合并所有缓冲区数据
                if len(buffer_data) == 1:
                    combined_data = buffer_data[0]
                else:
                    combined_data = pd.concat(buffer_data, ignore_index=True)

                # 根据任务类型动态获取表名
                table_name = self._get_table_name(task_type)
                if not table_name:
                    logger.warning(f"未知的任务类型: {task_type}")
                    return False

                # 批量保存到数据库
                self.db_operator.upsert(table_name, combined_data)
                logger.info(f"批量刷新成功: {table_name}, {len(combined_data)} 行数据")

                # 更新统计信息
                self.stats["batch_flushes"] += 1
                self.stats["total_rows_processed"] += len(combined_data)

                # 计算要减少的缓冲项目数（按行数计算）
                buffered_rows = sum(len(df) for df in buffer_data)

                # 清空缓冲区
                self.batch_buffers[task_type].clear()
                self.stats["buffered_items"] -= buffered_rows

                print(f"✅ 批量保存 {len(combined_data)} 行数据到 {table_name}")
                return True

            except Exception as e:
                logger.error(f"批量刷新失败: {task_type} - {e}")
                return False

    def _should_flush(self, task_type: str) -> bool:
        """检查是否应该刷新缓冲区（仅基于批量大小）

        Args:
            task_type: 任务类型

        Returns:
            bool: 是否应该刷新
        """
        with self.buffer_lock:
            # 只检查批量大小（按数据行数计算）
            if task_type in self.batch_buffers and self.batch_buffers[task_type]:
                total_rows = sum(len(df) for df in self.batch_buffers[task_type])
                if total_rows >= self.batch_size:
                    return True

            return False

    def _check_and_flush_all_buffers(self) -> None:
        """检查并刷新所有需要刷新的缓冲区"""
        current_time = time.time()
        if current_time - self.last_flush_time >= self.flush_interval_seconds:
            # 只刷新那些有数据但未达到批量大小的缓冲区
            flushed_any = False
            with self.buffer_lock:
                for task_type, buffer_data in self.batch_buffers.items():
                    if buffer_data:  # 有数据
                        total_rows = sum(len(df) for df in buffer_data)
                        if total_rows < self.batch_size:  # 未达到批量大小
                            if self._flush_buffer(task_type, force=True):
                                flushed_any = True

            if flushed_any:
                self.last_flush_time = current_time

    def _maybe_output_stats(self) -> None:
        """检查是否需要输出统计信息"""
        current_time = time.time()
        if current_time - self.stats["last_stats_output"] >= self.stats_output_interval:
            self._output_stats()
            self.stats["last_stats_output"] = current_time

    def _output_stats(self) -> None:
        """输出当前统计信息"""
        current_time = time.time()
        elapsed_time = current_time - self.stats["start_time"]

        # 计算处理速率
        processing_rate = (
            self.stats["total_processed"] / elapsed_time if elapsed_time > 0 else 0
        )
        success_rate = (
            (self.stats["successful_processed"] / self.stats["total_processed"] * 100)
            if self.stats["total_processed"] > 0
            else 0
        )

        print("\n" + "=" * 60)
        print("📈 数据处理统计信息")
        print("=" * 60)
        print(f"⏱️  运行时间: {timedelta(seconds=int(elapsed_time))}")
        print(f"📊 总处理任务: {self.stats['total_processed']}")
        print(f"✅ 成功处理: {self.stats['successful_processed']}")
        print(f"❌ 失败处理: {self.stats['failed_processed']}")
        print(f"📈 成功率: {success_rate:.1f}%")
        print(f"🚀 处理速率: {processing_rate:.2f} 任务/秒")
        print(f"📋 总处理行数: {self.stats['total_rows_processed']}")

        # 批量处理统计
        if self.enable_batch:
            print(f"🔄 批量刷新次数: {self.stats['batch_flushes']}")
            print(f"📦 当前缓冲项目: {self.stats['buffered_items']}")

            # 显示各缓冲区状态
            if self.batch_buffers:
                print("\n📦 缓冲区状态:")
                with self.buffer_lock:
                    for task_type, buffer_data in self.batch_buffers.items():
                        if buffer_data:
                            total_rows = sum(len(df) for df in buffer_data)
                            print(
                                f"  {task_type}: {len(buffer_data)} 个任务, {total_rows} 行数据"
                            )

        # 按任务类型统计
        if self.stats["task_type_stats"]:
            print("\n📋 按任务类型统计:")
            for task_type, stats in self.stats["task_type_stats"].items():
                task_success_rate = (
                    (stats["success"] / stats["count"] * 100)
                    if stats["count"] > 0
                    else 0
                )
                print(
                    f"  {task_type}: {stats['count']} 任务, {stats['success']} 成功 ({task_success_rate:.1f}%), {stats['rows']} 行"
                )

        print("=" * 60 + "\n")

        # 同时记录到日志
        logger.info(
            f"统计信息 - 总任务: {self.stats['total_processed']}, 成功: {self.stats['successful_processed']}, 失败: {self.stats['failed_processed']}, 成功率: {success_rate:.1f}%, 处理速率: {processing_rate:.2f} 任务/秒, 总行数: {self.stats['total_rows_processed']}, 批量刷新: {self.stats['batch_flushes']}, 缓冲项目: {self.stats['buffered_items']}"
        )

    def flush_all(self, force: bool = True) -> bool:
        """刷新所有缓冲区数据到数据库

        Args:
            force: 是否强制刷新所有数据（忽略批量大小限制）

        Returns:
            bool: 所有刷新是否成功
        """
        success = True
        flushed_types = []

        with self.buffer_lock:
            # 获取所有有数据的任务类型
            task_types_to_flush = [
                task_type
                for task_type, buffer_data in self.batch_buffers.items()
                if buffer_data
            ]

        if not task_types_to_flush:
            logger.debug("没有缓冲区数据需要刷新")
            return True

        logger.debug(f"开始刷新所有缓冲区: {len(task_types_to_flush)} 个任务类型")

        for task_type in task_types_to_flush:
            # 直接调用 _flush_buffer，避免嵌套锁
            if self._flush_buffer(task_type, force=force):
                flushed_types.append(task_type)
            else:
                success = False

        if flushed_types:
            logger.debug(f"批量刷新完成: {', '.join(flushed_types)}")

        return success

    def get_stats(self) -> Dict[str, Any]:
        """获取当前统计信息

        Returns:
            包含统计信息的字典
        """
        current_time = time.time()
        elapsed_time = current_time - self.stats["start_time"]
        processing_rate = (
            self.stats["total_processed"] / elapsed_time if elapsed_time > 0 else 0
        )
        success_rate = (
            (self.stats["successful_processed"] / self.stats["total_processed"] * 100)
            if self.stats["total_processed"] > 0
            else 0
        )

        # 获取缓冲区状态
        buffer_status = {}
        if self.enable_batch:
            with self.buffer_lock:
                for task_type, buffer_data in self.batch_buffers.items():
                    if buffer_data:
                        total_rows = sum(len(df) for df in buffer_data)
                        buffer_status[task_type] = {
                            "tasks": len(buffer_data),
                            "rows": total_rows,
                        }

        return {
            "elapsed_time": elapsed_time,
            "total_processed": self.stats["total_processed"],
            "successful_processed": self.stats["successful_processed"],
            "failed_processed": self.stats["failed_processed"],
            "success_rate": success_rate,
            "processing_rate": processing_rate,
            "total_rows_processed": self.stats["total_rows_processed"],
            "task_type_stats": self.stats["task_type_stats"].copy(),
            "batch_enabled": self.enable_batch,
            "batch_flushes": self.stats["batch_flushes"],
            "buffered_items": self.stats["buffered_items"],
            "buffer_status": buffer_status,
            "batch_size": self.batch_size,
            "flush_interval_seconds": self.flush_interval_seconds,
        }
