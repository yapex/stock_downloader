"""全量替换数据处理器

专门处理需要全量替换的表，如 stock_basic。
使用事务性的三步操作：创建临时表 -> 删除旧表 -> 重命名临时表。
"""

import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, List

import pandas as pd

from ..database.interfaces import IDBQueryer
from ..database.schema_loader import SchemaLoader
from ..writers.interfaces import IParquetWriter
from .interfaces import IDataProcessor

logger = logging.getLogger(__name__)


class FullReplaceDataProcessor(IDataProcessor):
    """全量替换数据处理器

    专门用于处理需要全量替换的表，避免重复数据问题。
    实现原子性的表替换操作：
    1. 将新数据写入临时表
    2. 删除旧表
    3. 将临时表重命名为正式表名
    """

    def __init__(
        self,
        parquet_writer: IParquetWriter,
        db_queryer: IDBQueryer,
        schema_loader: SchemaLoader,
    ):
        self.parquet_writer = parquet_writer
        self.db_queryer = db_queryer
        self.schema_loader = schema_loader
        self._temp_suffix = "_temp"

    def process(self, task_type: str, df: pd.DataFrame) -> bool:
        """处理数据，使用全量替换策略

        Args:
            task_type: 任务类型
            df: 要处理的数据框

        Returns:
            bool: 处理是否成功
        """
        try:
            logger.info(f"开始全量替换处理 {task_type}，数据行数: {len(df)}")

            # 1. 准备临时表名
            temp_table_name = f"{task_type}{self._temp_suffix}"

            # 2. 写入临时表
            success = self._write_to_temp_table(temp_table_name, df)
            if not success:
                logger.error(f"写入临时表失败: {temp_table_name}")
                return False

            # 3. 执行原子性替换操作
            success = self._atomic_replace_table(task_type, temp_table_name)
            if not success:
                logger.error(f"原子性替换操作失败: {task_type}")
                self._cleanup_temp_table(temp_table_name)
                return False

            logger.debug(f"✅ 全量替换处理完成: {task_type}")
            return True

        except Exception as e:
            logger.error(f"全量替换处理失败 {task_type}: {e}", exc_info=True)
            return False

    def _write_to_temp_table(self, temp_table_name: str, df: pd.DataFrame) -> bool:
        """将数据写入临时表

        Args:
            temp_table_name: 临时表名
            df: 数据框

        Returns:
            bool: 写入是否成功
        """
        try:
            # 添加分区列
            processed_df = self._add_partition_columns(df)
            # 获取分区列
            partition_cols = self._get_partition_columns(processed_df)
            # 使用 ParquetWriter 的 write_full_replace 方法
            self.parquet_writer.write_full_replace(processed_df, temp_table_name, partition_cols)
            return True

        except Exception as e:
            logger.error(f"写入临时表失败 {temp_table_name}: {e}")
            return False

    def _atomic_replace_table(self, table_name: str, temp_table_name: str) -> bool:
        """原子性地替换表

        Args:
            table_name: 正式表名
            temp_table_name: 临时表名

        Returns:
            bool: 替换是否成功
        """
        try:
            base_path = Path(self.parquet_writer.base_path)
            table_path = base_path / table_name
            temp_path = base_path / temp_table_name
            backup_path = base_path / f"{table_name}_backup"

            # 检查临时表是否存在
            if not temp_path.exists():
                logger.error(f"临时表不存在: {temp_path}")
                return False

            # 1. 如果旧表存在，先备份
            if table_path.exists():
                if backup_path.exists():
                    shutil.rmtree(backup_path)
                shutil.move(str(table_path), str(backup_path))
                logger.debug(f"旧表已备份: {table_name} -> {table_name}_backup")

            # 2. 将临时表重命名为正式表名
            shutil.move(str(temp_path), str(table_path))
            logger.debug(f"临时表已重命名: {temp_table_name} -> {table_name}")

            # 3. 删除备份（如果操作成功）
            if backup_path.exists():
                shutil.rmtree(backup_path)
                logger.debug(f"备份已删除: {table_name}_backup")

            return True

        except Exception as e:
            logger.error(f"原子性替换失败 {table_name}: {e}")
            # 尝试恢复备份
            self._restore_backup(table_name)
            return False

    def _restore_backup(self, table_name: str) -> None:
        """恢复备份表

        Args:
            table_name: 表名
        """
        try:
            base_path = Path(self.parquet_writer.base_path)
            table_path = base_path / table_name
            backup_path = base_path / f"{table_name}_backup"

            if backup_path.exists():
                if table_path.exists():
                    shutil.rmtree(table_path)
                shutil.move(str(backup_path), str(table_path))
                logger.info(f"已恢复备份表: {table_name}")

        except Exception as e:
            logger.error(f"恢复备份失败 {table_name}: {e}")

    def _cleanup_temp_table(self, temp_table_name: str) -> None:
        """清理临时表

        Args:
            temp_table_name: 临时表名
        """
        try:
            base_path = Path(self.parquet_writer.base_path)
            temp_path = base_path / temp_table_name

            if temp_path.exists():
                shutil.rmtree(temp_path)
                logger.debug(f"临时表已清理: {temp_table_name}")

        except Exception as e:
            logger.error(f"清理临时表失败 {temp_table_name}: {e}")

    def _add_partition_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加分区列

        Args:
            df: 原始数据框

        Returns:
            pd.DataFrame: 添加分区列后的数据框
        """
        # 对于 stock_basic 这类基础数据，通常不需要按时间分区
        # 但为了保持一致性，可以添加一个固定的年份分区
        processed_df = df.copy()

        # 如果数据中没有时间相关字段，使用当前年份作为分区
        if (
            "trade_date" not in processed_df.columns
            and "end_date" not in processed_df.columns
        ):
            current_year = datetime.now().year
            processed_df["year"] = current_year
            logger.debug(f"为基础数据添加年份分区: {current_year}")

        return processed_df

    def _get_partition_columns(self, df: pd.DataFrame) -> List[str]:
        """获取分区列名列表

        Args:
            df: 数据框

        Returns:
            List[str]: 分区列名列表
        """
        partition_cols = []

        # 检查是否有时间相关的分区列
        if "trade_date" in df.columns:
            partition_cols.append("trade_date")
        elif "end_date" in df.columns:
            partition_cols.append("end_date")
        elif "year" in df.columns:
            partition_cols.append("year")

        return partition_cols

    def shutdown(self) -> None:
        """关闭处理器，清理资源"""
        logger.debug("FullReplaceDataProcessor shutdown completed")
