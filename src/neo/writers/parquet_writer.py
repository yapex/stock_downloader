"""Parquet 写入器实现"""

import logging
from pathlib import Path
import shutil
from typing import List, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

from .interfaces import IParquetWriter

logger = logging.getLogger(__name__)


class ParquetWriter(IParquetWriter):
    """使用 PyArrow 将 DataFrame 写入分区的 Parquet 文件"""

    def __init__(self, base_path: str):
        """初始化写入器

        Args:
            base_path (str): 所有 Parquet 数据的根存储路径
        """
        self.base_path = Path(base_path)

    def write(
        self, data: pd.DataFrame, task_type: str, partition_cols: List[str]
    ) -> None:
        """将 DataFrame 写入到分区的 Parquet 文件中"""
        if data is None or data.empty:
            logger.debug("数据为空，跳过写入 Parquet 文件")
            return

        # 让 PyArrow 自己处理类型推断，移除强制字符串转换

        table = pa.Table.from_pandas(data)
        target_path = self.base_path / task_type

        try:
            pq.write_to_dataset(
                table,
                root_path=str(target_path),
                partition_cols=partition_cols,
                existing_data_behavior="overwrite_or_ignore",
                basename_template=f"part-{{i}}-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.parquet",
            )
            logger.info(f"✅ 成功将 {len(data)} 条数据写入到 {target_path}")
            
            # 记录实际创建的文件路径（debug级别）
            self._log_created_files(target_path, partition_cols, data)
        except Exception as e:
            logger.error(f"💥 写入 Parquet 数据到 {target_path} 失败: {e}")
            raise

    def write_full_replace(
        self, data: pd.DataFrame, task_type: str, partition_cols: List[str]
    ) -> None:
        """全量替换写入 (用于字典表等非 `symbol` 分区的数据)"""
        if data is None or data.empty:
            logger.debug("数据为空，跳过全量替换写入")
            return

        target_path = self.base_path / task_type

        try:
            # 删除现有数据（支持文件和目录两种情况）
            if target_path.exists():
                if target_path.is_file():
                    target_path.unlink()
                    logger.info(f"🗑️ 已删除现有数据文件: {target_path}")
                else:
                    shutil.rmtree(target_path)
                    logger.info(f"🗑️ 已删除现有数据目录: {target_path}")

            # 写入新数据
            table = pa.Table.from_pandas(data)
            pq.write_to_dataset(
                table,
                root_path=str(target_path),
                partition_cols=partition_cols,
                basename_template=f"part-{{i}}-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.parquet",
            )
            logger.debug(f"✅ 全量替换成功写入 {len(data)} 条数据到 {target_path}")
            
            # 记录实际创建的文件路径（debug级别）
            self._log_created_files(target_path, partition_cols, data)
        except Exception as e:
            logger.error(f"💥 全量替换写入到 {target_path} 失败: {e}")
            raise

    def write_full_replace_by_symbol(
        self, data: pd.DataFrame, task_type: str, partition_cols: List[str], symbol: str
    ) -> None:
        """按 symbol 全量替换写入 (用于按 `symbol` 分区的数据)"""
        if data is None or data.empty:
            logger.debug(f"数据为空，跳过对 {symbol} 的全量替换写入")
            return

        target_path = self.base_path / task_type

        try:
            # 确保 ts_code 列存在，以便 pyarrow 分区
            if "ts_code" not in data.columns:
                data["ts_code"] = symbol

            # 写入新数据
            table = pa.Table.from_pandas(data)
            pq.write_to_dataset(
                table,
                root_path=str(target_path),
                partition_cols=partition_cols,
                existing_data_behavior='delete_matching',
                basename_template=f"part-{{i}}-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.parquet",
            )
            logger.debug(
                f"✅ 全量替换成功写入 {len(data)} 条数据到 {target_path} for symbol {symbol}"
            )
            
            # 记录实际创建的文件路径（debug级别）
            self._log_created_files(target_path, partition_cols, data, symbol)
        except Exception as e:
            logger.error(
                f"💥 全量替换写入到 {target_path} for symbol {symbol} 失败: {e}"
            )
            raise
    
    def _log_created_files(self, target_path: Path, partition_cols: List[str], 
                          data: pd.DataFrame, symbol: Optional[str] = None) -> None:
        """记录实际创建的parquet文件路径
        
        Args:
            target_path: 目标路径
            partition_cols: 分区列
            data: 数据 DataFrame
            symbol: 股票代码（如果是按symbol分区）
        """
        try:
            # 根据分区列和数据内容推断文件路径
            if partition_cols and len(partition_cols) > 0:
                # 有分区的情况，根据第一个分区列的唯一值构造路径
                first_partition_col = partition_cols[0]
                if first_partition_col in data.columns:
                    unique_values = data[first_partition_col].unique()
                    for value in unique_values:
                        partition_path = target_path / f"{first_partition_col}={value}"
                        if symbol:
                            logger.debug(f"📁 [{symbol}] 数据写入到: {partition_path}/")
                        else:
                            logger.debug(f"📁 数据写入到: {partition_path}/")
            else:
                # 无分区的情况
                if symbol:
                    logger.debug(f"📁 [{symbol}] 数据写入到: {target_path}/")
                else:
                    logger.debug(f"📁 数据写入到: {target_path}/")
                                
        except Exception as e:
            # 记录文件路径失败不应影响主流程
            logger.debug(f"记录文件路径失败: {e}")
