"""Parquet 写入器实现"""

import logging
from pathlib import Path
import shutil
from typing import List
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

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

        # 清洗数据，避免 PyArrow 类型推断错误
        for col in data.select_dtypes(include=["object"]).columns:
            data[col] = data[col].astype(str)

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
            # 删除现有数据目录
            if target_path.exists():
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
        # 使用 ts_code=symbol 的分区格式，这是 Hive 分区标准
        symbol_partition_path = target_path / f"ts_code={symbol}"

        try:
            # 关键修复：只删除指定 symbol 的数据目录
            if symbol_partition_path.exists():
                shutil.rmtree(symbol_partition_path)
                logger.info(
                    f"🗑️ 已删除 {symbol} 的现有数据目录: {symbol_partition_path}"
                )

            # 确保 ts_code 是分区的一部分，以便写入到正确的子目录
            if "ts_code" not in data.columns:
                data["ts_code"] = symbol
            if "ts_code" not in partition_cols:
                partition_cols.insert(0, "ts_code")

            # 写入新数据
            table = pa.Table.from_pandas(data)
            pq.write_to_dataset(
                table,
                root_path=str(target_path),
                partition_cols=partition_cols,
                basename_template=f"part-{{i}}-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.parquet",
            )
            logger.debug(
                f"✅ 全量替换成功写入 {len(data)} 条数据到 {target_path} for symbol {symbol}"
            )
        except Exception as e:
            logger.error(
                f"💥 全量替换写入到 {target_path} for symbol {symbol} 失败: {e}"
            )
            raise
