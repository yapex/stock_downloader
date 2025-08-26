"""数据库操作器

提供数据库的基本操作功能，包括数据插入、更新、查询等。
"""

import logging
import pandas as pd
from functools import lru_cache
from typing import List, Dict, Any, Optional, Union

from .table_creator import SchemaTableCreator
from .connection import get_conn
from .interfaces import IDBOperator
from .types import TableName

logger = logging.getLogger(__name__)


class DBOperator(SchemaTableCreator, IDBOperator):
    """数据库操作器

    继承自SchemaTableCreator，提供数据库的基本操作功能。
    """

    def __init__(self, schema_file_path: str = None, conn=get_conn):
        """初始化数据库操作器

        Args:
            schema_file_path: Schema文件路径
            conn: 数据库连接函数
        """
        super().__init__(schema_file_path, conn)

    @classmethod
    def create_default(cls) -> "DBOperator":
        """创建默认的数据库操作器实例

        使用默认配置创建 DBOperator 实例，包括：
        - 默认的 schema 文件路径（从配置中获取）
        - 默认的数据库连接函数

        Returns:
            DBOperator: 默认配置的数据库操作器实例
        """
        return cls()

    def upsert(
        self,
        table_name: str,
        data: Union[pd.DataFrame, Dict[str, Any], List[Dict[str, Any]]],
    ) -> bool:
        """向表中插入或更新数据

        Args:
            table_name: 表名
            data: 要插入的数据，可以是DataFrame、字典或字典列表

        Returns:
            操作是否成功
        """
        # 数据验证
        if data is None:
            logger.debug(f"数据为空，跳过 upsert 操作: {table_name}")
            return True

        if isinstance(data, pd.DataFrame) and data.empty:
            logger.debug(f"DataFrame 为空，跳过 upsert 操作: {table_name}")
            return True

        if isinstance(data, (list, dict)) and not data:
            logger.debug(f"数据为空，跳过 upsert 操作: {table_name}")
            return True

        # 确保表存在
        if not self.table_exists(table_name):
            logger.info(f"表 '{table_name}' 不存在，正在创建...")
            if not self.create_table(table_name):
                logger.error(f"创建表 '{table_name}' 失败")
                return False

        # 获取表配置
        if not self._table_exists_in_schema(table_name):
            logger.error(f"表 '{table_name}' 在 schema 中不存在")
            return False

        table_config = self._get_table_config(table_name)
        primary_key = getattr(
            table_config, "primary_key", table_config.get("primary_key", [])
        )

        if not primary_key:
            raise ValueError(f"表 '{table_name}' 未定义主键，无法执行 upsert 操作")

        # 转换数据格式
        if isinstance(data, dict):
            df = pd.DataFrame([data])
        elif isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data.copy()

        if df.empty:
            logger.debug(f"处理后的数据为空，跳过 upsert 操作: {table_name}")
            return True

        # 检查必要的列是否存在
        missing_pk_cols = [col for col in primary_key if col not in df.columns]
        if missing_pk_cols:
            raise ValueError(
                f"数据中缺少主键列 {missing_pk_cols}，无法执行 upsert 操作"
            )

        # 检查DataFrame是否包含表的所有必需列
        columns = getattr(table_config, "columns", table_config.get("columns", []))
        table_columns = self._extract_column_names(columns)
        missing_cols = [col for col in table_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"DataFrame 缺少以下列: {missing_cols}")

        try:
            # 执行 upsert 操作
            if callable(self.conn):
                with self.conn() as conn:
                    self._perform_upsert(conn, table_name, df, primary_key)
            else:
                self._perform_upsert(self.conn, table_name, df, primary_key)

            logger.debug(f"📥 成功向表 '{table_name}' upsert {len(df)} 条记录")
            return True

        except Exception as e:
            logger.error(f"❌ upsert 操作失败 - 表: {table_name}, 错误: {e}")
            raise

    def _perform_upsert(
        self, conn, table_name: str, df: pd.DataFrame, primary_key: List[str]
    ) -> None:
        """执行实际的 upsert 操作

        Args:
            conn: 数据库连接
            table_name: 表名
            df: 数据DataFrame
            primary_key: 主键列表
        """
        # 构建 upsert SQL
        columns = df.columns.tolist()
        placeholders = ", ".join(["?" for _ in columns])
        column_names = ", ".join(columns)

        # 构建 ON CONFLICT 子句
        " AND ".join([f"excluded.{col} = {table_name}.{col}" for col in primary_key])
        update_columns = [col for col in columns if col not in primary_key]

        if update_columns:
            update_clause = ", ".join(
                [f"{col} = excluded.{col}" for col in update_columns]
            )
            sql = f"""
                INSERT INTO {table_name} ({column_names})
                VALUES ({placeholders})
                ON CONFLICT ({", ".join(primary_key)})
                DO UPDATE SET {update_clause}
            """
        else:
            # 如果没有非主键列，则忽略冲突
            sql = f"""
                INSERT INTO {table_name} ({column_names})
                VALUES ({placeholders})
                ON CONFLICT ({", ".join(primary_key)})
                DO NOTHING
            """

        # 批量插入数据
        self._upsert_batch_records(conn, sql, df)

    def _upsert_batch_records(self, conn, sql: str, df: pd.DataFrame) -> None:
        """执行批量upsert记录

        Args:
            conn: 数据库连接
            sql: SQL语句
            df: 数据DataFrame
        """
        data_tuples = [tuple(row) for row in df.values]
        conn.executemany(sql, data_tuples)
        conn.commit()

    def get_max_date(self, table_key: str) -> Optional[pd.Timestamp]:
        """根据 schema 中定义的 date_col，查询指定表中日期字段的最大值

        Args:
            table_key: 表在schema配置中的键名 (e.g., 'stock_basic')

        Returns:
            日期字段的最大值，如果表为空返回 "19901218"
            如果没有 date_col 则返回 "19901218"
        """
        before_first_open_day = "19901218"  # 中国股市开盘前一天
        if not self._table_exists_in_schema(table_key):
            raise ValueError(f"表配置 '{table_key}' 不存在于 schema 中")

        table_config = self._get_table_config(table_key)
        table_name = table_config.get("table_name")

        # 检查表是否定义了 date_col
        if "date_col" not in table_config or not table_config["date_col"]:
            logger.debug(f"表 '{table_name}' 未定义 date_col 字段，无法查询最大日期")
            return before_first_open_day

        date_col = table_config["date_col"]
        sql = f"SELECT MAX({date_col}) as max_date FROM {table_name}"

        try:
            if callable(self.conn):
                with self.conn() as conn:
                    result = conn.execute(sql).fetchone()
            else:
                result = self.conn.execute(sql).fetchone()

            if result and result[0] is not None:
                return result[0]
            else:
                logger.debug(f"表 '{table_name}' 为空或 {date_col} 字段无有效数据")
                return before_first_open_day

        except Exception as e:
            logger.error(f"❌ 查询表 '{table_name}' 最大日期失败: {e}")
            raise

    @lru_cache(maxsize=1)
    def get_all_symbols(self) -> List[str]:
        """获取所有股票代码

        Returns:
            股票代码列表
        """
        table_name = TableName.STOCK_BASIC.value

        # 构建查询SQL，添加过滤条件
        sql = f"SELECT DISTINCT ts_code FROM {table_name} WHERE ts_code IS NOT NULL AND ts_code != ''"

        try:
            if callable(self.conn):
                with self.conn() as conn:
                    result = conn.execute(sql).fetchall()
            else:
                result = self.conn.execute(sql).fetchall()

            # 提取 ts_code 列表，过滤空值
            ts_codes = [row[0] for row in result if row[0] is not None and row[0] != ""]
            logger.debug(f"从表 '{table_name}' 查询到 {len(ts_codes)} 个股票代码")
            return ts_codes

        except Exception as e:
            logger.error(f"❌ 查询表 '{table_name}' 的 ts_code 失败: {e}")
            raise
