import logging

import pandas as pd

from downloader.config import get_config
from downloader.database.db_connection import get_conn

from .db_table_create import SchemaTableCreator


logger = logging.getLogger(__name__)
config = get_config()


class DBOperator(SchemaTableCreator):
    def __init__(self, schema_file_path: str = None, conn=get_conn):
        super().__init__(schema_file_path, conn)

    def upsert(self, table_key: str, data: pd.DataFrame):
        """
        使用临时表和INSERT ... ON CONFLICT语句向指定表更新或插入数据 (Upsert)。

        Args:
            table_key: 表在schema配置中的键名 (e.g., 'stock_basic')
            data: 包含新数据的Pandas DataFrame
        """
        if data.empty:
            logger.warning(f"数据源为空，跳过对表 '{table_key}' 的操作。")
            return

        table_config = self.stock_schema[table_key]
        table_name = table_config.table_name
        primary_keys = table_config.primary_key
        columns = self._extract_column_names(table_config.columns)

        # 检查表是否存在
        if not self.table_exists(table_name):
            raise ValueError(
                f"表 '{table_name}' 不存在，无法执行 upsert 操作。请先创建表。"
            )

        if not primary_keys:
            raise ValueError(f"表 '{table_name}' 未定义主键，无法执行 upsert 操作。")

        # 检查DataFrame是否包含所有必需的列
        missing_columns = set(columns) - set(data.columns)
        if missing_columns:
            raise ValueError(f"DataFrame 缺少以下列: {missing_columns}")

        # 使用事务确保操作的原子性
        try:
            if callable(self.conn):
                with self.conn() as conn:
                    # with语句已经处理事务，不需要手动BEGIN/COMMIT
                    if len(data) == 1:
                        self._upsert_single_record(
                            conn, table_name, primary_keys, columns, data
                        )
                    else:
                        self._upsert_batch_records(
                            conn, table_key, table_name, primary_keys, columns, data
                        )
            else:
                # 对于直接连接对象，需要手动管理事务
                self.conn.execute("BEGIN TRANSACTION")
                try:
                    if len(data) == 1:
                        self._upsert_single_record(
                            self.conn, table_name, primary_keys, columns, data
                        )
                    else:
                        self._upsert_batch_records(
                            self.conn,
                            table_key,
                            table_name,
                            primary_keys,
                            columns,
                            data,
                        )
                    self.conn.execute("COMMIT")
                except Exception:
                    self.conn.execute("ROLLBACK")
                    raise
        except Exception as e:
            logger.error(f"Upsert操作失败: {e}")
            raise

    def _upsert_single_record(
        self,
        conn,
        table_name: str,
        primary_keys: list,
        columns: list,
        data: pd.DataFrame,
    ):
        """
        处理单条记录的upsert操作
        """
        record = data.iloc[0]

        # 构建INSERT ... ON CONFLICT语句
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])

        # 构建ON CONFLICT子句
        conflict_columns = ", ".join(primary_keys)
        update_assignments = ", ".join(
            [f"{col} = EXCLUDED.{col}" for col in columns if col not in primary_keys]
        )

        sql = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_columns})
        DO UPDATE SET {update_assignments}
        """

        values = [record[col] for col in columns]
        conn.execute(sql, values)
        logger.debug(f"单条记录upsert完成: {table_name}")

    def _upsert_batch_records(
        self,
        conn,
        table_key: str,
        table_name: str,
        primary_keys: list,
        columns: list,
        data: pd.DataFrame,
    ):
        """
        处理批量记录的upsert操作，使用临时表
        """
        temp_table_name = f"temp_{table_key}_{id(data)}"

        try:
            # 创建临时表
            table_config = self.stock_schema[table_key]
            temp_sql = self._generate_create_table_sql(table_config).replace(
                table_name, temp_table_name
            )
            conn.execute(temp_sql)

            # 将数据插入临时表
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["?" for _ in columns])
            insert_temp_sql = (
                f"INSERT INTO {temp_table_name} ({columns_str}) VALUES ({placeholders})"
            )

            for _, row in data.iterrows():
                values = [row[col] for col in columns]
                conn.execute(insert_temp_sql, values)

            # 执行批量upsert
            conflict_columns = ", ".join(primary_keys)
            update_assignments = ", ".join(
                [
                    f"{col} = EXCLUDED.{col}"
                    for col in columns
                    if col not in primary_keys
                ]
            )

            upsert_sql = f"""
            INSERT INTO {table_name} ({columns_str})
            SELECT {columns_str} FROM {temp_table_name}
            ON CONFLICT ({conflict_columns})
            DO UPDATE SET {update_assignments}
            """

            conn.execute(upsert_sql)
            logger.debug(f"批量记录upsert完成: {table_name}, 记录数: {len(data)}")

        finally:
            # 清理临时表
            try:
                conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            except Exception as e:
                logger.warning(f"清理临时表失败: {e}")

    def get_max_date(self, table_key: str) -> str | None:
        """
        根据 schema 中定义的 date_col，查询指定表中日期字段的最大值。

        Args:
            table_key: 表在schema配置中的键名 (e.g., 'stock_basic')

        Returns:
            str | None: 日期字段的最大值，如果表为空或没有 date_col 则返回 None
        """
        if table_key not in self.stock_schema:
            raise ValueError(f"表配置 '{table_key}' 不存在于 schema 中")

        table_config = self.stock_schema[table_key]
        table_name = table_config.table_name

        # 检查表是否定义了 date_col
        if not hasattr(table_config, "date_col") or not table_config.date_col:
            logger.warning(f"表 '{table_name}' 未定义 date_col 字段，无法查询最大日期")
            return None

        date_col = table_config.date_col
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
                logger.warning(f"表 '{table_name}' 为空或 {date_col} 字段无有效数据")
                return None

        except Exception as e:
            logger.error(f"查询表 '{table_name}' 最大日期失败: {e}")
            raise

    def get_all_symbols(self) -> list[str]:
        """
        查询 stock_basic 表，返回所有的 symbol。

        Returns:
            list[str]: 所有股票代码的列表
        """
        table_key = "stock_basic"

        if table_key not in self.stock_schema:
            raise ValueError(f"表配置 '{table_key}' 不存在于 schema 中")

        table_config = self.stock_schema[table_key]
        table_name = table_config.table_name
        sql = f"SELECT ts_code FROM {table_name}"

        try:
            if callable(self.conn):
                with self.conn() as conn:
                    result = conn.execute(sql).fetchall()
            else:
                result = self.conn.execute(sql).fetchall()

            # 提取 ts_code 列表
            ts_codes = [row[0] for row in result if row[0] is not None]
            logger.debug(f"从表 '{table_name}' 查询到 {len(ts_codes)} 个股票代码")
            return ts_codes

        except Exception as e:
            logger.error(f"查询表 '{table_name}' 的 ts_code 失败: {e}")
            raise
