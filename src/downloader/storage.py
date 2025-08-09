import pandas as pd
from pathlib import Path
from .utils import normalize_stock_code, get_table_name
import logging
import duckdb
from datetime import datetime
import threading
from typing import List, Dict

logger = logging.getLogger(__name__)


class DuckDBStorage:
    """
    基于 DuckDB 的统一数据存储层。

    设计原则：
    - 所有数据存储在单个 DuckDB 数据库中
    - 使用表来组织不同类型的数据
    - 支持高效的增量更新和查询
    - 提供统一的接口处理系统数据和股票数据
    - 支持多线程操作，每个线程使用独立的数据库连接
    """

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # 使用线程本地存储管理连接
        self._local = threading.local()

        # 主线程连接用于初始化
        main_conn = duckdb.connect(database=str(self.db_path), read_only=False)
        logger.debug(f"DuckDB 数据库已连接: {self.db_path.resolve()}")

        # 初始化元数据表，用于跟踪表的更新时间
        self._init_metadata_table(main_conn)
        main_conn.close()

        # 初始化完成后优化现有表
        self._optimize_existing_tables()

    @property
    def conn(self):
        """获取当前线程的数据库连接。"""
        if not hasattr(self._local, "connection"):
            self._local.connection = duckdb.connect(
                database=str(self.db_path), read_only=False
            )
            logger.debug(f"为线程 {threading.current_thread().name} 创建新的DuckDB连接")
        return self._local.connection

    def _init_metadata_table(self, conn=None):
        """初始化元数据表，用于跟踪各个数据表的最后更新时间。"""
        if conn is None:
            conn = self.conn
        conn.execute("""
            CREATE TABLE IF NOT EXISTS _metadata (
                table_name VARCHAR PRIMARY KEY,
                last_updated TIMESTAMP,
                data_type VARCHAR,
                entity_id VARCHAR
            )
        """)

        # 初始化组运行时间跟踪表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS _group_last_run (
                group_name VARCHAR PRIMARY KEY,
                last_run_ts TIMESTAMP
            )
        """)

    def _update_metadata(self, table_name: str, data_type: str, entity_id: str):
        """更新表的元数据信息。"""
        self.conn.execute(
            """
            INSERT OR REPLACE INTO _metadata (table_name, last_updated, data_type, entity_id)
            VALUES (?, NOW(), ?, ?)
        """,
            [table_name, data_type, entity_id],
        )

    def create_index_if_not_exists(
        self, table_name: str, column_name: str, index_name: str = None
    ) -> bool:
        """为指定表的列创建索引（如果不存在）"""
        if index_name is None:
            index_name = f"idx_{table_name}_{column_name}"

        try:
            # 检查索引是否已存在
            existing_indexes = self.conn.execute(
                "SELECT index_name FROM duckdb_indexes() WHERE table_name = ? AND index_name = ?",
                [table_name, index_name],
            ).fetchall()

            if existing_indexes:
                logger.debug(f"索引 {index_name} 已存在")
                return True

            # 创建索引
            create_index_sql = (
                f"CREATE INDEX {index_name} ON {table_name} ({column_name})"
            )
            self.conn.execute(create_index_sql)
            logger.info(f"成功创建索引: {index_name} on {table_name}({column_name})")
            return True

        except Exception as e:
            logger.error(f"创建索引失败 {index_name}: {e}")
            return False

    def optimize_stock_list_table(self) -> None:
        """优化 stock_list 表的查询性能"""
        table_name = "sys_stock_list"

        if not self.table_exists("system", "stock_list"):
            logger.warning("stock_list 表不存在，跳过索引创建")
            return

        # 为 ts_code 列创建索引
        self.create_index_if_not_exists(table_name, "ts_code", "idx_stock_list_ts_code")
        # 为其他常用列创建索引
        self.create_index_if_not_exists(table_name, "symbol", "idx_stock_list_symbol")
        self.create_index_if_not_exists(table_name, "market", "idx_stock_list_market")

    def optimize_business_tables_indexes(self) -> None:
        """为现有业务表的日期列添加索引"""
        try:
            # 获取所有业务表
            business_tables = self.list_business_tables()

            if not business_tables:
                logger.debug("没有找到业务表，跳过日期列索引创建")
                return

            logger.info(f"开始为 {len(business_tables)} 个业务表创建日期列索引")

            # 常见的日期列名
            date_columns = ["trade_date", "ann_date", "end_date", "report_date"]

            created_count = 0
            for table_info in business_tables:
                table_name = table_info["table_name"]

                try:
                    # 获取表的列信息
                    columns_info = self.conn.execute(
                        f"DESCRIBE {table_name}"
                    ).fetchall()
                    existing_columns = [col[0] for col in columns_info]

                    # 为存在的日期列创建索引
                    for date_col in date_columns:
                        if date_col in existing_columns:
                            success = self.create_index_if_not_exists(
                                table_name, date_col
                            )
                            if success:
                                created_count += 1
                                logger.debug(
                                    f"为表 {table_name} 的 {date_col} 列创建索引"
                                )

                except Exception as e:
                    logger.warning(f"为表 {table_name} 创建索引失败: {e}")
                    continue

            logger.info(f"业务表日期列索引创建完成，共创建 {created_count} 个索引")

        except Exception as e:
            logger.error(f"优化业务表索引失败: {e}")

    def _add_performance_indexes(
        self, table_name: str, data_type: str, columns: list
    ) -> None:
        """为新创建的表添加性能索引"""
        try:
            # 为 stock_list 表添加索引
            if data_type == "system" and table_name == "sys_stock_list":
                # 为 ts_code 列创建索引（如果存在）
                if "ts_code" in columns:
                    self.create_index_if_not_exists(table_name, "ts_code")
                # 为其他常用列创建索引
                if "symbol" in columns:
                    self.create_index_if_not_exists(table_name, "symbol")
                if "market" in columns:
                    self.create_index_if_not_exists(table_name, "market")

            # 为业务表的日期列添加索引
            else:
                # 常见的日期列名
                date_columns = [
                    "trade_date",
                    "ann_date",
                    "end_date",
                    "report_date",
                ]

                for date_col in date_columns:
                    if date_col in columns:
                        self.create_index_if_not_exists(table_name, date_col)
                        logger.debug(
                            f"为业务表 {table_name} 的日期列 {date_col} 创建索引"
                        )

        except Exception as e:
            logger.warning(f"添加性能索引失败 {table_name}: {e}")

    def _optimize_existing_tables(self) -> None:
        """为现有表添加索引优化"""
        try:
            # 优化 stock_list 表
            self.optimize_stock_list_table()

            # 优化业务表的日期列索引
            self.optimize_business_tables_indexes()

        except Exception as e:
            logger.warning(f"优化现有表索引失败: {e}")

    def table_exists(self, data_type: str, entity_id: str) -> bool:
        """检查指定的表是否存在。"""
        table_name = get_table_name(data_type, entity_id)
        tables = self.conn.execute("SHOW TABLES").fetchall()
        return (table_name,) in tables

    def get_table_last_updated(self, data_type: str, entity_id: str) -> datetime | None:
        """获取表的最后更新时间。"""
        table_name = get_table_name(data_type, entity_id)
        result = self.conn.execute(
            "SELECT last_updated FROM _metadata WHERE table_name = ?", [table_name]
        ).fetchone()
        return result[0] if result else None

    def get_latest_date(
        self, data_type: str, entity_id: str, date_col: str
    ) -> str | None:
        """获取指定表中指定日期列的最新值。"""
        if not self.table_exists(data_type, entity_id):
            return None

        table_name = get_table_name(data_type, entity_id)
        try:
            # 首先检查表中是否存在指定的日期列
            columns_result = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            column_names = [col[0].lower() for col in columns_result]

            if date_col.lower() not in column_names:
                # 如果指定的日期列不存在，尝试常见的日期列名
                common_date_cols = ["trade_date", "ann_date", "end_date"]
                available_date_col = None
                for col in common_date_cols:
                    if col.lower() in column_names:
                        available_date_col = col
                        break

                if available_date_col:
                    logger.warning(
                        f"表 {table_name} 中未找到列 '{date_col}'，使用 '{available_date_col}' 替代"
                    )
                    date_col = available_date_col
                else:
                    logger.error(f"表 {table_name} 中未找到任何有效的日期列")
                    return None

            result = self.conn.execute(
                f"SELECT MAX({date_col}) FROM {table_name}"
            ).fetchone()
            return result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"获取表 {table_name} 最新日期失败: {e}", exc_info=True)
            return None

    def get_latest_dates_batch(
        self, data_type: str, entity_ids: List[str], date_col: str
    ) -> Dict[str, str]:
        """批量获取多个实体的最新日期，提升查询性能"""
        latest_dates = {}

        # 构建所有可能的表名
        table_names = []
        entity_table_map = {}

        for entity_id in entity_ids:
            table_name = get_table_name(data_type, entity_id)
            if self.table_exists(data_type, entity_id):
                table_names.append(table_name)
                entity_table_map[table_name] = entity_id

        if not table_names:
            return latest_dates

        try:
            # 构建UNION ALL查询，一次性获取所有表的最新日期
            union_queries = []
            for table_name in table_names:
                # 先检查表结构，确保日期列存在
                try:
                    columns_result = self.conn.execute(
                        f"DESCRIBE {table_name}"
                    ).fetchall()
                    column_names = [col[0].lower() for col in columns_result]

                    actual_date_col = date_col
                    if date_col.lower() not in column_names:
                        # 尝试常见的日期列名
                        common_date_cols = [
                            "trade_date",
                            "ann_date",
                            "end_date",
                        ]
                        for col in common_date_cols:
                            if col.lower() in column_names:
                                actual_date_col = col
                                break
                        else:
                            logger.warning(
                                f"表 {table_name} 中未找到有效的日期列，跳过"
                            )
                            continue

                    union_queries.append(
                        f"SELECT '{entity_table_map[table_name]}' as entity_id, MAX({actual_date_col}) as max_date FROM {table_name}"
                    )
                except Exception as e:
                    logger.warning(f"检查表 {table_name} 结构失败，跳过: {e}")
                    continue

            if union_queries:
                # 执行批量查询
                batch_query = " UNION ALL ".join(union_queries)
                results = self.conn.execute(batch_query).fetchall()

                for entity_id, max_date in results:
                    if max_date:
                        latest_dates[entity_id] = max_date

                logger.debug(
                    f"批量查询 {data_type} 类型的 {len(entity_ids)} 个实体，获得 {len(latest_dates)} 个有效日期"
                )

        except Exception as e:
            logger.error(f"批量获取最新日期失败: {e}", exc_info=True)
            # 降级到单个查询
            logger.info("降级到单个查询模式")
            for entity_id in entity_ids:
                try:
                    date = self.get_latest_date(data_type, entity_id, date_col)
                    if date:
                        latest_dates[entity_id] = date
                except Exception:
                    continue

        return latest_dates

    def save_incremental(
        self, df: pd.DataFrame, data_type: str, entity_id: str, date_col: str
    ):
        """增量保存数据到表中。"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            logger.debug(f"[{data_type}/{entity_id}] 数据为空，跳过保存")
            return

        if date_col not in df.columns:
            logger.error(f"[{data_type}/{entity_id}] 缺少日期列 '{date_col}'")
            return

        table_name = get_table_name(data_type, entity_id)

        try:
            # 创建表（如果不存在）
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0"
            )

            # 删除重叠的日期数据，然后插入新数据
            min_date = df[date_col].min()
            self.conn.execute(
                f"DELETE FROM {table_name} WHERE {date_col} >= ?", [min_date]
            )
            self.conn.from_df(df).insert_into(table_name)

            # 更新元数据
            self._update_metadata(table_name, data_type, entity_id)

            logger.debug(f"[{data_type}/{entity_id}] 增量保存完成，共 {len(df)} 条记录")

        except Exception as e:
            logger.error(f"[{data_type}/{entity_id}] 增量保存失败: {e}", exc_info=True)

    def save_full(self, df: pd.DataFrame, data_type: str, entity_id: str):
        """全量保存数据，替换整个表。"""
        if not isinstance(df, pd.DataFrame):
            logger.warning(f"[{data_type}/{entity_id}] 数据类型错误，跳过保存")
            return

        table_name = get_table_name(data_type, entity_id)

        try:
            self.conn.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df"
            )
            self._update_metadata(table_name, data_type, entity_id)

            logger.debug(f"[{data_type}/{entity_id}] 全量保存完成，共 {len(df)} 条记录")

        except Exception as e:
            logger.error(f"[{data_type}/{entity_id}] 全量保存失败: {e}", exc_info=True)

    def query(
        self, data_type: str, entity_id: str, columns: list[str] = None
    ) -> pd.DataFrame:
        """查询指定表的数据。"""
        if not self.table_exists(data_type, entity_id):
            return pd.DataFrame()

        table_name = get_table_name(data_type, entity_id)

        try:
            if columns:
                cols_str = ", ".join(columns)
                query = f"SELECT {cols_str} FROM {table_name}"
            else:
                query = f"SELECT * FROM {table_name}"

            return self.conn.execute(query).df()
        except Exception as e:
            logger.error(f"查询表 {table_name} 失败: {e}", exc_info=True)
            return pd.DataFrame()

    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表数据的便捷方法。"""
        return self.query("system", "stock_list")

    def get_all_stock_codes(self) -> list[str]:
        """获取所有股票代码列表。"""
        try:
            # 优化：只查询 ts_code 列，避免加载整个股票列表表
            stock_df = self.query("system", "stock_list", columns=["ts_code"])
            if (
                stock_df is not None
                and not stock_df.empty
                and "ts_code" in stock_df.columns
            ):
                return stock_df["ts_code"].tolist()
            else:
                return []
        except Exception as e:
            logger.error(f"获取股票代码列表失败: {e}")
            return []

    def list_tables(self) -> list[str]:
        """列出数据库中的所有表。"""
        tables = self.conn.execute("SHOW TABLES").fetchall()
        return [table[0] for table in tables if not table[0].startswith("_")]

    # 为了向后兼容，保留旧接口
    def save(self, df: pd.DataFrame, data_type: str, entity_id: str, date_col: str):
        """向后兼容的增量保存接口。"""
        self.save_incremental(df, data_type, entity_id, date_col)

    def overwrite(self, df: pd.DataFrame, data_type: str, entity_id: str):
        """向后兼容的全量保存接口。"""
        self.save_full(df, data_type, entity_id)

    def get_last_run(self, group_name: str) -> datetime | None:
        """获取指定组的最后运行时间。"""
        try:
            result = self.conn.execute(
                "SELECT last_run_ts FROM _group_last_run WHERE group_name = ?",
                [group_name],
            ).fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"获取组 {group_name} 的 last_run 失败: {e}", exc_info=True)
            return None

    def set_last_run(self, group_name: str, timestamp: datetime):
        """设置指定组的最后运行时间。"""
        try:
            self.conn.execute(
                "INSERT OR REPLACE INTO _group_last_run (group_name, last_run_ts) VALUES (?, ?)",
                [group_name, timestamp],
            )
            logger.debug(f"已更新组 {group_name} 的 last_run_ts: {timestamp}")
        except Exception as e:
            logger.error(f"设置组 {group_name} 的 last_run 失败: {e}", exc_info=True)

    def bulk_insert(
        self, df: pd.DataFrame, data_type: str, entity_id: str, date_col: str = None
    ):
        """批量插入数据，使用优化的executemany策略。

        Args:
            df: 要插入的DataFrame
            data_type: 数据类型
            entity_id: 实体ID
            date_col: 日期列名，如果指定则使用增量插入，否则使用全量插入
        """
        if not isinstance(df, pd.DataFrame) or df.empty:
            logger.debug(f"[{data_type}/{entity_id}] 批量插入数据为空，跳过")
            return

        try:
            if date_col and date_col in df.columns:
                # 使用增量插入
                self._bulk_incremental_insert(df, data_type, entity_id, date_col)
                logger.debug(
                    f"[{data_type}/{entity_id}] 批量增量插入完成，共 {len(df)} 条记录"
                )
            else:
                # 使用全量插入
                self.save_full(df, data_type, entity_id)
                logger.debug(
                    f"[{data_type}/{entity_id}] 批量全量插入完成，共 {len(df)} 条记录"
                )

        except Exception as e:
            logger.error(f"[{data_type}/{entity_id}] 批量插入失败: {e}", exc_info=True)
            raise

    def _bulk_incremental_insert(
        self, df: pd.DataFrame, data_type: str, entity_id: str, date_col: str
    ):
        """执行优化的批量增量插入。

        使用executemany优化策略：
        1. 检查表是否存在，不存在则创建
        2. 处理列结构兼容性问题
        3. 批量删除重叠日期的数据
        4. 使用executemany批量插入新数据
        5. 更新元数据
        """
        table_name = get_table_name(data_type, entity_id)
        conn = self.conn

        try:
            # 开始事务以确保操作原子性
            conn.begin()

            # 1. 检查表是否存在
            table_exists = self.table_exists(data_type, entity_id)

            if not table_exists:
                # 表不存在，直接创建
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df LIMIT 0")
                logger.debug(f"[{data_type}/{entity_id}] 创建新表 {table_name}")
            else:
                # 表已存在，检查列结构兼容性
                existing_columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
                existing_col_names = [col[0] for col in existing_columns]
                new_col_names = list(df.columns)

                if existing_col_names != new_col_names:
                    logger.debug(
                        f"[{data_type}/{entity_id}] 检测到列结构不匹配:\n"
                        f"  现有表列数: {len(existing_col_names)}\n"
                        f"  新数据列数: {len(new_col_names)}\n"
                        f"  现有列: {existing_col_names}\n"
                        f"  新数据列: {new_col_names}"
                    )

                    # 处理列结构不匹配的情况
                    df = self._align_dataframe_with_table(
                        df, existing_col_names, new_col_names, data_type, entity_id
                    )

            # 2. 批量删除重叠的日期数据
            if not df.empty and date_col in df.columns:
                # 获取需要删除的日期范围
                min_date = df[date_col].min()
                max_date = df[date_col].max()

                # 使用参数化查询避免SQL注入
                delete_sql = (
                    f"DELETE FROM {table_name} WHERE {date_col} BETWEEN ? AND ?"
                )
                conn.execute(delete_sql, [min_date, max_date])

                # 记录删除的行数
                deleted_count = conn.rowcount if hasattr(conn, "rowcount") else 0
                logger.debug(
                    f"[{data_type}/{entity_id}] 删除了 {deleted_count} 条重叠数据"
                )

            # 3. 使用DuckDB的高效批量插入
            # DuckDB的from_df().insert_into()已经是优化的批量插入
            conn.from_df(df).insert_into(table_name)

            # 4. 为新创建的表添加性能索引
            if not table_exists and not df.empty:
                self._add_performance_indexes(
                    table_name, data_type, df.columns.tolist()
                )

            # 5. 更新元数据
            self._update_metadata(table_name, data_type, entity_id)

            # 提交事务
            conn.commit()

            logger.debug(
                f"[{data_type}/{entity_id}] 批量增量插入事务完成，插入 {len(df)} 条记录"
            )

        except Exception as e:
            # 回滚事务
            try:
                conn.rollback()
            except:
                pass
            logger.error(
                f"[{data_type}/{entity_id}] 批量增量插入事务失败: {e}", exc_info=True
            )
            raise

    def _align_dataframe_with_table(
        self,
        df: pd.DataFrame,
        existing_cols: list,
        new_cols: list,
        data_type: str,
        entity_id: str,
    ) -> pd.DataFrame:
        """对齐DataFrame与现有表的列结构。

        Args:
            df: 要插入的DataFrame
            existing_cols: 现有表的列名列表
            new_cols: 新数据的列名列表
            data_type: 数据类型
            entity_id: 实体ID

        Returns:
            对齐后的DataFrame
        """
        # 如果新数据列数更多，删除多余的列
        if len(new_cols) > len(existing_cols):
            extra_cols = [col for col in new_cols if col not in existing_cols]
            logger.info(f"[{data_type}/{entity_id}] 删除新数据中的额外列: {extra_cols}")
            df = df.drop(columns=extra_cols)

        # 如果新数据列数更少，添加缺失的列（填充默认值）
        elif len(new_cols) < len(existing_cols):
            missing_cols = [col for col in existing_cols if col not in new_cols]
            logger.info(f"[{data_type}/{entity_id}] 为新数据添加缺失列: {missing_cols}")
            for col in missing_cols:
                df[col] = None  # 使用None作为默认值

        # 确保列的顺序与现有表一致
        df = df.reindex(columns=existing_cols)

        return df

    def bulk_insert_multiple(
        self, data_batches: list[tuple[pd.DataFrame, str, str, str]]
    ):
        """批量插入多个数据批次，进一步优化性能。

        Args:
            data_batches: 数据批次列表，每个元素为(df, data_type, entity_id, date_col)
        """
        if not data_batches:
            logger.debug("批量插入数据批次为空，跳过")
            return

        conn = self.conn

        try:
            # 开始大事务
            conn.begin()

            for df, data_type, entity_id, date_col in data_batches:
                if df.empty:
                    continue

                table_name = get_table_name(data_type, entity_id)

                # 创建表（如果不存在）
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0"
                )

                # 处理增量插入
                if date_col and date_col in df.columns:
                    min_date = df[date_col].min()
                    max_date = df[date_col].max()
                    delete_sql = (
                        f"DELETE FROM {table_name} WHERE {date_col} BETWEEN ? AND ?"
                    )
                    conn.execute(delete_sql, [min_date, max_date])
                else:
                    # 全量替换
                    conn.execute(f"DELETE FROM {table_name}")

                # 批量插入
                conn.from_df(df).insert_into(table_name)

                # 更新元数据
                self._update_metadata(table_name, data_type, entity_id)

                logger.debug(
                    f"[{data_type}/{entity_id}] 多批次插入完成，共 {len(df)} 条记录"
                )

            # 提交整个大事务
            conn.commit()
            logger.info(f"批量插入多个数据批次完成，共处理 {len(data_batches)} 个批次")

        except Exception as e:
            try:
                conn.rollback()
            except:
                pass
            logger.error(f"批量插入多个数据批次失败: {e}", exc_info=True)
            raise

    def get_summary(self) -> list[dict]:
        """获取数据库中所有表的记录数摘要。"""
        summary_data = []
        tables = self.list_tables()
        for table_name in tables:
            try:
                count = self.conn.execute(
                    f'SELECT COUNT(*) FROM "{table_name}"'
                ).fetchone()[0]
                summary_data.append({"table_name": table_name, "record_count": count})
            except Exception as e:
                logger.error(f"无法获取表 '{table_name}' 的摘要: {e}", exc_info=True)
                summary_data.append({"table_name": table_name, "record_count": "错误"})
        return summary_data

    def list_business_tables(self) -> list[dict]:
        """返回所有业务表及其业务类型、股票代码。

        Returns:
            list[dict]: 包含 {'table_name', 'business_type', 'stock_code'} 的字典列表
        """
        import re

        business_tables = []
        tables = self.list_tables()

        # 正则模式：匹配业务类型_股票代码格式
        # 股票代码格式：6位数字.2位字母（如000001.SZ）转换后为6位数字_2位字母
        pattern = r"^([a-zA-Z][a-zA-Z0-9_]*?)_([0-9]{6}_[A-Z]{2})$"

        for table_name in tables:
            # 跳过系统表
            if table_name.startswith("sys_"):
                continue

            match = re.match(pattern, table_name)
            if match:
                business_type = match.group(1)
                stock_code_part = match.group(2)
                # 将下划线转换回点号，还原为标准股票代码格式
                stock_code = stock_code_part.replace("_", ".")

                business_tables.append(
                    {
                        "table_name": table_name,
                        "business_type": business_type,
                        "stock_code": stock_code,
                    }
                )

        return business_tables

    def get_business_stats(self) -> dict:
        """返回业务统计信息。

        Returns:
            dict: {业务类型: {'stock_count': int, 'table_count': int}}
        """
        business_tables = self.list_business_tables()
        stats = {}

        for table_info in business_tables:
            business_type = table_info["business_type"]

            if business_type not in stats:
                stats[business_type] = {"stock_count": 0, "table_count": 0}

            stats[business_type]["table_count"] += 1
            # 每个表对应一个股票，所以stock_count等于table_count
            stats[business_type]["stock_count"] += 1

        return stats

    def get_missing_symbols(self, stock_list_df: pd.DataFrame) -> dict:
        """返回缺失的股票代码。

        Args:
            stock_list_df: 包含股票列表的DataFrame，需要有'ts_code'列

        Returns:
            dict: {
                '业务类型1': ['缺失股票代码1', '缺失股票代码2'],
                '业务类型2': ['缺失股票代码3'],
                'overall_missing': ['总体缺失的股票代码']
            }
        """
        if (
            not isinstance(stock_list_df, pd.DataFrame)
            or "ts_code" not in stock_list_df.columns
        ):
            logger.error("stock_list_df必须是包含'ts_code'列的DataFrame")
            return {"overall_missing": []}

        # 获取所有应该存在的股票代码
        expected_stocks = set(stock_list_df["ts_code"].tolist())

        # 获取业务表信息
        business_tables = self.list_business_tables()

        # 按业务类型分组现有股票
        existing_stocks_by_type = {}
        for table_info in business_tables:
            business_type = table_info["business_type"]
            stock_code = table_info["stock_code"]

            if business_type not in existing_stocks_by_type:
                existing_stocks_by_type[business_type] = set()
            existing_stocks_by_type[business_type].add(stock_code)

        # 计算缺失的股票代码
        missing_by_type = {}
        all_existing_stocks = set()

        for business_type, existing_stocks in existing_stocks_by_type.items():
            missing_stocks = expected_stocks - existing_stocks
            if missing_stocks:
                missing_by_type[business_type] = sorted(list(missing_stocks))
            all_existing_stocks.update(existing_stocks)

        # 计算总体缺失（任何业务类型都没有的股票）
        overall_missing = expected_stocks - all_existing_stocks

        result = missing_by_type.copy()
        result["overall_missing"] = sorted(list(overall_missing))

        return result

    def get_existing_tables(self) -> List[str]:
        """
        获取数据库中存在的所有表名

        Returns:
            List[str]: 表名列表
        """
        try:
            tables = self.conn.execute("SHOW TABLES").fetchall()
            return [table[0] for table in tables]
        except Exception as e:
            self.logger.error(f"获取表列表失败: {e}")
            return []


__all__ = ["DuckDBStorage"]
