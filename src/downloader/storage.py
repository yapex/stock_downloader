import pandas as pd
from pathlib import Path
from .utils import normalize_stock_code
import logging
import duckdb
from datetime import datetime
import threading

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

    @property
    def conn(self):
        """获取当前线程的数据库连接。"""
        if not hasattr(self._local, 'connection'):
            self._local.connection = duckdb.connect(
                database=str(self.db_path), 
                read_only=False
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

    def _get_table_name(self, data_type: str, entity_id: str) -> str:
        """根据数据类型和实体ID生成规范的表名。"""
        if data_type == "system":
            # 系统表使用简单命名
            return f"sys_{entity_id}"
        else:
            # 股票数据表包含标准化的股票代码
            safe_entity_id = "".join(c if c.isalnum() else "_" for c in normalize_stock_code(entity_id))
            return f"{data_type}_{safe_entity_id}"

    def _update_metadata(self, table_name: str, data_type: str, entity_id: str):
        """更新表的元数据信息。"""
        self.conn.execute("""
            INSERT OR REPLACE INTO _metadata (table_name, last_updated, data_type, entity_id)
            VALUES (?, NOW(), ?, ?)
        """, [table_name, data_type, entity_id])

    def table_exists(self, data_type: str, entity_id: str) -> bool:
        """检查指定的表是否存在。"""
        table_name = self._get_table_name(data_type, entity_id)
        tables = self.conn.execute("SHOW TABLES").fetchall()
        return (table_name,) in tables

    def get_table_last_updated(self, data_type: str, entity_id: str) -> datetime | None:
        """获取表的最后更新时间。"""
        table_name = self._get_table_name(data_type, entity_id)
        result = self.conn.execute(
            "SELECT last_updated FROM _metadata WHERE table_name = ?",
            [table_name]
        ).fetchone()
        return result[0] if result else None

    def get_latest_date(self, data_type: str, entity_id: str, date_col: str) -> str | None:
        """获取指定表中指定日期列的最新值。"""
        if not self.table_exists(data_type, entity_id):
            return None
            
        table_name = self._get_table_name(data_type, entity_id)
        try:
            result = self.conn.execute(
                f"SELECT MAX({date_col}) FROM {table_name}"
            ).fetchone()
            return result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"获取表 {table_name} 最新日期失败: {e}")
            return None

    def save_incremental(self, df: pd.DataFrame, data_type: str, entity_id: str, date_col: str):
        """增量保存数据到表中。"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            logger.debug(f"[{data_type}/{entity_id}] 数据为空，跳过保存")
            return

        if date_col not in df.columns:
            logger.error(f"[{data_type}/{entity_id}] 缺少日期列 '{date_col}'")
            return

        table_name = self._get_table_name(data_type, entity_id)
        
        try:
            # 创建表（如果不存在）
            self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0")

            # 删除重叠的日期数据，然后插入新数据
            min_date = df[date_col].min()
            self.conn.execute(f"DELETE FROM {table_name} WHERE {date_col} >= ?", [min_date])
            self.conn.from_df(df).insert_into(table_name)
            
            # 更新元数据
            self._update_metadata(table_name, data_type, entity_id)
            
            logger.debug(f"[{data_type}/{entity_id}] 增量保存完成，共 {len(df)} 条记录")

        except Exception as e:
            logger.error(f"[{data_type}/{entity_id}] 增量保存失败: {e}")

    def save_full(self, df: pd.DataFrame, data_type: str, entity_id: str):
        """全量保存数据，替换整个表。"""
        if not isinstance(df, pd.DataFrame):
            logger.warning(f"[{data_type}/{entity_id}] 数据类型错误，跳过保存")
            return

        table_name = self._get_table_name(data_type, entity_id)
        
        try:
            self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
            self._update_metadata(table_name, data_type, entity_id)
            
            logger.debug(f"[{data_type}/{entity_id}] 全量保存完成，共 {len(df)} 条记录")

        except Exception as e:
            logger.error(f"[{data_type}/{entity_id}] 全量保存失败: {e}")

    def query(self, data_type: str, entity_id: str, columns: list[str] = None) -> pd.DataFrame:
        """查询指定表的数据。"""
        if not self.table_exists(data_type, entity_id):
            return pd.DataFrame()
            
        table_name = self._get_table_name(data_type, entity_id)
        
        try:
            if columns:
                cols_str = ", ".join(columns)
                query = f"SELECT {cols_str} FROM {table_name}"
            else:
                query = f"SELECT * FROM {table_name}"
                
            return self.conn.execute(query).df()
        except Exception as e:
            logger.error(f"查询表 {table_name} 失败: {e}")
            return pd.DataFrame()

    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表数据的便捷方法。"""
        return self.query("system", "stock_list")

    def list_tables(self) -> list[str]:
        """列出数据库中的所有表。"""
        tables = self.conn.execute("SHOW TABLES").fetchall()
        return [table[0] for table in tables if not table[0].startswith('_')]

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
                [group_name]
            ).fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"获取组 {group_name} 的 last_run 失败: {e}")
            return None

    def set_last_run(self, group_name: str, timestamp: datetime):
        """设置指定组的最后运行时间。"""
        try:
            self.conn.execute(
                "INSERT OR REPLACE INTO _group_last_run (group_name, last_run_ts) VALUES (?, ?)",
                [group_name, timestamp]
            )
            logger.debug(f"已更新组 {group_name} 的 last_run_ts: {timestamp}")
        except Exception as e:
            logger.error(f"设置组 {group_name} 的 last_run 失败: {e}")

    def get_summary(self) -> list[dict]:
        """获取数据库中所有表的记录数摘要。"""
        summary_data = []
        tables = self.list_tables()
        for table_name in tables:
            try:
                count = self.conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
                summary_data.append({"table_name": table_name, "record_count": count})
            except Exception as e:
                logger.error(f"无法获取表 '{table_name}' 的摘要: {e}")
                summary_data.append({"table_name": table_name, "record_count": "错误"})
        return summary_data


__all__ = ["DuckDBStorage"]
