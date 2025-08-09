import pandas as pd
from pathlib import Path
import logging
import duckdb
from datetime import datetime
import threading
from typing import List, Dict, Optional, Any
import time

logger = logging.getLogger(__name__)


class PartitionedStorage:
    """
    分区表架构的数据存储层
    
    设计原则：
    - 按数据类型分区，每种数据类型一张大表
    - 统一的数据模型和索引策略
    - 支持高效的多维度查询和分析
    - 向后兼容现有API
    """

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # 使用线程本地存储管理连接
        self._local = threading.local()

        # 主线程连接用于初始化
        main_conn = duckdb.connect(database=str(self.db_path), read_only=False)
        logger.debug(f"分区表DuckDB数据库已连接: {self.db_path.resolve()}")

        # 初始化分区表
        self._init_partitioned_tables(main_conn)
        main_conn.close()

    @property
    def conn(self):
        """获取当前线程的数据库连接"""
        if not hasattr(self._local, "connection"):
            self._local.connection = duckdb.connect(
                database=str(self.db_path), read_only=False
            )
            # 确保每个连接都初始化表结构
            self._init_partitioned_tables(self._local.connection)
            logger.debug(f"为线程 {threading.current_thread().name} 创建新的分区表DuckDB连接")
        return self._local.connection

    def _init_partitioned_tables(self, conn=None):
        """初始化分区表结构"""
        if conn is None:
            conn = self.conn

        # 创建日线数据表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_data (
                ts_code VARCHAR NOT NULL,
                trade_date VARCHAR NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                pre_close DOUBLE,
                change DOUBLE,
                pct_chg DOUBLE,
                vol DOUBLE,
                amount DOUBLE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (ts_code, trade_date)
            )
        """)

        # 创建财务数据表（简化版，包含核心字段）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS financial_data (
                ts_code VARCHAR NOT NULL,
                ann_date VARCHAR NOT NULL,
                end_date VARCHAR NOT NULL,
                report_type VARCHAR,
                total_revenue DOUBLE,
                revenue DOUBLE,
                n_income DOUBLE,
                n_income_attr_p DOUBLE,
                total_profit DOUBLE,
                operate_profit DOUBLE,
                ebit DOUBLE,
                ebitda DOUBLE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (ts_code, ann_date, end_date)
            )
        """)

        # 创建基本面数据表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fundamental_data (
                ts_code VARCHAR NOT NULL,
                trade_date VARCHAR NOT NULL,
                close DOUBLE,
                turnover_rate DOUBLE,
                turnover_rate_f DOUBLE,
                volume_ratio DOUBLE,
                pe DOUBLE,
                pe_ttm DOUBLE,
                pb DOUBLE,
                ps DOUBLE,
                ps_ttm DOUBLE,
                dv_ratio DOUBLE,
                dv_ttm DOUBLE,
                total_share DOUBLE,
                float_share DOUBLE,
                free_share DOUBLE,
                total_mv DOUBLE,
                circ_mv DOUBLE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (ts_code, trade_date)
            )
        """)

        # 创建股票列表系统表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sys_stock_list (
                ts_code VARCHAR PRIMARY KEY,
                symbol VARCHAR,
                name VARCHAR,
                area VARCHAR,
                industry VARCHAR,
                market VARCHAR,
                list_date VARCHAR,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # 创建分区元数据表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS _partition_metadata (
                table_name VARCHAR NOT NULL,
                partition_key VARCHAR NOT NULL,
                min_date VARCHAR,
                max_date VARCHAR,
                record_count BIGINT,
                last_updated TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (table_name, partition_key)
            )
        """)

        # 创建索引
        self._create_indexes(conn)

    def _create_indexes(self, conn=None):
        """创建性能优化索引"""
        if conn is None:
            conn = self.conn

        try:
            # 日线数据表索引
            conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_ts_code ON daily_data(ts_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_trade_date ON daily_data(trade_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_ts_date ON daily_data(ts_code, trade_date)")

            # 财务数据表索引
            conn.execute("CREATE INDEX IF NOT EXISTS idx_financial_ts_code ON financial_data(ts_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_financial_ann_date ON financial_data(ann_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_financial_end_date ON financial_data(end_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_financial_ts_ann ON financial_data(ts_code, ann_date)")

            # 基本面数据表索引
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fundamental_ts_code ON fundamental_data(ts_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fundamental_trade_date ON fundamental_data(trade_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fundamental_ts_date ON fundamental_data(ts_code, trade_date)")

            logger.debug("分区表索引创建完成")
        except Exception as e:
            logger.warning(f"创建分区表索引失败: {e}")

    def save_daily_data(self, df: pd.DataFrame) -> bool:
        """保存日线数据"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            logger.debug("日线数据为空，跳过保存")
            return False

        try:
            # 验证必需字段
            required_fields = ['ts_code', 'trade_date']
            if not all(field in df.columns for field in required_fields):
                logger.error(f"日线数据缺少必需字段: {required_fields}")
                return False

            # 添加时间戳
            df_copy = df.copy()
            df_copy['updated_at'] = datetime.now()
            if 'created_at' not in df_copy.columns:
                df_copy['created_at'] = datetime.now()

            # 获取表的列信息
            table_columns = self.conn.execute("DESCRIBE daily_data").fetchall()
            table_col_names = [col[0] for col in table_columns]
            
            # 只选择表中存在的列
            available_cols = [col for col in table_col_names if col in df_copy.columns]
            
            # 使用明确的列名插入数据
            cols_str = ', '.join(available_cols)
            self.conn.execute(f"INSERT OR REPLACE INTO daily_data ({cols_str}) SELECT {cols_str} FROM df_copy")
            
            # 更新元数据
            self._update_partition_metadata('daily_data', df_copy)
            
            logger.debug(f"日线数据保存完成，共 {len(df)} 条记录")
            return True
        except Exception as e:
            logger.error(f"保存日线数据失败: {e}", exc_info=True)
            return False

    def save_daily_data_incremental(self, df: pd.DataFrame) -> bool:
        """增量保存日线数据"""
        return self.save_daily_data(df)

    def save_financial_data(self, df: pd.DataFrame) -> bool:
        """保存财务数据"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            logger.debug("财务数据为空，跳过保存")
            return False

        try:
            # 验证必需字段
            required_fields = ['ts_code', 'ann_date', 'end_date']
            if not all(field in df.columns for field in required_fields):
                logger.error(f"财务数据缺少必需字段: {required_fields}")
                return False

            # 添加时间戳
            df_copy = df.copy()
            df_copy['updated_at'] = datetime.now()
            if 'created_at' not in df_copy.columns:
                df_copy['created_at'] = datetime.now()

            # 只插入表中存在的列
            table_columns = [desc[0] for desc in self.conn.execute("DESCRIBE financial_data").fetchall()]
            df_columns = [col for col in df_copy.columns if col in table_columns]
            
            if df_columns:
                df_filtered = df_copy[df_columns]
                # 使用明确的列名插入数据
                cols_str = ', '.join(df_columns)
                self.conn.execute(f"INSERT OR REPLACE INTO financial_data ({cols_str}) SELECT {cols_str} FROM df_filtered")
            else:
                logger.error("没有匹配的列可以插入财务数据表")
                return False
            
            # 更新元数据
            self._update_partition_metadata('financial_data', df_copy)
            
            logger.debug(f"财务数据保存完成，共 {len(df)} 条记录")
            return True
        except Exception as e:
            logger.error(f"保存财务数据失败: {e}", exc_info=True)
            return False

    def save_stock_list(self, df: pd.DataFrame) -> bool:
        """保存股票列表数据"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            logger.debug("股票列表数据为空，跳过保存")
            return False

        try:
            # 验证必需字段
            required_fields = ['ts_code']
            if not all(field in df.columns for field in required_fields):
                logger.error(f"股票列表数据缺少必需字段: {required_fields}")
                return False

            # 添加时间戳
            df_copy = df.copy()
            df_copy['updated_at'] = datetime.now()
            if 'created_at' not in df_copy.columns:
                df_copy['created_at'] = datetime.now()

            # 获取表的列信息
            table_columns = self.conn.execute("DESCRIBE sys_stock_list").fetchall()
            table_col_names = [col[0] for col in table_columns]
            
            # 只选择表中存在的列
            available_cols = [col for col in table_col_names if col in df_copy.columns]
            
            # 使用明确的列名插入数据
            cols_str = ', '.join(available_cols)
            self.conn.execute(f"INSERT OR REPLACE INTO sys_stock_list ({cols_str}) SELECT {cols_str} FROM df_copy")
            
            logger.debug(f"股票列表数据保存完成，共 {len(df)} 条记录")
            return True
        except Exception as e:
            logger.error(f"保存股票列表数据失败: {e}", exc_info=True)
            return False

    def query_daily_data_by_stock(self, ts_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """按股票查询日线数据"""
        try:
            where_clause = "WHERE ts_code = ?"
            params = [ts_code]
            
            if start_date:
                where_clause += " AND trade_date >= ?"
                params.append(start_date)
            if end_date:
                where_clause += " AND trade_date <= ?"
                params.append(end_date)
                
            query = f"SELECT * FROM daily_data {where_clause} ORDER BY trade_date"
            return self.conn.execute(query, params).df()
        except Exception as e:
            logger.error(f"查询股票 {ts_code} 日线数据失败: {e}")
            return pd.DataFrame()

    def query_daily_data_by_date_range(self, start_date: str, end_date: str, ts_codes: Optional[List[str]] = None) -> pd.DataFrame:
        """按日期范围查询日线数据"""
        try:
            where_clause = "WHERE trade_date >= ? AND trade_date <= ?"
            params = [start_date, end_date]
            
            if ts_codes:
                placeholders = ','.join(['?' for _ in ts_codes])
                where_clause += f" AND ts_code IN ({placeholders})"
                params.extend(ts_codes)
                
            query = f"SELECT * FROM daily_data {where_clause} ORDER BY ts_code, trade_date"
            return self.conn.execute(query, params).df()
        except Exception as e:
            logger.error(f"查询日期范围 {start_date}-{end_date} 日线数据失败: {e}")
            return pd.DataFrame()

    def query_financial_data_by_stock(self, ts_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """按股票查询财务数据"""
        try:
            where_clause = "WHERE ts_code = ?"
            params = [ts_code]
            
            if start_date:
                where_clause += " AND end_date >= ?"
                params.append(start_date)
            if end_date:
                where_clause += " AND end_date <= ?"
                params.append(end_date)
                
            query = f"SELECT * FROM financial_data {where_clause} ORDER BY end_date"
            return self.conn.execute(query, params).df()
        except Exception as e:
            logger.error(f"查询股票 {ts_code} 财务数据失败: {e}")
            return pd.DataFrame()

    def query_daily_data_by_stocks(self, ts_codes: List[str], start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """按股票列表查询日线数据"""
        try:
            placeholders = ','.join(['?' for _ in ts_codes])
            where_clause = f"WHERE ts_code IN ({placeholders})"
            params = ts_codes.copy()
            
            if start_date:
                where_clause += " AND trade_date >= ?"
                params.append(start_date)
            if end_date:
                where_clause += " AND trade_date <= ?"
                params.append(end_date)
                
            query = f"SELECT * FROM daily_data {where_clause} ORDER BY ts_code, trade_date"
            return self.conn.execute(query, params).df()
        except Exception as e:
            logger.error(f"查询股票列表日线数据失败: {e}")
            return pd.DataFrame()

    def get_latest_date_by_stock(self, ts_code: str, data_type: str) -> Optional[str]:
        """获取指定股票的最新日期"""
        try:
            if data_type == 'daily':
                query = "SELECT MAX(trade_date) FROM daily_data WHERE ts_code = ?"
            elif data_type == 'financial':
                query = "SELECT MAX(ann_date) FROM financial_data WHERE ts_code = ?"
            elif data_type == 'fundamental':
                query = "SELECT MAX(trade_date) FROM fundamental_data WHERE ts_code = ?"
            else:
                logger.error(f"不支持的数据类型: {data_type}")
                return None
                
            result = self.conn.execute(query, [ts_code]).fetchone()
            return result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"获取股票 {ts_code} 最新日期失败: {e}")
            return None

    def batch_get_latest_dates(self, ts_codes: List[str], data_type: str) -> Dict[str, str]:
        """批量获取股票最新日期"""
        latest_dates = {}
        try:
            if data_type == 'daily':
                query = "SELECT ts_code, MAX(trade_date) FROM daily_data WHERE ts_code IN ({}) GROUP BY ts_code"
            elif data_type == 'financial':
                query = "SELECT ts_code, MAX(ann_date) FROM financial_data WHERE ts_code IN ({}) GROUP BY ts_code"
            elif data_type == 'fundamental':
                query = "SELECT ts_code, MAX(trade_date) FROM fundamental_data WHERE ts_code IN ({}) GROUP BY ts_code"
            else:
                logger.error(f"不支持的数据类型: {data_type}")
                return latest_dates
                
            placeholders = ','.join(['?' for _ in ts_codes])
            query = query.format(placeholders)
            
            results = self.conn.execute(query, ts_codes).fetchall()
            for ts_code, max_date in results:
                if max_date:
                    latest_dates[ts_code] = max_date
                    
        except Exception as e:
            logger.error(f"批量获取最新日期失败: {e}")
            
        return latest_dates

    def _update_partition_metadata(self, table_name: str, df: pd.DataFrame):
        """更新分区元数据"""
        try:
            # 根据表类型确定日期列
            if table_name == 'daily_data' and 'trade_date' in df.columns:
                date_col = 'trade_date'
            elif table_name == 'financial_data' and 'ann_date' in df.columns:
                date_col = 'ann_date'
            elif table_name == 'fundamental_data' and 'trade_date' in df.columns:
                date_col = 'trade_date'
            else:
                return
                
            # 获取股票代码列表
            if 'ts_code' in df.columns:
                ts_codes = df['ts_code'].unique()
                
                for ts_code in ts_codes:
                    stock_data = df[df['ts_code'] == ts_code]
                    min_date = stock_data[date_col].min()
                    max_date = stock_data[date_col].max()
                    record_count = len(stock_data)
                    
                    self.conn.execute("""
                        INSERT OR REPLACE INTO _partition_metadata 
                        (table_name, partition_key, min_date, max_date, record_count, last_updated)
                        VALUES (?, ?, ?, ?, ?, NOW())
                    """, [table_name, ts_code, min_date, max_date, record_count])
                    
        except Exception as e:
            logger.warning(f"更新分区元数据失败: {e}")

    # ========== 兼容性接口方法 ==========
    
    def save(self, df: pd.DataFrame, data_type: str, entity_id: str, date_col: Optional[str] = None):
        """兼容原有的save接口，根据data_type路由到对应的保存方法"""
        if df is None or df.empty:
            logger.debug(f"数据为空，跳过保存: {data_type}_{entity_id}")
            return
            
        # 确保ts_code列存在（只在不存在时添加）
        if 'ts_code' not in df.columns:
            df = df.copy()
            df['ts_code'] = entity_id
            
        # 根据data_type路由到对应的保存方法（注意判断顺序，避免daily_basic被误判为daily）
        if data_type.startswith('stock_list') or data_type == 'system':
            return self.save_stock_list(df)
        elif data_type == 'daily_basic' or data_type.startswith('fundamental'):
            return self.save_fundamental_data(df)
        elif data_type.startswith('daily'):
            return self.save_daily_data(df)
        elif data_type.startswith('financials'):
            return self.save_financial_data(df)
        else:
            logger.warning(f"未知的数据类型: {data_type}，使用默认保存方法")
            return self.save_daily_data(df)
    
    def save_incremental(self, df: pd.DataFrame, data_type: str, entity_id: str, date_col: Optional[str] = None):
        """兼容原有的增量保存接口"""
        return self.save(df, data_type, entity_id, date_col)
    
    def save_full(self, df: pd.DataFrame, data_type: str, entity_id: str):
        """兼容原有的全量保存接口"""
        return self.save(df, data_type, entity_id)
    
    def overwrite(self, df: pd.DataFrame, data_type: str, entity_id: str):
        """兼容原有的覆盖保存接口 - 先删除该股票的所有数据，然后插入新数据"""
        if df is None or df.empty:
            logger.debug(f"数据为空，跳过覆盖: {data_type}_{entity_id}")
            return
            
        try:
            # 确保ts_code列存在
            if 'ts_code' not in df.columns:
                df = df.copy()
                df['ts_code'] = entity_id
                
            # 根据data_type确定要删除的表
            if data_type.startswith('stock_list') or data_type == 'system':
                table_name = 'sys_stock_list'
            elif data_type == 'daily_basic' or data_type.startswith('fundamental'):
                table_name = 'fundamental_data'
            elif data_type.startswith('daily'):
                table_name = 'daily_data'
            elif data_type.startswith('financials'):
                table_name = 'financial_data'
            else:
                table_name = 'daily_data'  # 默认
                
            # 先删除数据
            if data_type.startswith('stock_list') or data_type == 'system':
                # 股票列表数据：清空整个表
                self.conn.execute(f"DELETE FROM {table_name}")
                logger.debug(f"已清空表 {table_name} 的所有数据")
            else:
                # 其他数据：删除该股票的所有数据
                self.conn.execute(f"DELETE FROM {table_name} WHERE ts_code = ?", [entity_id])
                logger.debug(f"已删除股票 {entity_id} 在表 {table_name} 中的所有数据")
            
            # 然后插入新数据
            return self.save(df, data_type, entity_id)
            
        except Exception as e:
            logger.error(f"覆盖数据失败: {data_type}_{entity_id}, {e}")
            return False
    
    def get_latest_date(self, data_type: str, entity_id: str, date_col: Optional[str] = None) -> Optional[str]:
        """兼容原有的获取最新日期接口"""
        # 将data_type映射到内部数据类型（注意判断顺序）
        if data_type == 'daily_basic' or data_type.startswith('fundamental'):
            internal_type = 'fundamental'
        elif data_type.startswith('daily'):
            internal_type = 'daily'
        elif data_type.startswith('financials'):
            internal_type = 'financial'
        else:
            internal_type = 'daily'  # 默认
            
        return self.get_latest_date_by_stock(entity_id, internal_type)
    
    def get_latest_dates_batch(self, data_type: str, entity_ids: List[str], date_col: Optional[str] = None) -> Dict[str, str]:
        """兼容原有的批量获取最新日期接口"""
        # 将data_type映射到内部数据类型（注意判断顺序）
        if data_type == 'daily_basic' or data_type.startswith('fundamental'):
            internal_type = 'fundamental'
        elif data_type.startswith('daily'):
            internal_type = 'daily'
        elif data_type.startswith('financials'):
            internal_type = 'financial'
        else:
            internal_type = 'daily'  # 默认
            
        return self.batch_get_latest_dates(entity_ids, internal_type)
    
    def query(self, data_type: str, entity_id: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """兼容原有的查询接口"""
        try:
            if data_type == 'daily_basic' or data_type.startswith('fundamental'):
                result = self.query_fundamental_data_by_stock(entity_id)
            elif data_type.startswith('daily'):
                result = self.query_daily_data_by_stock(entity_id)
            elif data_type.startswith('financials'):
                result = self.query_financial_data_by_stock(entity_id)
            else:
                logger.warning(f"未知的数据类型: {data_type}")
                return pd.DataFrame()
                
            # 如果指定了列，则只返回这些列
            if columns and not result.empty:
                available_columns = [col for col in columns if col in result.columns]
                if available_columns:
                    result = result[available_columns]
                    
            return result
        except Exception as e:
            logger.error(f"查询数据失败: {data_type}_{entity_id}, {e}")
            return pd.DataFrame()
    
    def table_exists(self, data_type: str, entity_id: str) -> bool:
        """检查表是否存在（分区表架构下总是返回True，因为表是预创建的）"""
        # 在分区表架构下，表是预创建的，所以检查是否有数据
        try:
            if data_type.startswith('daily'):
                result = self.conn.execute("SELECT COUNT(*) FROM daily_data WHERE ts_code = ? LIMIT 1", [entity_id]).fetchone()
            elif data_type.startswith('financials'):
                result = self.conn.execute("SELECT COUNT(*) FROM financial_data WHERE ts_code = ? LIMIT 1", [entity_id]).fetchone()
            elif data_type.startswith('fundamental') or data_type == 'daily_basic':
                result = self.conn.execute("SELECT COUNT(*) FROM fundamental_data WHERE ts_code = ? LIMIT 1", [entity_id]).fetchone()
            else:
                return False
                
            return result[0] > 0 if result else False
        except Exception:
            return False
    
    def get_table_last_updated(self, data_type: str, entity_id: str) -> Optional[datetime]:
        """获取表的最后更新时间"""
        try:
            if data_type.startswith('daily'):
                result = self.conn.execute(
                    "SELECT MAX(updated_at) FROM daily_data WHERE ts_code = ?", [entity_id]
                ).fetchone()
            elif data_type.startswith('financials'):
                result = self.conn.execute(
                    "SELECT MAX(updated_at) FROM financial_data WHERE ts_code = ?", [entity_id]
                ).fetchone()
            elif data_type.startswith('fundamental') or data_type == 'daily_basic':
                result = self.conn.execute(
                    "SELECT MAX(updated_at) FROM fundamental_data WHERE ts_code = ?", [entity_id]
                ).fetchone()
            elif data_type == 'system':
                result = self.conn.execute(
                    "SELECT MAX(updated_at) FROM sys_stock_list"
                ).fetchone()
            else:
                return None
                
            return result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"获取表最后更新时间失败: {e}")
            return None
    
    def list_tables(self) -> List[str]:
        """列出所有表（返回分区表名称）"""
        return ['daily_data', 'financial_data', 'fundamental_data']
    
    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表（从系统表）"""
        try:
            return self.conn.execute("SELECT * FROM sys_stock_list ORDER BY ts_code").df()
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()
    
    def get_all_stock_codes(self) -> List[str]:
        """获取所有股票代码"""
        try:
            # 优先从股票列表系统表中获取所有股票代码
            result = self.conn.execute("SELECT ts_code FROM sys_stock_list ORDER BY ts_code").fetchall()
            if result:
                return [code[0] for code in result]
            
            # 如果系统表为空，则从各个数据表中获取股票代码作为备选
            logger.warning("sys_stock_list表为空，从数据表中获取股票代码")
            daily_codes = self.conn.execute("SELECT DISTINCT ts_code FROM daily_data").fetchall()
            financial_codes = self.conn.execute("SELECT DISTINCT ts_code FROM financial_data").fetchall()
            fundamental_codes = self.conn.execute("SELECT DISTINCT ts_code FROM fundamental_data").fetchall()
            
            all_codes = set()
            for codes in [daily_codes, financial_codes, fundamental_codes]:
                all_codes.update([code[0] for code in codes])
                
            return sorted(list(all_codes))
        except Exception as e:
            logger.error(f"获取所有股票代码失败: {e}")
            return []
    
    def get_summary(self) -> List[Dict[str, Any]]:
        """获取数据库摘要信息（兼容旧接口）"""
        try:
            summary = []
            
            # 获取每个股票在各个表中的记录数
            tables_info = [
                ('daily_data', 'daily'),
                ('financial_data', 'financials'), 
                ('fundamental_data', 'daily_basic')
            ]
            
            for table_name, data_type in tables_info:
                try:
                    # 获取每个股票的记录数
                    query = f"SELECT ts_code, COUNT(*) as record_count FROM {table_name} GROUP BY ts_code"
                    results = self.conn.execute(query).fetchall()
                    
                    for ts_code, record_count in results:
                        # 生成兼容的表名格式
                        table_name_compat = f"{data_type}_{ts_code.replace('.', '_')}"
                        summary.append({
                            "table_name": table_name_compat,
                            "record_count": record_count
                        })
                except Exception as e:
                    logger.warning(f"获取表 {table_name} 摘要失败: {e}")
                    continue
                    
            return summary
        except Exception as e:
            logger.error(f"获取数据库摘要失败: {e}")
            return []
    
    def list_business_tables(self) -> List[Dict[str, str]]:
        """列出业务表信息，返回业务类型和股票代码的映射"""
        try:
            business_tables = []
            
            # 从 daily_data 表获取股票代码
            daily_codes = self.conn.execute("SELECT DISTINCT ts_code FROM daily_data").fetchall()
            for code_tuple in daily_codes:
                business_tables.append({
                    'business_type': 'daily',
                    'stock_code': code_tuple[0]
                })
            
            # 从 financial_data 表获取股票代码
            financial_codes = self.conn.execute("SELECT DISTINCT ts_code FROM financial_data").fetchall()
            for code_tuple in financial_codes:
                business_tables.append({
                    'business_type': 'financials',
                    'stock_code': code_tuple[0]
                })
            
            # 从 fundamental_data 表获取股票代码
            fundamental_codes = self.conn.execute("SELECT DISTINCT ts_code FROM fundamental_data").fetchall()
            for code_tuple in fundamental_codes:
                business_tables.append({
                    'business_type': 'daily_basic',
                    'stock_code': code_tuple[0]
                })
            
            return business_tables
        except Exception as e:
            logger.error(f"获取业务表信息失败: {e}")
            return []
    
    def save_fundamental_data(self, df: pd.DataFrame) -> bool:
        """保存基本面数据"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            logger.debug("基本面数据为空，跳过保存")
            return False

        try:
            # 验证必需字段
            required_fields = ['ts_code', 'trade_date']
            if not all(field in df.columns for field in required_fields):
                logger.error(f"基本面数据缺少必需字段: {required_fields}")
                return False

            # 添加时间戳
            df_copy = df.copy()
            df_copy['updated_at'] = datetime.now()
            if 'created_at' not in df_copy.columns:
                df_copy['created_at'] = datetime.now()

            # 获取表的列信息
            table_columns = self.conn.execute("DESCRIBE fundamental_data").fetchall()
            table_col_names = [col[0] for col in table_columns]
            
            # 只选择表中存在的列
            available_cols = [col for col in table_col_names if col in df_copy.columns]
            
            # 使用明确的列名插入数据
            cols_str = ', '.join(available_cols)
            self.conn.execute(f"INSERT OR REPLACE INTO fundamental_data ({cols_str}) SELECT {cols_str} FROM df_copy")
            
            # 更新元数据
            self._update_partition_metadata('fundamental_data', df_copy)
            
            logger.debug(f"基本面数据保存完成，共 {len(df)} 条记录")
            return True
        except Exception as e:
            logger.error(f"保存基本面数据失败: {e}", exc_info=True)
            return False
    
    def query_fundamental_data_by_stock(self, ts_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """按股票查询基本面数据"""
        try:
            where_clause = "WHERE ts_code = ?"
            params = [ts_code]
            
            if start_date:
                where_clause += " AND trade_date >= ?"
                params.append(start_date)
            if end_date:
                where_clause += " AND trade_date <= ?"
                params.append(end_date)
                
            query = f"SELECT * FROM fundamental_data {where_clause} ORDER BY trade_date"
            return self.conn.execute(query, params).df()
        except Exception as e:
            logger.error(f"查询股票 {ts_code} 基本面数据失败: {e}")
            return pd.DataFrame()


# 为了向后兼容，创建一个别名
DuckDBStorage = PartitionedStorage
