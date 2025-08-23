"""数据库表创建器

基于Schema配置创建和管理数据库表。
"""

from pathlib import Path
import logging
import tomllib
from box import Box
from typing import Dict

from ..configs import get_config
from .connection import get_conn
from .interfaces import ISchemaTableCreator

logger = logging.getLogger(__name__)


class SchemaTableCreator(ISchemaTableCreator):
    """基于Schema配置的表创建器"""

    def __init__(self, schema_file_path: str = None, conn=get_conn):
        """初始化表创建器

        Args:
            schema_file_path: Schema文件路径
            conn: 数据库连接函数
        """
        self.conn = conn
        config = get_config()
        schema_file_path = schema_file_path or (
            Path.cwd() / config.database.schema_file_path
        )
        with open(schema_file_path, "rb") as f:
            self.stock_schema = Box(tomllib.load(f))

        # 检测 schema 格式：是否有 tables 属性
        self._has_tables_section = hasattr(self.stock_schema, "tables")

    def _get_table_config(self, table_name: str) -> Box:
        """根据 schema 格式获取表配置"""
        if self._has_tables_section:
            return self.stock_schema.tables[table_name]
        else:
            return self.stock_schema[table_name]

    def _table_exists_in_schema(self, table_name: str) -> bool:
        """检查表是否在 schema 中存在"""
        if self._has_tables_section:
            return table_name in self.stock_schema.tables
        else:
            return table_name in self.stock_schema

    def _get_table_schema(self, table_name: str) -> Box:
        """获取表的schema"""
        return self.stock_schema[table_name]

    def _map_schema_type_to_duckdb(self, schema_type: str) -> str:
        """将schema中的类型映射到DuckDB类型

        Args:
            schema_type: schema中定义的类型（TEXT, REAL, INTEGER, DATE, BOOLEAN）

        Returns:
            DuckDB对应的数据类型
        """
        type_mapping = {
            "TEXT": "VARCHAR",
            "REAL": "DOUBLE",
            "INTEGER": "INTEGER",
            "DATE": "DATE",
            "BOOLEAN": "BOOLEAN",
        }
        return type_mapping.get(schema_type.upper(), "VARCHAR")

    def _build_column_definition(self, col) -> str:
        """构建单个列的定义

        Args:
            col: 列配置信息

        Returns:
            列定义字符串
        """
        if isinstance(col, dict) and "name" in col and "type" in col:
            # 新格式：{name: "字段名", type: "类型"}
            col_name = col["name"]
            col_type = col["type"]
            duckdb_type = self._map_schema_type_to_duckdb(col_type)
            return f"{col_name} {duckdb_type}"
        else:
            # 兼容旧格式：直接是字符串
            return f"{col} VARCHAR"

    def _build_columns_sql(self, columns) -> str:
        """构建所有列的SQL定义

        Args:
            columns: 列配置列表

        Returns:
            列定义SQL字符串
        """
        if not columns:
            return ""

        column_definitions = []
        for col in columns:
            column_definitions.append(self._build_column_definition(col))

        return ", ".join(column_definitions)

    def _build_primary_key_sql(self, primary_key) -> str:
        """构建主键SQL定义

        Args:
            primary_key: 主键配置

        Returns:
            主键定义SQL字符串
        """
        if not primary_key:
            return ""

        if isinstance(primary_key, list):
            pk_columns = ", ".join(primary_key)
        else:
            pk_columns = str(primary_key)

        return f", PRIMARY KEY ({pk_columns})"

    def _extract_column_names(self, columns) -> list:
        """从列配置中提取列名

        Args:
            columns: 列配置列表或字典

        Returns:
            列名列表
        """
        if not columns:
            return []

        # 支持字典格式：{"col_name": {"type": "TEXT"}}
        if isinstance(columns, dict):
            return list(columns.keys())

        # 检查columns是否为有效格式
        if not isinstance(columns, (list, tuple)):
            raise ValueError("不支持的列配置格式")

        column_names = []
        for col in columns:
            if isinstance(col, dict) and "name" in col:
                # 新格式：{name: "字段名", type: "类型"}
                column_names.append(col["name"])
            elif isinstance(col, str):
                # 兼容旧格式：直接是字符串
                column_names.append(col)
            else:
                raise ValueError("不支持的列配置格式")

        return column_names

    def _generate_create_table_sql(self, table_config: Box) -> str:
        """生成创建表的SQL语句

        Args:
            table_config: 表配置

        Returns:
            CREATE TABLE SQL语句
        """
        table_name = table_config.table_name
        columns = table_config.get("columns", [])
        primary_key = table_config.get("primary_key", [])

        # 构建列定义
        columns_sql = self._build_columns_sql(columns)

        # 构建主键定义
        primary_key_sql = self._build_primary_key_sql(primary_key)

        # 组合完整的SQL
        if columns_sql:
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql}{primary_key_sql})"
        else:
            # 如果没有列定义，创建一个基本表结构
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY)"

        return sql

    def create_table(self, table_name: str) -> bool:
        """创建表

        Args:
            table_name: 表名

        Returns:
            创建是否成功
        """
        try:
            if not self._table_exists_in_schema(table_name):
                logger.error(f"表 '{table_name}' 在 schema 中不存在")
                return False

            table_config = self._get_table_config(table_name)
            sql = self._generate_create_table_sql(table_config)

            logger.debug(f"创建表 SQL: {sql}")

            if callable(self.conn):
                with self.conn() as conn:
                    conn.execute(sql)
            else:
                self.conn.execute(sql)

            logger.info(f"表 '{table_name}' 创建成功")
            return True

        except Exception as e:
            logger.error(f"创建表 '{table_name}' 失败: {e}")
            return False

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在于数据库中

        Args:
            table_name: 表名

        Returns:
            表是否存在
        """
        try:
            if callable(self.conn):
                with self.conn() as conn:
                    result = conn.execute(
                        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                        [table_name],
                    ).fetchone()
                    return result[0] > 0
            else:
                result = self.conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                    [table_name],
                ).fetchone()
                return result[0] > 0
        except Exception as e:
            logger.debug(f"检查表 {table_name} 是否存在时出错: {e}")
            return False

    def drop_table(self, table_name: str) -> bool:
        """删除表

        Args:
            table_name: 表名

        Returns:
            删除是否成功
        """
        try:
            if not self._table_exists_in_schema(table_name):
                logger.error(f"表 '{table_name}' 在 schema 中不存在")
                return False

            sql = f"DROP TABLE IF EXISTS {table_name}"
            logger.debug(f"删除表 SQL: {sql}")

            if callable(self.conn):
                with self.conn() as conn:
                    conn.execute(sql)
            else:
                self.conn.execute(sql)

            logger.info(f"表 '{table_name}' 删除成功")
            return True

        except Exception as e:
            logger.error(f"删除表 '{table_name}' 失败: {e}")
            return False

    def drop_all_tables(self) -> Dict[str, bool]:
        """删除所有表

        Returns:
            每个表的删除结果
        """
        results = {}

        if self._has_tables_section:
            tables = self.stock_schema.tables
        else:
            tables = self.stock_schema

        for table_name in tables.keys():
            results[table_name] = self.drop_table(table_name)

        return results

    def create_all_tables(self) -> Dict[str, bool]:
        """创建所有表

        Returns:
            每个表的创建结果
        """
        results = {}

        if self._has_tables_section:
            tables = self.stock_schema.tables
        else:
            tables = self.stock_schema

        for table_name in tables.keys():
            results[table_name] = self.create_table(table_name)

        return results
