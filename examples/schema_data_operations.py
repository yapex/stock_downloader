#!/usr/bin/env python3
"""
基于Schema的数据操作示例

演示如何使用动态表创建器进行数据插入和查询操作
"""

import pandas as pd
from schema_driven_table_creator import SchemaTableCreator


class SchemaDataOperator:
    """基于Schema的数据操作器"""
    
    def __init__(self, schema_file_path: str, db_path: str = ":memory:"):
        self.creator = SchemaTableCreator(schema_file_path, db_path)
        self.conn = None
        
    def _extract_column_names(self, columns) -> list:
        """
        从columns配置中提取字段名列表
        
        Args:
            columns: 字段配置，可能是字符串列表或包含name/type的字典列表
            
        Returns:
            字段名列表
        """
        column_names = []
        for col in columns:
            if isinstance(col, dict) and 'name' in col:
                # 新格式：{name: "字段名", type: "类型"}
                column_names.append(col['name'])
            else:
                # 兼容旧格式：直接是字符串
                column_names.append(col)
        return column_names
        
    def initialize(self):
        """初始化：加载schema、连接数据库、创建表"""
        print("🚀 初始化数据操作器...")
        
        # 加载schema
        self.creator.load_schema()
        print(f"✅ 加载了 {len(self.creator.schema_config)} 个表配置")
        
        # 连接数据库
        self.conn = self.creator.connect_db()
        print("✅ 数据库连接成功")
        
        # 创建所有表
        results = self.creator.create_all_tables()
        success_count = sum(1 for success in results.values() if success)
        print(f"✅ 成功创建 {success_count}/{len(results)} 个表")
        
    def insert_sample_data(self, table_name: str, data: pd.DataFrame):
        """插入示例数据"""
        if not self.conn:
            raise ValueError("请先初始化")
            
        try:
            # 注册DataFrame到DuckDB
            temp_view_name = f"temp_{table_name}"
            self.conn.register(temp_view_name, data)
            
            # 获取表的实际名称
            table_config = self.creator.schema_config[table_name]
            actual_table_name = table_config.table_name
            
            # 插入数据
            columns = ", ".join(data.columns)
            insert_sql = f"""
            INSERT INTO {actual_table_name} ({columns})
            SELECT {columns} FROM {temp_view_name}
            """
            
            self.conn.execute(insert_sql)
            
            # 清理临时视图
            self.conn.unregister(temp_view_name)
            
            print(f"✅ 成功插入 {len(data)} 条记录到表 {actual_table_name}")
            
        except Exception as e:
            print(f"❌ 插入数据失败: {e}")
            
    def query_data(self, table_name: str, limit: int = 10) -> pd.DataFrame:
        """查询数据"""
        if not self.conn:
            raise ValueError("请先初始化")
            
        table_config = self.creator.schema_config[table_name]
        actual_table_name = table_config.table_name
        
        sql = f"SELECT * FROM {actual_table_name} LIMIT {limit}"
        result = self.conn.execute(sql).fetchdf()
        
        print(f"📊 从表 {actual_table_name} 查询到 {len(result)} 条记录")
        return result
        
    def get_table_stats(self, table_name: str):
        """获取表统计信息"""
        if not self.conn:
            raise ValueError("请先初始化")
            
        table_config = self.creator.schema_config[table_name]
        actual_table_name = table_config.table_name
        
        # 获取记录数
        count_sql = f"SELECT COUNT(*) as count FROM {actual_table_name}"
        count_result = self.conn.execute(count_sql).fetchone()
        record_count = count_result[0] if count_result else 0
        
        print(f"📈 表 {actual_table_name} 统计信息:")
        print(f"  - 记录数: {record_count}")
        print(f"  - 主键: {table_config.primary_key}")
        print(f"  - 字段数: {len(table_config.columns)}")
        
        return {
            'table_name': actual_table_name,
            'record_count': record_count,
            'primary_key': table_config.primary_key,
            'column_count': len(table_config.columns)
        }
        
    def upsert_data(self, table_key: str, data: pd.DataFrame):
        """
        使用临时表和INSERT ... ON CONFLICT语句向指定表更新或插入数据 (Upsert)。
        
        Args:
            table_key: 表在schema配置中的键名 (e.g., 'stock_basic')
            data: 包含新数据的Pandas DataFrame
        """
        if data.empty:
            print(f"ℹ️ 数据源为空，跳过对表 '{table_key}' 的操作。")
            return

        table_config = self.creator.schema_config[table_key]
        table_name = table_config.table_name
        primary_keys = table_config.primary_key
        columns = self._extract_column_names(table_config.columns)
        
        if not primary_keys:
            raise ValueError(f"表 '{table_name}' 未定义主键，无法执行 upsert 操作。")

        # 校验DataFrame是否包含所有schema定义的列
        missing_cols = set(columns) - set(data.columns)
        if missing_cols:
            raise ValueError(f"DataFrame 缺少以下列: {missing_cols}")

        try:
            self.conn.execute("BEGIN TRANSACTION;")
            
            if len(data) == 1:
                # 单条数据直接插入
                self._upsert_single_record(table_name, primary_keys, columns, data)
            else:
                # 多条数据使用临时表批量处理
                self._upsert_batch_records(table_key, table_name, primary_keys, columns, data)
            
            self.conn.execute("COMMIT;")
            print(f"✅ 成功对表 {table_name} 执行了 Upsert 操作，处理了 {len(data)} 条记录。")
            
        except Exception as e:
            self.conn.execute("ROLLBACK;")
            print(f"❌ 对表 {table_name} 执行 Upsert 失败: {e}")
            raise
    
    def _upsert_single_record(self, table_name: str, primary_keys: list, columns: list, data: pd.DataFrame):
        """处理单条记录的upsert操作"""
        # 构建INSERT ... ON CONFLICT语句
        columns_str = ", ".join(f'"{c}"' for c in columns)
        values_str = ", ".join(["?" for _ in columns])
        primary_keys_str = ", ".join(f'"{pk}"' for pk in primary_keys)
        
        update_columns = [col for col in columns if col not in primary_keys]
        update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
        
        if update_set:  # 如果有非主键字段需要更新
            sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({values_str})
            ON CONFLICT ({primary_keys_str}) DO UPDATE SET {update_set}
            """
        else:  # 如果只有主键字段，使用DO NOTHING
            sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({values_str})
            ON CONFLICT ({primary_keys_str}) DO NOTHING
            """
        
        # 获取数据值
        values = [data.iloc[0][col] for col in columns]
        self.conn.execute(sql, values)
    
    def _upsert_batch_records(self, table_key: str, table_name: str, primary_keys: list, columns: list, data: pd.DataFrame):
        """处理批量记录的upsert操作，使用临时表"""
        temp_table_name = f"temp_upsert_{table_key}_{id(data)}"
        
        try:
            # 1. 创建临时表
            columns_def = ", ".join([f'"{col}" VARCHAR' for col in columns])
            create_temp_sql = f"CREATE TEMPORARY TABLE {temp_table_name} ({columns_def})"
            self.conn.execute(create_temp_sql)
            
            # 2. 将数据插入临时表
            temp_view_name = f"temp_view_{table_key}"
            self.conn.register(temp_view_name, data)
            
            columns_str = ", ".join(f'"{c}"' for c in columns)
            insert_temp_sql = f"""
            INSERT INTO {temp_table_name} ({columns_str})
            SELECT {columns_str} FROM {temp_view_name}
            """
            self.conn.execute(insert_temp_sql)
            
            # 3. 使用INSERT ... ON CONFLICT进行upsert
            primary_keys_str = ", ".join(f'"{pk}"' for pk in primary_keys)
            update_columns = [col for col in columns if col not in primary_keys]
            
            if update_columns:  # 如果有非主键字段需要更新
                update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
                upsert_sql = f"""
                INSERT INTO {table_name} ({columns_str})
                SELECT {columns_str} FROM {temp_table_name}
                ON CONFLICT ({primary_keys_str}) DO UPDATE SET {update_set}
                """
            else:  # 如果只有主键字段，使用DO NOTHING
                upsert_sql = f"""
                INSERT INTO {table_name} ({columns_str})
                SELECT {columns_str} FROM {temp_table_name}
                ON CONFLICT ({primary_keys_str}) DO NOTHING
                """
            
            self.conn.execute(upsert_sql)
            
            # 4. 清理临时资源
            self.conn.unregister(temp_view_name)
            
        finally:
            # 确保临时表被删除
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            except:
                pass
            
    def close(self):
        """关闭连接"""
        if self.creator:
            self.creator.close()


def create_sample_stock_basic_data() -> pd.DataFrame:
    """创建示例股票基本信息数据"""
    data = {
        'ts_code': ['000001.SZ', '000002.SZ', '600000.SH'],
        'symbol': ['000001', '000002', '600000'],
        'name': ['平安银行', '万科A', '浦发银行'],
        'area': ['深圳', '深圳', '上海'],
        'industry': ['银行', '房地产', '银行'],
        'cnspell': ['PAYH', 'WKA', 'PFYH'],
        'market': ['主板', '主板', '主板'],
        'list_date': ['19910403', '19910129', '19991110'],
        'act_name': ['平安银行股份有限公司', '万科企业股份有限公司', '上海浦东发展银行股份有限公司'],
        'act_ent_type': ['股份有限公司', '股份有限公司', '股份有限公司']
    }
    return pd.DataFrame(data)


def create_sample_stock_daily_data() -> pd.DataFrame:
    """创建示例股票日线数据"""
    data = {
        'ts_code': ['000001.SZ', '000001.SZ', '000002.SZ'],
        'trade_date': ['20240101', '20240102', '20240101'],
        'open': [10.50, 10.60, 8.20],
        'high': [10.80, 10.90, 8.50],
        'low': [10.40, 10.50, 8.10],
        'close': [10.70, 10.80, 8.30],
        'pre_close': [10.45, 10.70, 8.15],
        'change': [0.25, 0.10, 0.15],
        'pct_chg': [2.39, 0.93, 1.84],
        'vol': [1000000, 800000, 1200000],
        'amount': [10700000, 8640000, 9960000]
    }
    return pd.DataFrame(data)


def main():
    """主函数 - 演示数据操作功能"""
    print("🚀 基于Schema的数据操作示例")
    print("=" * 50)
    
    schema_file = "/Users/yapex/workspace/stock_downloader/stock_schema.toml"
    operator = SchemaDataOperator(schema_file)
    
    try:
        # 1. 初始化
        operator.initialize()
        
        # 2. 插入股票基本信息数据
        print("\n📝 插入股票基本信息数据...")
        stock_basic_data = create_sample_stock_basic_data()
        operator.insert_sample_data('stock_basic', stock_basic_data)
        
        # 3. 插入股票日线数据
        print("\n📝 插入股票日线数据...")
        stock_daily_data = create_sample_stock_daily_data()
        operator.insert_sample_data('stock_daily', stock_daily_data)
        
        # 4. 查询数据
        print("\n📊 查询股票基本信息...")
        basic_result = operator.query_data('stock_basic')
        print(basic_result.to_string(index=False))
        
        print("\n📊 查询股票日线数据...")
        daily_result = operator.query_data('stock_daily')
        print(daily_result.to_string(index=False))
        
        # 5. 获取表统计信息
        print("\n📈 表统计信息:")
        operator.get_table_stats('stock_basic')
        operator.get_table_stats('stock_daily')
        
        # 6. 演示复杂查询
        print("\n🔍 演示复杂查询 - 股票基本信息与日线数据关联:")
        join_sql = """
        SELECT 
            b.ts_code,
            b.name,
            b.industry,
            d.trade_date,
            d.close,
            d.pct_chg
        FROM stock_basic b
        JOIN stock_daily d ON b.ts_code = d.ts_code
        ORDER BY d.trade_date, b.ts_code
        """
        
        join_result = operator.conn.execute(join_sql).fetchdf()
        print(join_result.to_string(index=False))
        
        # 7. 演示 Upsert 功能
        print("\n🔄 演示 Upsert 功能 - 增量更新股票基本信息:")
        
        # 创建包含更新和新增数据的DataFrame
        upsert_data = pd.DataFrame({
            'ts_code': ['000001.SZ', '000003.SZ'],  # 000001.SZ更新，000003.SZ新增
            'symbol': ['000001', '000003'],
            'name': ['平安银行(更新)', '国农科技'],  # 更新平安银行名称
            'area': ['深圳', '深圳'],
            'industry': ['银行', '农业'],
            'cnspell': ['PAYH', 'NGKJ'],
            'market': ['主板', '主板'],
            'list_date': ['19910403', '19970515'],
            'act_name': ['平安银行股份有限公司(更新)', '深圳中国农大科技股份有限公司'],
            'act_ent_type': ['股份有限公司', '股份有限公司']
        })
        
        operator.upsert_data('stock_basic', upsert_data)
        
        # 查看更新后的结果
        print("\n📊 Upsert 后的股票基本信息:")
        updated_result = operator.query_data('stock_basic', limit=10)
        print(updated_result.to_string(index=False))
        
        # 显示统计信息变化
        operator.get_table_stats('stock_basic')
        
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        
    finally:
        operator.close()
        print("\n🔚 程序结束")


if __name__ == "__main__":
    main()