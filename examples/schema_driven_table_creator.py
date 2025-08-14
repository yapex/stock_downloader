#!/usr/bin/env python3
"""
基于Schema的动态数据库表创建原型

这是一个最小功能原型，演示如何根据stock_schema.toml配置文件
动态创建DuckDB表结构。

功能:
1. 读取stock_schema.toml配置文件
2. 解析表结构信息
3. 动态生成CREATE TABLE SQL语句
4. 在DuckDB中创建表
"""

import tomllib
import duckdb
from pathlib import Path
from typing import Dict, List, Any
from box import Box


class SchemaTableCreator:
    """基于Schema配置的表创建器"""
    
    def __init__(self, schema_file_path: str, db_path: str = ":memory:"):
        """
        初始化表创建器
        
        Args:
            schema_file_path: schema配置文件路径
            db_path: 数据库路径，默认使用内存数据库
        """
        self.schema_file_path = Path(schema_file_path)
        self.db_path = db_path
        self.conn = None
        self.schema_config = None
        
    def load_schema(self) -> Box:
        """加载schema配置文件"""
        if not self.schema_file_path.exists():
            raise FileNotFoundError(f"Schema文件不存在: {self.schema_file_path}")
            
        with open(self.schema_file_path, 'rb') as f:
            schema_data = tomllib.load(f)
            
        self.schema_config = Box(schema_data)
        return self.schema_config
        
    def connect_db(self) -> duckdb.DuckDBPyConnection:
        """连接到DuckDB数据库"""
        self.conn = duckdb.connect(self.db_path)
        return self.conn
        
    def generate_create_table_sql(self, table_config: Box) -> str:
        """
        根据表配置生成CREATE TABLE SQL语句
        
        Args:
            table_config: 表配置信息
            
        Returns:
            CREATE TABLE SQL语句
        """
        table_name = table_config.table_name
        columns = table_config.columns
        primary_key = table_config.get('primary_key', [])
        
        # 生成列定义（简化版本，所有列都设为VARCHAR）
        column_definitions = []
        for col in columns:
            column_definitions.append(f"{col} VARCHAR")
            
        columns_sql = ",\n    ".join(column_definitions)
        
        # 生成主键约束
        primary_key_sql = ""
        if primary_key:
            pk_columns = ", ".join(primary_key)
            primary_key_sql = f",\n    PRIMARY KEY ({pk_columns})"
            
        sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
    {columns_sql}{primary_key_sql}
)"""
        
        return sql
        
    def create_table(self, table_name: str) -> bool:
        """
        创建指定的表
        
        Args:
            table_name: 表名（在schema配置中的键名）
            
        Returns:
            创建是否成功
        """
        if not self.schema_config:
            raise ValueError("请先加载schema配置")
            
        if not self.conn:
            raise ValueError("请先连接数据库")
            
        if table_name not in self.schema_config:
            raise ValueError(f"表配置不存在: {table_name}")
            
        table_config = self.schema_config[table_name]
        sql = self.generate_create_table_sql(table_config)
        
        try:
            self.conn.execute(sql)
            print(f"✅ 表 {table_config.table_name} 创建成功")
            print(f"📝 SQL: {sql}")
            return True
        except Exception as e:
            print(f"❌ 表 {table_config.table_name} 创建失败: {e}")
            return False
            
    def create_all_tables(self) -> Dict[str, bool]:
        """
        创建所有配置的表
        
        Returns:
            每个表的创建结果
        """
        if not self.schema_config:
            raise ValueError("请先加载schema配置")
            
        results = {}
        for table_name in self.schema_config.keys():
            results[table_name] = self.create_table(table_name)
            
        return results
        
    def show_table_info(self, table_name: str) -> None:
        """显示表信息"""
        if not self.conn:
            raise ValueError("请先连接数据库")
            
        try:
            # 获取表结构
            result = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            print(f"\n📊 表 {table_name} 结构:")
            print("列名\t\t类型\t\tNULL\t\t键")
            print("-" * 50)
            for row in result:
                print(f"{row[0]:<15} {row[1]:<15} {row[2]:<10} {row[3]}")
                
        except Exception as e:
            print(f"❌ 获取表信息失败: {e}")
            
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            

def main():
    """主函数 - 演示基本功能"""
    print("🚀 基于Schema的动态表创建原型")
    print("=" * 50)
    
    # 配置文件路径
    schema_file = "/Users/yapex/workspace/stock_downloader/stock_schema.toml"
    
    # 创建表创建器
    creator = SchemaTableCreator(schema_file)
    
    try:
        # 1. 加载schema配置
        print("\n📖 加载schema配置...")
        schema = creator.load_schema()
        print(f"✅ 成功加载 {len(schema)} 个表配置")
        
        # 2. 连接数据库
        print("\n🔗 连接数据库...")
        creator.connect_db()
        print("✅ 数据库连接成功")
        
        # 3. 创建stock_basic表（第一个表）
        print("\n🏗️ 创建stock_basic表...")
        success = creator.create_table('stock_basic')
        
        if success:
            # 4. 显示表信息
            creator.show_table_info('stock_basic')
            
        # 5. 演示创建所有表
        print("\n🏗️ 创建所有表...")
        results = creator.create_all_tables()
        
        print("\n📈 创建结果汇总:")
        for table_name, result in results.items():
            status = "✅ 成功" if result else "❌ 失败"
            print(f"  {table_name}: {status}")
            
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        
    finally:
        creator.close()
        print("\n🔚 程序结束")


if __name__ == "__main__":
    main()