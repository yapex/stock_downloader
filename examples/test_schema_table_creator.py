#!/usr/bin/env python3
"""
测试基于Schema的动态表创建器
"""

import pytest
import tempfile
import tomllib
import sys
from pathlib import Path

# 添加当前目录到Python路径
sys.path.insert(0, str(Path(__file__).parent))
from schema_driven_table_creator import SchemaTableCreator


def test_schema_table_creator_basic():
    """测试基本的表创建功能"""
    # 使用项目中的schema文件
    schema_file = "/Users/yapex/workspace/stock_downloader/stock_schema.toml"
    
    # 使用内存数据库
    creator = SchemaTableCreator(schema_file, ":memory:")
    
    try:
        # 加载schema
        schema = creator.load_schema()
        assert len(schema) > 0
        assert 'stock_basic' in schema
        
        # 连接数据库
        conn = creator.connect_db()
        assert conn is not None
        
        # 创建stock_basic表
        success = creator.create_table('stock_basic')
        assert success is True
        
        # 验证表是否存在
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [table[0] for table in tables]
        assert 'stock_basic' in table_names
        
        # 验证表结构
        columns = conn.execute("DESCRIBE stock_basic").fetchall()
        column_names = [col[0] for col in columns]
        
        # 检查必要的列是否存在
        expected_columns = ['ts_code', 'symbol', 'name']
        for col in expected_columns:
            assert col in column_names
            
    finally:
        creator.close()


def test_create_all_tables():
    """测试创建所有表"""
    schema_file = "/Users/yapex/workspace/stock_downloader/stock_schema.toml"
    creator = SchemaTableCreator(schema_file, ":memory:")
    
    try:
        # 加载schema并连接数据库
        schema = creator.load_schema()
        creator.connect_db()
        
        # 创建所有表
        results = creator.create_all_tables()
        
        # 验证所有表都创建成功
        for table_name, success in results.items():
            assert success is True, f"表 {table_name} 创建失败"
            
        # 验证表数量
        tables = creator.conn.execute("SHOW TABLES").fetchall()
        assert len(tables) == len(schema)
        
    finally:
        creator.close()


def test_generate_create_table_sql():
    """测试SQL生成功能"""
    schema_file = "/Users/yapex/workspace/stock_downloader/stock_schema.toml"
    creator = SchemaTableCreator(schema_file)
    
    # 加载schema
    schema = creator.load_schema()
    
    # 测试stock_basic表的SQL生成
    stock_basic_config = schema.stock_basic
    sql = creator.generate_create_table_sql(stock_basic_config)
    
    # 验证SQL包含必要的元素
    assert "CREATE TABLE IF NOT EXISTS stock_basic" in sql
    assert "ts_code VARCHAR" in sql
    assert "PRIMARY KEY (ts_code)" in sql
    
    # 测试有复合主键的表
    if 'stock_daily' in schema:
        stock_daily_config = schema.stock_daily
        sql = creator.generate_create_table_sql(stock_daily_config)
        assert "CREATE TABLE IF NOT EXISTS stock_daily" in sql
        # stock_daily应该有复合主键
        if len(stock_daily_config.primary_key) > 1:
            pk_str = ", ".join(stock_daily_config.primary_key)
            assert f"PRIMARY KEY ({pk_str})" in sql


def test_error_handling():
    """测试错误处理"""
    # 测试不存在的schema文件
    with pytest.raises(FileNotFoundError):
        creator = SchemaTableCreator("/nonexistent/file.toml")
        creator.load_schema()
    
    # 测试未加载schema就创建表
    creator = SchemaTableCreator("/Users/yapex/workspace/stock_downloader/stock_schema.toml")
    creator.connect_db()
    
    with pytest.raises(ValueError, match="请先加载schema配置"):
        creator.create_table('stock_basic')
        
    creator.close()
    
    # 测试未连接数据库就创建表
    creator = SchemaTableCreator("/Users/yapex/workspace/stock_downloader/stock_schema.toml")
    creator.load_schema()
    
    with pytest.raises(ValueError, match="请先连接数据库"):
        creator.create_table('stock_basic')


if __name__ == "__main__":
    # 运行基本测试
    test_schema_table_creator_basic()
    test_create_all_tables()
    test_generate_create_table_sql()
    test_error_handling()
    print("✅ 所有测试通过！")