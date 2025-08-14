#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 Upsert 功能的各种场景

这个测试文件专门验证 SchemaDataOperator 的 upsert_data 方法，
包括单条记录、批量记录、更新和插入操作的各种场景。
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from schema_driven_table_creator import SchemaTableCreator
from schema_data_operations import SchemaDataOperator

def test_single_record_insert():
    """测试单条记录插入"""
    print("\n🧪 测试单条记录插入...")
    
    operator = SchemaDataOperator('stock_schema.toml')
    
    # 创建表
    operator.initialize()
    
    # 单条记录插入
    single_data = pd.DataFrame([{
        'ts_code': '000001.SZ',
        'symbol': '000001',
        'name': '平安银行',
        'area': '深圳',
        'industry': '银行',
        'cnspell': 'PAYH',
        'market': '主板',
        'list_date': '19910403',
        'act_name': '平安银行股份有限公司',
        'act_ent_type': '股份有限公司'
    }])
    
    operator.upsert_data('stock_basic', single_data)
    
    # 验证插入结果
    result = operator.query_data('stock_basic')
    assert len(result) == 1
    assert result.iloc[0]['name'] == '平安银行'
    print("✅ 单条记录插入测试通过")
    
    operator.close()

def test_single_record_update():
    """测试单条记录更新"""
    print("\n🧪 测试单条记录更新...")
    
    operator = SchemaDataOperator('stock_schema.toml')
    
    # 创建表并插入初始数据
    operator.initialize()
    
    initial_data = pd.DataFrame([{
        'ts_code': '000001.SZ',
        'symbol': '000001',
        'name': '平安银行',
        'area': '深圳',
        'industry': '银行',
        'cnspell': 'PAYH',
        'market': '主板',
        'list_date': '19910403',
        'act_name': '平安银行股份有限公司',
        'act_ent_type': '股份有限公司'
    }])
    
    operator.insert_sample_data('stock_basic', initial_data)
    
    # 更新数据
    update_data = pd.DataFrame([{
        'ts_code': '000001.SZ',
        'symbol': '000001',
        'name': '平安银行(更新)',
        'area': '深圳',
        'industry': '银行',
        'cnspell': 'PAYH',
        'market': '主板',
        'list_date': '19910403',
        'act_name': '平安银行股份有限公司(更新)',
        'act_ent_type': '股份有限公司'
    }])
    
    operator.upsert_data('stock_basic', update_data)
    
    # 验证更新结果
    result = operator.query_data('stock_basic')
    assert len(result) == 1
    assert result.iloc[0]['name'] == '平安银行(更新)'
    assert result.iloc[0]['act_name'] == '平安银行股份有限公司(更新)'
    print("✅ 单条记录更新测试通过")
    
    operator.close()

def test_batch_records_insert():
    """测试批量记录插入"""
    print("\n🧪 测试批量记录插入...")
    
    operator = SchemaDataOperator('stock_schema.toml')
    
    # 创建表
    operator.initialize()
    
    # 批量数据插入
    batch_data = pd.DataFrame([
        {
            'ts_code': '000001.SZ',
            'symbol': '000001',
            'name': '平安银行',
            'area': '深圳',
            'industry': '银行',
            'cnspell': 'PAYH',
            'market': '主板',
            'list_date': '19910403',
            'act_name': '平安银行股份有限公司',
            'act_ent_type': '股份有限公司'
        },
        {
            'ts_code': '000002.SZ',
            'symbol': '000002',
            'name': '万科A',
            'area': '深圳',
            'industry': '房地产',
            'cnspell': 'WKA',
            'market': '主板',
            'list_date': '19910129',
            'act_name': '万科企业股份有限公司',
            'act_ent_type': '股份有限公司'
        },
        {
            'ts_code': '600000.SH',
            'symbol': '600000',
            'name': '浦发银行',
            'area': '上海',
            'industry': '银行',
            'cnspell': 'PFYH',
            'market': '主板',
            'list_date': '19991110',
            'act_name': '上海浦东发展银行股份有限公司',
            'act_ent_type': '股份有限公司'
        }
    ])
    
    operator.upsert_data('stock_basic', batch_data)
    
    # 验证插入结果
    result = operator.query_data('stock_basic')
    assert len(result) == 3
    print("✅ 批量记录插入测试通过")
    
    operator.close()

def test_batch_records_mixed_operations():
    """测试批量记录混合操作（插入+更新）"""
    print("\n🧪 测试批量记录混合操作...")
    
    operator = SchemaDataOperator('stock_schema.toml')
    
    # 创建表并插入初始数据
    operator.initialize()
    
    initial_data = pd.DataFrame([
        {
            'ts_code': '000001.SZ',
            'symbol': '000001',
            'name': '平安银行',
            'area': '深圳',
            'industry': '银行',
            'cnspell': 'PAYH',
            'market': '主板',
            'list_date': '19910403',
            'act_name': '平安银行股份有限公司',
            'act_ent_type': '股份有限公司'
        },
        {
            'ts_code': '000002.SZ',
            'symbol': '000002',
            'name': '万科A',
            'area': '深圳',
            'industry': '房地产',
            'cnspell': 'WKA',
            'market': '主板',
            'list_date': '19910129',
            'act_name': '万科企业股份有限公司',
            'act_ent_type': '股份有限公司'
        }
    ])
    
    operator.insert_sample_data('stock_basic', initial_data)
    
    # 混合操作：更新现有记录 + 插入新记录
    mixed_data = pd.DataFrame([
        {
            'ts_code': '000001.SZ',  # 更新现有记录
            'symbol': '000001',
            'name': '平安银行(更新)',
            'area': '深圳',
            'industry': '银行',
            'cnspell': 'PAYH',
            'market': '主板',
            'list_date': '19910403',
            'act_name': '平安银行股份有限公司(更新)',
            'act_ent_type': '股份有限公司'
        },
        {
            'ts_code': '600000.SH',  # 插入新记录
            'symbol': '600000',
            'name': '浦发银行',
            'area': '上海',
            'industry': '银行',
            'cnspell': 'PFYH',
            'market': '主板',
            'list_date': '19991110',
            'act_name': '上海浦东发展银行股份有限公司',
            'act_ent_type': '股份有限公司'
        },
        {
            'ts_code': '000003.SZ',  # 插入新记录
            'symbol': '000003',
            'name': '国农科技',
            'area': '深圳',
            'industry': '农业',
            'cnspell': 'NGKJ',
            'market': '主板',
            'list_date': '19970515',
            'act_name': '深圳中国农大科技股份有限公司',
            'act_ent_type': '股份有限公司'
        }
    ])
    
    operator.upsert_data('stock_basic', mixed_data)
    
    # 验证结果
    result = operator.query_data('stock_basic')
    assert len(result) == 4  # 原有2条 + 新增2条
    
    # 验证更新操作
    updated_record = result[result['ts_code'] == '000001.SZ'].iloc[0]
    assert updated_record['name'] == '平安银行(更新)'
    assert updated_record['act_name'] == '平安银行股份有限公司(更新)'
    
    # 验证插入操作
    new_records = result[result['ts_code'].isin(['600000.SH', '000003.SZ'])]
    assert len(new_records) == 2
    
    print("✅ 批量记录混合操作测试通过")
    
    operator.close()

def test_composite_primary_key_upsert():
    """测试复合主键的upsert操作"""
    print("\n🧪 测试复合主键upsert操作...")
    
    operator = SchemaDataOperator('stock_schema.toml')
    
    # 创建表
    operator.initialize()
    
    # 插入初始日线数据
    initial_data = pd.DataFrame([
        {
            'ts_code': '000001.SZ',
            'trade_date': '20240101',
            'open': 10.5,
            'high': 10.8,
            'low': 10.4,
            'close': 10.7,
            'pre_close': 10.45,
            'change': 0.25,
            'pct_chg': 2.39,
            'vol': 1000000,
            'amount': 10700000
        },
        {
            'ts_code': '000001.SZ',
            'trade_date': '20240102',
            'open': 10.6,
            'high': 10.9,
            'low': 10.5,
            'close': 10.8,
            'pre_close': 10.7,
            'change': 0.1,
            'pct_chg': 0.93,
            'vol': 800000,
            'amount': 8640000
        }
    ])
    
    operator.insert_sample_data('stock_daily', initial_data)
    
    # Upsert操作：更新现有记录 + 插入新记录
    upsert_data = pd.DataFrame([
        {
            'ts_code': '000001.SZ',  # 更新现有记录
            'trade_date': '20240101',
            'open': 10.5,
            'high': 10.8,
            'low': 10.4,
            'close': 10.75,  # 更新收盘价
            'pre_close': 10.45,
            'change': 0.3,   # 更新涨跌额
            'pct_chg': 2.87, # 更新涨跌幅
            'vol': 1000000,
            'amount': 10750000  # 更新成交额
        },
        {
            'ts_code': '000001.SZ',  # 插入新记录
            'trade_date': '20240103',
            'open': 10.8,
            'high': 11.0,
            'low': 10.7,
            'close': 10.9,
            'pre_close': 10.8,
            'change': 0.1,
            'pct_chg': 0.93,
            'vol': 900000,
            'amount': 9810000
        }
    ])
    
    operator.upsert_data('stock_daily', upsert_data)
    
    # 验证结果
    result = operator.query_data('stock_daily')
    assert len(result) == 3  # 原有2条 + 新增1条
    
    # 验证更新操作
    updated_record = result[
        (result['ts_code'] == '000001.SZ') & 
        (result['trade_date'] == '20240101')
    ].iloc[0]
    assert float(updated_record['close']) == 10.75
    assert float(updated_record['change']) == 0.3
    assert float(updated_record['pct_chg']) == 2.87
    assert float(updated_record['amount']) == 10750000
    
    # 验证插入操作
    new_record = result[
        (result['ts_code'] == '000001.SZ') & 
        (result['trade_date'] == '20240103')
    ]
    assert len(new_record) == 1
    assert float(new_record.iloc[0]['close']) == 10.9
    
    print("✅ 复合主键upsert操作测试通过")
    
    operator.close()

def test_empty_data_handling():
    """测试空数据处理"""
    print("\n🧪 测试空数据处理...")
    
    operator = SchemaDataOperator('stock_schema.toml')
    
    # 创建表
    operator.initialize()
    
    # 测试空DataFrame
    empty_data = pd.DataFrame()
    operator.upsert_data('stock_basic', empty_data)
    
    # 验证表仍为空
    result = operator.query_data('stock_basic')
    assert len(result) == 0
    
    print("✅ 空数据处理测试通过")
    
    operator.close()

def main():
    """运行所有测试"""
    print("🚀 开始运行 Upsert 功能测试套件...")
    
    try:
        test_single_record_insert()
        test_single_record_update()
        test_batch_records_insert()
        test_batch_records_mixed_operations()
        test_composite_primary_key_upsert()
        test_empty_data_handling()
        
        print("\n🎉 所有 Upsert 功能测试通过！")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        raise

if __name__ == "__main__":
    main()