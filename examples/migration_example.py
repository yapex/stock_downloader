#!/usr/bin/env python3
"""
数据迁移工具使用示例

本示例展示如何使用 DataMigration 工具将现有的"每股票每数据类型一张表"架构
迁移到"按数据类型分区表"架构。
"""

import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from downloader.migration import DataMigration
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """主函数：演示数据迁移流程"""
    
    # 数据库文件路径
    db_path = "data/stock.db"
    
    print("=== 股票数据迁移工具示例 ===")
    print(f"数据库路径: {db_path}")
    print()
    
    # 1. 初始化迁移工具
    print("1. 初始化迁移工具...")
    migration = DataMigration(db_path)
    
    # 2. 分析现有数据结构
    print("\n2. 分析现有数据结构...")
    analysis = migration.analyze_existing_data()
    
    print(f"   总表数: {analysis['total_tables']}")
    print(f"   数据类型: {list(analysis['data_types'].keys())}")
    
    for data_type, info in analysis['data_types'].items():
        print(f"   - {data_type}: {info['table_count']} 张表, {len(info['stock_codes'])} 只股票")
    
    if analysis['total_tables'] == 0:
        print("   没有找到需要迁移的数据，退出。")
        return
    
    # 3. 选择迁移方式
    print("\n3. 迁移选项:")
    print("   a) 迁移所有数据")
    print("   b) 按数据类型迁移")
    print("   c) 迁移指定股票")
    print("   d) 仅分析，不迁移")
    
    choice = input("\n请选择 (a/b/c/d): ").lower().strip()
    
    if choice == 'a':
        # 迁移所有数据
        print("\n开始迁移所有数据...")
        success = migration.migrate_all_data(batch_size=1000)
        if success:
            print("✓ 所有数据迁移完成")
        else:
            print("✗ 部分数据迁移失败")
            
    elif choice == 'b':
        # 按数据类型迁移
        data_types = list(analysis['data_types'].keys())
        print(f"\n可用的数据类型: {data_types}")
        data_type = input("请输入要迁移的数据类型: ").strip()
        
        if data_type in data_types:
            print(f"\n开始迁移数据类型: {data_type}")
            success = migration.migrate_data_type(data_type, batch_size=1000)
            if success:
                print(f"✓ 数据类型 {data_type} 迁移完成")
            else:
                print(f"✗ 数据类型 {data_type} 迁移失败")
        else:
            print(f"✗ 无效的数据类型: {data_type}")
            
    elif choice == 'c':
        # 迁移指定股票
        all_stocks = set()
        for info in analysis['data_types'].values():
            all_stocks.update(info['stock_codes'])
        
        print(f"\n可用的股票代码: {sorted(list(all_stocks))[:10]}...")
        stock_codes_input = input("请输入股票代码 (用逗号分隔): ").strip()
        stock_codes = [code.strip() for code in stock_codes_input.split(',') if code.strip()]
        
        if stock_codes:
            print(f"\n开始迁移股票: {stock_codes}")
            success = migration.migrate_all_data(stock_codes=stock_codes, batch_size=1000)
            if success:
                print(f"✓ 股票 {stock_codes} 迁移完成")
            else:
                print(f"✗ 股票 {stock_codes} 迁移失败")
        else:
            print("✗ 未提供有效的股票代码")
            
    elif choice == 'd':
        print("\n仅分析模式，不执行迁移。")
        
    else:
        print("\n✗ 无效的选择")
        return
    
    # 4. 验证迁移结果（如果执行了迁移）
    if choice in ['a', 'b', 'c']:
        print("\n4. 验证迁移结果...")
        verification = migration.verify_migration()
        
        if verification['success']:
            print("✓ 迁移验证通过")
            for data_type, result in verification['details'].items():
                if result['success']:
                    print(f"   - {data_type}: ✓ 数据一致 ({result['old_count']} -> {result['new_count']} 行)")
                else:
                    print(f"   - {data_type}: ✗ 数据不一致 ({result['old_count']} vs {result['new_count']} 行)")
        else:
            print("✗ 迁移验证失败")
    
    # 5. 清理选项
    if choice in ['a', 'b', 'c']:
        print("\n5. 清理旧表选项:")
        cleanup_choice = input("是否删除旧表? (y/N): ").lower().strip()
        
        if cleanup_choice == 'y':
            print("\n开始清理旧表...")
            # 先试运行
            dry_run_result = migration.cleanup_old_tables(dry_run=True)
            print(f"试运行结果: 将删除 {len(dry_run_result)} 张表")
            
            confirm = input("确认删除? (y/N): ").lower().strip()
            if confirm == 'y':
                cleanup_result = migration.cleanup_old_tables(dry_run=False)
                print(f"✓ 已删除 {len(cleanup_result)} 张旧表")
            else:
                print("取消删除操作")
        else:
            print("保留旧表")
    
    print("\n=== 迁移完成 ===")

if __name__ == "__main__":
    main()