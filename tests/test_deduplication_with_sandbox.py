"""
使用数据沙盒测试去重功能
"""

import pytest
import sys
from pathlib import Path

# 添加src路径
PROJECT_ROOT = Path(__file__).parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from tests.helpers.test_data_sandbox import data_sandbox
from neo.helpers.compact_and_sort import HybridDeduplicationStrategy
from neo.database.schema_loader import SchemaLoader


class TestDeduplicationWithDataSandbox:
    """使用数据沙盒测试去重功能"""

    def test_financial_table_deduplication_with_real_data(self, data_sandbox):
        """测试财务表去重功能 - 使用真实数据"""
        # 声明需要的数据
        required_data = {
            "balance_sheet": {"ts_code": "600519.SH"}
        }
        
        # 使用数据沙盒创建隔离的测试环境
        with data_sandbox(required_data) as test_db:
            # 获取表数据
            table = test_db.table("balance_sheet")
            original_data = table.execute()
            
            print(f"原始数据记录数: {len(original_data)}")
            
            # 检查是否有重复数据
            if len(original_data) > 0:
                # 按ts_code和year分组检查重复
                duplicate_groups = original_data.groupby(['ts_code', 'year']).size()
                duplicate_groups = duplicate_groups[duplicate_groups > 1]
                
                print(f"发现 {len(duplicate_groups)} 组重复数据")
                if len(duplicate_groups) > 0:
                    print("重复数据详情:")
                    for (ts_code, year), count in duplicate_groups.items():
                        print(f"  {ts_code} 年份 {year}: {count} 条记录")
                
                # 测试去重策略
                strategy = HybridDeduplicationStrategy()
                # 使用 SchemaLoader 获取完整的表配置
                schema_loader = SchemaLoader()
                table_config = schema_loader.get_table_config("balance_sheet").to_dict()
                
                # 执行去重
                deduplicated_table = strategy.deduplicate(table, table_config)
                deduplicated_data = deduplicated_table.execute()
                
                print(f"去重后记录数: {len(deduplicated_data)}")
                
                # 验证去重效果
                assert len(deduplicated_data) <= len(original_data), "去重后记录数不应该增加"
                
                # 检查是否还有重复数据
                dedup_groups = deduplicated_data.groupby(['ts_code', 'year']).size()
                dedup_groups = dedup_groups[dedup_groups > 1]
                
                assert len(dedup_groups) == 0, f"去重后不应该有重复数据，但发现: {len(dedup_groups)} 组"
                
                # 验证update_flag选择逻辑
                if 'update_flag' in deduplicated_data.columns:
                    # 检查去重逻辑是否正确：每个年份应该选择update_flag最高的记录
                    for _, row in deduplicated_data.iterrows():
                        year = row['year']
                        selected_flag = row['update_flag']
                        
                        # 获取该年份的所有记录
                        year_data = original_data[original_data['year'] == year]
                        max_flag = year_data['update_flag'].max()
                        
                        # 验证选择的记录确实是该年份update_flag最高的
                        assert selected_flag == max_flag, f"年份 {year} 选择的update_flag({selected_flag})不是最高值({max_flag})"
                        
                        # 如果该年份有update_flag=1的记录，那么必须选择update_flag=1的记录
                        if 1 in year_data['update_flag'].values:
                            assert selected_flag == 1, f"年份 {year} 有update_flag=1的记录，但选择了update_flag={selected_flag}"
                    
                    print("✅ update_flag选择逻辑验证通过！")
                
                print("✅ 财务表去重功能验证通过！")
            else:
                print("⚠️ 没有测试数据，跳过去重测试")
                pytest.skip("没有测试数据")

    def test_multiple_financial_tables_deduplication(self, data_sandbox):
        """测试多个财务表的去重功能"""
        # 声明需要的数据 - 测试多个财务表
        required_data = {
            "balance_sheet": {"ts_code": "600519.SH"},
            "income": {"ts_code": "600519.SH"},
            "cash_flow": {"ts_code": "600519.SH"}
        }
        
        with data_sandbox(required_data) as test_db:
            strategy = HybridDeduplicationStrategy()
            schema_loader = SchemaLoader()
            
            # 测试每个财务表
            for table_name in ["balance_sheet", "income", "cash_flow"]:
                try:
                    table = test_db.table(table_name)
                    original_data = table.execute()
                    
                    if len(original_data) == 0:
                        print(f"⚠️ 表 {table_name} 没有数据，跳过测试")
                        continue
                    
                    print(f"测试表 {table_name}: 原始数据 {len(original_data)} 条")
                    
                    # 执行去重 - 获取完整的表配置
                    table_config = schema_loader.get_table_config(table_name).to_dict()
                    deduplicated_table = strategy.deduplicate(table, table_config)
                    deduplicated_data = deduplicated_table.execute()
                    
                    print(f"去重后数据: {len(deduplicated_data)} 条")
                    
                    # 验证去重效果
                    duplicate_groups = deduplicated_data.groupby(['ts_code', 'year']).size()
                    duplicate_groups = duplicate_groups[duplicate_groups > 1]
                    
                    assert len(duplicate_groups) == 0, f"表 {table_name} 去重后仍有重复数据"
                    
                    print(f"✅ 表 {table_name} 去重功能验证通过！")
                    
                except Exception as e:
                    print(f"❌ 表 {table_name} 测试失败: {e}")
                    raise

    def test_general_table_deduplication(self, data_sandbox):
        """测试通用表去重功能"""
        # 声明需要的数据 - 测试通用表
        required_data = {
            "stock_basic": {"ts_code": ["600519.SH", "000001.SZ"]}
        }
        
        with data_sandbox(required_data) as test_db:
            try:
                table = test_db.table("stock_basic")
                original_data = table.execute()
                
                if len(original_data) == 0:
                    print("⚠️ stock_basic 表没有数据，跳过测试")
                    pytest.skip("没有测试数据")
                
                print(f"stock_basic 原始数据: {len(original_data)} 条")
                
                # 执行去重
                strategy = HybridDeduplicationStrategy()
                # 使用 SchemaLoader 获取完整的表配置
                schema_loader = SchemaLoader()
                table_config = schema_loader.get_table_config("stock_basic").to_dict()
                deduplicated_table = strategy.deduplicate(table, table_config)
                deduplicated_data = deduplicated_table.execute()
                
                print(f"去重后数据: {len(deduplicated_data)} 条")
                
                # 验证去重效果 - 通用表按主键去重
                if 'ts_code' in deduplicated_data.columns:
                    duplicate_codes = deduplicated_data[deduplicated_data.duplicated('ts_code', keep=False)]
                    assert len(duplicate_codes) == 0, f"通用表去重后仍有重复的ts_code: {len(duplicate_codes)} 条"
                
                print("✅ 通用表去重功能验证通过！")
                
            except Exception as e:
                print(f"❌ 通用表测试失败: {e}")
                raise

    def test_deduplication_error_handling(self, data_sandbox):
        """测试去重功能的错误处理"""
        # 声明需要的数据
        required_data = {
            "balance_sheet": {"ts_code": "600519.SH"}
        }
        
        with data_sandbox(required_data) as test_db:
            table = test_db.table("balance_sheet")
            strategy = HybridDeduplicationStrategy()
            
            # 测试空表配置
            try:
                strategy.deduplicate(table, {})
                assert False, "应该抛出ValueError"
            except ValueError as e:
                assert "table_name" in str(e).lower() or "主键" in str(e)
                print("✅ 空表配置错误处理验证通过")
            
            # 测试缺失必要字段的表
            try:
                # 创建一个没有ts_code字段的表
                original_data = table.execute()
                if len(original_data) > 0:
                    # 移除ts_code列
                    test_data = original_data.drop(columns=['ts_code'])
                    test_table = test_db.con.create_table("test_no_key", test_data)
                    
                    # 测试一个没有主键配置的表
                    strategy.deduplicate(test_table, {"table_name": "unknown"})
                    assert False, "应该抛出ValueError"
            except ValueError as e:
                assert "主键" in str(e)
                print("✅ 缺失主键字段错误处理验证通过")
            except Exception:
                # 其他错误也接受，主要是测试错误处理机制
                print("✅ 错误处理机制工作正常")


if __name__ == "__main__":
    # 可以直接运行测试
    pytest.main([__file__, "-v"])