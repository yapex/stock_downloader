"""集成测试：验证compact_and_sort的真实去重功能"""

import pytest
import tempfile
import shutil
from pathlib import Path
import duckdb
import ibis
from unittest.mock import patch

# 添加src路径
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

# 导入需要的模块
from scripts.compact_and_sort import optimize_table
from neo.helpers.compact_and_sort import (
    CompactAndSortServiceImpl,
    HybridDeduplicationStrategy,
    UUIDFilenameGenerator
)


class TestCompactAndSortIntegration:
    """集成测试：使用真实数据验证去重功能"""

    @pytest.fixture
    def temp_data_dir(self):
        """创建临时数据目录"""
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def memory_db(self):
        """创建内存数据库连接"""
        return duckdb.connect(":memory:")

    @pytest.fixture
    def real_db_connection(self):
        """连接到真实的数据库"""
        # 尝试连接到metadata.db
        db_path = PROJECT_ROOT / "data" / "metadata.db"
        if not db_path.exists():
            pytest.skip(f"真实数据库不存在: {db_path}")
        return duckdb.connect(str(db_path))

    @pytest.fixture
    def populated_memory_db(self, memory_db, real_db_connection):
        """从真实数据库拷贝600519.SH的balance_sheet数据到内存数据库"""
        try:
            # 检查balance_sheet表是否存在
            tables = real_db_connection.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables]
            
            if 'balance_sheet' not in table_names:
                pytest.skip("balance_sheet表不存在")
            
            # 拷贝600519.SH的所有数据
            result = real_db_connection.execute("""
                SELECT * FROM balance_sheet 
                WHERE ts_code = '600519.SH'
            """).fetchall()
            
            if not result:
                pytest.skip("没有找到600519.SH的balance_sheet数据")
            
            # 获取列名
            columns = [desc[0] for desc in real_db_connection.description]
            
            # 在内存数据库中创建表
            columns_def = []
            for col in columns:
                # 简单的类型映射
                if col in ['update_flag']:
                    columns_def.append(f"{col} INTEGER")
                elif col in ['total_assets', 'total_liab', 'total_share', 'total_cur_assets', 'total_cur_liab']:
                    columns_def.append(f"{col} DOUBLE")
                else:
                    columns_def.append(f"{col} VARCHAR")
            
            create_table_sql = f"""
                CREATE TABLE balance_sheet (
                    {', '.join(columns_def)}
                )
            """
            memory_db.execute(create_table_sql)
            
            # 插入数据
            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = f"INSERT INTO balance_sheet VALUES ({placeholders})"
            memory_db.executemany(insert_sql, result)
            
            print(f"成功拷贝{len(result)}条600519.SH的balance_sheet数据到内存数据库")
            
            # 显示数据样本
            sample_data = memory_db.execute("SELECT * FROM balance_sheet LIMIT 3").fetchall()
            print("数据样本:")
            for row in sample_data:
                print(f"  {row}")
            
            return memory_db
            
        except Exception as e:
            pytest.skip(f"无法从真实数据库拷贝数据: {e}")

    def test_integration_setup(self, populated_memory_db):
        """测试数据库设置是否正确"""
        # 验证数据总数
        result = populated_memory_db.execute("SELECT COUNT(*) FROM balance_sheet").fetchone()
        print(f"600519.SH的balance_sheet数据总数: {result[0]}")
        
        # 检查是否有重复数据（按ts_code和年份分组）
        duplicate_groups = populated_memory_db.execute("""
            SELECT ts_code, year, COUNT(*) as count 
            FROM balance_sheet 
            GROUP BY ts_code, year 
            HAVING COUNT(*) > 1
        """).fetchall()
        
        print(f"重复数据组: {duplicate_groups}")
        
        # 检查update_flag的分布
        update_flag_dist = populated_memory_db.execute("""
            SELECT year, update_flag, COUNT(*) 
            FROM balance_sheet 
            GROUP BY year, update_flag 
            ORDER BY year, update_flag
        """).fetchall()
        
        print("update_flag分布:")
        for row in update_flag_dist:
            print(f"  {row}")

    def test_compact_and_sort_with_real_data(self, populated_memory_db):
        """测试真实的去重功能（专注于去重逻辑）"""
        # 直接使用去重策略测试去重功能
        print("开始测试去重功能...")
        
        # 创建一个新的 ibis 连接，并从原数据库拷贝数据
        con = ibis.duckdb.connect(":memory:")
        
        # 从原数据库获取数据并导入到新连接中
        data = populated_memory_db.execute("SELECT * FROM balance_sheet").fetchall()
        columns = [desc[0] for desc in populated_memory_db.description]
        
        # 创建表结构
        columns_def = []
        for col in columns:
            if col in ['update_flag']:
                columns_def.append(f"{col} INTEGER")
            elif col in ['total_assets', 'total_liab', 'total_share', 'total_cur_assets', 'total_cur_liab']:
                columns_def.append(f"{col} DOUBLE")
            else:
                columns_def.append(f"{col} VARCHAR")
        
        create_table_sql = f"""
            CREATE TABLE balance_sheet (
                {', '.join(columns_def)}
            )
        """
        con.raw_sql(create_table_sql)
        
        # 插入数据
        duckdb_con = con.con
        placeholders = ', '.join(['?' for _ in columns])
        insert_sql = f"INSERT INTO balance_sheet VALUES ({placeholders})"
        duckdb_con.executemany(insert_sql, data)
        
        # 获取ibis表
        table = con.table("balance_sheet")
        
        # 测试去重策略
        strategy = HybridDeduplicationStrategy()
        table_config = {"table_name": "balance_sheet"}
        
        # 执行去重
        print("执行去重策略...")
        deduplicated_table = strategy.deduplicate(table, table_config)
        
        # 验证结果
        result = deduplicated_table.execute()
        original_count = len(table.execute())
        deduplicated_count = len(result)
        
        print(f"去重前记录数: {original_count}")
        print(f"去重后记录数: {deduplicated_count}")
        
        # 验证去重效果
        assert deduplicated_count <= original_count, "去重后记录数不应该增加"
        assert deduplicated_count < original_count, "应该有重复数据被去除"
        
        # 检查每个(ts_code, year)组合是否只有一条记录
        duplicate_groups = result.groupby(['ts_code', 'year']).size()
        duplicate_groups = duplicate_groups[duplicate_groups > 1]
        
        print(f"去重后重复组数量: {len(duplicate_groups)}")
        assert len(duplicate_groups) == 0, f"去重后不应该有重复数据，但发现: {duplicate_groups}"
        
        # 验证去重选择逻辑
        print("验证update_flag选择...")
        update_flag_counts = result['update_flag'].value_counts()
        print(f"去重后update_flag分布: {update_flag_counts.to_dict()}")
        
        # 验证每个年份都选择了该年份中update_flag最大值
        original_df = table.execute()
        for year in result['year'].unique():
            year_result = result[result['year'] == year]
            year_original = original_df[original_df['year'] == year]
            
            selected_flag = year_result['update_flag'].iloc[0]
            max_flag = year_original['update_flag'].max()
            
            assert selected_flag == max_flag, f"年份 {year} 应该选择update_flag={max_flag}，但实际选择了{selected_flag}"
        
        # 验证去重前后的具体数据
        print("\n去重效果详细验证:")
        
        # 统计去重前的年份分布
        original_df = table.execute()
        original_year_counts = original_df.groupby(['year', 'update_flag']).size().unstack(fill_value=0)
        print("去重前年份-update_flag分布:")
        print(original_year_counts)
        
        # 统计去重后的年份分布
        dedup_year_counts = result.groupby(['year', 'update_flag']).size().unstack(fill_value=0)
        print("\n去重后年份-update_flag分布:")
        print(dedup_year_counts)
        
        # 验证每个年份都只保留了一条记录（去重效果）
        for year in result['year'].unique():
            year_data = result[result['year'] == year]
            assert len(year_data) == 1, f"年份{year}应该只有1条记录，实际有{len(year_data)}条"
            
            # 验证选择的是该年份中update_flag最大值
            year_original = original_df[original_df['year'] == year]
            max_flag = year_original['update_flag'].max()
            selected_flag = year_data['update_flag'].iloc[0]
            assert selected_flag == max_flag, f"年份{year}应该选择update_flag={max_flag}，但实际选择了{selected_flag}"
        
        print("✅ 去重功能验证通过！")

    def test_deduplication_strategy_directly(self, populated_memory_db):
        """直接测试去重策略"""
        # 创建ibis表对象
        con = ibis.duckdb.connect(":memory:")
        
        # 从内存数据库拷贝数据到ibis连接
        data = populated_memory_db.execute("SELECT * FROM balance_sheet").fetchall()
        columns = [desc[0] for desc in populated_memory_db.description]
        
        # 创建DuckDB表
        columns_def = []
        for col in columns:
            if col in ['update_flag']:
                columns_def.append(f"{col} INTEGER")
            elif col in ['total_assets', 'total_liab', 'total_share', 'total_cur_assets', 'total_cur_liab']:
                columns_def.append(f"{col} DOUBLE")
            else:
                columns_def.append(f"{col} VARCHAR")
        
        create_table_sql = f"""
            CREATE TABLE test_table (
                {', '.join(columns_def)}
            )
        """
        con.raw_sql(create_table_sql)
        
        # 插入数据 - 使用 DuckDB 连接直接插入
        # ibis 的 raw_sql 不支持参数化插入，我们需要使用底层连接
        duckdb_con = con.con  # 获取底层 DuckDB 连接
        placeholders = ', '.join(['?' for _ in columns])
        insert_sql = f"INSERT INTO test_table VALUES ({placeholders})"
        duckdb_con.executemany(insert_sql, data)
        
        # 获取ibis表
        table = con.table("test_table")
        
        # 测试去重策略
        strategy = HybridDeduplicationStrategy()
        table_config = {"table_name": "balance_sheet"}
        
        # 执行去重
        deduplicated_table = strategy.deduplicate(table, table_config)
        
        # 验证结果
        result = deduplicated_table.execute()
        original_count = len(data)
        deduplicated_count = len(result)
        
        print(f"去重前记录数: {original_count}")
        print(f"去重后记录数: {deduplicated_count}")
        
        # 验证去重效果
        assert deduplicated_count <= original_count, "去重后记录数不应该增加"
        
        # 验证去重逻辑：每个分组应该选择最大的update_flag值
        # 根据原始数据验证去重选择是否正确
        import pandas as pd
        original_df = pd.DataFrame(data, columns=columns)
        
        # 根据ts_code和year分组，检查每个分组的选择是否正确
        for _, selected_row in result.iterrows():
            if 'ts_code' in selected_row and 'year' in selected_row and 'update_flag' in selected_row:
                ts_code = selected_row['ts_code']
                year = selected_row['year']
                selected_flag = selected_row['update_flag']
                
                # 获取该分组的所有记录
                group_data = original_df[
                    (original_df['ts_code'] == ts_code) & 
                    (original_df['year'] == year)
                ]
                
                # 验证选择的是该分组中update_flag最大的记录
                max_flag = group_data['update_flag'].max()
                assert selected_flag == max_flag, f"年份 {year} 分组应该选择update_flag={max_flag}，但实际选择了{selected_flag}"
        
        print("✅ 去重逻辑验证通过！")

    def test_uuid_filename_generation(self):
        """测试UUID文件名生成"""
        generator = UUIDFilenameGenerator()
        
        filename1 = generator.generate_filename("balance_sheet")
        filename2 = generator.generate_filename("balance_sheet")
        filename3 = generator.generate_filename("income")
        
        # 验证文件名格式
        assert filename1.startswith("balance_sheet-"), "文件名应该以表名开头"
        assert filename1.endswith(".parquet"), "文件名应该以.parquet结尾"
        assert "-" in filename1, "文件名应该包含UUID分隔符"
        
        # 验证唯一性
        assert filename1 != filename2, "两次生成的文件名应该不同"
        
        # 验证不同表名的文件名不同
        assert filename1 != filename3, "不同表名的文件名应该不同"
        
        print(f"生成的文件名: {filename1}, {filename2}, {filename3}")


if __name__ == "__main__":
    # 可以直接运行测试
    test = TestCompactAndSortIntegration()
    
    # 运行单个测试
    print("运行UUID文件名生成测试...")
    test.test_uuid_filename_generation()
    
    print("运行去重策略测试...")
    # 需要先设置数据库连接
    print("需要数据库连接，跳过直接运行")
    
    print("集成测试准备完成!")