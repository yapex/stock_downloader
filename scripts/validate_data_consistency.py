# 数据一致性验证脚本

# 对比旧的 DuckDB 数据库和新的 Parquet 数据湖之间的数据, 确保其完全一致。

import duckdb
import pandas as pd
import logging
from typing import List

from neo.configs import get_config

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataValidator:
    """数据验证器，用于比较两个数据源"""

    def __init__(self):
        config = get_config()
        self.old_db_path = config.database.path
        self.new_db_path = config.database.metadata_path
        logging.info(f"旧数据库 (stock.db): {self.old_db_path}")
        logging.info(f"新元数据 (metadata.db): {self.new_db_path}")

    def _get_tables(self, con: duckdb.DuckDBPyConnection) -> List[str]:
        """获取数据库中的所有表名"""
        return con.execute("SHOW TABLES").fetchdf()['name'].tolist()

    def _standardize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化 DataFrame 以进行比较"""
        # 1. 将所有列名转为小写
        df.columns = [col.lower() for col in df.columns]
        # 2. 将 object/categorical 类型转为 str，以避免 dtype 差异
        for col in df.select_dtypes(include=['object', 'category']).columns:
            df[col] = df[col].astype(str)
        # 3. 按所有列排序，确保顺序一致
        df = df.sort_values(by=list(df.columns)).reset_index(drop=True)
        return df

    def validate(self):
        """执行所有表的验证"""
        try:
            with duckdb.connect(self.old_db_path, read_only=True) as old_con, \
                 duckdb.connect(self.new_db_path, read_only=True) as new_con:

                old_tables = self._get_tables(old_con)
                new_tables = self._get_tables(new_con)

                common_tables = sorted(list(set(old_tables) & set(new_tables)))
                if not common_tables:
                    logging.warning("在两个数据库中没有找到共同的表进行比较。")
                    return

                logging.info(f"将要比较的共同表: {common_tables}")
                all_passed = True

                for table in common_tables:
                    logging.info(f"--- 开始验证表: {table} ---")
                    try:
                        # 1. 比较行数
                        old_count = old_con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                        new_count = new_con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                        logging.info(f"行数比较: 旧={old_count}, 新={new_count}")
                        if old_count != new_count:
                            logging.error(f"❌ 表 {table} 行数不匹配!")
                            all_passed = False
                            continue
                        
                        if old_count == 0:
                            logging.info(f"✅ 表 {table} 为空，跳过内容比较。")
                            continue

                        # 2. 比较内容
                        logging.info("行数匹配，开始进行内容深度比较...")
                        old_df = self._standardize_df(old_con.execute(f"SELECT * FROM {table}").fetch_df())
                        new_df = self._standardize_df(new_con.execute(f"SELECT * FROM {table}").fetch_df())

                        pd.testing.assert_frame_equal(old_df, new_df)
                        logging.info(f"✅ 表 {table} 内容完全一致!")

                    except Exception as e:
                        logging.error(f"❌ 表 {table} 验证失败: {e}")
                        all_passed = False

            logging.info("=========================================")
            if all_passed:
                logging.info("🏆 所有共同表的验证都已通过!")
            else:
                logging.error("💔 部分表的验证失败，请检查以上日志。")

        except Exception as e:
            logging.error(f"执行验证时发生严重错误: {e}")

if __name__ == "__main__":
    validator = DataValidator()
    validator.validate()
