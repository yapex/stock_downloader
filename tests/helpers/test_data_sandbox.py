"""
数据沙盒测试工具
"""

import ibis
import pandas as pd
import pytest
from pathlib import Path
from typing import List, Dict, Any

from neo.configs import get_config


class LazyDataEnvironment:
    """
    一个智能的测试数据环境，按需从源数据库拷贝最小化的数据子集到
    一个临时的内存数据库沙箱中，供测试使用。
    """

    def __init__(self, table_queries: dict = None):
        """
        初始化懒加载数据环境
        
        Args:
            table_queries: 字典格式的表查询规格，格式为 {
                "table_name": {"ts_code": "000001.SZ", "start_date": "20230101"},
                "another_table": {"ts_code": ["000001.SZ", "000002.SZ"]}
            }
        """
        self.table_queries = table_queries or {}
        self.tables = list(self.table_queries.keys()) if table_queries else []
        self.test_con = None

    def _fix_null_columns(self, df):
        """
        修复全为NULL的列，将其转换为适当的默认类型。

        DuckDB不支持创建NULL类型的列，所以我们需要将这些列
        转换为具有明确类型的列。
        """

        df = df.copy()

        for col in df.columns:
            # 检查是否为全NULL列
            if df[col].isna().all():
                # 根据列名推断适当的类型
                if any(
                    keyword in col.lower()
                    for keyword in [
                        "amount",
                        "value",
                        "profit",
                        "income",
                        "asset",
                        "debt",
                        "equity",
                        "cap",
                        "payable",
                        "receivable",
                    ]
                ):
                    # 金额字段使用float64
                    df[col] = df[col].astype("float64")
                elif "date" in col.lower():
                    # 日期字段使用string
                    df[col] = df[col].astype("string")
                elif any(
                    keyword in col.lower() for keyword in ["code", "type", "flag"]
                ):
                    # 代码和标识字段使用string
                    df[col] = df[col].astype("string")
                else:
                    # 默认使用float64
                    df[col] = df[col].astype("float64")

        return df

    def merge_parquet_data(self, temp_data_path: Path):
        """
        扫描指定路径下的 Parquet 文件，并将它们加载或追加到内存数据库中。
        """
        if not self.test_con:
            return

        if temp_data_path and temp_data_path.exists():
            for parquet_file in temp_data_path.glob("**/*.parquet"):
                try:
                    # 通过相对路径获取 table_name，更健-壮
                    relative_path = parquet_file.relative_to(temp_data_path)
                    table_name = relative_path.parts[0]
                except (IndexError, ValueError):
                    continue

                temp_df = pd.read_parquet(parquet_file)
                if temp_df.empty:
                    continue

                existing_tables = self.test_con.list_tables()
                if table_name in existing_tables:
                    self.test_con.insert(table_name, temp_df)
                else:
                    self.test_con.create_table(table_name, temp_df, overwrite=True)

    def __enter__(self) -> ibis.BaseBackend:
        # 1. 连接到源数据库和创建内存沙箱
        config = get_config()
        source_con = ibis.duckdb.connect(config.database.metadata_path)
        self.test_con = ibis.duckdb.connect()

        # 2. 根据表查询规格加载数据
        for table_name, query_specs in self.table_queries.items():
            if table_name not in source_con.list_tables():
                print(f"⚠️  表 {table_name} 不存在于源数据库中")
                continue

            query = source_con.table(table_name)

            # a. 应用传入的过滤器进行筛选
            if "ts_code" in query_specs and "ts_code" in query.columns:
                ts_codes = query_specs["ts_code"]
                if isinstance(ts_codes, list):
                    query = query.filter(query.ts_code.isin(ts_codes))
                else:
                    query = query.filter(query.ts_code == ts_codes)

            date_col = next(
                (col for col in ["end_date", "trade_date"] if col in query.columns),
                None,
            )
            if date_col:
                if query_specs.get("start_date"):
                    query = query.filter(query[date_col] >= query_specs["start_date"])
                if query_specs.get("end_date"):
                    query = query.filter(query[date_col] <= query_specs["end_date"])

            # b. 执行查询并加载到内存沙箱
            try:
                subset_df = query.execute()
                if not subset_df.empty:
                    # 修复数据类型问题
                    subset_df = self._fix_null_columns(subset_df)
                    
                    # 加载到内存数据库
                    existing_tables = self.test_con.list_tables()
                    if table_name in existing_tables:
                        self.test_con.insert(table_name, subset_df)
                    else:
                        self.test_con.create_table(table_name, subset_df, overwrite=True)
                    
                    print(f"✅ 表 {table_name} 加载成功: {len(subset_df)} 条记录")
                else:
                    print(f"⚠️  表 {table_name} 没有数据")
            except Exception as e:
                print(f"❌ 加载表 {table_name} 失败: {str(e)}")
                continue

        return self.test_con

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.test_con:
            self.test_con.con.close()


@pytest.fixture(scope="session")
def project_root() -> Path:
    """返回项目的根目录"""
    return Path(__file__).parent.parent


@pytest.fixture
def data_sandbox():
    """
    数据沙箱工厂：按需从真实数据库提取最小化数据子集到内存沙箱中
    
    这是一个 pytest fixture 工厂，提供声明式的测试数据准备方式。测试用例只需声明需要什么数据，
    而不用关心如何从数据库获取和过滤数据。每个测试都会获得一个完全隔离的内存数据库环境，
    确保测试之间不会相互干扰，同时不会影响生产数据库。
    
    Args:
        table_specs (dict): 表查询规格字典，格式为 {
            "table_name": {
                "ts_code": "000001.SZ" 或 ["000001.SZ", "600519.SH"],  # 单个或多个股票代码
                "start_date": "20230101",  # 可选，开始日期
                "end_date": "20231231"     # 可选，结束日期
            },
            "another_table": {"ts_code": ["000001.SZ", "000002.SZ"]}
        }
        
        支持的过滤条件：
        - ts_code: 单个股票代码(字符串)或多个股票代码(列表)
        - start_date: 可选，过滤该日期及之后的记录
        - end_date: 可选，过滤该日期及之前的记录
    
    Returns:
        callable: 返回一个可调用的函数，该函数返回 LazyDataEnvironment 上下文管理器，
                  提供隔离的测试数据库连接。支持 with 语句使用。
    
    Usage:
        def test_my_feature(data_sandbox):
            # 声明需要的数据
            required_data = {
                "income": {"ts_code": "000001.SZ"},
                "daily_basic": {
                    "ts_code": ["000001.SZ", "600519.SH"],
                    "start_date": "20230101",
                    "end_date": "20231231"
                }
            }
            
            # 使用数据沙箱创建隔离的测试环境
            with data_sandbox(required_data) as test_db:
                engine = FeatureEngine(db_client=test_db)
                result = engine.compute(features=["my_feature"], ts_codes=["000001.SZ"])
                # 进行断言...
    
    Examples:
        更多用法示例请参考：tests/financial/feature/test_data_sandbox_example.py
    """
    def _prepare_data(table_specs: dict):
        return LazyDataEnvironment(table_queries=table_specs)
    
    return _prepare_data