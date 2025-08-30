#!/usr/bin/env python3

import duckdb
import logging

# 设置日志
from src.neo.helpers.utils import setup_logging
setup_logging("direct_read_test", "info")

logger = logging.getLogger(__name__)

def verify_parquet_files_directly(task_type: str, symbol: str):
    """直接读取 Parquet 文件进行验证，完全绕过 metadata.db"""
    logger.info(f"--- 开始直接验证 Parquet 文件: {task_type} for {symbol} ---")
    
    # DuckDB 支持 glob 语法来读取分区目录下的所有文件
    parquet_path_pattern = f"data/parquet/{task_type}/**/*.parquet"
    
    try:
        # 使用内存模式的 DuckDB 连接
        with duckdb.connect(":memory:") as con:
            logger.info(f"使用查询: SELECT count(*) FROM read_parquet('{parquet_path_pattern}') WHERE ts_code = '{symbol}'")
            
            # 执行查询
            result = con.execute(
                f"SELECT count(*) FROM read_parquet(? , hive_partitioning=1, union_by_name=True) WHERE ts_code = ?", 
                [parquet_path_pattern, symbol]
            ).fetchone()
            
            count = result[0] if result else 0
            
            if count > 0:
                logger.info(f"✅ 验证成功: 在 {task_type} 的 Parquet 文件中找到了 {count} 条关于 {symbol} 的记录。")
            else:
                logger.error(f"❌ 验证失败: 在 {task_type} 的 Parquet 文件中没有找到关于 {symbol} 的记录。")
                
    except Exception as e:
        logger.error(f"执行直接验证时发生异常: {e}", exc_info=True)

if __name__ == "__main__":
    target_symbol = "000001.SZ"
    verify_parquet_files_directly("cash_flow", target_symbol)
    verify_parquet_files_directly("income_statement", target_symbol)