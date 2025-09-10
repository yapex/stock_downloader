#!/usr/bin/env python3
"""
分红数据比较脚本
功能：比较 Tushare API 的分红数据与本地 Parquet 文件中的分红数据，识别差异
作者：YaPEX
日期：2025-09-10
"""

import pandas as pd
import logging
import sys
from datetime import datetime
from pathlib import Path

# 设置日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)

logger = logging.getLogger(__name__)

# 添加项目路径到 sys.path
project_root = Path(__file__).parent
if str(project_root / "src") not in sys.path:
    sys.path.insert(0, str(project_root / "src"))

try:
    from neo.containers import AppContainer
    from neo.downloader.fetcher_builder import FetcherBuilder
    from neo.database.operator import ParquetDBQueryer
    import duckdb
except ImportError as e:
    logger.error(f"导入项目依赖失败：{e}")
    logger.error("请确保项目依赖已正确安装且路径设置正确")
    sys.exit(1)

# 常量配置
SYMBOL = "600519.SH"
DIVIDEND_TASK_TYPE = "dividend"

def init_services():
    """初始化容器和服务"""
    logger.info("正在初始化服务...")
    try:
        # 初始化依赖注入容器
        container = AppContainer()
        
        # 获取必要的服务实例
        fetcher_builder = container.fetcher_builder()
        db_queryer = container.db_queryer()
        
        logger.info("✅ 服务初始化成功")
        return fetcher_builder, db_queryer
        
    except Exception as e:
        logger.error(f"❌ 服务初始化失败：{e}")
        raise

def fetch_tushare_dividend_data(fetcher_builder, symbol):
    """从 Tushare API 获取分红数据"""
    logger.info(f"正在从 Tushare API 获取 {symbol} 的分红数据...")
    try:
        # 创建 dividend 类型的数据获取器
        fetcher = fetcher_builder.build_by_task(DIVIDEND_TASK_TYPE, symbol=symbol)
        
        # 获取分红数据
        tushare_data = fetcher()
        
        if tushare_data is None or tushare_data.empty:
            logger.warning(f"⚠️  Tushare API 返回的 {symbol} 分红数据为空")
            return pd.DataFrame()
        
        logger.info(f"✅ 成功获取 Tushare 分红数据，记录数：{len(tushare_data)}")
        logger.debug(f"Tushare 数据列：{list(tushare_data.columns)}")
        
        return tushare_data
        
    except Exception as e:
        logger.error(f"❌ 获取 Tushare 分红数据失败：{e}")
        raise

def query_local_dividend_data(db_queryer, symbol):
    """从本地 Parquet 文件查询分红数据"""
    logger.info(f"正在从本地数据库查询 {symbol} 的分红数据...")
    try:
        # 构建查询 SQL
        parquet_base_path = db_queryer.parquet_base_path
        dividend_table_path = parquet_base_path / "dividend"
        
        if not dividend_table_path.exists():
            logger.warning(f"⚠️  本地分红数据表路径不存在：{dividend_table_path}")
            return pd.DataFrame()
        
        parquet_pattern = str(dividend_table_path / "**" / "*.parquet")
        
        # 使用 DuckDB 查询 Parquet 文件
        conn = duckdb.connect(":memory:")
        sql = f"""
            SELECT * FROM read_parquet('{parquet_pattern}') 
            WHERE ts_code = '{symbol}'
            ORDER BY end_date
        """
        
        local_data = conn.execute(sql).df()
        conn.close()
        
        if local_data.empty:
            logger.warning(f"⚠️  本地数据库中未找到 {symbol} 的分红数据")
        else:
            logger.info(f"✅ 成功查询本地分红数据，记录数：{len(local_data)}")
            logger.debug(f"本地数据列：{list(local_data.columns)}")
        
        return local_data
        
    except Exception as e:
        logger.error(f"❌ 查询本地分红数据失败：{e}")
        raise

def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("分红数据比较脚本开始执行")
    logger.info(f"目标股票代码：{SYMBOL}")
    logger.info(f"执行时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    try:
        # 1. 初始化容器和服务
        fetcher_builder, db_queryer = init_services()
        
        # 2. 获取 Tushare 数据
        tushare_data = fetch_tushare_dividend_data(fetcher_builder, SYMBOL)
        
        # 3. 查询本地数据
        local_data = query_local_dividend_data(db_queryer, SYMBOL)
        
        # TODO: 数据预处理
        # TODO: 执行比较分析
        # TODO: 生成报告
        
        logger.info("✅ 分红数据比较脚本执行完成")
        
    except Exception as e:
        logger.error(f"❌ 脚本执行失败：{e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
