#!/usr/bin/env python3
"""验证下载数据的完整性脚本

此脚本用于：
1. 从 check_and_redown 日志文件中读取需要下载的股票列表
2. 验证这些股票在 metadata.db 中是否有数据
3. 按比例抽样检查 parquet 文件是否包含相应数据
4. 生成验证报告
"""

import logging
import sys
import json
import random
from pathlib import Path
from typing import List, Set, Dict, Tuple
import duckdb

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from neo.configs import get_config

logger = logging.getLogger(__name__)


def setup_logging():
    """设置日志配置"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def get_latest_check_log() -> Path:
    """获取最新的check_and_redown日志文件"""
    logs_dir = project_root / "logs"
    if not logs_dir.exists():
        raise FileNotFoundError("logs目录不存在")
    
    log_files = list(logs_dir.glob("check_and_redown_*.json"))
    if not log_files:
        raise FileNotFoundError("没有找到check_and_redown日志文件")
    
    # 返回最新的日志文件
    latest_log = max(log_files, key=lambda f: f.stat().st_mtime)
    logger.info(f"使用最新的日志文件: {latest_log}")
    return latest_log


def load_missing_stocks_from_log(log_file: Path) -> Dict[str, List[str]]:
    """从日志文件中加载缺失股票信息"""
    with open(log_file, 'r', encoding='utf-8') as f:
        log_data = json.load(f)
    
    missing_by_table = {}
    for table_name, table_data in log_data["missing_by_table"].items():
        if table_data["count"] > 0:
            missing_by_table[table_name] = table_data["stocks"]
    
    return missing_by_table


def verify_stocks_in_database(conn: duckdb.DuckDBPyConnection, 
                             table_name: str, 
                             stock_codes: List[str]) -> Tuple[List[str], List[str]]:
    """验证股票在数据库中的存在情况
    
    Returns:
        Tuple[List[str], List[str]]: (存在的股票, 仍然缺失的股票)
    """
    if not stock_codes:
        return [], []
    
    try:
        # 检查表是否存在
        check_table_sql = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        table_exists = conn.execute(check_table_sql).fetchone()[0] > 0
        
        if not table_exists:
            logger.warning(f"表 {table_name} 不存在")
            return [], stock_codes
        
        # 构建 IN 查询条件
        codes_str = "', '".join(stock_codes)
        sql = f"""
            SELECT DISTINCT ts_code 
            FROM {table_name} 
            WHERE ts_code IN ('{codes_str}')
        """
        
        result = conn.execute(sql).fetchall()
        existing_stocks = [row[0] for row in result]
        missing_stocks = [code for code in stock_codes if code not in existing_stocks]
        
        return existing_stocks, missing_stocks
        
    except Exception as e:
        logger.error(f"验证表 {table_name} 时发生错误: {e}")
        return [], stock_codes


def sample_verify_parquet_files(table_name: str, 
                               existing_stocks: List[str], 
                               sample_ratio: float = 0.1) -> Dict[str, any]:
    """抽样验证parquet文件中的数据
    
    Args:
        table_name: 表名
        existing_stocks: 在数据库中存在的股票列表
        sample_ratio: 抽样比例，默认10%
        
    Returns:
        Dict: 验证结果
    """
    if not existing_stocks:
        return {"sampled_count": 0, "verified_count": 0, "success_rate": 0.0}
    
    # 计算抽样数量（至少1个，最多10个）
    sample_count = max(1, min(10, int(len(existing_stocks) * sample_ratio)))
    sampled_stocks = random.sample(existing_stocks, sample_count)
    
    parquet_dir = project_root / "data/parquet" / table_name
    if not parquet_dir.exists():
        logger.warning(f"Parquet目录不存在: {parquet_dir}")
        return {"sampled_count": sample_count, "verified_count": 0, "success_rate": 0.0}
    
    verified_count = 0
    
    for stock_code in sampled_stocks:
        # 查找包含该股票的parquet文件
        found_files = list(parquet_dir.rglob("*.parquet"))
        stock_found = False
        
        for parquet_file in found_files[:5]:  # 最多检查5个文件
            try:
                # 使用DuckDB直接查询parquet文件
                with duckdb.connect() as temp_conn:
                    check_sql = f"""
                        SELECT COUNT(*) 
                        FROM read_parquet('{parquet_file}') 
                        WHERE ts_code = '{stock_code}'
                        LIMIT 1
                    """
                    result = temp_conn.execute(check_sql).fetchone()
                    if result and result[0] > 0:
                        stock_found = True
                        break
            except Exception as e:
                # 忽略单个文件的读取错误
                continue
        
        if stock_found:
            verified_count += 1
    
    success_rate = verified_count / sample_count if sample_count > 0 else 0.0
    
    return {
        "sampled_count": sample_count,
        "verified_count": verified_count,
        "success_rate": success_rate,
        "sampled_stocks": sampled_stocks
    }


def generate_verification_report(verification_results: Dict[str, any]) -> Path:
    """生成验证报告"""
    from datetime import datetime
    
    logs_dir = project_root / "logs"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = logs_dir / f"verification_report_{timestamp}.json"
    
    # 添加汇总信息
    total_missing = sum(len(table_data["original_missing"]) for table_data in verification_results["tables"].values())
    total_found = sum(len(table_data["now_existing"]) for table_data in verification_results["tables"].values())
    total_still_missing = sum(len(table_data["still_missing"]) for table_data in verification_results["tables"].values())
    
    verification_results["summary"] = {
        "total_originally_missing": total_missing,
        "total_now_found": total_found,
        "total_still_missing": total_still_missing,
        "overall_success_rate": total_found / total_missing if total_missing > 0 else 0.0
    }
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(verification_results, f, indent=2, ensure_ascii=False)
    
    logger.info(f"📊 验证报告已保存到: {report_file}")
    return report_file


def main():
    """主函数"""
    setup_logging()
    logger.info("开始验证下载数据的完整性...")
    
    try:
        # 1. 获取最新的check_and_redown日志文件
        log_file = get_latest_check_log()
        
        # 2. 从日志文件中加载缺失股票信息
        missing_by_table = load_missing_stocks_from_log(log_file)
        if not missing_by_table:
            logger.info("✅ 没有需要验证的缺失股票")
            return
        
        logger.info(f"需要验证 {len(missing_by_table)} 个表的数据")
        
        # 3. 连接数据库并验证
        config = get_config()
        db_path = Path(config.database.metadata_path)
        if not db_path.is_absolute():
            db_path = project_root / db_path
        
        verification_results = {
            "timestamp": log_file.stem.replace("check_and_redown_", ""),
            "tables": {}
        }
        
        with duckdb.connect(str(db_path), read_only=True) as conn:
            for table_name, missing_stocks in missing_by_table.items():
                logger.info(f"验证表 {table_name} (原缺失 {len(missing_stocks)} 个股票)...")
                
                # 验证数据库中的存在情况
                existing_stocks, still_missing = verify_stocks_in_database(
                    conn, table_name, missing_stocks
                )
                
                logger.info(f"  数据库验证: {len(existing_stocks)} 已找到, {len(still_missing)} 仍缺失")
                
                # 抽样验证parquet文件
                parquet_verification = sample_verify_parquet_files(table_name, existing_stocks)
                
                logger.info(f"  Parquet抽样验证: {parquet_verification['verified_count']}/{parquet_verification['sampled_count']} "
                           f"({parquet_verification['success_rate']:.1%} 成功率)")
                
                verification_results["tables"][table_name] = {
                    "original_missing": missing_stocks,
                    "now_existing": existing_stocks,
                    "still_missing": still_missing,
                    "parquet_verification": parquet_verification
                }
        
        # 4. 生成验证报告
        report_file = generate_verification_report(verification_results)
        
        # 5. 输出汇总信息
        summary = verification_results["summary"]
        logger.info(f"\n📈 验证汇总:")
        logger.info(f"  原缺失股票总数: {summary['total_originally_missing']}")
        logger.info(f"  现已找到股票数: {summary['total_now_found']}")
        logger.info(f"  仍然缺失股票数: {summary['total_still_missing']}")
        logger.info(f"  整体成功率: {summary['overall_success_rate']:.1%}")
        
        if summary['total_still_missing'] > 0:
            logger.warning(f"⚠️  仍有 {summary['total_still_missing']} 个股票数据缺失，建议重新运行 check_and_redown.py")
        else:
            logger.info("🎉 所有股票数据验证通过！")
            
        logger.info("验证完成")
        
    except Exception as e:
        logger.error(f"验证过程中发生错误: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
