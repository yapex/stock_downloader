#!/usr/bin/env python3
"""检查并重新下载缺失数据的股票脚本

此脚本用于：
1. 从 config.toml 读取配置信息
2. 连接到 DuckDB 数据库
3. 识别在 financial 和 hfq 任务组中缺失数据的股票
4. 为这些股票提交 Huey 重新下载任务
"""

import logging
import sys
from pathlib import Path
from typing import List, Set, Dict
import duckdb

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from neo.configs import get_config
from neo.app import container

logger = logging.getLogger(__name__)


def setup_logging():
    """设置日志配置"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def get_database_path() -> Path:
    """从配置文件获取数据库路径"""
    config = get_config()
    db_path = Path(config.database.metadata_path)
    if not db_path.is_absolute():
        db_path = project_root / db_path
    return db_path


def get_task_group_tables(group_name: str) -> List[str]:
    """从配置文件获取指定任务组的表名列表"""
    config = get_config()
    task_groups = config.get("task_groups", {})
    return task_groups.get(group_name, [])


def get_all_active_stocks(conn: duckdb.DuckDBPyConnection) -> List[str]:
    """从 stock_basic 表获取所有活跃股票的 ts_code 列表"""
    try:
        sql = """
            SELECT DISTINCT ts_code 
            FROM stock_basic 
            WHERE ts_code IS NOT NULL AND ts_code != ''
            ORDER BY ts_code
        """
        result = conn.execute(sql).fetchall()
        stocks = [row[0] for row in result]
        logger.info(f"从 stock_basic 表获取到 {len(stocks)} 个活跃股票")
        return stocks
    except Exception as e:
        logger.error(f"查询 stock_basic 表失败: {e}")
        return []


def find_missing_data_by_table(
    conn: duckdb.DuckDBPyConnection, tables: List[str]
) -> Dict[str, Set[str]]:
    """为每个表单独查找缺失数据的股票

    Returns:
        Dict[str, Set[str]]: 表名到缺失股票集合的映射
    """
    if not tables:
        logger.warning("没有指定要检查的表")
        return {}

    missing_by_table = {}

    for table in tables:
        try:
            # 检查表是否存在
            check_table_sql = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table}'"
            table_exists = conn.execute(check_table_sql).fetchone()[0] > 0

            if not table_exists:
                logger.warning(f"表 {table} 不存在，跳过检查")
                missing_by_table[table] = set()
                continue

            # 使用 NOT EXISTS 找出在当前表中没有数据的股票（更高效）
            sql = f"""
                SELECT sb.ts_code
                FROM stock_basic sb
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table} t 
                    WHERE t.ts_code = sb.ts_code
                )
                ORDER BY sb.ts_code
            """

            result = conn.execute(sql).fetchall()
            table_missing = {row[0] for row in result}

            logger.info(f"表 {table} 中缺失数据的股票数量: {len(table_missing)}")
            missing_by_table[table] = table_missing

        except Exception as e:
            logger.error(f"检查表 {table} 时发生错误: {e}")
            missing_by_table[table] = set()
            continue

    return missing_by_table


def submit_precise_redownload_tasks(missing_by_table: Dict[str, Set[str]]):
    """为每个表的缺失数据股票提交精确的重新下载任务"""
    if not missing_by_table:
        logger.info("没有发现缺失数据的股票，无需提交重新下载任务")
        return

    # 构建任务类型到股票代码的映射
    task_stock_mapping = {}
    total_tasks = 0

    for table_name, missing_stocks in missing_by_table.items():
        if not missing_stocks:
            continue

        missing_list = sorted(list(missing_stocks))
        logger.info(
            f"为表 '{table_name}' 的 {len(missing_list)} 个缺失股票准备下载任务"
        )

        # 显示缺失股票列表（限制输出长度）
        if len(missing_list) <= 10:
            logger.info(f"  缺失股票: {missing_list}")
        else:
            logger.info(
                f"  缺失股票: {missing_list[:5]} ... (省略 {len(missing_list) - 10} 个) ... {missing_list[-5:]}"
            )

        # 将表名作为任务类型，缺失股票作为股票列表
        task_stock_mapping[table_name] = missing_list
        total_tasks += len(missing_list)

    if task_stock_mapping:
        logger.info(f"🚀 准备提交 {total_tasks} 个精确的重新下载任务")
        logger.info(f"任务映射: {list(task_stock_mapping.keys())}")
        
        # 通过 AppService 提交任务
        app_service = container.app_service()
        app_service.build_and_submit_downloads(task_stock_mapping)
        
        logger.info(f"🎉 已成功提交 {total_tasks} 个精确的重新下载任务")
    else:
        logger.info("没有需要提交的任务")


def main():
    """主函数"""
    setup_logging()
    logger.info("开始检查并重新下载缺失数据的股票...")

    try:
        # 1. 获取数据库路径
        db_path = get_database_path()
        if not db_path.exists():
            logger.error(f"数据库文件不存在: {db_path}")
            return

        logger.info(f"连接到数据库: {db_path}")

        # 2. 获取任务组配置
        financial_tables = get_task_group_tables("financial")
        hfq_tables = get_task_group_tables("hfq")

        logger.info(f"financial 任务组包含表: {financial_tables}")
        logger.info(f"hfq 任务组包含表: {hfq_tables}")

        # 合并所有需要检查的表
        all_tables = list(set(financial_tables + hfq_tables))
        logger.info(f"总共需要检查的表: {all_tables}")

        # 3. 连接数据库并查询
        with duckdb.connect(str(db_path), read_only=True) as conn:
            # 4. 获取所有活跃股票
            all_stocks = get_all_active_stocks(conn)
            if not all_stocks:
                logger.error("无法获取活跃股票列表")
                return

            # 5. 找出每个表的缺失数据股票
            missing_by_table = find_missing_data_by_table(conn, all_tables)

        # 6. 输出结果
        total_missing = sum(len(stocks) for stocks in missing_by_table.values())
        if total_missing > 0:
            all_missing_stocks = set()
            for table, stocks in missing_by_table.items():
                all_missing_stocks.update(stocks)

            missing_list = sorted(list(all_missing_stocks))
            logger.info(f"发现 {len(missing_list)} 个股票存在数据缺失")

            # 只显示前10个和后10个股票作为示例
            if len(missing_list) <= 20:
                for i, stock in enumerate(missing_list, 1):
                    logger.info(f"  {i:3d}. {stock}")
            else:
                logger.info("前10个缺失数据的股票:")
                for i, stock in enumerate(missing_list[:10], 1):
                    logger.info(f"  {i:3d}. {stock}")
                logger.info(f"  ... (省略 {len(missing_list) - 20} 个股票) ...")
                logger.info("后10个缺失数据的股票:")
                for i, stock in enumerate(missing_list[-10:], len(missing_list) - 9):
                    logger.info(f"  {i:3d}. {stock}")

            # 7. 提交精确的重新下载任务
            submit_precise_redownload_tasks(missing_by_table)

        else:
            logger.info("✅ 所有股票的数据都完整，无需重新下载")

        logger.info("检查完成")

    except Exception as e:
        logger.error(f"执行过程中发生错误: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
