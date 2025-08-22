#!/usr/bin/env python3
"""下载股票基础信息并保存到数据库的脚本"""

import sys
import logging
import time
from pathlib import Path

# 添加项目路径到sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from neo.config import get_config  # noqa: E402
from neo.downloader.fetcher_builder import FetcherBuilder  # noqa: E402
from neo.task_bus.types import TaskType  # noqa: E402
from neo.database.operator import DBOperator  # noqa: E402

config = get_config()
# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def main():
    """主函数：下载股票基础信息并保存到数据库"""

    try:
        # 获取配置
        logger.info(f"使用数据库: {config.database.path}")
        logger.info(f"数据库类型: {config.database.type}")

        # 初始化数据库操作器
        db_operator = DBOperator()
        logger.info("数据库操作器初始化完成")

        # 创建数据获取器
        fetcher_builder = FetcherBuilder()
        stock_basic_fetcher = fetcher_builder.build_by_task(TaskType.STOCK_BASIC)
        logger.info("数据获取器创建完成")

        # 下载股票基础信息（带重试机制）
        logger.info("开始下载股票基础信息...")
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                stock_basic_data = stock_basic_fetcher()
                logger.info(f"下载完成，共获取 {len(stock_basic_data)} 条股票基础信息")
                break
            except Exception as e:
                logger.warning(f"第 {attempt + 1} 次下载尝试失败: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                else:
                    logger.error("所有下载尝试都失败了")
                    raise

        # 显示部分数据
        logger.info("前5条数据预览:")
        logger.info(f"\n{stock_basic_data.head().to_string()}")

        # 确保表存在
        logger.info("检查并创建数据库表...")
        table_created = db_operator.create_table("stock_basic")
        if table_created:
            logger.info("stock_basic 表创建成功")
        else:
            logger.info("stock_basic 表已存在或创建失败，继续执行")

        # 保存到数据库
        logger.info("开始保存数据到数据库...")
        db_operator.upsert("stock_basic", stock_basic_data)
        logger.info("数据保存完成")

        # 验证保存结果
        logger.info("验证数据保存结果...")
        all_symbols = db_operator.get_all_symbols()
        logger.info(f"数据库中共有 {len(all_symbols)} 只股票")
        logger.info(f"前10只股票代码: {all_symbols[:10]}")

        logger.info("脚本执行完成")

    except Exception as e:
        logger.error(f"脚本执行失败: {e}")
        raise


if __name__ == "__main__":
    main()
