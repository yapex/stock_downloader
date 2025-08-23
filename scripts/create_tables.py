#!/usr/bin/env python3
"""
创建所有数据库表的脚本

使用 SchemaTableCreator 根据 schema 配置创建所有表
"""

import sys
import logging
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from neo.database.table_creator import SchemaTableCreator  # noqa: E402

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def main():
    """删除并重新创建所有表"""
    try:
        logger.info("开始重新创建数据库表...")

        # 创建表创建器实例
        creator = SchemaTableCreator()

        # 先删除所有表
        logger.info("删除现有表...")
        drop_results = creator.drop_all_tables()
        
        # 统计删除结果
        successful_drops = [table for table, success in drop_results.items() if success]
        failed_drops = [table for table, success in drop_results.items() if not success]
        
        if successful_drops:
            logger.info(
                f"成功删除 {len(successful_drops)} 个表: {', '.join(successful_drops)}"
            )
        
        if failed_drops:
            logger.warning(
                f"删除失败 {len(failed_drops)} 个表: {', '.join(failed_drops)}"
            )

        # 创建所有表
        logger.info("创建新表...")
        create_results = creator.create_all_tables()

        # 统计创建结果
        successful_creates = [table for table, success in create_results.items() if success]
        failed_creates = [table for table, success in create_results.items() if not success]

        logger.info("表创建完成！")
        logger.info(
            f"成功创建 {len(successful_creates)} 个表: {', '.join(successful_creates)}"
        )

        if failed_creates:
            logger.warning(
                f"创建失败 {len(failed_creates)} 个表: {', '.join(failed_creates)}"
            )
            return 1

        logger.info("所有表重新创建成功！")
        return 0

    except Exception as e:
        logger.error(f"重新创建表时发生错误: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
