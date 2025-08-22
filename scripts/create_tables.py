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
    """创建所有表"""
    try:
        logger.info("开始创建数据库表...")

        # 创建表创建器实例
        creator = SchemaTableCreator()

        # 创建所有表
        results = creator.create_all_tables()

        # 统计结果
        successful_tables = [table for table, success in results.items() if success]
        failed_tables = [table for table, success in results.items() if not success]

        logger.info("表创建完成！")
        logger.info(
            f"成功创建 {len(successful_tables)} 个表: {', '.join(successful_tables)}"
        )

        if failed_tables:
            logger.warning(
                f"创建失败 {len(failed_tables)} 个表: {', '.join(failed_tables)}"
            )
            return 1

        logger.info("所有表创建成功！")
        return 0

    except Exception as e:
        logger.error(f"创建表时发生错误: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
