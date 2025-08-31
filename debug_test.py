#!/usr/bin/env python3

"""独立的下载诊断测试脚本

用于在无队列、单进程的环境下，测试下载器的纯粹性能和速率限制器的真实效果。
"""

import time
import logging
import sys
from pathlib import Path

# 将项目根目录添加到 sys.path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root / 'src'))

from neo.helpers.utils import setup_logging
from neo.containers import AppContainer

# 设置日志
setup_logging("diagnostic_test", "info")
logger = logging.getLogger(__name__)


def run_diagnostic_test():
    """
    执行一个独立的、无队列的下载测试，以验证速率限制器的真实性能。
    """
    try:
        container = AppContainer()
        downloader = container.downloader()
    except Exception as e:
        logger.error(f"初始化容器或下载器失败: {e}", exc_info=True)
        return

    task_type = 'stock_adj_hfq'
    symbol = '000001.SZ'  # 使用一个固定的股票代码进行测试
    total_requests = 195

    logger.info("--- 开始诊断测试 ---")
    logger.info(f"任务类型: {task_type}")
    logger.info(f"请求总数: {total_requests}")
    logger.info("--------------------")

    success_count = 0
    failure_count = 0

    start_time = time.time()

    for i in range(total_requests):
        try:
            logger.info(f"发起第 {i + 1}/{total_requests} 次请求...")
            result = downloader.download(task_type, symbol=symbol)

            if result is not None and not result.empty:
                success_count += 1
                logger.debug(f"第 {i + 1} 次请求成功，获取到 {len(result)} 条数据。")
            else:
                # API调用成功但返回空数据，也算一种失败
                failure_count += 1
                logger.warning(f"第 {i + 1} 次请求返回空数据或None。")

        except Exception as e:
            failure_count += 1
            logger.error(f"第 {i + 1} 次请求执行失败: {e}")

    end_time = time.time()

    total_duration = end_time - start_time
    actual_rate = (success_count / total_duration) * 60 if total_duration > 0 else 0

    logger.info("--- 诊断测试完成 ---")
    logger.info(f"总耗时: {total_duration:.2f} 秒")
    logger.info(f"成功请求: {success_count}")
    logger.info(f"失败请求: {failure_count}")
    logger.info(f"实际速率: {actual_rate:.2f} 次/分钟")
    logger.info("--------------------")


if __name__ == "__main__":
    run_diagnostic_test()
