#!/usr/bin/env python3
"""
Producer主程序
独立运行的producer进程，生成下载任务并放入Huey队列
"""

import logging
import sys
from pathlib import Path
from typing import List

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

from downloader.config import get_config
from downloader.producer.downloader_manager import DownloaderManager
from downloader.task.task_scheduler import create_task_configs

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main(task_groups: List[str] = None):
    """Producer主函数
    
    Args:
        task_groups: 要执行的任务组列表，如果为None则执行所有任务
    """
    try:
        # 加载配置
        config = get_config()
        logger.info(f"Producer启动，任务组: {task_groups or 'all'}")
        
        # 创建任务配置
        if task_groups is None:
            task_groups = ['all']
            
        task_configs = create_task_configs(task_groups)
        logger.info(f"创建了 {len(task_configs)} 个任务配置")
        
        # 创建下载管理器
        downloader_manager = DownloaderManager()
        
        # 添加下载任务到队列
        for task_config in task_configs:
            downloader_manager.add_download_tasks([task_config])
            
        logger.info(f"所有任务已添加到队列，共 {len(task_configs)} 个任务")
        
    except Exception as e:
        logger.error(f"Producer运行出错: {e}")
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Stock Downloader Producer')
    parser.add_argument(
        '--groups', 
        nargs='+', 
        help='任务组列表，例如: --groups stock_basic financial'
    )
    
    args = parser.parse_args()
    main(args.groups)