"""Neo包主程序

基于四层架构的最小可运行程序。
"""

import logging
from typing import List

from neo.task_bus.types import DownloadTaskConfig, TaskType, TaskPriority
from neo.downloader import SimpleDownloader
from neo.data_processor import SimpleDataProcessor
from neo.database import DBOperator
from neo.task_bus import HueyTaskBus
from neo.config import get_config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NeoApp:
    """Neo应用主类
    
    整合四层架构的各个组件。
    """
    
    def __init__(self):
        """初始化Neo应用"""
        logger.info("初始化Neo应用...")
        
        # 初始化各层组件
        self.database = DBOperator()
        self.data_processor = SimpleDataProcessor()
        self.downloader = SimpleDownloader()
        self.task_bus = HueyTaskBus(self.data_processor)
        
        logger.info("Neo应用初始化完成")
    
    def run_producer_mode(self, tasks: List[DownloadTaskConfig]):
        """运行生产者模式
        
        Args:
            tasks: 要执行的任务列表
        """
        logger.info(f"启动生产者模式，任务数量: {len(tasks)}")
        
        for task in tasks:
            try:
                # 使用downloader下载数据
                result = self.downloader.download(task)
                
                # 将结果提交到任务总线
                self.task_bus.submit_task(result)
                
                logger.info(f"任务已提交: {task.task_type.value}, symbol: {task.symbol}")
                
            except Exception as e:
                logger.error(f"任务执行失败: {task.task_type.value}, symbol: {task.symbol}, error: {e}")
        
        logger.info("生产者模式执行完成")
    
    def run_consumer_mode(self):
        """运行消费者模式
        
        注意：实际使用中应该通过命令行启动Huey consumer。
        """
        logger.info("启动消费者模式")
        self.task_bus.start_consumer()
    
    def run_demo(self):
        """运行演示程序
        
        展示完整的数据流：下载 -> 任务总线 -> 数据处理 -> 数据库
        """
        logger.info("=== Neo包演示程序 ===")
        
        # 创建示例任务
        tasks = [
            DownloadTaskConfig(
                symbol="000001",
                task_type=TaskType.STOCK_BASIC,
                priority=TaskPriority.HIGH
            ),
            DownloadTaskConfig(
                symbol="000002",
                task_type=TaskType.STOCK_DAILY,
                priority=TaskPriority.MEDIUM
            ),
            DownloadTaskConfig(
                symbol="000003",
                task_type=TaskType.DAILY_BASIC,
                priority=TaskPriority.LOW
            )
        ]
        
        # 执行任务（同步模式，用于演示）
        for task in tasks:
            logger.info(f"\n--- 处理任务: {task.task_type.value}, symbol: {task.symbol} ---")
            
            # 1. 下载数据
            logger.info("1. 执行下载...")
            result = self.downloader.download(task)
            
            if result.success:
                logger.info(f"下载成功，数据行数: {len(result.data)}")
                
                # 2. 数据处理
                logger.info("2. 执行数据处理...")
                process_success = self.data_processor.process(result)
                
                if process_success:
                    logger.info("数据处理成功")
                else:
                    logger.warning("数据处理失败")
            else:
                logger.warning(f"下载失败: {result.error}")
        
        logger.info("\n=== 演示程序完成 ===")


def main():
    """主函数"""
    try:
        app = NeoApp()
        app.run_demo()
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        raise


if __name__ == "__main__":
    main()