#!/usr/bin/env python3
"""
Consumer主程序
独立运行的consumer进程，从Huey队列中获取任务并处理
"""

import logging
import signal
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

from downloader.config import get_config
from downloader.producer.huey_tasks import huey

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def signal_handler(signum, frame):
    """信号处理器，优雅关闭"""
    logger.info(f"接收到信号 {signum}，正在关闭consumer...")
    sys.exit(0)


def main():
    """Consumer主函数"""
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 加载配置
    config = get_config()
    logger.info(f"Consumer启动，配置: batch_size={config.consumer.batch_size}")
    
    try:
        # 启动Huey consumer
        logger.info("启动Huey consumer...")
        from huey.consumer import Consumer
        
        consumer = Consumer(huey)
        consumer.run()
        
    except KeyboardInterrupt:
        logger.info("接收到中断信号，正在关闭...")
    except Exception as e:
        logger.error(f"Consumer运行出错: {e}")
        raise
    finally:
        logger.info("Consumer已关闭")


if __name__ == "__main__":
    main()