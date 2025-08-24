#!/usr/bin/env python3
"""带详细日志的调试"""

import asyncio
import logging
import sys
from neo.helpers.app_service import AppService
from neo.task_bus.types import TaskType, DownloadTaskConfig

# 配置详细日志到控制台
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)

# 设置特定模块的日志级别
logging.getLogger('neo.tasks.huey_tasks').setLevel(logging.DEBUG)
logging.getLogger('neo.data_processor').setLevel(logging.DEBUG)
logging.getLogger('neo.downloader').setLevel(logging.INFO)
logging.getLogger('huey').setLevel(logging.INFO)

async def debug_with_logs():
    """带详细日志运行单个任务"""
    print("🔍 开始带详细日志的调试...")
    
    # 创建AppService，启用进度管理器
    app_service = AppService.create_default(with_progress=True)
    
    # 创建任务
    tasks = [
        DownloadTaskConfig(
            task_type=TaskType.stock_basic,
            symbol="000001.SZ"
        )
    ]
    
    print(f"任务配置: task_type={tasks[0].task_type} (type: {type(tasks[0].task_type)})")
    
    try:
        # 直接调用异步方法，避免嵌套asyncio.run
        await app_service._run_downloader_async(tasks)
        print("✅ 任务执行完成")
        
    except Exception as e:
        print(f"❌ 任务执行失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_with_logs())
