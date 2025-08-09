#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import sys
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 添加src目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# 设置详细日志到文件
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('debug_output.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)

from downloader.app import DownloaderApp
from downloader.progress_manager import progress_manager

def test_with_detailed_logs():
    """测试并输出详细日志"""
    print("开始详细日志测试...")
    
    # 创建应用
    app = DownloaderApp()
    
    print(f"初始进度管理器状态: total={progress_manager.total_tasks}, completed={progress_manager.completed_tasks}")
    
    # 运行下载
    try:
        success = app.run_download(
            config_path='config.yaml',
            group_name='default',
            symbols=['000001.SZ'],
            force=True
        )
        print(f"下载完成，成功: {success}")
    except Exception as e:
        print(f"下载失败: {e}")
        import traceback
        traceback.print_exc()
    
    # 最终状态
    print(f"最终进度管理器状态: total={progress_manager.total_tasks}, completed={progress_manager.completed_tasks}")
    print("详细日志已保存到 debug_output.log")

if __name__ == "__main__":
    test_with_detailed_logs()