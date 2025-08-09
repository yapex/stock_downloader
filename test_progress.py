#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import sys
import os

# 添加src目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from downloader.progress_manager import progress_manager

def test_progress_bar():
    """测试进度条功能"""
    print("测试进度条功能...")
    
    # 初始化进度条
    progress_manager.initialize(5, "测试进度")
    
    # 模拟任务处理
    for i in range(5):
        time.sleep(1)
        progress_manager.increment(True, f"任务_{i+1}")
        print(f"完成任务 {i+1}")
    
    # 完成进度条
    progress_manager.finish()
    print("进度条测试完成")

if __name__ == "__main__":
    test_progress_bar()