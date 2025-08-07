#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据下载器 - 主入口点

这是一个简洁的入口点，直接导入并运行 downloader.main 模块
使得用户可以使用 `uv run dl` 命令
"""

from src.downloader.main import app

if __name__ == "__main__":
    app()
