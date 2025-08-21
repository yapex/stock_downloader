#!/usr/bin/env python3
"""
股票下载器启动脚本
提供producer和consumer的统一启动入口
"""

import argparse
import subprocess
import sys
from pathlib import Path


def run_producer(task_groups=None):
    """运行producer"""
    cmd = [sys.executable, "producer_main.py"]
    if task_groups:
        cmd.extend(["--groups"] + task_groups)
    
    print(f"启动Producer: {' '.join(cmd)}")
    subprocess.run(cmd)


def run_consumer():
    """运行consumer"""
    cmd = [sys.executable, "consumer_main.py"]
    print(f"启动Consumer: {' '.join(cmd)}")
    subprocess.run(cmd)


def main():
    parser = argparse.ArgumentParser(description='股票下载器启动脚本')
    parser.add_argument(
        'mode', 
        choices=['producer', 'consumer'], 
        help='运行模式: producer 或 consumer'
    )
    parser.add_argument(
        '--groups', 
        nargs='+', 
        help='Producer模式下的任务组列表，例如: --groups stock_basic financial'
    )
    
    args = parser.parse_args()
    
    if args.mode == 'producer':
        run_producer(args.groups)
    elif args.mode == 'consumer':
        run_consumer()


if __name__ == "__main__":
    main()