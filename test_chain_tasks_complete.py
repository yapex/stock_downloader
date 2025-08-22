#!/usr/bin/env python3
"""测试完整的链式任务流程

验证下载任务完成后自动触发数据处理任务的完整流程。
"""

import sys
import os
from pathlib import Path
import pandas as pd
from unittest.mock import Mock, patch

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

from neo.tasks.huey_tasks import download_task, process_data_task
from neo.task_bus.types import TaskType


def test_download_task_with_chain():
    """测试下载任务及其链式调用数据处理任务"""
    print("🧪 开始测试完整链式任务流程...")
    
    # Mock SimpleDownloader 以避免实际网络请求
    mock_data = pd.DataFrame({
        'ts_code': ['000001.SZ'],
        'trade_date': ['20240101'],
        'open': [10.50],
        'high': [11.20],
        'low': [10.30],
        'close': [11.00],
        'volume': [1000000]
    })
    
    # Mock TaskResult
    from neo.task_bus.types import TaskResult
    mock_task_result = TaskResult(
        config=None,
        success=True,
        data=mock_data
    )
    
    # Mock SimpleDataProcessor 以验证调用
    mock_processor = Mock()
    mock_processor.process.return_value = True
    
    with patch('neo.tasks.huey_tasks.SimpleDownloader') as mock_downloader_class, \
         patch('neo.tasks.huey_tasks.SimpleDataProcessor') as mock_processor_class:
        
        # 设置 Mock 行为
        mock_downloader = Mock()
        mock_downloader.download.return_value = mock_task_result
        mock_downloader_class.return_value = mock_downloader
        
        mock_processor_class.return_value = mock_processor
        
        # 执行下载任务
        symbol = "000001.SZ"
        task_type = "stock_daily"
        
        print(f"📤 执行下载任务: {symbol}")
        result = download_task.call_local(task_type, symbol)
        
        # 验证下载任务结果
        assert result['success'] == True
        assert result['config']['symbol'] == symbol
        assert result['config']['task_type'] == task_type
        print(f"✅ 下载任务执行成功: {symbol}")
        
        # 验证 SimpleDownloader 被调用
        mock_downloader_class.assert_called_once()
        mock_downloader.download.assert_called_once_with(task_type, symbol)
        print("✅ SimpleDownloader 被正确调用")
        
        # 验证 SimpleDataProcessor 被调用（通过链式调用）
        mock_processor_class.assert_called_once()
        mock_processor.process.assert_called_once()
        print("✅ SimpleDataProcessor 被链式调用")
        
        # 验证传递给数据处理器的参数
        call_args = mock_processor.process.call_args[0][0]  # 获取第一个参数
        assert call_args.config.symbol == symbol
        assert call_args.config.task_type.name == task_type
        assert