"""测试 Huey Pipeline 功能

验证 download_task 和 process_data_task 通过 pipeline 链接时的参数传递是否正确。
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from neo.task_bus.types import TaskType


def _download_task_logic(task_type: TaskType, symbol: str):
    """下载任务的核心逻辑（用于测试）"""
    try:
        # 从中心化的 app.py 获取共享的容器实例
        from neo.app import container
        
        downloader = container.downloader()
        result = downloader.download(task_type, symbol)
        
        return result
    except Exception as e:
        return None


def _process_data_task_logic(task_type: TaskType, symbol: str, data: pd.DataFrame):
    """数据处理任务的核心逻辑（用于测试）"""
    try:
        # 创建异步数据处理器并运行
        import asyncio
        from neo.tasks.huey_tasks import _process_data_async
        
        async def process_async():
            try:
                # 直接使用传入的数据，不再重复下载
                success = (
                    data is not None and not data.empty if data is not None else False
                )
                if success and data is not None:
                    return await _process_data_async(task_type, data)
                else:
                    return False
            except Exception as e:
                return False

        return asyncio.run(process_async())
    except Exception as e:
        return False


class TestHueyPipeline:
    """测试 Huey Pipeline 功能"""

    def test_download_task_return_format(self):
        """测试 download_task 返回值格式是否符合 pipeline 要求"""
        with patch('neo.app.container') as mock_container:
            # 模拟下载器
            mock_downloader = Mock()
            mock_data = pd.DataFrame({'test': [1, 2, 3]})
            mock_downloader.download.return_value = mock_data
            mock_container.downloader.return_value = mock_downloader
            
            # 调用任务逻辑
            result = _download_task_logic(TaskType.stock_basic, "000001")
            
            # 验证返回值格式：应该直接返回 DataFrame
            assert isinstance(result, pd.DataFrame)
            assert result.equals(mock_data)

    def test_download_task_exception_return_format(self):
        """测试 download_task 异常时返回值格式"""
        with patch('neo.app.container') as mock_container:
            # 模拟下载器抛出异常
            mock_downloader = Mock()
            mock_downloader.download.side_effect = Exception("下载失败")
            mock_container.downloader.return_value = mock_downloader
            
            # 调用任务逻辑
            result = _download_task_logic(TaskType.stock_basic, "000001")
            
            # 验证异常时返回值格式：应该返回 None
            assert result is None

    def test_process_data_task_parameter_order(self):
        """测试 process_data_task 参数接收顺序是否正确"""
        # 准备测试数据
        test_symbol = "000001"
        test_task_type = TaskType.stock_basic
        test_data = pd.DataFrame({'test': [1, 2, 3]})
        
        with patch('neo.tasks.huey_tasks._process_data_async') as mock_process_async:
            mock_process_async.return_value = True
            
            # 调用任务逻辑
            result = _process_data_task_logic(test_task_type, test_symbol, test_data)
            
            # 验证参数传递正确
            mock_process_async.assert_called_once_with(test_task_type, test_data)
            assert result is True

    def test_process_data_task_with_none_data(self):
        """测试 process_data_task 处理 None 数据的情况"""
        test_symbol = "000001"
        test_task_type = TaskType.stock_basic
        test_data = None
        
        # 调用任务逻辑
        result = _process_data_task_logic(test_task_type, test_symbol, test_data)
        
        # None 数据应该返回 False
        assert result is False

    def test_process_data_task_with_empty_dataframe(self):
        """测试 process_data_task 处理空 DataFrame 的情况"""
        test_symbol = "000001"
        test_task_type = TaskType.stock_basic
        test_data = pd.DataFrame()  # 空 DataFrame
        
        # 调用任务逻辑
        result = _process_data_task_logic(test_task_type, test_symbol, test_data)
        
        # 空 DataFrame 应该返回 False
        assert result is False

    def test_pipeline_parameter_passing_simulation(self):
        """模拟 pipeline 参数传递过程"""
        # 模拟 download_task.s(task_type, symbol).then(process_data_task, task_type, symbol) 的参数传递
        
        # 第一步：download_task 返回 data
        with patch('neo.app.container') as mock_container:
            mock_downloader = Mock()
            mock_data = pd.DataFrame({'test': [1, 2, 3]})
            mock_downloader.download.return_value = mock_data
            mock_container.downloader.return_value = mock_downloader
            
            download_result = _download_task_logic(TaskType.stock_basic, "000001")
            
        # 第二步：模拟 Huey pipeline 参数传递规则
        # .then(process_data_task, task_type, symbol) 中的 task_type 和 symbol 会作为前两个参数
        # download_task 返回的 data 会作为第三个参数
        task_type_from_then = TaskType.stock_basic
        symbol_from_then = "000001"
        data_from_download = download_result
        
        # 第三步：验证 process_data_task 能正确接收这些参数
        with patch('neo.tasks.huey_tasks._process_data_async') as mock_process_async:
            mock_process_async.return_value = True
            
            process_result = _process_data_task_logic(
                task_type_from_then,
                symbol_from_then, 
                data_from_download
            )
            
            # 验证参数传递正确
            mock_process_async.assert_called_once_with(
                task_type_from_then, 
                data_from_download
            )
            assert process_result is True