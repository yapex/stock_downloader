import unittest
from unittest.mock import Mock, patch

from downloader.tasks.base import IncrementalTaskHandler


class MockIncrementalTaskHandler(IncrementalTaskHandler):
    """用于测试的模拟增量任务处理器"""
    
    def get_data_type(self) -> str:
        return "test_data"
        
    def get_date_col(self) -> str:
        return "trade_date"
        
    def fetch_data(self, ts_code: str, start_date: str, end_date: str):
        # 返回简单的模拟数据
        import pandas as pd
        return pd.DataFrame({"ts_code": [ts_code], "trade_date": ["20240101"]})


class TestBrokenPipeErrorHandling(unittest.TestCase):
    """测试BrokenPipeError异常处理"""
    
    def setUp(self):
        """设置测试环境"""
        self.task_config = {
            "name": "测试任务",
            "type": "test"
        }
        
        # Mock依赖对象
        self.mock_fetcher = Mock()
        self.mock_storage = Mock()
        self.mock_storage.get_latest_date.return_value = None
        
        self.handler = MockIncrementalTaskHandler(
            self.task_config, 
            self.mock_fetcher, 
            self.mock_storage
        )
    
    @patch('downloader.tasks.base.tqdm')
    def test_progress_bar_broken_pipe_fallback(self, mock_tqdm_class):
        """测试进度条BrokenPipeError时的降级处理"""
        
        # 配置tqdm构造函数抛出BrokenPipeError
        mock_tqdm_class.side_effect = BrokenPipeError("模拟管道断开")
        
        # 准备测试数据
        target_symbols = ["000001.SZ", "000002.SZ"]
        
        # 使用patch确保日志不会真正输出到stdout
        with patch.object(self.handler, '_log_warning') as mock_log_warning:
            with patch.object(self.handler, '_process_single_symbol') as mock_process:
                mock_process.return_value = True  # 成功
                
                # 执行测试
                self.handler.execute(target_symbols=target_symbols)
                
                # 验证警告日志被调用
                mock_log_warning.assert_called()
                warning_message = mock_log_warning.call_args[0][0]
                self.assertIn("进度条初始化失败", warning_message)
                self.assertIn("静默模式", warning_message)
                
                # 验证处理仍然继续进行
                self.assertEqual(mock_process.call_count, 2)  # 两只股票都被处理了
    
    @patch('downloader.tasks.base.tqdm')
    def test_progress_bar_set_description_broken_pipe(self, mock_tqdm_class):
        """测试进度条更新描述时的BrokenPipeError处理"""
        
        # 创建一个mock进度条对象
        mock_progress_bar = Mock()
        mock_progress_bar.set_description.side_effect = BrokenPipeError("更新描述失败")
        mock_progress_bar.close = Mock()
        mock_progress_bar.__iter__ = Mock(return_value=iter(["000001.SZ"]))
        
        mock_tqdm_class.return_value = mock_progress_bar
        
        # 为了测试set_description的异常处理，我们需要mock相关方法
        with patch.object(self.handler, '_log_debug') as mock_log_debug:
            with patch.object(self.handler, 'fetch_data') as mock_fetch_data:
                mock_fetch_data.return_value = None  # 简化数据处理逻辑
                
                target_symbols = ["000001.SZ"]
                self.handler.execute(target_symbols=target_symbols)
                
                # 验证set_description被调用并且异常被处理
                mock_progress_bar.set_description.assert_called()
                
                # 验证debug日志记录了进度条更新失败
                mock_log_debug.assert_called()
                debug_message = mock_log_debug.call_args[0][0]
                self.assertIn("进度条更新失败", debug_message)
    
    def test_progress_bar_close_with_list_fallback(self):
        """测试当进度条降级为普通列表时，不会调用close方法"""
        
        target_symbols = ["000001.SZ", "000002.SZ"]
        
        # Mock tqdm抛出异常，导致使用列表替代
        with patch('downloader.tasks.base.tqdm', side_effect=BrokenPipeError("初始化失败")):
            with patch.object(self.handler, '_process_single_symbol') as mock_process:
                mock_process.return_value = True
                with patch.object(self.handler, '_log_warning'):
                    
                    # 执行测试 - 不应该抛出AttributeError
                    try:
                        self.handler.execute(target_symbols=target_symbols)
                        success = True
                    except AttributeError as e:
                        if "close" in str(e):
                            success = False
                        else:
                            success = True
                    
                    self.assertTrue(success, "不应该因为调用列表的close方法而失败")
    
    @patch('downloader.tasks.base.tqdm')
    def test_retry_progress_bar_broken_pipe_fallback(self, mock_tqdm_class):
        """测试进度条的BrokenPipeError处理（不再有重试进度条）"""
        
        # 模拟进度条初始化失败
        mock_tqdm_class.side_effect = BrokenPipeError("进度条失败")
        
        with patch.object(self.handler, '_log_warning') as mock_log_warning:
            with patch.object(self.handler, '_process_single_symbol') as mock_process:
                mock_process.return_value = True  # 处理成功
                
                target_symbols = ["000001.SZ"]
                self.handler.execute(target_symbols=target_symbols)
                
                # 检查是否记录了进度条初始化失败的警告
                warning_calls = [call[0][0] for call in mock_log_warning.call_args_list]
                init_warning_found = any("进度条初始化失败" in msg for msg in warning_calls)
                self.assertTrue(init_warning_found, "应该记录进度条初始化失败的警告")


if __name__ == '__main__':
    unittest.main()
