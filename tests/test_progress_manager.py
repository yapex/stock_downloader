"""进度管理器测试

测试ProgressManager及相关类的功能。
"""

import pytest
from unittest.mock import Mock, patch

from src.neo.tqmd import (
    IProgressTracker,
    IProgressTrackerFactory,
    ProgressManager,
    ProgressTrackerFactory,
    TqdmProgressTracker,
)


class TestTqdmProgressTracker:
    """TqdmProgressTracker测试"""
    
    @patch('src.neo.tqmd.progress_manager.tqdm')
    def test_start_tracking_main_progress(self, mock_tqdm):
        """测试开始主进度跟踪"""
        tracker = TqdmProgressTracker(is_nested=False)
        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar
        
        tracker.start_tracking(10, "测试进度")
        
        mock_tqdm.assert_called_once_with(
            total=10,
            desc="测试进度",
            position=0,
            leave=True,
            ncols=80
        )
    
    @patch('src.neo.tqmd.progress_manager.tqdm')
    def test_start_tracking_nested_progress(self, mock_tqdm):
        """测试开始嵌套进度跟踪"""
        tracker = TqdmProgressTracker(is_nested=True)
        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar
        
        tracker.start_tracking(5, "子任务")
        
        mock_tqdm.assert_called_once_with(
            total=5,
            desc="子任务",
            position=1,
            leave=False,
            ncols=80
        )
    
    @patch('src.neo.tqmd.progress_manager.tqdm')
    def test_update_progress(self, mock_tqdm):
        """测试更新进度"""
        tracker = TqdmProgressTracker()
        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar
        
        tracker.start_tracking(10)
        tracker.update_progress(2, "更新描述")
        
        mock_pbar.update.assert_called_once_with(2)
        mock_pbar.set_description.assert_called_once_with("更新描述")
    
    @patch('src.neo.tqmd.progress_manager.tqdm')
    def test_update_progress_without_description(self, mock_tqdm):
        """测试不带描述的进度更新"""
        tracker = TqdmProgressTracker()
        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar
        
        tracker.start_tracking(10)
        tracker.update_progress(1)
        
        mock_pbar.update.assert_called_once_with(1)
        mock_pbar.set_description.assert_not_called()
    
    def test_update_progress_without_start(self):
        """测试未开始跟踪时更新进度"""
        tracker = TqdmProgressTracker()
        # 不应该抛出异常
        tracker.update_progress(1)
    
    @patch('src.neo.tqmd.progress_manager.tqdm')
    def test_finish_tracking(self, mock_tqdm):
        """测试完成进度跟踪"""
        tracker = TqdmProgressTracker()
        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar
        
        tracker.start_tracking(10)
        tracker.finish_tracking()
        
        mock_pbar.close.assert_called_once()
    
    def test_finish_tracking_without_start(self):
        """测试未开始跟踪时完成跟踪"""
        tracker = TqdmProgressTracker()
        # 不应该抛出异常
        tracker.finish_tracking()


class TestProgressTrackerFactory:
    """ProgressTrackerFactory测试"""
    
    def test_create_tracker_single_task(self):
        """测试创建单任务跟踪器"""
        factory = ProgressTrackerFactory()
        tracker = factory.create_tracker("single", is_nested=False)
        
        assert isinstance(tracker, TqdmProgressTracker)
    
    def test_create_tracker_nested_task(self):
        """测试创建嵌套任务跟踪器"""
        factory = ProgressTrackerFactory()
        tracker = factory.create_tracker("group", is_nested=True)
        
        assert isinstance(tracker, TqdmProgressTracker)


class TestProgressManager:
    """ProgressManager测试"""
    
    def setup_method(self):
        """测试前置设置"""
        self.mock_factory = Mock(spec=IProgressTrackerFactory)
        self.manager = ProgressManager(self.mock_factory)
    
    def test_start_group_progress(self):
        """测试开始组进度跟踪"""
        mock_tracker = Mock(spec=IProgressTracker)
        self.mock_factory.create_tracker.return_value = mock_tracker
        
        self.manager.start_group_progress(5, "处理组")
        
        self.mock_factory.create_tracker.assert_called_once_with("group", is_nested=False)
        mock_tracker.start_tracking.assert_called_once_with(5, "处理组")
    
    def test_start_task_progress_without_group(self):
        """测试在没有组进度的情况下开始任务进度（兼容性方法）"""
        # start_task_progress现在是空的兼容性方法，不会调用工厂
        self.manager.start_task_progress(10, "处理任务")
        
        # 验证没有调用工厂（因为是兼容性方法）
        self.mock_factory.create_tracker.assert_not_called()
    
    def test_start_task_progress_with_group(self):
        """测试在有组进度的情况下开始任务进度（兼容性方法）"""
        # 先开始组进度
        group_tracker = Mock(spec=IProgressTracker)
        self.mock_factory.create_tracker.return_value = group_tracker
        
        self.manager.start_group_progress(5, "处理组")
        self.manager.start_task_progress(10, "处理任务")
        
        # 验证只调用了一次（只有组进度，任务进度是兼容性方法）
        self.mock_factory.create_tracker.assert_called_once_with("group", is_nested=False)
    
    def test_update_group_progress(self):
        """测试更新组进度"""
        mock_tracker = Mock(spec=IProgressTracker)
        self.mock_factory.create_tracker.return_value = mock_tracker
        
        self.manager.start_group_progress(5)
        self.manager.update_group_progress(1, "更新组")
        
        mock_tracker.update_progress.assert_called_once_with(1, "更新组")
    
    def test_update_group_progress_without_start(self):
        """测试未开始组进度时更新"""
        # 不应该抛出异常
        self.manager.update_group_progress(1)
    
    def test_update_task_progress(self):
        """测试更新任务进度（兼容性方法）"""
        # update_task_progress现在是空的兼容性方法
        self.manager.update_task_progress(2, "更新任务")
        
        # 验证没有调用工厂（因为是兼容性方法）
        self.mock_factory.create_tracker.assert_not_called()
    
    def test_update_task_progress_without_start(self):
        """测试未开始任务进度时更新"""
        # 不应该抛出异常
        self.manager.update_task_progress(1)
    
    def test_finish_task_progress(self):
        """测试完成任务进度（兼容性方法）"""
        # finish_task_progress现在是空的兼容性方法
        self.manager.finish_task_progress()
        
        # 验证没有调用工厂（因为是兼容性方法）
        self.mock_factory.create_tracker.assert_not_called()
    
    def test_finish_group_progress(self):
        """测试完成组进度"""
        mock_tracker = Mock(spec=IProgressTracker)
        self.mock_factory.create_tracker.return_value = mock_tracker
        
        self.manager.start_group_progress(5)
        self.manager.finish_group_progress()
        
        mock_tracker.finish_tracking.assert_called_once()
    
    def test_finish_all(self):
        """测试完成所有进度"""
        group_tracker = Mock(spec=IProgressTracker)
        self.mock_factory.create_tracker.return_value = group_tracker
        
        self.manager.start_group_progress(5)
        self.manager.finish_all()
        
        # 验证只调用了组进度的finish_tracking
        group_tracker.finish_tracking.assert_called_once()
    
    def test_finish_all_without_start(self):
        """测试未开始任何进度时完成所有"""
        # 不应该抛出异常
        self.manager.finish_all()
    
    def test_start_task_type_progress(self):
        """测试开始任务类型进度跟踪"""
        mock_tracker = Mock(spec=IProgressTracker)
        self.mock_factory.create_tracker.return_value = mock_tracker
        
        self.manager.start_task_type_progress("stock_basic", 100)
        
        self.mock_factory.create_tracker.assert_called_once_with("task_type", is_nested=False)
        mock_tracker.start_tracking.assert_called_once_with(100, "stock_basic: 0/100")
    
    def test_start_task_type_progress_with_group(self):
        """测试在有组进度的情况下开始任务类型进度"""
        group_tracker = Mock(spec=IProgressTracker)
        task_type_tracker = Mock(spec=IProgressTracker)
        self.mock_factory.create_tracker.side_effect = [group_tracker, task_type_tracker]
        
        self.manager.start_group_progress(5, "处理组")
        self.manager.start_task_type_progress("stock_basic", 100)
        
        calls = self.mock_factory.create_tracker.call_args_list
        assert len(calls) == 2
        assert calls[0] == (("group",), {"is_nested": False})
        assert calls[1] == (("task_type",), {"is_nested": True})
    
    def test_update_task_type_progress(self):
        """测试更新任务类型进度"""
        mock_tracker = Mock(spec=IProgressTracker)
        self.mock_factory.create_tracker.return_value = mock_tracker
        
        self.manager.start_task_type_progress("stock_basic", 100)
        self.manager.update_task_type_progress("stock_basic", 1, 50, 100)
        
        mock_tracker.update_progress.assert_called_once_with(1, "stock_basic: 50/100")
    
    def test_update_task_type_progress_without_description(self):
        """测试不带描述的任务类型进度更新"""
        mock_tracker = Mock(spec=IProgressTracker)
        self.mock_factory.create_tracker.return_value = mock_tracker
        
        self.manager.start_task_type_progress("stock_basic", 100)
        self.manager.update_task_type_progress("stock_basic", 2)
        
        mock_tracker.update_progress.assert_called_once_with(2, None)
    
    def test_finish_task_type_progress(self):
        """测试完成任务类型进度"""
        mock_tracker = Mock(spec=IProgressTracker)
        self.mock_factory.create_tracker.return_value = mock_tracker
        
        self.manager.start_task_type_progress("stock_basic", 100)
        self.manager.finish_task_type_progress("stock_basic")
        
        mock_tracker.finish_tracking.assert_called_once()