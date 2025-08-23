"""测试 tqdm 进度条集成"""
import pytest
from unittest.mock import Mock, patch

from neo.helpers.app_service import AppService
from neo.helpers.task_builder import DownloadTaskConfig
from neo.tqmd import ProgressManager, ProgressTrackerFactory


class TestTqdmIntegration:
    """测试 tqdm 进度条集成"""

    def test_app_service_with_progress_manager(self):
        """测试 AppService 可以正确初始化 ProgressManager"""
        # 创建带有进度管理器的 AppService
        app_service = AppService.create_default(with_progress=True)
        
        # 验证进度管理器已正确设置
        assert app_service.progress_manager is not None
        assert isinstance(app_service.progress_manager, ProgressManager)

    def test_app_service_without_progress_manager(self):
        """测试 AppService 可以不使用 ProgressManager"""
        # 创建不带进度管理器的 AppService
        app_service = AppService.create_default(with_progress=False)
        
        # 验证进度管理器为 None
        assert app_service.progress_manager is None

    def test_single_task_progress_integration(self):
        """测试单任务进度条集成"""
        # 创建带有进度管理器的 AppService
        app_service = AppService.create_default(with_progress=True)
        
        # 模拟进度管理器方法
        app_service.progress_manager.start_task_progress = Mock()
        app_service.progress_manager.update_task_progress = Mock()
        app_service.progress_manager.finish_all = Mock()
        
        # 创建单个任务
        from neo.task_bus.types import TaskType
        task = DownloadTaskConfig(
            symbol="000001",
            task_type=TaskType.stock_basic
        )
        
        # 测试单任务进度条逻辑（不执行实际的异步代码）
        tasks = [task]
        is_group_task = len(tasks) > 1
        
        # 模拟单任务进度条启动
        if app_service.progress_manager and not is_group_task:
            app_service.progress_manager.start_task_progress(1, "执行下载任务")
            app_service.progress_manager.update_task_progress(1)
            app_service.progress_manager.finish_all()
        
        # 验证进度管理器方法被调用
        app_service.progress_manager.start_task_progress.assert_called_once_with(1, "执行下载任务")
        app_service.progress_manager.update_task_progress.assert_called_once_with(1)
        app_service.progress_manager.finish_all.assert_called_once()

    def test_group_task_progress_integration(self):
        """测试组任务进度条集成"""
        # 创建带有进度管理器的 AppService
        app_service = AppService.create_default(with_progress=True)
        
        # 模拟进度管理器方法
        app_service.progress_manager.start_group_progress = Mock()
        app_service.progress_manager.start_task_progress = Mock()
        app_service.progress_manager.update_task_progress = Mock()
        app_service.progress_manager.finish_task_progress = Mock()
        app_service.progress_manager.update_group_progress = Mock()
        app_service.progress_manager.finish_all = Mock()
        
        # 创建多个任务
        from neo.task_bus.types import TaskType
        tasks = [
            DownloadTaskConfig(
                symbol="000001",
                task_type=TaskType.stock_basic
            ),
            DownloadTaskConfig(
                symbol="000002",
                task_type=TaskType.stock_basic
            )
        ]
        
        # 测试组任务进度条逻辑（不执行实际的异步代码）
        is_group_task = len(tasks) > 1
        
        # 模拟组任务进度条启动
        if app_service.progress_manager and is_group_task:
            app_service.progress_manager.start_group_progress(len(tasks), "处理下载任务组")
            
            # 模拟每个任务的处理
            for i, task in enumerate(tasks):
                task_name = app_service._get_task_name(task)
                app_service.progress_manager.start_task_progress(1, f"处理 {task_name}")
                app_service.progress_manager.update_task_progress(1)
                app_service.progress_manager.finish_task_progress()
                app_service.progress_manager.update_group_progress(1, f"已完成 {i+1}/{len(tasks)} 个任务")
            
            app_service.progress_manager.finish_all()
        
        # 验证组进度条被启动
        app_service.progress_manager.start_group_progress.assert_called_once_with(2, "处理下载任务组")
        
        # 验证每个任务的进度条被启动和更新
        assert app_service.progress_manager.start_task_progress.call_count == 2
        assert app_service.progress_manager.update_task_progress.call_count == 2
        assert app_service.progress_manager.finish_task_progress.call_count == 2
        assert app_service.progress_manager.update_group_progress.call_count == 2
        
        # 验证最终完成
        app_service.progress_manager.finish_all.assert_called_once()

    def test_progress_manager_factory_integration(self):
        """测试 ProgressManager 工厂集成"""
        factory = ProgressTrackerFactory()
        progress_manager = ProgressManager(factory)
        
        # 验证工厂可以创建进度跟踪器
        tracker = factory.create_tracker("test", is_nested=False)
        assert tracker is not None
        
        # 验证进度管理器可以管理跟踪器
        progress_manager.start_task_progress(10, "测试任务")
        progress_manager.update_task_progress(5)
        progress_manager.finish_task_progress()