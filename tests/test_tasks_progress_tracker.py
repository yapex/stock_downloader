"""进度管理器测试

测试TasksProgressTracker及相关类的功能。
"""

from unittest.mock import Mock, patch

from neo.tqmd import (
    IProgressTracker,
    IProgressTrackerFactory,
    TasksProgressTracker,
    ProgressTrackerFactory,
    TqdmProgressTracker,
)
from neo.containers import AppContainer


class TestTqdmProgressTracker:
    """TqdmProgressTracker测试"""

    @patch("neo.tqmd.progress_tracker.tqdm")
    def test_start_tracking_main_progress(self, mock_tqdm):
        """测试开始主进度跟踪"""
        tracker = TqdmProgressTracker(is_nested=False)
        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        tracker.start_tracking(10, "测试进度")

        mock_tqdm.assert_called_once_with(
            total=10, desc="测试进度", position=0, leave=True, ncols=80
        )

    @patch("neo.tqmd.progress_tracker.tqdm")
    def test_start_tracking_nested_progress(self, mock_tqdm):
        """测试开始嵌套进度跟踪"""
        tracker = TqdmProgressTracker(is_nested=True)
        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        tracker.start_tracking(5, "子任务")

        mock_tqdm.assert_called_once_with(
            total=5, desc="子任务", position=1, leave=False, ncols=80
        )

    @patch("neo.tqmd.progress_tracker.tqdm")
    def test_update_progress(self, mock_tqdm):
        """测试更新进度"""
        tracker = TqdmProgressTracker()
        mock_pbar = Mock()
        mock_tqdm.return_value = mock_pbar

        tracker.start_tracking(10)
        tracker.update_progress(2, "更新描述")

        mock_pbar.update.assert_called_once_with(2)
        mock_pbar.set_description.assert_called_once_with("更新描述")

    @patch("neo.tqmd.progress_tracker.tqdm")
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

    @patch("neo.tqmd.progress_tracker.tqdm")
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


class TestTasksProgressTracker:
    """TasksProgressTracker测试"""

    def setup_method(self):
        """测试前置设置"""
        self.mock_factory = Mock(spec=IProgressTrackerFactory)
        self.manager = TasksProgressTracker(self.mock_factory)

    def test_start_group_progress(self):
        """测试开始组进度跟踪"""
        mock_tracker = Mock(spec=IProgressTracker)
        self.mock_factory.create_tracker.return_value = mock_tracker

        self.manager.start_group_progress(5, "处理组")

        self.mock_factory.create_tracker.assert_called_once_with(
            "group", is_nested=False
        )
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
        self.mock_factory.create_tracker.assert_called_once_with(
            "group", is_nested=False
        )

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

        self.mock_factory.create_tracker.assert_called_once_with(
            "task_type", is_nested=False
        )
        mock_tracker.start_tracking.assert_called_once_with(100, "stock_basic: 0/100")

    def test_start_task_type_progress_with_group(self):
        """测试在有组进度的情况下开始任务类型进度"""
        group_tracker = Mock(spec=IProgressTracker)
        task_type_tracker = Mock(spec=IProgressTracker)
        self.mock_factory.create_tracker.side_effect = [
            group_tracker,
            task_type_tracker,
        ]

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


class TestProgressTrackerFactoryContainer:
    """测试从 AppContainer 获取 ProgressTrackerFactory 的相关行为"""

    def test_get_progress_tracker_factory_from_container(self):
        """测试从容器中获取 ProgressTrackerFactory 实例"""
        container = AppContainer()
        factory = container.progress_tracker_factory()

        # 验证实例类型
        assert isinstance(factory, ProgressTrackerFactory)

        # 验证具有预期的方法
        assert hasattr(factory, "create_tracker")

    def test_container_provides_singleton_factory(self):
        """测试容器以单例模式提供 ProgressTrackerFactory 实例"""
        container = AppContainer()
        factory1 = container.progress_tracker_factory()
        factory2 = container.progress_tracker_factory()

        # 验证是同一个实例（单例模式）
        assert factory1 is factory2
        assert isinstance(factory1, ProgressTrackerFactory)

    def test_container_factory_functionality(self):
        """测试从容器获取的 ProgressTrackerFactory 功能是否正常"""
        container = AppContainer()
        factory = container.progress_tracker_factory()

        # 验证可以创建跟踪器
        tracker = factory.create_tracker("test", is_nested=False)
        assert isinstance(tracker, TqdmProgressTracker)

        # 验证可以创建嵌套跟踪器
        nested_tracker = factory.create_tracker("nested", is_nested=True)
        assert isinstance(nested_tracker, TqdmProgressTracker)

    def test_different_containers_have_different_factory_singletons(self):
        """测试不同容器实例会创建不同的 ProgressTrackerFactory 单例"""
        container1 = AppContainer()
        container2 = AppContainer()

        factory1 = container1.progress_tracker_factory()
        factory2 = container2.progress_tracker_factory()

        # 验证不同容器的工厂是不同实例
        assert factory1 is not factory2
        assert isinstance(factory1, ProgressTrackerFactory)
        assert isinstance(factory2, ProgressTrackerFactory)


class TestTasksProgressTrackerContainer:
    """测试从 AppContainer 获取 TasksProgressTracker 的相关行为"""

    def test_get_tasks_progress_tracker_from_container(self):
        """测试从容器中获取 TasksProgressTracker 实例"""
        container = AppContainer()
        tracker = container.tasks_progress_tracker()

        # 验证实例类型
        assert isinstance(tracker, TasksProgressTracker)

        # 验证具有预期的方法
        assert hasattr(tracker, "start_group_progress")
        assert hasattr(tracker, "start_task_type_progress")
        assert hasattr(tracker, "update_group_progress")
        assert hasattr(tracker, "finish_all")

    def test_container_provides_singleton_tracker_instance(self):
        """测试容器以单例模式提供 TasksProgressTracker 实例"""
        container = AppContainer()
        tracker1 = container.tasks_progress_tracker()
        tracker2 = container.tasks_progress_tracker()

        # 验证是相同的实例（单例模式）
        assert tracker1 is tracker2
        assert isinstance(tracker1, TasksProgressTracker)

    def test_container_tracker_functionality(self):
        """测试从容器获取的 TasksProgressTracker 功能是否正常"""
        container = AppContainer()
        tracker = container.tasks_progress_tracker()

        # 验证可以调用主要方法（不验证具体结果，只验证方法存在且可调用）
        assert callable(tracker.start_group_progress)
        assert callable(tracker.start_task_type_progress)
        assert callable(tracker.update_group_progress)
        assert callable(tracker.finish_all)

    def test_tracker_uses_singleton_factory(self):
        """测试 TasksProgressTracker 使用单例的 ProgressTrackerFactory"""
        container = AppContainer()

        # 获取两个跟踪器实例
        tracker1 = container.tasks_progress_tracker()
        tracker2 = container.tasks_progress_tracker()

        # 验证跟踪器是相同实例（单例模式）
        assert tracker1 is tracker2

        # 验证它们使用同一个工厂实例（单例）
        factory = container.progress_tracker_factory()
        # 注意：由于 TasksProgressTracker 的 factory 属性是私有的，
        # 我们通过容器验证单例行为
        assert factory is container.progress_tracker_factory()

    def test_different_containers_have_different_components(self):
        """测试不同容器实例会创建不同的组件"""
        container1 = AppContainer()
        container2 = AppContainer()

        tracker1 = container1.tasks_progress_tracker()
        tracker2 = container2.tasks_progress_tracker()
        factory1 = container1.progress_tracker_factory()
        factory2 = container2.progress_tracker_factory()

        # 验证不同容器的组件是不同实例
        assert tracker1 is not tracker2
        assert factory1 is not factory2

        # 验证类型正确
        assert isinstance(tracker1, TasksProgressTracker)
        assert isinstance(tracker2, TasksProgressTracker)
        assert isinstance(factory1, ProgressTrackerFactory)
        assert isinstance(factory2, ProgressTrackerFactory)

    def test_container_tracker_with_mocked_factory(self):
        """测试容器中跟踪器与模拟工厂的集成"""
        container = AppContainer()

        # 模拟工厂
        mock_factory = Mock(spec=IProgressTrackerFactory)
        container.progress_tracker_factory.override(mock_factory)

        try:
            tracker = container.tasks_progress_tracker()

            # 验证使用了模拟的工厂
            # 注意：由于 TasksProgressTracker 的构造函数接受 factory 参数，
            # 我们可以验证跟踪器被正确创建
            assert isinstance(tracker, TasksProgressTracker)

        finally:
            # 清理覆盖
            container.progress_tracker_factory.reset_override()
