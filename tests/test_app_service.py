"""AppService 测试

测试 AppService 类的功能，包括工厂方法。
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from neo.helpers.app_service import AppService, DataProcessorRunner, ServiceFactory
from neo.helpers.huey_consumer_manager import HueyConsumerManager
from neo.task_bus.types import DownloadTaskConfig, TaskType
from neo.containers import AppContainer


class TestAppService:
    """AppService 类测试"""

    def test_init(self):
        """测试 AppService 初始化"""
        # 创建 AppService 实例
        app_service = AppService()

        # 验证实例创建成功
        assert isinstance(app_service, AppService)

    def test_create_default(self):
        """测试 create_default 工厂方法"""
        # 调用工厂方法
        app_service = AppService.create_default()

        # 验证返回的实例
        assert isinstance(app_service, AppService)

    @patch('neo.helpers.app_service.DataProcessorRunner.run_data_processor')
    def test_run_data_processor(self, mock_run_data_processor):
        """测试 run_data_processor 方法"""
        app_service = AppService()

        # 运行数据处理器
        app_service.run_data_processor()

        # 验证 DataProcessorRunner.run_data_processor 被调用
        mock_run_data_processor.assert_called_once()

    def test_cleanup(self):
        """测试 cleanup 方法"""
        app_service = AppService()
        
        # cleanup 方法应该能正常调用而不抛出异常
        app_service.cleanup()

    def test_destructor(self):
        """测试析构函数"""
        with patch.object(AppService, 'cleanup') as mock_cleanup:
            app_service = AppService()
            del app_service
            
            # 验证析构时调用了 cleanup
            mock_cleanup.assert_called_once()

    def test_create_default_with_progress(self):
        """测试创建带进度管理器的默认实例"""
        app_service = AppService.create_default(with_progress=True)
        
        assert isinstance(app_service, AppService)
        assert app_service.tasks_progress_tracker is not None

    def test_create_default_without_progress(self):
        """测试创建不带进度管理器的默认实例"""
        app_service = AppService.create_default(with_progress=False)
        
        assert isinstance(app_service, AppService)
        assert app_service.tasks_progress_tracker is None

    def test_run_downloader_dry_run(self):
        """测试试运行模式"""
        app_service = AppService()
        
        # 创建测试任务
        tasks = [
            DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001"),
            DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000002"),
        ]
        
        with patch.object(app_service, '_print_dry_run_info') as mock_print:
            app_service.run_downloader(tasks, dry_run=True)
            
            # 验证调用了试运行信息打印
            mock_print.assert_called_once_with(tasks)

    def test_print_dry_run_info(self):
        """测试试运行信息打印"""
        app_service = AppService()
        
        tasks = [
            DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001"),
            DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000002"),
        ]
        
        with patch('builtins.print') as mock_print:
            app_service._print_dry_run_info(tasks)
            
            # 验证打印了正确的信息
            assert mock_print.call_count >= 1
            # 检查是否包含任务数量信息
            first_call_args = str(mock_print.call_args_list[0])
            assert "2" in first_call_args  # 任务数量

    def test_get_task_name(self):
        """测试获取任务名称"""
        app_service = AppService()
        
        # 测试有 symbol 的任务
        task_with_symbol = DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001")
        name = app_service._get_task_name(task_with_symbol)
        assert "000001" in name
        assert "stock_basic" in name
        
        # 测试无 symbol 的任务
        task_without_symbol = DownloadTaskConfig(task_type=TaskType.stock_basic, symbol=None)
        name = app_service._get_task_name(task_without_symbol)
        assert "stock_basic" in name

    def test_group_tasks_by_type(self):
        """测试按任务类型分组"""
        app_service = AppService()
        
        tasks = [
            DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001"),
            DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000002"),
            DownloadTaskConfig(task_type=TaskType.stock_daily, symbol="000001"),
        ]
        
        groups = app_service._group_tasks_by_type(tasks)
        
        assert "stock_basic" in groups
        assert "stock_daily" in groups
        assert len(groups["stock_basic"]) == 2
        assert len(groups["stock_daily"]) == 1

    @patch('neo.tasks.huey_tasks.download_task')
    def test_execute_download_task_with_submission_success(self, mock_download_task):
        """测试成功提交下载任务"""
        app_service = AppService()
        
        # 模拟任务结果
        mock_result = Mock()
        mock_download_task.return_value = mock_result
        
        task = DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001")
        
        result = app_service._execute_download_task_with_submission(task)
        
        assert result == mock_result
        mock_download_task.assert_called_once_with(TaskType.stock_basic, "000001")

    @patch('neo.tasks.huey_tasks.download_task')
    def test_execute_download_task_with_submission_failure(self, mock_download_task):
        """测试提交下载任务失败"""
        app_service = AppService()
        
        # 模拟任务提交失败
        mock_download_task.side_effect = Exception("Task submission failed")
        
        task = DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001")
        
        result = app_service._execute_download_task_with_submission(task)
        
        assert result is None
        mock_download_task.assert_called_once_with(TaskType.stock_basic, "000001")

    @pytest.mark.asyncio
    @patch('neo.helpers.app_service.HueyConsumerManager.start_consumer_async', new_callable=AsyncMock)
    @patch('neo.helpers.app_service.HueyConsumerManager.wait_for_all_tasks_completion', new_callable=AsyncMock)
    @patch('neo.helpers.app_service.HueyConsumerManager.stop_consumer_async', new_callable=AsyncMock)
    async def test_run_downloader_async_with_progress(self, mock_stop_consumer, mock_wait_completion, mock_start_consumer):
        """测试带进度管理器的异步下载"""
        # 创建带进度管理器的 AppService
        mock_progress_tracker = Mock()
        app_service = AppService(tasks_progress_tracker=mock_progress_tracker)
        
        tasks = [
            DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001"),
            DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000002"),
        ]
        
        with patch.object(app_service, '_execute_download_task_with_submission') as mock_execute:
            mock_execute.return_value = Mock()
            
            await app_service._run_downloader_async(tasks)
            
            # 验证任务被执行
            assert mock_execute.call_count == 2
            
            # 验证进度管理器被调用
            mock_progress_tracker.start_group_progress.assert_called_once_with(2, "处理下载任务")
            mock_progress_tracker.start_task_type_progress.assert_called_once()
            mock_progress_tracker.finish_all.assert_called_once()
            
            # 验证消费者管理器方法被调用
            mock_wait_completion.assert_called_once()
            mock_stop_consumer.assert_called_once()

    @pytest.mark.asyncio
    @patch('neo.helpers.app_service.HueyConsumerManager.wait_for_all_tasks_completion', new_callable=AsyncMock)
    @patch('neo.helpers.app_service.HueyConsumerManager.stop_consumer_async', new_callable=AsyncMock)
    async def test_run_downloader_async_without_progress(self, mock_stop_consumer, mock_wait_completion):
        """测试不带进度管理器的异步下载"""
        
        app_service = AppService(tasks_progress_tracker=None)
        
        tasks = [
            DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001"),
        ]
        
        with patch.object(app_service, '_execute_download_task_with_submission') as mock_execute:
            mock_execute.return_value = Mock()
            
            await app_service._run_downloader_async(tasks)
            
            # 验证任务被执行
            mock_execute.assert_called_once()
            
            # 验证消费者管理器方法被调用
            mock_wait_completion.assert_called_once()
            mock_stop_consumer.assert_called_once()

    @pytest.mark.asyncio
    @patch('neo.helpers.app_service.HueyConsumerManager.start_consumer_async', new_callable=AsyncMock)
    @patch('neo.helpers.app_service.HueyConsumerManager.wait_for_all_tasks_completion', new_callable=AsyncMock)
    @patch('neo.helpers.app_service.HueyConsumerManager.stop_consumer_async', new_callable=AsyncMock)
    async def test_run_downloader_async_with_task_failure(self, mock_stop_consumer, mock_wait_completion, mock_start_consumer):
        """测试异步下载中任务失败的情况"""
        
        mock_progress_tracker = Mock()
        app_service = AppService(tasks_progress_tracker=mock_progress_tracker)
        
        tasks = [
            DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001"),
            DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000002"),
        ]
        
        with patch.object(app_service, '_execute_download_task_with_submission') as mock_execute:
            # 第一个任务成功，第二个任务失败
            mock_execute.side_effect = [Mock(), None]
            
            await app_service._run_downloader_async(tasks)
            
            # 验证两个任务都被尝试执行
            assert mock_execute.call_count == 2
            
            # 验证进度管理器仍然被正确调用
            mock_progress_tracker.start_group_progress.assert_called_once_with(2, "处理下载任务")
            mock_progress_tracker.start_task_type_progress.assert_called_once()
            mock_progress_tracker.finish_all.assert_called_once()
            
            # 验证消费者管理器方法被调用
            mock_wait_completion.assert_called_once()
            mock_stop_consumer.assert_called_once()

    @pytest.mark.asyncio
    @patch('neo.helpers.app_service.HueyConsumerManager.start_consumer_async', new_callable=AsyncMock)
    @patch('neo.helpers.app_service.HueyConsumerManager.wait_for_all_tasks_completion', new_callable=AsyncMock)
    @patch('neo.helpers.app_service.HueyConsumerManager.stop_consumer_async', new_callable=AsyncMock)
    async def test_run_downloader_async_progress_update(self, mock_stop_consumer, mock_wait_completion, mock_start_consumer):
        """测试异步下载中进度更新"""
        
        mock_progress_tracker = Mock()
        app_service = AppService(tasks_progress_tracker=mock_progress_tracker)
        
        tasks = [
            DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001"),
            DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000002"),
        ]
        
        with patch.object(app_service, '_execute_download_task_with_submission') as mock_execute:
            mock_result = Mock()
            mock_execute.return_value = mock_result
            
            await app_service._run_downloader_async(tasks)
            
            # 验证进度更新被调用了正确的次数
            assert mock_progress_tracker.update_task_type_progress.call_count == 2
            assert mock_progress_tracker.update_group_progress.call_count == 2
            
            # 验证进度管理器的其他方法也被调用
            mock_progress_tracker.start_group_progress.assert_called_once_with(2, "处理下载任务")
            mock_progress_tracker.start_task_type_progress.assert_called_once()
            mock_progress_tracker.finish_all.assert_called_once()
            
            # 验证消费者管理器方法被调用
            mock_wait_completion.assert_called_once()
            mock_stop_consumer.assert_called_once()


class TestDataProcessorRunner:
    """测试 DataProcessorRunner 类"""
    
    def test_run_data_processor(self):
        """测试运行数据处理器"""
        with patch.object(HueyConsumerManager, 'setup_signal_handlers') as mock_setup_signal, \
             patch.object(HueyConsumerManager, 'setup_huey_logging') as mock_setup_logging, \
             patch.object(HueyConsumerManager, 'run_consumer_standalone') as mock_run_consumer:
            
            DataProcessorRunner.run_data_processor()
            
            # 验证 HueyConsumerManager 的方法被正确调用
            mock_setup_signal.assert_called_once()
            mock_setup_logging.assert_called_once()
            mock_run_consumer.assert_called_once()


class TestServiceFactory:
    """ServiceFactory 类测试"""

    def test_create_app_service_with_defaults(self):
        """测试使用默认参数创建 AppService"""
        # 调用工厂方法
        app_service = ServiceFactory.create_app_service()

        # 验证返回的实例
        assert isinstance(app_service, AppService)

    def test_create_app_service_with_custom_params(self):
        """测试使用自定义参数创建 AppService"""
        # 调用工厂方法
        app_service = ServiceFactory.create_app_service(with_progress=False)

        # 验证返回的实例
        assert isinstance(app_service, AppService)


class TestAppServiceContainer:
    """测试从 AppContainer 获取 AppService 的相关行为"""

    def test_get_app_service_from_container(self):
        """测试能从容器获取 AppService 实例"""
        container = AppContainer()

        app_service = container.app_service()

        assert isinstance(app_service, AppService)
        assert hasattr(app_service, "tasks_progress_tracker")

    def test_container_provides_different_service_instances(self):
        """测试容器每次提供不同的 AppService 实例（工厂模式）"""
        container = AppContainer()

        service1 = container.app_service()
        service2 = container.app_service()

        assert service1 is not service2
        assert isinstance(service1, AppService)
        assert isinstance(service2, AppService)

    def test_container_service_functionality(self):
        """测试容器获取的 AppService 功能正常"""
        container = AppContainer()

        app_service = container.app_service()

        # 验证基本功能可用
        assert hasattr(app_service, "run_data_processor")
        assert callable(app_service.run_data_processor)

        # 验证依赖注入正确
        assert app_service.tasks_progress_tracker is not None

    def test_container_service_dependencies_injection(self):
        """测试容器正确注入了依赖"""
        container = AppContainer()

        app_service = container.app_service()

        # 验证依赖类型正确
        from neo.tqmd import TasksProgressTracker

        assert isinstance(app_service.tasks_progress_tracker, TasksProgressTracker)

    def test_different_containers_have_different_services(self):
        """测试不同容器实例有不同的服务"""
        container1 = AppContainer()
        container2 = AppContainer()

        service1 = container1.app_service()
        service2 = container2.app_service()

        # 不同容器的实例应该不同
        assert service1 is not service2
        assert isinstance(service1, AppService)
        assert isinstance(service2, AppService)

    def test_container_service_with_mocked_dependencies(self):
        """测试容器服务与模拟依赖的集成"""
        container = AppContainer()

        app_service = container.app_service()

        # 验证实例创建成功
        assert isinstance(app_service, AppService)
