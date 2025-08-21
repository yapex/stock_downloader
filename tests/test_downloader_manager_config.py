"""测试 DownloaderManager 配置功能"""

import pytest
from unittest.mock import patch, MagicMock
from box import Box

from downloader.producer.downloader_manager import (
    DownloaderManager,
    create_task_type_config_from_config,
    get_task_types_from_group,
    TASK_NAME_TO_TYPE,
    PRIORITY_VALUE_TO_ENUM,
)
from downloader.producer.fetcher_builder import TaskType
from downloader.task.types import TaskPriority
from downloader.task.task_scheduler import TaskTypeConfig


class TestConfigFunctions:
    """测试配置相关函数"""

    @patch('downloader.producer.downloader_manager.get_config')
    def test_create_task_type_config_from_config_with_priorities(self, mock_get_config):
        """测试从配置创建任务类型配置（包含优先级）"""
        # 模拟配置
        mock_config = Box({
            'download_tasks': {
                'stock_basic': {'priority': 0},  # HIGH
                'stock_daily': {'priority': 1},  # MEDIUM
                'balance_sheet': {'priority': 2},  # LOW
                'unknown_task': {'priority': 1},  # 未知任务，应被忽略
            }
        })
        mock_get_config.return_value = mock_config
        
        # 调用函数
        config = create_task_type_config_from_config()
        
        # 验证结果
        assert isinstance(config, TaskTypeConfig)
        assert config.get_priority(TaskType.STOCK_BASIC) == TaskPriority.HIGH
        assert config.get_priority(TaskType.STOCK_DAILY) == TaskPriority.MEDIUM
        assert config.get_priority(TaskType.BALANCE_SHEET) == TaskPriority.LOW
        
        # 验证 get_config 被正确调用
        mock_get_config.assert_called_with(None)

    @patch('downloader.producer.downloader_manager.get_config')
    def test_create_task_type_config_from_config_no_download_tasks(self, mock_get_config):
        """测试从配置创建任务类型配置（无 download_tasks）"""
        # 模拟配置（无 download_tasks）
        mock_config = Box({})
        mock_get_config.return_value = mock_config
        
        # 调用函数
        config = create_task_type_config_from_config()
        
        # 验证结果（应使用默认配置）
        assert isinstance(config, TaskTypeConfig)
        # 验证默认优先级
        assert config.get_priority(TaskType.STOCK_BASIC) == TaskPriority.HIGH
        
        # 验证 get_config 被正确调用
        mock_get_config.assert_called_with(None)

    @patch('downloader.producer.downloader_manager.get_config')
    def test_create_task_type_config_invalid_priority(self, mock_get_config):
        """测试无效优先级值的处理"""
        # 模拟配置（包含无效优先级）
        mock_config = Box({
            'download_tasks': {
                'stock_basic': {'priority': 999},  # 无效优先级
                'stock_daily': {'priority': 1},    # 有效优先级
            }
        })
        mock_get_config.return_value = mock_config
        
        # 调用函数
        config = create_task_type_config_from_config()
        
        # 验证结果
        assert isinstance(config, TaskTypeConfig)
        # stock_basic 应使用默认优先级（因为 999 无效）
        assert config.get_priority(TaskType.STOCK_BASIC) == TaskPriority.HIGH
        # stock_daily 应使用配置的优先级
        assert config.get_priority(TaskType.STOCK_DAILY) == TaskPriority.MEDIUM
        mock_get_config.assert_called_with(None)

    @patch('downloader.producer.downloader_manager.get_config')
    def test_get_task_types_from_group(self, mock_get_config):
        """测试从任务组获取任务类型"""
        # 模拟配置
        mock_config = Box({
            'task_groups': {
                'test_group': ['stock_basic', 'stock_daily'],
                'financial_group': ['balance_sheet', 'income_statement'],
            }
        })
        mock_get_config.return_value = mock_config
        
        # 测试获取任务类型
        task_types = get_task_types_from_group('test_group')
        
        # 验证结果
        assert len(task_types) == 2
        assert TaskType.STOCK_BASIC in task_types
        assert TaskType.STOCK_DAILY in task_types
        mock_get_config.assert_called_with(None)

    @patch('downloader.producer.downloader_manager.get_config')
    def test_get_task_types_from_group_unknown_group(self, mock_get_config):
        """测试获取未知任务组"""
        # 模拟配置
        mock_config = Box({
            'task_groups': {
                'test_group': ['stock_basic'],
            }
        })
        mock_get_config.return_value = mock_config
        
        # 测试获取未知任务组
        with pytest.raises(ValueError, match="未找到任务组: unknown_group"):
            get_task_types_from_group('unknown_group')
        mock_get_config.assert_called_with(None)

    @patch('downloader.producer.downloader_manager.get_config')
    def test_get_task_types_from_group_unknown_task(self, mock_get_config):
        """测试任务组包含未知任务类型"""
        # 模拟配置
        mock_config = Box({
            'task_groups': {
                'test_group': ['stock_basic', 'unknown_task'],
            }
        })
        mock_get_config.return_value = mock_config
        
        # 测试获取包含未知任务的任务组
        with pytest.raises(ValueError, match="未知的任务类型: unknown_task"):
            get_task_types_from_group('test_group')
        mock_get_config.assert_called_with(None)


class TestDownloaderManagerConfig:
    """测试 DownloaderManager 配置功能"""

    @patch('downloader.producer.downloader_manager.create_task_type_config_from_config')
    def test_create_from_config(self, mock_create_config):
        """测试从配置创建 DownloaderManager"""
        # 模拟 TaskTypeConfig
        mock_task_config = MagicMock(spec=TaskTypeConfig)
        mock_create_config.return_value = mock_task_config
        
        # 创建 DownloaderManager
        manager = DownloaderManager.create_from_config(
            max_workers=8,
            enable_progress_bar=False
        )
        
        # 验证结果
        assert manager.max_workers == 8
        assert manager.enable_progress_bar == False
        assert manager.scheduler.task_type_config == mock_task_config
        mock_create_config.assert_called_once()

    @patch('downloader.producer.downloader_manager.get_task_types_from_group')
    def test_download_group(self, mock_get_task_types):
        """测试下载任务组功能"""
        # 模拟任务类型
        mock_get_task_types.return_value = [TaskType.STOCK_BASIC, TaskType.STOCK_DAILY]
        
        # 创建 DownloaderManager（使用 mock 执行器）
        mock_executor = MagicMock()
        mock_executor.execute.return_value.success = True
        
        manager = DownloaderManager(
            max_workers=1,
            task_executor=mock_executor,
            enable_progress_bar=False
        )
        
        # 模拟执行（不实际运行）
        with patch.object(manager, 'start'), \
             patch.object(manager, 'run') as mock_run, \
             patch.object(manager, 'stop'):
            
            mock_run.return_value = manager.stats
            
            # 调用 download_group
            stats = manager.download_group(
                group='test_group',
                symbols=['000001', '600519'],
                max_retries=2
            )
            
            # 验证调用
            mock_get_task_types.assert_called_once_with('test_group', None)
            assert stats == manager.stats

    def test_task_name_to_type_mapping(self):
        """测试任务名称到类型的映射"""
        # 验证映射关系
        assert TASK_NAME_TO_TYPE['stock_basic'] == TaskType.STOCK_BASIC
        assert TASK_NAME_TO_TYPE['stock_daily'] == TaskType.STOCK_DAILY
        assert TASK_NAME_TO_TYPE['daily_basic'] == TaskType.DAILY_BASIC
        assert TASK_NAME_TO_TYPE['balance_sheet'] == TaskType.BALANCE_SHEET
        assert TASK_NAME_TO_TYPE['income_statement'] == TaskType.INCOME_STATEMENT
        assert TASK_NAME_TO_TYPE['cash_flow'] == TaskType.CASH_FLOW

    def test_priority_value_to_enum_mapping(self):
        """测试优先级数值到枚举的映射"""
        assert PRIORITY_VALUE_TO_ENUM[0] == TaskPriority.HIGH
        assert PRIORITY_VALUE_TO_ENUM[1] == TaskPriority.MEDIUM
        assert PRIORITY_VALUE_TO_ENUM[2] == TaskPriority.LOW
    
    @patch('downloader.producer.downloader_manager.get_config')
    def test_create_from_config_with_downloader_settings(self, mock_get_config):
        """测试从配置文件读取下载器设置"""
        # 模拟配置
        mock_config = Box({
            'downloader': {
                'max_workers': 8,
                'enable_progress_bar': False,
            },
            'download_tasks': {
                'stock_basic': {'priority': 0},
            }
        })
        mock_get_config.return_value = mock_config
        
        # 创建管理器
        manager = DownloaderManager.create_from_config()
        
        # 验证配置被正确读取
        assert manager.max_workers == 8
        assert manager.enable_progress_bar == False
        mock_get_config.assert_called_with(None)
    
    @patch('downloader.producer.downloader_manager.get_config')
    def test_create_from_config_with_default_downloader_settings(self, mock_get_config):
        """测试配置文件中没有下载器设置时使用默认值"""
        # 模拟配置（没有 downloader 部分）
        mock_config = Box({
            'download_tasks': {
                'stock_basic': {'priority': 0},
            }
        })
        mock_get_config.return_value = mock_config
        
        # 创建管理器
        manager = DownloaderManager.create_from_config()
        
        # 验证使用默认值
        assert manager.max_workers == 4
        assert manager.enable_progress_bar == True
        mock_get_config.assert_called_with(None)