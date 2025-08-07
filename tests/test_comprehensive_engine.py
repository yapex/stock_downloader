"""
DownloadEngine 引擎模块的全面单元测试
覆盖正常流程、异常情况、边界条件和所有分支逻辑
"""

import pytest
from unittest.mock import MagicMock, patch, Mock
from queue import Queue, Full, Empty
import pandas as pd
from datetime import datetime
import logging

from downloader.engine import DownloadEngine
from downloader.models import DownloadTask, TaskType, Priority
from downloader.retry_policy import RetryPolicy, BackoffStrategy


class TestDownloadEngineInitialization:
    """测试引擎初始化"""
    
    def test_engine_init_default_config(self):
        """测试使用默认配置初始化引擎"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config)
        
        assert engine.max_producers == 2
        assert engine.max_consumers == 1
        assert engine.producer_queue_size == 1000
        assert engine.data_queue_size == 500
        assert engine.force_run is False
        assert engine.symbols_overridden is False
        assert engine.group_name == "default"
        assert engine.task_queue is None  # 未初始化
        
    def test_engine_init_custom_config(self):
        """测试使用自定义配置初始化引擎"""
        config = {
            "downloader": {
                "max_producers": 5,
                "max_consumers": 3,
                "producer_queue_size": 2000,
                "data_queue_size": 1000,
                "retry_policy": {
                    "max_attempts": 5,
                    "base_delay": 2.0,
                    "max_delay": 120.0,
                    "backoff_factor": 3.0
                }
            },
            "database": {"path": "custom.db"}
        }
        
        engine = DownloadEngine(config, force_run=True, symbols_overridden=True, group_name="test")
        
        assert engine.max_producers == 5
        assert engine.max_consumers == 3
        assert engine.producer_queue_size == 2000
        assert engine.data_queue_size == 1000
        assert engine.force_run is True
        assert engine.symbols_overridden is True
        assert engine.group_name == "test"
        assert engine.retry_policy.max_attempts == 5
        assert engine.retry_policy.base_delay == 2.0
        assert engine.retry_policy.max_delay == 120.0
        assert engine.retry_policy.backoff_factor == 3.0

    @patch('downloader.engine.entry_points')
    def test_task_handler_discovery_success(self, mock_entry_points):
        """测试成功发现任务处理器"""
        mock_ep1 = Mock()
        mock_ep1.name = "daily"
        mock_ep1.load.return_value = "DailyHandler"
        
        mock_ep2 = Mock()
        mock_ep2.name = "stock_list" 
        mock_ep2.load.return_value = "StockListHandler"
        
        mock_entry_points.return_value = [mock_ep1, mock_ep2]
        
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config)
        
        assert len(engine.task_registry) == 2
        assert engine.task_registry["daily"] == "DailyHandler"
        assert engine.task_registry["stock_list"] == "StockListHandler"

    @patch('downloader.engine.entry_points')
    def test_task_handler_discovery_failure(self, mock_entry_points, caplog):
        """测试任务处理器发现失败"""
        mock_entry_points.side_effect = Exception("Entry point error")
        
        config = {"downloader": {}, "database": {"path": "test.db"}}
        
        with caplog.at_level(logging.ERROR):
            engine = DownloadEngine(config)
        
        assert len(engine.task_registry) == 0
        assert "自动发现任务处理器时发生错误" in caplog.text

    def test_retry_policy_building_default(self):
        """测试默认重试策略构建"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config)
        
        assert engine.retry_policy.max_attempts == 3
        assert engine.retry_policy.base_delay == 1.0
        assert engine.retry_policy.max_delay == 30.0
        assert engine.retry_policy.backoff_factor == 2.0

    def test_retry_policy_building_custom(self):
        """测试自定义重试策略构建"""
        config = {
            "downloader": {
                "retry_policy": {
                    "max_attempts": 5,
                    "base_delay": 0.5,
                    "max_delay": 30.0,
                    "backoff_factor": 1.5
                }
            },
            "database": {"path": "test.db"}
        }
        engine = DownloadEngine(config)
        
        assert engine.retry_policy.max_attempts == 5
        assert engine.retry_policy.base_delay == 0.5
        assert engine.retry_policy.max_delay == 30.0
        assert engine.retry_policy.backoff_factor == 1.5


class TestQueueSetup:
    """测试队列设置"""
    
    def test_setup_queues_success(self):
        """测试成功设置队列"""
        config = {
            "downloader": {
                "max_producers": 3,
                "max_consumers": 2,
                "producer_queue_size": 100,
                "data_queue_size": 200
            },
            "database": {"path": "test.db"},
            "fetcher": {"rate_limit": 120},
            "consumer": {
                "batch_size": 50,
                "flush_interval": 10.0,
                "max_retries": 2
            }
        }
        
        with patch('downloader.engine.ProducerPool') as mock_producer_pool, \
             patch('downloader.engine.ConsumerPool') as mock_consumer_pool:
            
            engine = DownloadEngine(config)
            engine._setup_queues()
            
            # 验证队列创建
            assert isinstance(engine.task_queue, Queue)
            assert isinstance(engine.data_queue, Queue)
            assert engine.task_queue.maxsize == 100
            assert engine.data_queue.maxsize == 200
            
            # 验证Producer Pool创建
            mock_producer_pool.assert_called_once_with(
                max_producers=3,
                task_queue=engine.task_queue,
                data_queue=engine.data_queue,
                retry_policy_config=engine.retry_policy,
                dead_letter_path="logs/dead_letter.jsonl",
                fetcher_rate_limit=120
            )
            
            # 验证Consumer Pool创建
            mock_consumer_pool.assert_called_once_with(
                max_consumers=2,
                data_queue=engine.data_queue,
                batch_size=50,
                flush_interval=10.0,
                db_path="test.db",
                max_retries=2
            )

    @patch('downloader.engine.logger')
    def test_setup_queues_logging(self, mock_logger):
        """测试队列设置日志记录"""
        config = {
            "downloader": {
                "max_producers": 4,
                "max_consumers": 3
            },
            "database": {"path": "test.db"}
        }
        
        with patch('downloader.engine.ProducerPool'), \
             patch('downloader.engine.ConsumerPool'):
            
            engine = DownloadEngine(config)
            engine._setup_queues()
            
            # 验证日志调用
            mock_logger.info.assert_any_call("初始化队列和线程池...")
            mock_logger.info.assert_any_call("队列初始化完成 - 生产者: 4, 消费者: 3")


class TestBuildDownloadTasks:
    """测试构建下载任务"""
    
    def test_build_stock_list_task(self):
        """测试构建股票列表任务"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config)
        
        target_symbols = ["000001.SZ", "600001.SH"]
        enabled_tasks = [
            {
                "type": "stock_list",
                "params": {"list_status": "L"}
            }
        ]
        
        tasks = engine._build_download_tasks(target_symbols, enabled_tasks)
        
        assert len(tasks) == 1
        task = tasks[0]
        assert task.symbol == "system"
        assert task.task_type == TaskType.STOCK_LIST
        assert task.priority == Priority.HIGH
        assert task.params['task_config'] == enabled_tasks[0]
        assert task.params['force_run'] == engine.force_run

    def test_build_stock_specific_tasks(self):
        """测试构建股票特定任务"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config)
        
        target_symbols = ["000001.SZ", "600001.SH"]
        enabled_tasks = [
            {
                "type": "daily",
                "params": {"adjust": "qfq"}
            },
            {
                "type": "daily_basic"
            }
        ]
        
        with patch.object(engine, '_determine_date_range', return_value=("20230101", "20231231")):
            tasks = engine._build_download_tasks(target_symbols, enabled_tasks)
        
        assert len(tasks) == 4  # 2 symbols * 2 task types
        
        # 验证任务类型和符号分布
        daily_tasks = [t for t in tasks if t.task_type == TaskType.DAILY]
        daily_basic_tasks = [t for t in tasks if t.task_type == TaskType.DAILY_BASIC]
        
        assert len(daily_tasks) == 2
        assert len(daily_basic_tasks) == 2
        
        symbols = set(t.symbol for t in tasks)
        assert symbols == set(target_symbols)

    def test_build_tasks_unknown_type(self, caplog):
        """测试构建未知类型任务"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config)
        
        target_symbols = ["000001.SZ"]
        enabled_tasks = [
            {
                "type": "unknown_task_type"
            }
        ]
        
        with caplog.at_level(logging.WARNING):
            tasks = engine._build_download_tasks(target_symbols, enabled_tasks)
        
        assert len(tasks) == 0
        assert "未知的任务类型: unknown_task_type" in caplog.text

    def test_build_tasks_no_new_data(self):
        """测试无新数据需要下载的情况"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config)
        
        target_symbols = ["000001.SZ"]
        enabled_tasks = [
            {
                "type": "daily"
            }
        ]
        
        # 模拟没有新数据需要下载（start_date > end_date）
        with patch.object(engine, '_determine_date_range', return_value=("20231201", "20231130")):
            tasks = engine._build_download_tasks(target_symbols, enabled_tasks)
        
        assert len(tasks) == 0
        assert engine.execution_stats['total_tasks'] == 0

    def test_build_tasks_execution_stats(self):
        """测试任务构建统计信息"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config)
        
        target_symbols = ["000001.SZ", "600001.SH"]
        enabled_tasks = [
            {"type": "daily"},
            {"type": "daily_basic"},
            {"type": "stock_list"}
        ]
        
        with patch.object(engine, '_determine_date_range', return_value=("20230101", "20231231")):
            tasks = engine._build_download_tasks(target_symbols, enabled_tasks)
        
        assert len(tasks) == 5  # 2*2 + 1 stock_list
        assert engine.execution_stats['total_tasks'] == 5
        assert engine.execution_stats['tasks_by_type']['daily'] == 2
        assert engine.execution_stats['tasks_by_type']['daily_basic'] == 2
        assert engine.execution_stats['tasks_by_type']['stock_list'] == 1


class TestDateRangeDetermination:
    """测试日期范围确定"""
    
    @patch('downloader.engine.DuckDBStorage')
    def test_determine_date_range_force_run(self, mock_storage_class):
        """测试强制运行时的日期范围确定"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config, force_run=True)
        
        task_spec = {
            "date_range": {
                "start_date": "20230101",
                "end_date": "20231231"
            }
        }
        
        start_date, end_date = engine._determine_date_range(task_spec, "000001.SZ")
        
        # 当前实现在强制模式下使用默认起始日期
        assert start_date == "19901219"  # 默认起始日期
        assert end_date.startswith("202")  # 当前日期

    @patch('downloader.engine.DuckDBStorage')
    def test_determine_date_range_incremental(self, mock_storage_class):
        """测试增量更新的日期范围确定"""
        # 设置存储mock
        mock_storage = Mock()
        mock_storage.get_latest_date.return_value = "20230630"
        mock_storage_class.return_value = mock_storage
        
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config, force_run=False)
        
        task_spec = {
            "type": "daily",  # 指定任务类型以便日期列判断生效
            "date_range": {
                "start_date": "20230101",
                "end_date": "20231231"
            }
        }
        
        start_date, end_date = engine._determine_date_range(task_spec, "000001.SZ")
        
        # 现在应该从最新日期的下一天开始（增量下载）
        assert start_date == "20230701"  # 20230630 + 1 day
        assert end_date.startswith("202")  # 当前日期
        
        # 验证存储被正确调用
        mock_storage.get_latest_date.assert_called_once_with('daily', '000001.SZ', 'trade_date')

    @patch('downloader.engine.DuckDBStorage')
    def test_determine_date_range_no_existing_data(self, mock_storage_class):
        """测试无现有数据时的日期范围确定"""
        # 设置存储mock
        mock_storage = Mock()
        mock_storage.get_latest_date.return_value = None
        mock_storage_class.return_value = mock_storage
        
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config, force_run=False)
        
        task_spec = {
            "date_range": {
                "start_date": "20230101",
                "end_date": "20231231"
            }
        }
        
        start_date, end_date = engine._determine_date_range(task_spec, "000001.SZ")
        
        # 当前实现总是返回默认日期范围  
        assert start_date == "19901219"  # 默认起始日期
        assert end_date.startswith("202")  # 当前日期


class TestEngineExceptionHandling:
    """测试引擎异常处理"""
    
    def test_queue_setup_producer_pool_failure(self):
        """测试ProducerPool创建失败"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config)
        
        with patch('downloader.engine.ProducerPool', side_effect=Exception("ProducerPool failed")), \
             patch('downloader.engine.ConsumerPool'):
            
            with pytest.raises(Exception, match="ProducerPool failed"):
                engine._setup_queues()

    def test_queue_setup_consumer_pool_failure(self):
        """测试ConsumerPool创建失败"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config)
        
        with patch('downloader.engine.ProducerPool'), \
             patch('downloader.engine.ConsumerPool', side_effect=Exception("ConsumerPool failed")):
            
            with pytest.raises(Exception, match="ConsumerPool failed"):
                engine._setup_queues()

    def test_date_range_determination_storage_error(self, caplog):
        """测试存储访问错误时的日期范围确定"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config, force_run=False)
        
        task_spec = {
            "date_range": {
                "start_date": "20230101",
                "end_date": "20231231"
            }
        }
        
        with patch('downloader.engine.DuckDBStorage', side_effect=Exception("Storage error")):
            # 应该回退到强制模式
            start_date, end_date = engine._determine_date_range(task_spec, "000001.SZ")
            
            # 当前实现总是返回默认日期范围
            assert start_date == "19901219"  # 默认起始日期
            assert end_date.startswith("202")  # 当前日期

    def test_empty_task_list_handling(self):
        """测试空任务列表处理"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config)
        
        target_symbols = ["000001.SZ"]
        enabled_tasks = []
        
        tasks = engine._build_download_tasks(target_symbols, enabled_tasks)
        
        assert len(tasks) == 0
        assert engine.execution_stats['total_tasks'] == 0
        assert engine.execution_stats['tasks_by_type'] == {}

    def test_empty_symbols_handling(self):
        """测试空股票列表处理"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config)
        
        target_symbols = []
        enabled_tasks = [{"type": "daily"}]
        
        tasks = engine._build_download_tasks(target_symbols, enabled_tasks)
        
        assert len(tasks) == 0
        assert engine.execution_stats['total_tasks'] == 0


class TestEngineStatistics:
    """测试引擎统计功能"""
    
    def test_execution_stats_initialization(self):
        """测试执行统计初始化"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config)
        
        stats = engine.execution_stats
        assert stats['total_tasks'] == 0
        assert stats['successful_tasks'] == 0
        assert stats['failed_tasks'] == 0
        assert stats['skipped_tasks'] == 0
        assert stats['tasks_by_type'] == {}
        assert stats['success_by_symbol'] == {}
        assert stats['failed_symbols'] == []
        assert stats['processing_start_time'] is None
        assert stats['processing_end_time'] is None

    def test_execution_stats_task_counting(self):
        """测试任务统计计数"""
        config = {"downloader": {}, "database": {"path": "test.db"}}
        engine = DownloadEngine(config)
        
        target_symbols = ["000001.SZ", "600001.SH", "000002.SZ"]
        enabled_tasks = [
            {"type": "daily"},
            {"type": "daily_basic"},
            {"type": "stock_list"}
        ]
        
        with patch.object(engine, '_determine_date_range', return_value=("20230101", "20231231")):
            tasks = engine._build_download_tasks(target_symbols, enabled_tasks)
        
        # 验证任务计数
        assert engine.execution_stats['total_tasks'] == 7  # 3*2 + 1 stock_list
        assert engine.execution_stats['tasks_by_type']['daily'] == 3
        assert engine.execution_stats['tasks_by_type']['daily_basic'] == 3  
        assert engine.execution_stats['tasks_by_type']['stock_list'] == 1
