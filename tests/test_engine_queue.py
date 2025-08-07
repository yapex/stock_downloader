"""
测试基于队列的 DownloadEngine
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os
from datetime import datetime

from src.downloader.engine import DownloadEngine
from src.downloader.models import TaskType, Priority


class TestDownloadEngineQueue(unittest.TestCase):
    """测试基于队列的 DownloadEngine"""
    
    def setUp(self):
        """设置测试环境"""
        self.temp_dir = tempfile.mkdtemp()
        
        # 基础配置
        self.config = {
            "database": {
                "path": os.path.join(self.temp_dir, "test.db")
            },
            "downloader": {
                "symbols": ["000001.SZ", "000002.SZ"],
                "max_producers": 2,
                "max_consumers": 1,
                "producer_queue_size": 100,
                "data_queue_size": 50,
                "retry_policy": {
                    "max_retries": 2,
                    "base_delay": 0.1
                }
            },
            "consumer": {
                "batch_size": 10,
                "flush_interval": 1.0,
                "max_retries": 2
            },
            "fetcher": {
                "rate_limit": 100
            },
            "tasks": [
                {
                    "type": "stock_list",
                    "name": "更新股票列表",
                    "enabled": True
                },
                {
                    "type": "daily",
                    "name": "日线数据",
                    "enabled": True
                }
            ]
        }
    
    def tearDown(self):
        """清理测试环境"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_engine_initialization(self):
        """测试引擎初始化"""
        engine = DownloadEngine(
            config=self.config,
            force_run=True,
            symbols_overridden=False,
            group_name="test"
        )
        
        # 检查基本属性
        self.assertEqual(engine.config, self.config)
        self.assertTrue(engine.force_run)
        self.assertFalse(engine.symbols_overridden)
        self.assertEqual(engine.group_name, "test")
        
        # 检查队列配置
        self.assertEqual(engine.max_producers, 2)
        self.assertEqual(engine.max_consumers, 1)
        self.assertEqual(engine.producer_queue_size, 100)
        self.assertEqual(engine.data_queue_size, 50)
        
        # 检查统计信息初始化
        stats = engine.execution_stats
        self.assertEqual(stats['total_tasks'], 0)
        self.assertEqual(stats['successful_tasks'], 0)
        self.assertEqual(stats['failed_tasks'], 0)
        self.assertIsNone(stats['processing_start_time'])
    
    def test_build_retry_policy(self):
        """测试重试策略构建"""
        engine = DownloadEngine(config=self.config)
        policy = engine.retry_policy
        
        self.assertEqual(policy.max_retries, 2)
        self.assertEqual(policy.base_delay, 0.1)
    
    def test_prepare_target_symbols(self):
        """测试准备目标股票列表"""
        engine = DownloadEngine(config=self.config)
        
        # 测试有股票相关任务的情况
        enabled_tasks = [
            {"type": "daily", "name": "日线数据"},
            {"type": "stock_list", "name": "股票列表"}
        ]
        
        symbols = engine._prepare_target_symbols(enabled_tasks)
        self.assertEqual(symbols, ["000001.SZ", "000002.SZ"])
        
        # 测试只有系统级任务的情况
        system_tasks = [{"type": "stock_list", "name": "股票列表"}]
        symbols = engine._prepare_target_symbols(system_tasks)
        self.assertEqual(symbols, [])
    
    def test_build_download_tasks(self):
        """测试构建下载任务"""
        engine = DownloadEngine(config=self.config, force_run=True)
        
        target_symbols = ["000001.SZ", "000002.SZ"]
        enabled_tasks = [
            {"type": "stock_list", "name": "股票列表"},
            {"type": "daily", "name": "日线数据"}
        ]
        
        tasks = engine._build_download_tasks(target_symbols, enabled_tasks)
        
        # 应该有 1 个 stock_list 任务 + 2 个 daily 任务
        self.assertEqual(len(tasks), 3)
        
        # 检查 stock_list 任务
        stock_list_tasks = [t for t in tasks if t.task_type == TaskType.STOCK_LIST]
        self.assertEqual(len(stock_list_tasks), 1)
        self.assertEqual(stock_list_tasks[0].symbol, "system")
        self.assertEqual(stock_list_tasks[0].priority, Priority.HIGH)
        
        # 检查 daily 任务
        daily_tasks = [t for t in tasks if t.task_type == TaskType.DAILY]
        self.assertEqual(len(daily_tasks), 2)
        symbols_in_tasks = {t.symbol for t in daily_tasks}
        self.assertEqual(symbols_in_tasks, {"000001.SZ", "000002.SZ"})
        
        # 检查统计信息更新
        self.assertEqual(engine.execution_stats['total_tasks'], 3)
        self.assertEqual(engine.execution_stats['tasks_by_type']['stock_list'], 1)
        self.assertEqual(engine.execution_stats['tasks_by_type']['daily'], 2)
    
    def test_determine_date_range_force_run(self):
        """测试强制运行时的日期范围确定"""
        engine = DownloadEngine(config=self.config, force_run=True)
        
        task_spec = {"type": "daily", "name": "日线数据"}
        start_date, end_date = engine._determine_date_range(task_spec, "000001.SZ")
        
        self.assertEqual(start_date, "19901219")
        self.assertTrue(end_date.isdigit() and len(end_date) == 8)  # YYYYMMDD格式
    
    def test_determine_date_range_normal(self):
        """测试正常运行时的日期范围确定"""
        engine = DownloadEngine(config=self.config, force_run=False)
        
        task_spec = {"type": "daily", "name": "日线数据"}
        start_date, end_date = engine._determine_date_range(task_spec, "000001.SZ")
        
        # 由于新架构中延迟获取最新日期，现在应该返回默认范围
        self.assertEqual(start_date, "19901219")
        self.assertTrue(end_date.isdigit() and len(end_date) == 8)
    
    @patch('src.downloader.engine.ProducerPool')
    @patch('src.downloader.engine.ConsumerPool')
    @patch('src.downloader.engine.Queue')
    def test_setup_queues(self, mock_queue_class, mock_consumer_pool, mock_producer_pool):
        """测试队列和线程池设置"""
        # 模拟Queue实例
        mock_task_queue = Mock()
        mock_data_queue = Mock()
        mock_queue_class.side_effect = [mock_task_queue, mock_data_queue]
        
        # 模拟线程池实例
        mock_producer = Mock()
        mock_consumer = Mock()
        mock_producer_pool.return_value = mock_producer
        mock_consumer_pool.return_value = mock_consumer
        
        engine = DownloadEngine(config=self.config)
        engine._setup_queues()
        
        # 检查队列创建
        self.assertEqual(mock_queue_class.call_count, 2)
        mock_queue_class.assert_any_call(maxsize=100)  # task_queue
        mock_queue_class.assert_any_call(maxsize=50)   # data_queue
        
        # 检查生产者池创建
        mock_producer_pool.assert_called_once()
        producer_args = mock_producer_pool.call_args
        self.assertEqual(producer_args[1]['max_producers'], 2)
        self.assertEqual(producer_args[1]['task_queue'], mock_task_queue)
        self.assertEqual(producer_args[1]['data_queue'], mock_data_queue)
        
        # 检查消费者池创建
        mock_consumer_pool.assert_called_once()
        consumer_args = mock_consumer_pool.call_args
        self.assertEqual(consumer_args[1]['max_consumers'], 1)
        self.assertEqual(consumer_args[1]['data_queue'], mock_data_queue)
        self.assertEqual(consumer_args[1]['batch_size'], 10)
        self.assertEqual(consumer_args[1]['flush_interval'], 1.0)
        
        # 检查引擎属性设置
        self.assertEqual(engine.task_queue, mock_task_queue)
        self.assertEqual(engine.data_queue, mock_data_queue)
        self.assertEqual(engine.producer_pool, mock_producer)
        self.assertEqual(engine.consumer_pool, mock_consumer)
    
    def test_backward_compatibility(self):
        """测试向后兼容性"""
        mock_fetcher = Mock()
        mock_storage = Mock()
        
        engine = DownloadEngine(
            config=self.config,
            fetcher=mock_fetcher,
            storage=mock_storage
        )
        
        # 测试属性访问器
        self.assertEqual(engine.fetcher, mock_fetcher)
        self.assertEqual(engine.storage, mock_storage)
        self.assertIsNone(engine.buffer_pool)  # 旧架构兼容
    
    def test_get_execution_stats(self):
        """测试获取执行统计信息"""
        engine = DownloadEngine(config=self.config)
        
        # 修改统计信息
        engine.execution_stats['total_tasks'] = 10
        engine.execution_stats['successful_tasks'] = 8
        
        stats = engine.get_execution_stats()
        
        # 检查返回的是副本
        self.assertEqual(stats['total_tasks'], 10)
        self.assertEqual(stats['successful_tasks'], 8)
        self.assertIsNot(stats, engine.execution_stats)
        
        # 修改返回的统计不应影响原始数据
        stats['total_tasks'] = 999
        self.assertEqual(engine.execution_stats['total_tasks'], 10)
    
    def test_invalid_task_types(self):
        """测试无效任务类型处理"""
        config_with_invalid_task = self.config.copy()
        config_with_invalid_task['tasks'] = [
            {"type": "invalid_type", "name": "无效任务", "enabled": True},
            {"type": "daily", "name": "有效任务", "enabled": True}
        ]
        
        engine = DownloadEngine(config=config_with_invalid_task, force_run=True)
        
        target_symbols = ["000001.SZ"]
        enabled_tasks = [
            {"type": "invalid_type", "name": "无效任务"},
            {"type": "daily", "name": "有效任务"}
        ]
        
        with patch('src.downloader.engine.logger') as mock_logger:
            tasks = engine._build_download_tasks(target_symbols, enabled_tasks)
            
            # 应该警告无效任务类型
            mock_logger.warning.assert_called_with("未知的任务类型: invalid_type")
            
            # 只应该创建有效的任务
            self.assertEqual(len(tasks), 1)
            self.assertEqual(tasks[0].task_type, TaskType.DAILY)


if __name__ == '__main__':
    unittest.main()
