"""EngineV2 测试模块"""

import pytest
import threading
import time
from unittest.mock import Mock, MagicMock, patch
from concurrent.futures import Future

from src.downloader.engine_v2 import EngineV2, SystemTask, SystemTaskType, EngineEvents
from src.downloader.interfaces import IConfig, IEventBus, IProducer
from src.downloader.models import DownloadTask, TaskType, Priority


class TestEngineV2:
    """EngineV2 测试类"""
    
    @pytest.fixture
    def mock_config(self):
        """模拟配置"""
        config = Mock()
        downloader_config = Mock()
        downloader_config.max_workers = 4
        config.get_downloader_config.return_value = downloader_config
        return config
    
    @pytest.fixture
    def mock_event_bus(self):
        """模拟事件总线"""
        return Mock()
    
    @pytest.fixture
    def mock_producer(self):
        """模拟生产者"""
        return Mock()
    
    @pytest.fixture
    def engine(self, mock_config, mock_event_bus, mock_producer):
        """创建引擎实例"""
        return EngineV2(mock_config, mock_event_bus, mock_producer)
    
    def test_init(self, mock_config, mock_event_bus, mock_producer):
        """测试初始化"""
        engine = EngineV2(mock_config, mock_event_bus, mock_producer)
        
        assert engine._config is mock_config
        assert engine._event_bus is mock_event_bus
        assert engine._producer is mock_producer
        assert not engine._running
        assert not engine._shutdown_requested
        assert engine._executor is None
        assert engine._system_executor is None
        assert len(engine._active_tasks) == 0
        assert engine._completed_tasks == 0
        assert engine._failed_tasks == 0
        
        # 验证事件监听器注册
        expected_calls = [
            ("producer.task.ready", engine._on_task_ready),
            ("producer.batch.ready", engine._on_batch_ready),
            ("system.shutdown", engine._on_system_shutdown),
            ("system.health_check", engine._on_health_check)
        ]
        
        for event_type, handler in expected_calls:
            mock_event_bus.subscribe.assert_any_call(event_type, handler)
    
    def test_start(self, engine, mock_event_bus, mock_producer):
        """测试启动引擎"""
        engine.start()
        
        assert engine._running
        assert not engine._shutdown_requested
        assert engine._executor is not None
        assert engine._system_executor is not None
        
        # 验证生产者启动
        mock_producer.start.assert_called_once()
        
        # 验证启动事件发布
        calls = mock_event_bus.publish.call_args_list
        start_call = None
        for call in calls:
            if call[0][0] == EngineEvents.ENGINE_STARTED:
                start_call = call
                break
        
        assert start_call is not None
        event_data = start_call[0][1]
        assert event_data["max_workers"] == 4
        assert "timestamp" in event_data
        
        # 清理
        engine.stop()
    
    def test_start_already_running(self, engine):
        """测试重复启动"""
        engine.start()
        assert engine._running
        
        # 再次启动应该被忽略
        engine.start()
        assert engine._running
        
        # 清理
        engine.stop()
    
    def test_stop(self, engine, mock_event_bus, mock_producer):
        """测试停止引擎"""
        engine.start()
        assert engine._running
        
        engine.stop()
        
        assert not engine._running
        assert engine._shutdown_requested
        assert engine._executor is None
        assert engine._system_executor is None
        
        # 验证生产者停止
        mock_producer.stop.assert_called_once()
        
        # 验证停止事件发布
        calls = mock_event_bus.publish.call_args_list
        stop_call = None
        for call in calls:
            if call[0][0] == EngineEvents.ENGINE_STOPPED:
                stop_call = call
                break
        
        assert stop_call is not None
        event_data = stop_call[0][1]
        assert event_data["completed_tasks"] == 0
        assert event_data["failed_tasks"] == 0
        assert "timestamp" in event_data
    
    def test_stop_not_running(self, engine):
        """测试停止未运行的引擎"""
        assert not engine._running
        
        # 停止未运行的引擎应该被忽略
        engine.stop()
        assert not engine._running
    
    def test_submit_system_task(self, engine):
        """测试提交系统任务"""
        engine.start()
        
        task = SystemTask(
            task_type=SystemTaskType.HEALTH_CHECK,
            params={"test": "data"}
        )
        
        future = engine.submit_system_task(task)
        assert isinstance(future, Future)
        
        # 等待任务完成
        result = future.result(timeout=5.0)
        assert isinstance(result, dict)
        assert "running" in result
        
        # 清理
        engine.stop()
    
    def test_submit_system_task_not_running(self, engine):
        """测试在引擎未运行时提交系统任务"""
        task = SystemTask(
            task_type=SystemTaskType.HEALTH_CHECK,
            params={}
        )
        
        with pytest.raises(RuntimeError, match="引擎未运行"):
            engine.submit_system_task(task)
    
    def test_get_status(self, engine):
        """测试获取状态"""
        # 未启动状态
        status = engine.get_status()
        assert not status["running"]
        assert not status["shutdown_requested"]
        assert status["active_tasks"] == 0
        assert status["completed_tasks"] == 0
        assert status["failed_tasks"] == 0
        assert not status["executor_alive"]
        assert not status["system_executor_alive"]
        
        # 启动后状态
        engine.start()
        status = engine.get_status()
        assert status["running"]
        assert not status["shutdown_requested"]
        assert status["executor_alive"]
        assert status["system_executor_alive"]
        
        # 清理
        engine.stop()
    
    def test_on_task_ready_valid_task(self, engine, mock_event_bus):
        """测试处理有效任务就绪事件"""
        engine.start()
        
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={"start_date": "2023-01-01"}
        )
        
        data = {"task": task}
        
        with patch.object(engine, '_submit_business_task') as mock_submit:
            engine._on_task_ready(data)
            mock_submit.assert_called_once_with(task)
        
        # 清理
        engine.stop()
    
    def test_on_task_ready_invalid_data(self, engine):
        """测试处理无效任务数据"""
        engine.start()
        
        # 无效数据格式
        engine._on_task_ready("invalid")
        engine._on_task_ready({"no_task": "data"})
        engine._on_task_ready({"task": "not_a_task"})
        
        # 应该没有异常抛出
        
        # 清理
        engine.stop()
    
    def test_on_batch_ready_valid_tasks(self, engine):
        """测试处理有效批量任务"""
        engine.start()
        
        tasks = [
            DownloadTask(
                symbol="000001.SZ",
                task_type=TaskType.DAILY,
                params={}
            ),
            DownloadTask(
                symbol="000002.SZ",
                task_type=TaskType.DAILY_BASIC,
                params={}
            )
        ]
        
        data = {"tasks": tasks}
        
        with patch.object(engine, '_submit_business_task') as mock_submit:
            engine._on_batch_ready(data)
            assert mock_submit.call_count == 2
            mock_submit.assert_any_call(tasks[0])
            mock_submit.assert_any_call(tasks[1])
        
        # 清理
        engine.stop()
    
    def test_on_batch_ready_invalid_data(self, engine):
        """测试处理无效批量任务数据"""
        engine.start()
        
        # 无效数据格式
        engine._on_batch_ready("invalid")
        engine._on_batch_ready({"no_tasks": "data"})
        engine._on_batch_ready({"tasks": "not_a_list"})
        engine._on_batch_ready({"tasks": ["not_a_task"]})
        
        # 应该没有异常抛出
        
        # 清理
        engine.stop()
    
    def test_on_system_shutdown(self, engine):
        """测试处理系统关闭事件"""
        engine.start()
        
        with patch.object(engine, 'submit_system_task') as mock_submit:
            engine._on_system_shutdown({"reason": "test"})
            
            mock_submit.assert_called_once()
            args = mock_submit.call_args[0]
            task = args[0]
            assert isinstance(task, SystemTask)
            assert task.task_type == SystemTaskType.SHUTDOWN
            assert task.params == {"reason": "test"}
        
        # 清理
        engine.stop()
    
    def test_on_health_check(self, engine):
        """测试处理健康检查事件"""
        engine.start()
        
        with patch.object(engine, 'submit_system_task') as mock_submit:
            engine._on_health_check({"check_type": "full"})
            
            mock_submit.assert_called_once()
            args = mock_submit.call_args[0]
            task = args[0]
            assert isinstance(task, SystemTask)
            assert task.task_type == SystemTaskType.HEALTH_CHECK
            assert task.params == {"check_type": "full"}
        
        # 清理
        engine.stop()
    
    def test_submit_business_task(self, engine, mock_event_bus, mock_producer):
        """测试提交业务任务"""
        engine.start()
        
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={}
        )
        
        engine._submit_business_task(task)
        
        # 验证任务提交事件
        mock_event_bus.publish.assert_any_call(
            EngineEvents.TASK_SUBMITTED,
            {
                "task_id": task.task_id,
                "task_type": task.task_type.value,
                "priority": task.priority.value
            }
        )
        
        # 等待任务完成
        time.sleep(0.1)
        
        # 验证生产者调用
        mock_producer.submit_task.assert_called_with(task)
        
        # 清理
        engine.stop()
    
    def test_submit_business_task_not_running(self, engine):
        """测试在引擎未运行时提交业务任务"""
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={}
        )
        
        # 引擎未运行，任务应该被忽略
        engine._submit_business_task(task)
        
        assert len(engine._active_tasks) == 0
    
    def test_submit_business_task_shutdown_requested(self, engine):
        """测试在请求关闭时提交业务任务"""
        engine.start()
        engine._shutdown_requested = True
        
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={}
        )
        
        # 关闭请求中，任务应该被忽略
        engine._submit_business_task(task)
        
        assert len(engine._active_tasks) == 0
        
        # 清理
        engine.stop()
    
    def test_execute_business_task_success(self, engine, mock_event_bus, mock_producer):
        """测试成功执行业务任务"""
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={}
        )
        
        engine._execute_business_task(task)
        
        # 验证事件发布
        mock_event_bus.publish.assert_any_call(
            EngineEvents.TASK_STARTED,
            {
                "task_id": task.task_id,
                "task_type": task.task_type.value
            }
        )
        
        mock_event_bus.publish.assert_any_call(
            EngineEvents.TASK_COMPLETED,
            {
                "task_id": task.task_id,
                "task_type": task.task_type.value
            }
        )
        
        # 验证生产者调用
        mock_producer.submit_task.assert_called_with(task)
        
        # 验证计数器
        assert engine._completed_tasks == 1
        assert engine._failed_tasks == 0
    
    def test_execute_business_task_failure(self, engine, mock_event_bus, mock_producer):
        """测试执行业务任务失败"""
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={}
        )
        
        # 模拟生产者抛出异常
        mock_producer.submit_task.side_effect = Exception("Test error")
        
        engine._execute_business_task(task)
        
        # 验证失败事件发布
        mock_event_bus.publish.assert_any_call(
            EngineEvents.TASK_FAILED,
            {
                "task_id": task.task_id,
                "task_type": task.task_type.value,
                "error": "Test error"
            }
        )
        
        # 验证计数器
        assert engine._completed_tasks == 0
        assert engine._failed_tasks == 1
    
    def test_handle_shutdown_task(self, engine):
        """测试处理关闭任务"""
        engine.start()
        
        task = SystemTask(
            task_type=SystemTaskType.SHUTDOWN,
            params={"reason": "test"}
        )
        
        with patch.object(engine, 'stop') as mock_stop:
            result = engine._handle_shutdown_task(task)
            mock_stop.assert_called_once()
            assert result == {"status": "shutdown_initiated"}
    
    def test_handle_health_check_task(self, engine):
        """测试处理健康检查任务"""
        engine.start()
        
        task = SystemTask(
            task_type=SystemTaskType.HEALTH_CHECK,
            params={}
        )
        
        result = engine._handle_health_check_task(task)
        
        assert isinstance(result, dict)
        assert "running" in result
        assert result["running"]
        
        # 清理
        engine.stop()
    
    def test_handle_metrics_report_task(self, engine):
        """测试处理指标报告任务"""
        # 设置一些测试数据
        engine._completed_tasks = 10
        engine._failed_tasks = 2
        
        task = SystemTask(
            task_type=SystemTaskType.METRICS_REPORT,
            params={}
        )
        
        result = engine._handle_metrics_report_task(task)
        
        assert isinstance(result, dict)
        assert result["completed_tasks"] == 10
        assert result["failed_tasks"] == 2
        assert result["active_tasks"] == 0
        assert result["success_rate"] == 10 / 12
    
    def test_handle_cleanup_task(self, engine):
        """测试处理清理任务"""
        engine.start()
        
        # 添加一些模拟的已完成任务
        mock_future = Mock()
        mock_future.done.return_value = True
        engine._active_tasks["task1"] = mock_future
        engine._active_tasks["task2"] = mock_future
        
        task = SystemTask(
            task_type=SystemTaskType.CLEANUP,
            params={}
        )
        
        result = engine._handle_cleanup_task(task)
        
        assert result["cleaned_tasks"] == 2
        assert len(engine._active_tasks) == 0
        
        # 清理
        engine.stop()
    
    def test_system_task_with_callback(self, engine):
        """测试带回调的系统任务"""
        engine.start()
        
        callback_result = None
        
        def callback(result):
            nonlocal callback_result
            callback_result = result
        
        task = SystemTask(
            task_type=SystemTaskType.HEALTH_CHECK,
            params={},
            callback=callback
        )
        
        result = engine._execute_system_task(task)
        
        assert callback_result is not None
        assert callback_result == result
        
        # 清理
        engine.stop()
    
    def test_concurrent_task_execution(self, engine, mock_producer):
        """测试并发任务执行"""
        engine.start()
        
        # 创建多个任务
        tasks = [
            DownloadTask(
                symbol=f"00000{i}.SZ",
                task_type=TaskType.DAILY,
                params={}
            )
            for i in range(5)
        ]
        
        # 提交所有任务
        for task in tasks:
            engine._submit_business_task(task)
        
        # 等待任务完成
        time.sleep(0.5)
        
        # 验证所有任务都被提交到生产者
        assert mock_producer.submit_task.call_count == 5
        
        # 清理
        engine.stop()
    
    def test_thread_safety(self, engine):
        """测试线程安全性"""
        engine.start()
        
        results = []
        errors = []
        
        def worker():
            try:
                for i in range(10):
                    status = engine.get_status()
                    results.append(status)
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)
        
        # 创建多个线程同时访问引擎状态
        threads = [threading.Thread(target=worker) for _ in range(5)]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # 验证没有错误发生
        assert len(errors) == 0
        assert len(results) == 50
        
        # 清理
        engine.stop()