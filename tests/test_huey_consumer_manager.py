"""HueyConsumerManager 测试

测试 HueyConsumerManager 类的功能，包括 Consumer 生命周期管理。
"""

import asyncio
import signal
import sys
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from neo.helpers.huey_consumer_manager import HueyConsumerManager


class TestHueyConsumerManager:
    """HueyConsumerManager 类测试"""

    def setup_method(self):
        """每个测试方法前的设置"""
        # 重置类变量
        HueyConsumerManager._consumer_instance = None

    def teardown_method(self):
        """每个测试方法后的清理"""
        # 清理类变量
        HueyConsumerManager._consumer_instance = None

    def test_setup_signal_handlers(self):
        """测试信号处理器设置"""
        with patch('signal.signal') as mock_signal:
            HueyConsumerManager.setup_signal_handlers()
            
            # 验证设置了正确的信号处理器
            assert mock_signal.call_count == 2
            mock_signal.assert_any_call(signal.SIGINT, mock_signal.call_args_list[0][0][1])
            mock_signal.assert_any_call(signal.SIGTERM, mock_signal.call_args_list[1][0][1])

    def test_setup_huey_logging(self):
        """测试 Huey 日志配置"""
        with patch('logging.basicConfig') as mock_basic_config, \
             patch('logging.getLogger') as mock_get_logger:
            
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            HueyConsumerManager.setup_huey_logging()
            
            # 验证基础日志配置
            mock_basic_config.assert_called_once()
            
            # 验证 Huey 日志级别设置
            mock_get_logger.assert_called_with("huey")
            mock_logger.setLevel.assert_called_once()

    @patch('neo.helpers.huey_consumer_manager.get_config')
    @patch('huey.consumer.Consumer')
    def test_start_consumer_new_instance(self, mock_consumer_class, mock_get_config):
        """测试启动新的 Consumer 实例"""
        # 模拟配置
        mock_config = Mock()
        mock_config.huey.max_workers = 4
        mock_get_config.return_value = mock_config
        
        # 模拟 Consumer 实例
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # 调用方法
        result = HueyConsumerManager.start_consumer()
        
        # 验证结果
        assert result == mock_consumer
        assert HueyConsumerManager._consumer_instance == mock_consumer
        
        # 验证 Consumer 创建参数
        mock_consumer_class.assert_called_once()
        call_args = mock_consumer_class.call_args
        assert call_args[1]['workers'] == 4
        assert call_args[1]['worker_type'] == 'thread'

    @patch('neo.helpers.huey_consumer_manager.get_config')
    @patch('huey.consumer.Consumer')
    def test_start_consumer_existing_instance(self, mock_consumer_class, mock_get_config):
        """测试返回已存在的 Consumer 实例"""
        # 设置已存在的实例
        existing_consumer = Mock()
        HueyConsumerManager._consumer_instance = existing_consumer
        
        # 调用方法
        result = HueyConsumerManager.start_consumer()
        
        # 验证返回已存在的实例
        assert result == existing_consumer
        # 验证没有创建新实例
        mock_consumer_class.assert_not_called()

    def test_stop_consumer_with_instance(self):
        """测试停止存在的 Consumer 实例"""
        # 创建模拟 Consumer
        mock_consumer = Mock()
        HueyConsumerManager._consumer_instance = mock_consumer
        
        # 调用方法
        result = HueyConsumerManager.stop_consumer()
        
        # 验证结果
        assert result is True
        assert HueyConsumerManager._consumer_instance is None
        mock_consumer.stop.assert_called_once()

    def test_stop_consumer_without_instance(self):
        """测试停止不存在的 Consumer 实例"""
        # 确保没有实例
        HueyConsumerManager._consumer_instance = None
        
        # 调用方法
        result = HueyConsumerManager.stop_consumer()
        
        # 验证结果
        assert result is True

    def test_stop_consumer_with_exception(self):
        """测试停止 Consumer 时发生异常"""
        # 创建会抛出异常的模拟 Consumer
        mock_consumer = Mock()
        mock_consumer.stop.side_effect = Exception("Stop failed")
        HueyConsumerManager._consumer_instance = mock_consumer
        
        # 调用方法
        result = HueyConsumerManager.stop_consumer()
        
        # 验证结果
        assert result is False
        # 实例应该仍然存在，因为停止失败
        assert HueyConsumerManager._consumer_instance == mock_consumer

    def test_get_consumer_instance_with_instance(self):
        """测试获取存在的 Consumer 实例"""
        mock_consumer = Mock()
        HueyConsumerManager._consumer_instance = mock_consumer
        
        result = HueyConsumerManager.get_consumer_instance()
        
        assert result == mock_consumer

    def test_get_consumer_instance_without_instance(self):
        """测试获取不存在的 Consumer 实例"""
        HueyConsumerManager._consumer_instance = None
        
        result = HueyConsumerManager.get_consumer_instance()
        
        assert result is None

    def test_stop_consumer_if_running_with_instance(self):
        """测试有实例时的条件停止"""
        mock_consumer = Mock()
        HueyConsumerManager._consumer_instance = mock_consumer
        
        with patch.object(HueyConsumerManager, 'stop_consumer', return_value=True) as mock_stop:
            result = HueyConsumerManager.stop_consumer_if_running()
            
            assert result is True
            mock_stop.assert_called_once()

    def test_stop_consumer_if_running_without_instance(self):
        """测试无实例时的条件停止"""
        HueyConsumerManager._consumer_instance = None
        
        with patch.object(HueyConsumerManager, 'stop_consumer') as mock_stop:
            result = HueyConsumerManager.stop_consumer_if_running()
            
            assert result is True
            mock_stop.assert_not_called()

    @patch('neo.helpers.huey_consumer_manager.get_config')
    @patch('huey.consumer.Consumer')
    @patch('asyncio.new_event_loop')
    @patch('asyncio.set_event_loop')
    @patch('concurrent.futures.ThreadPoolExecutor')
    def test_run_consumer_standalone_success(self, mock_executor, mock_set_loop, 
                                           mock_new_loop, mock_consumer_class, mock_get_config):
        """测试独立运行 Consumer 成功"""
        # 模拟配置
        mock_config = Mock()
        mock_config.huey.max_workers = 4
        mock_get_config.return_value = mock_config
        
        # 模拟事件循环
        mock_loop = Mock()
        mock_new_loop.return_value = mock_loop
        
        # 模拟 Consumer
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # 模拟 ThreadPoolExecutor
        mock_executor_instance = Mock()
        mock_future = Mock()
        mock_executor_instance.submit.return_value = mock_future
        mock_executor.return_value.__enter__.return_value = mock_executor_instance
        
        # 调用方法
        HueyConsumerManager.run_consumer_standalone()
        
        # 验证调用
        mock_new_loop.assert_called_once()
        mock_set_loop.assert_called_once_with(mock_loop)
        mock_executor_instance.submit.assert_called_once()
        mock_future.result.assert_called_once()
        mock_loop.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_wait_for_all_tasks_completion_no_tasks(self):
        """测试等待任务完成 - 无任务情况"""
        with patch('neo.configs.huey') as mock_huey:
            mock_huey.pending_count.return_value = 0
            
            # 模拟没有 Consumer 实例
            HueyConsumerManager._consumer_instance = None
            
            # 调用方法
            await HueyConsumerManager.wait_for_all_tasks_completion(max_wait_time=5)
            
            # 验证调用了 pending_count
            assert mock_huey.pending_count.call_count >= 1

    @pytest.mark.asyncio
    async def test_wait_for_all_tasks_completion_with_tasks(self):
        """测试等待任务完成 - 有任务情况"""
        with patch('neo.configs.huey') as mock_huey, \
             patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            # 模拟任务数量变化：先有任务，后无任务
            call_count = 0
            def mock_pending_count():
                nonlocal call_count
                call_count += 1
                if call_count <= 2:
                    return 2 if call_count == 1 else 1
                else:
                    return 0
            
            mock_huey.pending_count = mock_pending_count
            
            # 模拟 Consumer 实例，同步模拟活跃任务数的变化
            mock_consumer = Mock()
            mock_consumer._pool = Mock()
            mock_consumer._pool._threads = 4
            
            # 动态模拟 _idle 列表的变化，与 pending_count 同步
            def mock_idle_property(self):
                # 当没有 pending 任务时，所有线程都应该是空闲的
                current_pending = mock_huey.pending_count()
                if current_pending == 0:
                    return [Mock(), Mock(), Mock(), Mock()]  # 4个空闲线程
                else:
                    # 有任务时，部分线程忙碌
                    return [Mock(), Mock()]  # 2个空闲线程，意味着2个在工作
            
            type(mock_consumer._pool)._idle = property(mock_idle_property)
            HueyConsumerManager._consumer_instance = mock_consumer
            
            # 调用方法
            await HueyConsumerManager.wait_for_all_tasks_completion(max_wait_time=5)
            
            # 验证调用了 pending_count 足够多次（至少3次，但可能更多由于稳定性检查）
            assert call_count >= 3
            assert mock_sleep.call_count >= 2  # 前几次有任务时会sleep

    @pytest.mark.asyncio
    async def test_start_consumer_async(self):
        """测试异步启动 Consumer"""
        with patch.object(HueyConsumerManager, 'start_consumer') as mock_start, \
             patch('asyncio.get_event_loop') as mock_get_loop, \
             patch('neo.helpers.huey_consumer_manager.get_config') as mock_get_config:
            
            # 模拟配置
            mock_config = Mock()
            mock_config.huey.max_workers = 4
            mock_get_config.return_value = mock_config
            
            # 模拟 Consumer
            mock_consumer = Mock()
            mock_consumer.run = Mock()
            mock_start.return_value = mock_consumer
            
            # 模拟事件循环和任务，让 run_in_executor 实际执行传入的函数
            mock_loop = Mock()
            mock_task = Mock()
            
            def mock_run_in_executor(executor, func):
                # 实际执行传入的函数以触发 start_consumer 调用
                func()
                return mock_task
            
            mock_loop.run_in_executor.side_effect = mock_run_in_executor
            mock_get_loop.return_value = mock_loop
            
            # 调用方法
            result = await HueyConsumerManager.start_consumer_async()
            
            # 验证结果
            assert result == mock_task
            # start_consumer 会在 run_consumer_sync 函数内部被调用
            mock_start.assert_called_once()
            # run_in_executor 会被调用
            mock_loop.run_in_executor.assert_called_once()
            # 验证 consumer.run 被调用
            mock_consumer.run.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_consumer_async_with_task(self):
        """测试异步停止 Consumer - 有任务"""
        with patch.object(HueyConsumerManager, 'stop_consumer_if_running') as mock_stop:
            # 创建模拟任务
            mock_task = AsyncMock()
            
            # 调用方法
            await HueyConsumerManager.stop_consumer_async(mock_task)
            
            # 验证调用
            mock_task.cancel.assert_called_once()
            mock_stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_consumer_async_without_task(self):
        """测试异步停止 Consumer - 无任务"""
        with patch.object(HueyConsumerManager, 'stop_consumer_if_running') as mock_stop:
            # 调用方法
            await HueyConsumerManager.stop_consumer_async()
            
            # 验证调用
            mock_stop.assert_called_once()


class TestHueyConsumerManagerIntegration:
    """HueyConsumerManager 集成测试"""

    def setup_method(self):
        """每个测试方法前的设置"""
        # 重置类变量
        HueyConsumerManager._consumer_instance = None

    def teardown_method(self):
        """每个测试方法后的清理"""
        # 清理类变量
        HueyConsumerManager._consumer_instance = None

    @patch('neo.helpers.huey_consumer_manager.get_config')
    @patch('huey.consumer.Consumer')
    def test_consumer_lifecycle(self, mock_consumer_class, mock_get_config):
        """测试 Consumer 完整生命周期"""
        # 模拟配置
        mock_config = Mock()
        mock_config.huey.max_workers = 4
        mock_get_config.return_value = mock_config
        
        # 模拟 Consumer
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # 1. 启动 Consumer
        consumer1 = HueyConsumerManager.start_consumer()
        assert consumer1 == mock_consumer
        assert HueyConsumerManager.get_consumer_instance() == mock_consumer
        
        # 2. 再次启动应该返回同一实例
        consumer2 = HueyConsumerManager.start_consumer()
        assert consumer2 == mock_consumer
        assert consumer1 is consumer2
        
        # 3. 停止 Consumer
        result = HueyConsumerManager.stop_consumer()
        assert result is True
        assert HueyConsumerManager.get_consumer_instance() is None
        
        # 4. 再次停止应该仍然成功
        result = HueyConsumerManager.stop_consumer()
        assert result is True