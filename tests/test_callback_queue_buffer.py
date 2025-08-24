"""CallbackQueueBuffer测试用例

测试数据缓冲器的核心功能。
"""

import pytest
import pandas as pd
import threading
import time
from unittest.mock import Mock
from src.neo.data_processor.data_buffer import CallbackQueueBuffer, get_data_buffer


class TestCallbackQueueBuffer:
    """CallbackQueueBuffer测试类"""

    def setup_method(self):
        """测试前准备"""
        # 重置单例
        CallbackQueueBuffer._instance = None
        self.buffer = CallbackQueueBuffer(flush_interval=0.1)
        self.mock_callback = Mock(return_value=True)

    def teardown_method(self):
        """测试后清理"""
        if hasattr(self, "buffer"):
            self.buffer.shutdown()
        CallbackQueueBuffer._instance = None

    def test_register_type(self):
        """测试注册数据类型"""
        self.buffer.register_type("test_type", self.mock_callback, max_size=50)

        assert "test_type" in self.buffer._callbacks
        assert self.buffer._callbacks["test_type"] == self.mock_callback
        assert self.buffer._max_sizes["test_type"] == 50

    def test_add_data_success(self):
        """测试成功添加数据"""
        self.buffer.register_type("test_type", self.mock_callback)

        test_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        self.buffer.add("test_type", test_df)

        assert len(self.buffer._buffers["test_type"]) == 1

    def test_add_data_unregistered_type(self):
        """测试添加未注册类型的数据"""
        test_df = pd.DataFrame({"col1": [1, 2]})

        with pytest.raises(ValueError, match="Data type 'unknown_type' not registered"):
            self.buffer.add("unknown_type", test_df)

    def test_auto_flush_on_max_size(self):
        """测试达到最大行数时自动刷新

        验证自动刷新是基于DataFrame的总行数而非DataFrame的数量
        """
        self.buffer.register_type("test_type", self.mock_callback, max_size=3)

        # 添加第一个DataFrame（1行），未达到阈值
        test_df1 = pd.DataFrame({"col1": [1], "col2": ["a"]})
        self.buffer.add("test_type", test_df1)

        # 验证缓冲区状态：1行数据，未触发刷新
        buffer_sizes = self.buffer.get_buffer_sizes()
        assert buffer_sizes["test_type"] == 1
        assert len(self.buffer._buffers["test_type"]) == 1
        self.mock_callback.assert_not_called()

        # 添加第二个DataFrame（1行），总共2行，仍未达到阈值
        test_df2 = pd.DataFrame({"col1": [2], "col2": ["b"]})
        self.buffer.add("test_type", test_df2)

        # 验证缓冲区状态：2行数据，未触发刷新
        buffer_sizes = self.buffer.get_buffer_sizes()
        assert buffer_sizes["test_type"] == 2
        assert len(self.buffer._buffers["test_type"]) == 2
        self.mock_callback.assert_not_called()

        # 添加第三个DataFrame（2行），总共4行，超过阈值，应触发刷新
        test_df3 = pd.DataFrame({"col1": [3, 4], "col2": ["c", "d"]})
        self.buffer.add("test_type", test_df3)

        # 验证自动刷新被触发
        self.mock_callback.assert_called_once()

        # 验证缓冲区被清空
        buffer_sizes = self.buffer.get_buffer_sizes()
        assert buffer_sizes.get("test_type", 0) == 0
        assert len(self.buffer._buffers.get("test_type", [])) == 0

    def test_manual_flush_specific_type(self):
        """测试手动刷新指定类型"""
        self.buffer.register_type("type1", self.mock_callback)
        mock_callback2 = Mock(return_value=True)
        self.buffer.register_type("type2", mock_callback2)

        test_df1 = pd.DataFrame({"col1": [1]})
        test_df2 = pd.DataFrame({"col1": [2]})

        self.buffer.add("type1", test_df1)
        self.buffer.add("type2", test_df2)

        result = self.buffer.flush("type1")

        assert result is True
        self.mock_callback.assert_called_once()
        mock_callback2.assert_not_called()
        assert len(self.buffer._buffers["type1"]) == 0
        assert len(self.buffer._buffers["type2"]) == 1

    def test_manual_flush_all_types(self):
        """测试手动刷新所有类型"""
        self.buffer.register_type("type1", self.mock_callback)
        mock_callback2 = Mock(return_value=True)
        self.buffer.register_type("type2", mock_callback2)

        test_df1 = pd.DataFrame({"col1": [1]})
        test_df2 = pd.DataFrame({"col1": [2]})

        self.buffer.add("type1", test_df1)
        self.buffer.add("type2", test_df2)

        result = self.buffer.flush()

        assert result is True
        self.mock_callback.assert_called_once()
        mock_callback2.assert_called_once()
        assert len(self.buffer._buffers["type1"]) == 0
        assert len(self.buffer._buffers["type2"]) == 0

    def test_flush_empty_buffer(self):
        """测试刷新空缓冲区"""
        self.buffer.register_type("test_type", self.mock_callback)

        result = self.buffer.flush("test_type")

        assert result is True
        self.mock_callback.assert_not_called()

    def test_flush_callback_failure(self):
        """测试回调函数失败的情况"""
        failed_callback = Mock(return_value=False)
        self.buffer.register_type("test_type", failed_callback)

        test_df = pd.DataFrame({"col1": [1]})
        self.buffer.add("test_type", test_df)

        result = self.buffer.flush("test_type")

        assert result is False
        failed_callback.assert_called_once()
        # 数据应该被放回缓冲区
        assert len(self.buffer._buffers["test_type"]) == 1

    def test_flush_callback_exception(self):
        """测试回调函数抛出异常的情况"""
        exception_callback = Mock(side_effect=Exception("Test exception"))
        self.buffer.register_type("test_type", exception_callback)

        test_df = pd.DataFrame({"col1": [1]})
        self.buffer.add("test_type", test_df)

        result = self.buffer.flush("test_type")

        assert result is False
        exception_callback.assert_called_once()
        # 数据应该被放回缓冲区
        assert len(self.buffer._buffers["test_type"]) == 1

    def test_data_combination(self):
        """测试数据合并功能"""
        self.buffer.register_type("test_type", self.mock_callback)

        test_df1 = pd.DataFrame({"col1": [1], "col2": ["a"]})
        test_df2 = pd.DataFrame({"col1": [2], "col2": ["b"]})

        self.buffer.add("test_type", test_df1)
        self.buffer.add("test_type", test_df2)
        self.buffer.flush("test_type")

        # 验证回调函数接收到合并后的数据
        self.mock_callback.assert_called_once()
        call_args = self.mock_callback.call_args
        combined_df = call_args[0][1]

        assert len(combined_df) == 2
        assert list(combined_df["col1"]) == [1, 2]
        assert list(combined_df["col2"]) == ["a", "b"]

    def test_timed_flush(self):
        """测试定时刷新功能"""
        # 使用很短的刷新间隔
        buffer = CallbackQueueBuffer(flush_interval=0.05)
        buffer.register_type("test_type", self.mock_callback)

        test_df = pd.DataFrame({"col1": [1]})
        buffer.add("test_type", test_df)

        # 等待定时刷新触发
        time.sleep(0.1)

        self.mock_callback.assert_called()
        buffer.shutdown()

    def test_shutdown(self):
        """测试关闭功能"""
        self.buffer.register_type("test_type", self.mock_callback)

        test_df = pd.DataFrame({"col1": [1]})
        self.buffer.add("test_type", test_df)

        self.buffer.shutdown()

        # 关闭时应该刷新所有数据
        self.mock_callback.assert_called_once()
        assert len(self.buffer._buffers) == 0
        assert len(self.buffer._callbacks) == 0
        assert len(self.buffer._max_sizes) == 0

    def test_thread_safety(self):
        """测试线程安全性"""
        self.buffer.register_type("test_type", self.mock_callback, max_size=100)

        def add_data(thread_id):
            for i in range(10):
                test_df = pd.DataFrame({"thread": [thread_id], "value": [i]})
                self.buffer.add("test_type", test_df)

        threads = []
        for i in range(5):
            thread = threading.Thread(target=add_data, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # 验证所有数据都被正确添加
        total_items = sum(len(buffer) for buffer in self.buffer._buffers.values())
        assert total_items == 50


class TestSingleton:
    """单例模式测试类"""

    def teardown_method(self):
        """测试后清理"""
        if CallbackQueueBuffer._instance:
            CallbackQueueBuffer._instance.shutdown()
        CallbackQueueBuffer._instance = None

    def test_singleton_same_instance(self):
        """测试单例返回相同实例"""
        instance1 = CallbackQueueBuffer.get_instance()
        instance2 = CallbackQueueBuffer.get_instance()

        assert instance1 is instance2

    def test_singleton_thread_safety(self):
        """测试单例的线程安全性"""
        instances = []

        def get_instance():
            instances.append(CallbackQueueBuffer.get_instance())

        threads = []
        for _ in range(10):
            thread = threading.Thread(target=get_instance)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # 所有实例应该是同一个对象
        first_instance = instances[0]
        for instance in instances[1:]:
            assert instance is first_instance

    def test_get_data_buffer_convenience_function(self):
        """测试便利函数（现在返回异步版本）"""
        from src.neo.data_processor.data_buffer import AsyncCallbackQueueBuffer

        buffer1 = get_data_buffer()
        buffer2 = get_data_buffer()

        assert buffer1 is buffer2
        assert isinstance(buffer1, AsyncCallbackQueueBuffer)
