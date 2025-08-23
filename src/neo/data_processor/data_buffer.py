"""数据缓冲器模块

提供线程安全的数据缓冲和批量处理功能，以及基于asyncio的异步数据缓冲功能。
"""

import asyncio
import threading
from collections import defaultdict, deque
from typing import Dict, Callable, Optional, Deque, Union, Awaitable
import pandas as pd
from .interfaces import IDataBuffer


class CallbackQueueBuffer(IDataBuffer):
    """基于回调的队列缓冲器
    
    提供线程安全的数据缓冲、批量处理和定时刷新功能。
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __init__(self, flush_interval: float = 30.0):
        """初始化缓冲器
        
        Args:
            flush_interval: 定时刷新间隔（秒）
        """
        self._buffers: Dict[str, Deque[pd.DataFrame]] = defaultdict(deque)
        self._callbacks: Dict[str, Callable[[str, pd.DataFrame], bool]] = {}
        self._max_sizes: Dict[str, int] = {}
        self._flush_interval = flush_interval
        self._shutdown_event = threading.Event()
        self._flush_thread: Optional[threading.Thread] = None
        self._buffer_lock = threading.RLock()
        self._start_flush_thread()
    
    @classmethod
    def get_instance(cls, flush_interval: float = 30.0) -> 'CallbackQueueBuffer':
        """获取线程安全的单例实例
        
        Args:
            flush_interval: 定时刷新间隔（秒）
            
        Returns:
            CallbackQueueBuffer: 单例实例
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(flush_interval)
        return cls._instance
    
    def register_type(self, data_type: str, callback: Callable[[str, pd.DataFrame], bool], max_size: int = 100) -> None:
        """注册数据类型和对应的回调函数
        
        Args:
            data_type: 数据类型标识
            callback: 数据处理回调函数
            max_size: 缓冲区最大大小
        """
        with self._buffer_lock:
            self._callbacks[data_type] = callback
            self._max_sizes[data_type] = max_size
    
    def add(self, data_type: str, item: pd.DataFrame) -> None:
        """添加数据到缓冲区
        
        Args:
            data_type: 数据类型标识
            item: 要添加的数据
        """
        with self._buffer_lock:
            if data_type not in self._callbacks:
                raise ValueError(f"Data type '{data_type}' not registered")
            
            self._buffers[data_type].append(item)
            
            # 检查是否需要立即刷新 - 基于DataFrame行数而非队列大小
            max_size = self._max_sizes.get(data_type, 100)
            total_rows = sum(len(df) for df in self._buffers[data_type])
            if total_rows >= max_size:
                self._flush_buffer(data_type)
    
    def flush(self, data_type: Optional[str] = None) -> bool:
        """刷新缓冲区数据
        
        Args:
            data_type: 指定要刷新的数据类型，None表示刷新所有类型
            
        Returns:
            bool: 刷新是否成功
        """
        if data_type is None:
            # 刷新所有类型
            success = True
            with self._buffer_lock:
                for dt in list(self._buffers.keys()):
                    if not self._flush_buffer(dt):
                        success = False
            return success
        else:
            return self._flush_buffer(data_type)
    
    def get_buffer_sizes(self) -> Dict[str, int]:
        """获取各数据类型的缓冲区大小（总行数）
        
        Returns:
            各数据类型的缓冲区总行数字典
        """
        with self._buffer_lock:
            return {data_type: sum(len(df) for df in buffer) for data_type, buffer in self._buffers.items()}
    
    def shutdown(self) -> None:
        """关闭缓冲器，清理资源
        """
        self._shutdown_event.set()
        if self._flush_thread and self._flush_thread.is_alive():
            self._flush_thread.join(timeout=5.0)
        
        # 最后一次刷新所有数据
        self.flush()
        
        with self._buffer_lock:
            self._buffers.clear()
            self._callbacks.clear()
            self._max_sizes.clear()
    
    def _start_flush_thread(self) -> None:
        """启动定时刷新线程
        """
        self._flush_thread = threading.Thread(target=self._timed_flush_loop, daemon=True)
        self._flush_thread.start()
    
    def _timed_flush_loop(self) -> None:
        """定时刷新循环
        """
        while not self._shutdown_event.is_set():
            try:
                if self._shutdown_event.wait(self._flush_interval):
                    break
                self.flush()
            except Exception:
                # 忽略刷新过程中的异常，避免线程退出
                pass
    
    def _flush_buffer(self, data_type: str) -> bool:
        """刷新指定类型的缓冲区
        
        Args:
            data_type: 数据类型标识
            
        Returns:
            bool: 刷新是否成功
        """
        with self._buffer_lock:
            if data_type not in self._buffers or not self._buffers[data_type]:
                return True
            
            callback = self._callbacks.get(data_type)
            if not callback:
                return False
            
            # 合并所有数据
            data_frames = list(self._buffers[data_type])
            self._buffers[data_type].clear()
            
            if not data_frames:
                return True
            
            try:
                # 合并DataFrame
                combined_df = pd.concat(data_frames, ignore_index=True)
                result = callback(data_type, combined_df)
                if not result:
                    # 如果回调返回False，将数据放回缓冲区
                    self._buffers[data_type].extendleft(reversed(data_frames))
                return result
            except Exception:
                # 如果处理失败，将数据放回缓冲区
                self._buffers[data_type].extendleft(reversed(data_frames))
                return False


class AsyncCallbackQueueBuffer:
    """基于asyncio的异步回调队列缓冲器
    
    使用asyncio事件循环实现无锁异步数据缓冲、批量处理和定时刷新功能。
    所有操作都在同一个事件循环中调度，避免了并发问题。
    """
    
    _instance = None
    _lock = asyncio.Lock()
    
    def __init__(self, flush_interval: float = 30.0):
        """初始化异步缓冲器
        
        Args:
            flush_interval: 定时刷新间隔（秒）
        """
        self._buffers: Dict[str, Deque[pd.DataFrame]] = defaultdict(deque)
        self._callbacks: Dict[str, Union[Callable[[str, pd.DataFrame], bool], Callable[[str, pd.DataFrame], Awaitable[bool]]]] = {}
        self._max_sizes: Dict[str, int] = {}
        self._flush_interval = flush_interval
        self._shutdown_flag = False
        self._timer_task: Optional[asyncio.Task] = None
        self._start_timer_task()
    
    @classmethod
    async def get_instance(cls, flush_interval: float = 30.0) -> 'AsyncCallbackQueueBuffer':
        """获取异步单例实例
        
        Args:
            flush_interval: 定时刷新间隔（秒）
            
        Returns:
            AsyncCallbackQueueBuffer: 单例实例
        """
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(flush_interval)
        return cls._instance
    
    def register_type(self, data_type: str, callback: Union[Callable[[str, pd.DataFrame], bool], Callable[[str, pd.DataFrame], Awaitable[bool]]], max_size: int = 100) -> None:
        """注册数据类型和对应的回调函数
        
        Args:
            data_type: 数据类型标识
            callback: 数据处理回调函数（可以是同步或异步函数）
            max_size: 缓冲区最大大小（基于DataFrame行数）
        """
        self._callbacks[data_type] = callback
        self._max_sizes[data_type] = max_size
    
    async def add(self, data_type: str, item: pd.DataFrame) -> None:
        """添加数据到缓冲区
        
        Args:
            data_type: 数据类型标识
            item: 要添加的数据
        """
        if data_type not in self._callbacks:
            raise ValueError(f"Data type '{data_type}' not registered")
        
        self._buffers[data_type].append(item)
        
        # 检查是否需要立即刷新 - 基于DataFrame行数而非队列大小
        max_size = self._max_sizes.get(data_type, 100)
        total_rows = sum(len(df) for df in self._buffers[data_type])
        if total_rows >= max_size:
            # 使用create_task使得刷新操作不会阻塞当前的add调用
            asyncio.create_task(self._flush_buffer(data_type))
    
    async def flush(self, data_type: Optional[str] = None) -> bool:
        """刷新缓冲区数据
        
        Args:
            data_type: 指定要刷新的数据类型，None表示刷新所有类型
            
        Returns:
            bool: 刷新是否成功
        """
        if data_type is None:
            # 刷新所有类型
            flush_tasks = [self._flush_buffer(dt) for dt in list(self._buffers.keys())]
            results = await asyncio.gather(*flush_tasks, return_exceptions=True)
            return all(result is True for result in results if not isinstance(result, Exception))
        else:
            return await self._flush_buffer(data_type)
    
    def get_buffer_sizes(self) -> Dict[str, int]:
        """获取各数据类型的缓冲区大小（总行数）
        
        Returns:
            各数据类型的缓冲区总行数字典
        """
        return {data_type: sum(len(df) for df in buffer) for data_type, buffer in self._buffers.items()}
    
    async def shutdown(self) -> None:
        """关闭异步缓冲器，清理资源
        """
        self._shutdown_flag = True
        
        # 取消定时任务
        if self._timer_task and not self._timer_task.done():
            self._timer_task.cancel()
            try:
                await self._timer_task
            except asyncio.CancelledError:
                pass
        
        # 最后一次刷新所有数据
        await self.flush()
        
        # 清理缓冲区
        self._buffers.clear()
        self._callbacks.clear()
        self._max_sizes.clear()
    
    def _start_timer_task(self) -> None:
        """启动定时刷新任务
        """
        self._timer_task = asyncio.create_task(self._timed_flush_loop())
    
    async def _timed_flush_loop(self) -> None:
        """定时刷新循环
        """
        while not self._shutdown_flag:
            try:
                await asyncio.sleep(self._flush_interval)
                if not self._shutdown_flag:
                    await self.flush()
            except asyncio.CancelledError:
                break
            except Exception:
                # 忽略刷新过程中的异常，避免任务退出
                pass
    
    async def _flush_buffer(self, data_type: str) -> bool:
        """刷新指定类型的缓冲区
        
        Args:
            data_type: 数据类型标识
            
        Returns:
            bool: 刷新是否成功
        """
        if data_type not in self._buffers or not self._buffers[data_type]:
            return True
        
        callback = self._callbacks.get(data_type)
        if not callback:
            return False
        
        # 获取所有数据并清空缓冲区
        data_frames = list(self._buffers[data_type])
        self._buffers[data_type].clear()
        
        if not data_frames:
            return True
        
        try:
            # 合并DataFrame
            combined_df = pd.concat(data_frames, ignore_index=True)
            
            # 执行回调函数（支持同步和异步）
            if asyncio.iscoroutinefunction(callback):
                result = await callback(data_type, combined_df)
            else:
                # 在事件循环的线程池中运行同步回调，避免阻塞
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(None, callback, data_type, combined_df)
            
            if not result:
                # 如果回调返回False，将数据放回缓冲区
                self._buffers[data_type].extendleft(reversed(data_frames))
            return result
        except Exception:
            # 如果处理失败，将数据放回缓冲区
            self._buffers[data_type].extendleft(reversed(data_frames))
            return False


# 便利函数
def get_data_buffer(flush_interval: float = 30.0) -> AsyncCallbackQueueBuffer:
    """获取数据缓冲器单例实例（异步版本）
    
    Args:
        flush_interval: 定时刷新间隔（秒）
        
    Returns:
        AsyncCallbackQueueBuffer: 异步缓冲器实例
    """
    # 使用asyncio.run来获取实例，确保返回的是实际对象而不是协程
    try:
        asyncio.get_running_loop()
        # 如果已经在事件循环中，创建一个任务
        if AsyncCallbackQueueBuffer._instance is None:
            AsyncCallbackQueueBuffer._instance = AsyncCallbackQueueBuffer(flush_interval)
        return AsyncCallbackQueueBuffer._instance
    except RuntimeError:
        # 如果没有运行的事件循环，使用asyncio.run
        return asyncio.run(AsyncCallbackQueueBuffer.get_instance(flush_interval))


def get_sync_data_buffer(flush_interval: float = 30.0) -> CallbackQueueBuffer:
    """获取同步数据缓冲器单例实例
    
    Args:
        flush_interval: 定时刷新间隔（秒）
        
    Returns:
        CallbackQueueBuffer: 同步缓冲器实例
    """
    return CallbackQueueBuffer.get_instance(flush_interval)


async def get_async_data_buffer(flush_interval: float = 30.0) -> AsyncCallbackQueueBuffer:
    """获取异步数据缓冲器单例实例
    
    Args:
        flush_interval: 定时刷新间隔（秒）
        
    Returns:
        AsyncCallbackQueueBuffer: 异步缓冲器实例
    """
    return await AsyncCallbackQueueBuffer.get_instance(flush_interval)