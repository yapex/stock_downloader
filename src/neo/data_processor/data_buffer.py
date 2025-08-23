"""数据缓冲器模块

提供线程安全的数据缓冲和批量处理功能。
"""

import threading
import time
from collections import defaultdict, deque
from typing import Dict, Callable, Optional, Deque
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


# 便利函数
def get_data_buffer(flush_interval: float = 30.0) -> CallbackQueueBuffer:
    """获取数据缓冲器单例实例
    
    Args:
        flush_interval: 定时刷新间隔（秒）
        
    Returns:
        CallbackQueueBuffer: 缓冲器实例
    """
    return CallbackQueueBuffer.get_instance(flush_interval)