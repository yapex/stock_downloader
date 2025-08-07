import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class BufferedData:
    """缓冲的数据项"""
    df: pd.DataFrame
    data_type: str
    entity_id: str
    date_col: str
    timestamp: float
    task_name: str


class DataBufferPool:
    """
    数据缓冲池：将数据下载和数据库写入分离
    
    设计原则：
    1. 下载的数据先存入缓冲池，不立即写入数据库
    2. 后台线程定期批量写入数据库
    3. 支持按时间、数量、内存大小等条件触发写入
    4. 提供手动刷新接口
    5. 线程安全
    
    优势：
    - 减少数据库写入频率，提高性能
    - 更好地利用tushare API配额
    - 支持批量写入优化
    - 降低数据库锁竞争
    """

    def __init__(
        self,
        storage,
        max_buffer_size: int = 100,  # 最大缓冲数据项数量
        max_buffer_memory_mb: int = 50,  # 最大缓冲内存（MB）
        flush_interval_seconds: int = 30,  # 自动刷新间隔（秒）
        auto_flush: bool = True,  # 是否启用自动刷新
    ):
        self.storage = storage
        self.max_buffer_size = max_buffer_size
        self.max_buffer_memory_mb = max_buffer_memory_mb
        self.flush_interval_seconds = flush_interval_seconds
        self.auto_flush = auto_flush
        
        # 缓冲区：按 (data_type, entity_id) 分组存储
        self._buffer: Dict[Tuple[str, str], List[BufferedData]] = defaultdict(list)
        self._buffer_lock = threading.RLock()
        
        # 统计信息
        self._stats = {
            'total_buffered': 0,
            'total_flushed': 0,
            'last_flush_time': None,
            'flush_count': 0
        }
        
        # 后台刷新线程
        self._flush_thread = None
        self._stop_event = threading.Event()
        
        if self.auto_flush:
            self.start_auto_flush()
    
    def add_data(
        self, 
        df: pd.DataFrame, 
        data_type: str, 
        entity_id: str, 
        date_col: str,
        task_name: str = "unknown"
    ) -> bool:
        """
        添加数据到缓冲池
        
        Args:
            df: 要缓冲的数据
            data_type: 数据类型
            entity_id: 实体ID（如股票代码）
            date_col: 日期列名
            task_name: 任务名称
            
        Returns:
            bool: 是否成功添加
        """
        if df is None or df.empty:
            return True
            
        buffered_data = BufferedData(
            df=df.copy(),
            data_type=data_type,
            entity_id=entity_id,
            date_col=date_col,
            timestamp=time.time(),
            task_name=task_name
        )
        
        with self._buffer_lock:
            key = (data_type, entity_id)
            self._buffer[key].append(buffered_data)
            self._stats['total_buffered'] += 1
            
            logger.debug(
                f"数据已添加到缓冲池: {data_type}_{entity_id}, "
                f"当前缓冲项: {self.get_buffer_size()}"
            )
            
            # 检查是否需要触发刷新
            if self._should_flush():
                self._trigger_flush()
                
        return True
    
    def _should_flush(self) -> bool:
        """检查是否应该触发刷新"""
        # 检查缓冲项数量
        if self.get_buffer_size() >= self.max_buffer_size:
            logger.debug(f"缓冲项数量达到上限 {self.max_buffer_size}，触发刷新")
            return True
            
        # 检查内存使用
        memory_mb = self.get_buffer_memory_mb()
        if memory_mb >= self.max_buffer_memory_mb:
            logger.debug(f"缓冲内存达到上限 {self.max_buffer_memory_mb}MB，触发刷新")
            return True
            
        return False
    
    def _trigger_flush(self):
        """触发立即刷新（在锁内调用）"""
        # 在后台线程中执行刷新，避免阻塞当前线程
        flush_thread = threading.Thread(target=self._flush_all, daemon=True)
        flush_thread.start()
    
    def get_buffer_size(self) -> int:
        """获取当前缓冲项总数"""
        with self._buffer_lock:
            return sum(len(items) for items in self._buffer.values())
    
    def get_buffer_memory_mb(self) -> float:
        """估算当前缓冲区内存使用（MB）"""
        with self._buffer_lock:
            total_bytes = 0
            for items in self._buffer.values():
                for item in items:
                    # 粗略估算DataFrame内存使用
                    total_bytes += item.df.memory_usage(deep=True).sum()
            return total_bytes / (1024 * 1024)
    
    def flush_all(self) -> int:
        """手动刷新所有缓冲数据"""
        return self._flush_all()
    
    def _flush_all(self) -> int:
        """内部刷新方法"""
        flushed_count = 0
        
        with self._buffer_lock:
            if not self._buffer:
                return 0
                
            # 复制缓冲区数据并清空
            buffer_copy = dict(self._buffer)
            self._buffer.clear()
            
        logger.info(f"开始刷新缓冲池，共 {sum(len(items) for items in buffer_copy.values())} 项")
        
        # 在锁外执行数据库写入
        for (data_type, entity_id), items in buffer_copy.items():
            try:
                flushed_count += self._flush_entity_data(data_type, entity_id, items)
            except Exception as e:
                logger.error(f"刷新 {data_type}_{entity_id} 数据失败: {e}")
                # 失败的数据重新加入缓冲区
                with self._buffer_lock:
                    self._buffer[(data_type, entity_id)].extend(items)
        
        # 更新统计信息
        with self._buffer_lock:
            self._stats['total_flushed'] += flushed_count
            self._stats['last_flush_time'] = datetime.now()
            self._stats['flush_count'] += 1
            
        logger.info(f"缓冲池刷新完成，写入 {flushed_count} 项数据")
        return flushed_count
    
    def _flush_entity_data(self, data_type: str, entity_id: str, items: List[BufferedData]) -> int:
        """刷新单个实体的数据"""
        if not items:
            return 0
            
        # 按时间排序
        items.sort(key=lambda x: x.timestamp)
        
        # 合并同类型数据
        if len(items) == 1:
            # 单项数据直接写入
            item = items[0]
            self.storage.save_incremental(item.df, data_type, entity_id, item.date_col)
            logger.debug(f"写入单项数据: {data_type}_{entity_id}, {len(item.df)} 行")
            return 1
        else:
            # 多项数据合并后写入
            try:
                # 使用第一项的date_col（假设同一实体的date_col相同）
                date_col = items[0].date_col
                
                # 合并所有DataFrame
                combined_df = pd.concat([item.df for item in items], ignore_index=True)
                
                # 去重并排序
                if date_col in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(subset=[date_col])
                    combined_df = combined_df.sort_values(date_col)
                
                self.storage.save_incremental(combined_df, data_type, entity_id, date_col)
                logger.debug(
                    f"合并写入数据: {data_type}_{entity_id}, "
                    f"合并 {len(items)} 项为 {len(combined_df)} 行"
                )
                return len(items)
                
            except Exception as e:
                logger.error(f"合并数据失败，逐项写入: {e}")
                # 合并失败时逐项写入
                count = 0
                for item in items:
                    try:
                        self.storage.save_incremental(item.df, data_type, entity_id, item.date_col)
                        count += 1
                    except Exception as item_e:
                        logger.error(f"写入单项数据失败: {item_e}")
                return count
    
    def start_auto_flush(self):
        """启动自动刷新线程"""
        if self._flush_thread and self._flush_thread.is_alive():
            return
            
        self._stop_event.clear()
        self._flush_thread = threading.Thread(target=self._auto_flush_worker, daemon=True)
        self._flush_thread.start()
        logger.info(f"自动刷新线程已启动，间隔 {self.flush_interval_seconds} 秒")
    
    def stop_auto_flush(self):
        """停止自动刷新线程"""
        if self._flush_thread:
            self._stop_event.set()
            self._flush_thread.join(timeout=5)
            logger.info("自动刷新线程已停止")
    
    def _auto_flush_worker(self):
        """自动刷新工作线程"""
        while not self._stop_event.wait(self.flush_interval_seconds):
            try:
                if self.get_buffer_size() > 0:
                    self._flush_all()
            except Exception as e:
                logger.error(f"自动刷新失败: {e}")
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        with self._buffer_lock:
            stats = self._stats.copy()
            stats['current_buffer_size'] = self.get_buffer_size()
            stats['current_buffer_memory_mb'] = self.get_buffer_memory_mb()
            return stats
    
    def shutdown(self):
        """关闭缓冲池，刷新所有数据"""
        logger.info("正在关闭数据缓冲池...")
        
        # 停止自动刷新
        self.stop_auto_flush()
        
        # 刷新所有剩余数据
        remaining = self.get_buffer_size()
        if remaining > 0:
            logger.info(f"刷新剩余的 {remaining} 项缓冲数据")
            self._flush_all()
        
        logger.info("数据缓冲池已关闭")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()