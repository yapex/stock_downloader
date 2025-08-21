import logging
import threading
from collections import defaultdict
from typing import Dict, List

import pandas as pd

from downloader.task.types import TaskType
from downloader.task.types import TaskResult
from .interfaces import IDataProcessor, IBatchSaver


logger = logging.getLogger(__name__)


class DataProcessor:
    """数据处理器实现
    
    负责处理TaskResult，按任务类型分组数据，并批量保存到数据库。
    """
    
    def __init__(self, batch_saver: IBatchSaver, batch_size: int = 100):
        """初始化数据处理器
        
        Args:
            batch_saver: 批量保存器
            batch_size: 批量处理大小
        """
        self.batch_saver = batch_saver
        self.batch_size = batch_size
        self._data_groups: Dict[TaskType, List[pd.DataFrame]] = defaultdict(list)
        self._lock = threading.Lock()
        self._processed_count = 0
        self._failed_count = 0
    
    def process_task_result(self, task_result: TaskResult) -> None:
        """处理单个任务结果
        
        Args:
            task_result: 任务执行结果
        """
        if not task_result.success:
            logger.warning(
                f"跳过失败的任务结果: {task_result.config.task_type.name}, "
                f"symbol: {task_result.config.symbol}, error: {task_result.error}"
            )
            with self._lock:
                self._failed_count += 1
            return
        
        if task_result.data is None or task_result.data.empty:
            logger.warning(
                f"跳过空数据的任务结果: {task_result.config.task_type.name}, "
                f"symbol: {task_result.config.symbol}"
            )
            return
        
        task_type = task_result.config.task_type
        
        with self._lock:
            self._data_groups[task_type].append(task_result.data)
            self._processed_count += 1
            
            # 检查是否需要批量保存
            total_rows = sum(len(df) for df in self._data_groups[task_type])
            if total_rows >= self.batch_size:
                self._save_batch(task_type)
    
    def flush_pending_data(self) -> None:
        """刷新待处理数据，强制保存所有缓存的数据"""
        with self._lock:
            for task_type in list(self._data_groups.keys()):
                if self._data_groups[task_type]:
                    self._save_batch(task_type)
    
    def _save_batch(self, task_type: TaskType) -> None:
        """保存指定任务类型的批量数据
        
        Args:
            task_type: 任务类型
        
        Note:
            此方法需要在持有锁的情况下调用
        """
        if not self._data_groups[task_type]:
            return
        
        try:
            # 合并所有DataFrame
            combined_data = pd.concat(self._data_groups[task_type], ignore_index=True)
            
            # 清空缓存
            self._data_groups[task_type].clear()
            
            # 保存数据
            logger.info(f"开始批量保存 {len(combined_data)} 条 {task_type.name} 数据")
            success = self.batch_saver.save_batch(task_type, combined_data)
            
            if success:
                logger.info(f"成功批量保存 {len(combined_data)} 条 {task_type.name} 数据")
            else:
                logger.error(f"批量保存 {task_type.name} 数据失败")
                
        except Exception as e:
            logger.error(f"批量保存 {task_type.name} 数据时发生异常: {e}")
            # 清空缓存以避免重复处理
            self._data_groups[task_type].clear()
    
    def get_stats(self) -> Dict[str, int]:
        """获取处理统计信息
        
        Returns:
            包含处理统计信息的字典
        """
        with self._lock:
            pending_count = sum(len(dfs) for dfs in self._data_groups.values())
            return {
                'processed_count': self._processed_count,
                'failed_count': self._failed_count,
                'pending_count': pending_count
            }