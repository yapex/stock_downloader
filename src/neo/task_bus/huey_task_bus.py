"""基于Huey的任务总线实现

基于Huey的任务队列管理和调度。
"""

import logging
from typing import Dict, Any
from huey import SqliteHuey

from ..config import get_config
from ..data_processor.interfaces import IDataProcessor
from .interfaces import ITaskBus
from .types import TaskResult

logger = logging.getLogger(__name__)

# 全局Huey实例
_huey_instance = None
_task_registered = False
_global_process_task_result = None


def _get_registered_task():
    """获取已注册的任务函数"""
    global _global_process_task_result
    return _global_process_task_result


def get_huey() -> SqliteHuey:
    """获取Huey实例（单例模式）"""
    global _huey_instance
    if _huey_instance is None:
        config = get_config()
        _huey_instance = SqliteHuey(
            filename=config.huey.db_file,
            immediate=config.huey.immediate
        )
    return _huey_instance


class HueyTaskBus(ITaskBus):
    """基于Huey的任务总线实现"""
    
    def __init__(self, data_processor: IDataProcessor):
        """初始化任务总线
        
        Args:
            data_processor: 数据处理器实例
        """
        self.huey = get_huey()
        self.data_processor = data_processor
        self._register_tasks()
    
    def _register_tasks(self):
        """注册Huey任务"""
        global _task_registered
        
        if _task_registered:
            # 如果任务已经注册，直接获取已注册的任务
            self._process_task_result = _get_registered_task()
            return
        
        # 保存self引用供任务函数使用
        task_bus_instance = self
        
        @self.huey.task()
        def process_task_result(task_result_data: Dict[str, Any]) -> None:
            """处理TaskResult的Huey任务
            
            Args:
                task_result_data: 序列化的TaskResult数据
            """
            try:
                # 反序列化TaskResult
                task_result = task_bus_instance._deserialize_task_result(task_result_data)
                
                # 使用DataProcessor处理TaskResult
                success = task_bus_instance.data_processor.process(task_result)
                
                if success:
                    logger.info(f"TaskResult处理完成: {task_result.config.task_type.value}, symbol: {task_result.config.symbol}")
                else:
                    logger.warning(f"TaskResult处理失败: {task_result.config.task_type.value}, symbol: {task_result.config.symbol}")
                
            except Exception as e:
                logger.error(f"处理TaskResult时出错: {e}")
                raise
        
        # 保存任务函数引用
        self._process_task_result = process_task_result
        _task_registered = True
        
        # 保存全局任务引用
        global _global_process_task_result
        _global_process_task_result = process_task_result
    
    def submit_task(self, task_result: TaskResult) -> None:
        """提交任务到队列
        
        Args:
            task_result: 任务执行结果
        """
        # 序列化TaskResult为字典格式
        task_result_data = self._serialize_task_result(task_result)
        
        # 提交到Huey队列
        self._process_task_result(task_result_data)
        
        logger.debug(f"TaskResult已提交到队列: {task_result.config.task_type.value}, symbol: {task_result.config.symbol}")
    
    def start_consumer(self) -> None:
        """启动消费者
        
        注意：这个方法在实际使用中应该通过命令行启动Huey consumer。
        这里只是为了接口完整性而提供。
        """
        logger.info("请使用命令行启动Huey consumer: huey_consumer neo.task_bus.huey")
    
    def _serialize_task_result(self, task_result: TaskResult) -> Dict[str, Any]:
        """序列化TaskResult为字典格式"""
        return {
            'config': {
                'symbol': task_result.config.symbol,
                'task_type': task_result.config.task_type.value,
                'priority': task_result.config.priority.value,
                'max_retries': task_result.config.max_retries
            },
            'success': task_result.success,
            'data': task_result.data.to_dict() if task_result.data is not None else None,
            'error': str(task_result.error) if task_result.error else None,
            'retry_count': task_result.retry_count
        }
    
    def _deserialize_task_result(self, task_result_data: Dict[str, Any]) -> TaskResult:
        """反序列化TaskResult"""
        from .types import DownloadTaskConfig, TaskType, TaskPriority
        import pandas as pd
        
        config_data = task_result_data['config']
        
        # 处理TaskType
        task_type = TaskType(config_data['task_type'])
        
        # 处理TaskPriority
        priority = TaskPriority(config_data['priority'])
        
        config = DownloadTaskConfig(
            symbol=config_data['symbol'],
            task_type=task_type,
            priority=priority,
            max_retries=config_data['max_retries']
        )
        
        # 处理数据
        data = task_result_data['data']
        if data is not None:
            data = pd.DataFrame(data)
        
        # 处理错误
        error = None
        if task_result_data['error']:
            error = Exception(task_result_data['error'])
        
        return TaskResult(
            config=config,
            success=task_result_data['success'],
            data=data,
            error=error,
            retry_count=task_result_data.get('retry_count', 0)
        )