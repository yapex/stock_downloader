"""基于Huey的任务总线实现

基于Huey的任务队列管理和调度。
"""

import logging
from typing import Dict, Any

from .interfaces import ITaskBus
from .types import TaskResult
from .huey_config import huey

logger = logging.getLogger(__name__)


def _deserialize_task_result_static(task_result_data: Dict[str, Any]) -> 'TaskResult':
    """静态方法：反序列化任务结果
    
    Args:
        task_result_data: 序列化的TaskResult数据
        
    Returns:
        TaskResult: 反序列化后的任务结果
    """
    import pandas as pd
    from ..downloader.types import DownloadTaskConfig, TaskType, TaskPriority
    
    # 反序列化配置
    config_data = task_result_data["config"]
    config = DownloadTaskConfig(
        symbol=config_data["symbol"],
        task_type=TaskType(config_data["task_type"]),
        priority=TaskPriority(config_data["priority"]),
    )
    
    # 反序列化数据
    data = None
    if task_result_data["data"]:
        data = pd.DataFrame(task_result_data["data"])
    
    # 反序列化错误
    error = None
    if task_result_data["error"]:
        error = Exception(task_result_data["error"])
    
    return TaskResult(
        config=config,
        success=task_result_data["success"],
        data=data,
        error=error,
        retry_count=task_result_data.get("retry_count", 0),
    )


# 模块级别的任务定义
@huey.task(retries=2, retry_delay=5)
def process_task_result(task_result_data: Dict[str, Any]) -> None:
    """处理任务结果

    Args:
        task_result_data: 序列化的TaskResult数据
    """
    try:
        from . import tasks

        # 反序列化任务结果
        task_result = _deserialize_task_result_static(task_result_data)
        logger.info(f"开始处理任务结果: {task_result.config.symbol}")

        # 使用全局数据处理器获取函数
        processor = tasks.get_data_processor()
        processor.process(task_result)

        logger.info(f"任务结果处理完成: {task_result.config.symbol}")

    except Exception as e:
        logger.error(f"处理TaskResult时出错: {e}")
        raise


class HueyTaskBus(ITaskBus):
    """基于Huey的任务总线实现"""

    def __init__(self, data_processor, config):
        """初始化HueyTaskBus

        Args:
            data_processor: 数据处理器实例
            config: 配置对象
        """
        self.data_processor = data_processor
        self.config = config

        # 配置Huey日志级别
        huey_logger = logging.getLogger("huey")
        huey_logger.setLevel(logging.CRITICAL)
        huey_logger.propagate = False

        # 使用全局Huey实例
        self.huey = huey

        # 设置全局数据处理器
        if data_processor is not None:
            from . import tasks

            tasks.set_data_processor(data_processor)

        # 引用模块级别的任务函数
        self.process_task_result = process_task_result





    def submit_task(self, task_result: TaskResult) -> None:
        """提交任务到队列

        Args:
            task_result: 任务结果对象
        """
        try:
            # 序列化TaskResult
            task_result_data = self._serialize_task_result(task_result)

            # 异步提交任务到队列
            process_task_result(task_result_data)

            logger.info(f"任务已提交到队列: {task_result.config.symbol}")

        except Exception as e:
            logger.error(f"提交任务到队列时出错: {e}")
            raise

    def start_consumer(self) -> None:
        """启动任务消费者

        启动消费者进程来处理队列中的任务。
        在测试环境中，可能会使用 immediate 模式立即执行任务。
        """
        if self.huey.immediate:
            # 在 immediate 模式下，任务会立即执行，无需启动消费者
            logger.debug("Huey 处于 immediate 模式，任务将立即执行")
            return

        # 在生产环境中，启动消费者进程
        from huey.consumer import Consumer

        consumer = Consumer(self.huey)
        logger.info("启动 Huey 消费者")
        consumer.run()

    def _serialize_task_result(self, task_result: TaskResult) -> Dict[str, Any]:
        """序列化TaskResult为字典格式"""
        return {
            "config": {
                "symbol": task_result.config.symbol,
                "task_type": task_result.config.task_type.value,
                "priority": task_result.config.priority.value,
                "max_retries": task_result.config.max_retries,
            },
            "success": task_result.success,
            "data": task_result.data.to_dict("records")
            if task_result.data is not None
            else None,
            "error": str(task_result.error) if task_result.error else None,
            "retry_count": task_result.retry_count,
        }

    def _get_data_processor(self):
        """获取数据处理器实例

        如果构造函数中没有注入数据处理器，则使用默认实现。
        """
        if self.data_processor is not None:
            return self.data_processor

        # 延迟导入避免循环导入
        from ..data_processor.simple_data_processor import SimpleDataProcessor

        return SimpleDataProcessor()

    def _process_task_result_impl(self, task_result_data: Dict[str, Any]) -> None:
        """处理TaskResult的实现函数

        Args:
            task_result_data: 序列化的TaskResult数据
        """
        try:
            # 获取数据处理器
            processor = self._get_data_processor()

            # 反序列化TaskResult
            task_result = self._deserialize_task_result(task_result_data)

            logger.info(f"开始处理任务结果: {task_result.config.symbol}")

            # 处理任务结果
            processor.process(task_result)

            logger.info(f"任务结果处理完成: {task_result.config.symbol}")

        except Exception as e:
            logger.error(f"处理TaskResult时出错: {e}")
            raise

    def _deserialize_task_result(self, task_result_data: Dict[str, Any]) -> TaskResult:
        """反序列化TaskResult

        Args:
            task_result_data: 序列化的TaskResult数据

        Returns:
            TaskResult: 反序列化后的TaskResult对象
        """
        # 延迟导入避免循环导入
        from .types import DownloadTaskConfig, TaskType, TaskPriority, TaskResult
        import pandas as pd

        config_data = task_result_data["config"]

        # 处理TaskType
        task_type = TaskType(config_data["task_type"])

        # 处理TaskPriority
        priority = TaskPriority(config_data["priority"])

        config = DownloadTaskConfig(
            symbol=config_data["symbol"],
            task_type=task_type,
            priority=priority,
            max_retries=config_data["max_retries"],
        )

        # 处理数据
        data = task_result_data["data"]
        if data is not None:
            # 数据已经是 'records' 格式，直接转换为 DataFrame
            data = pd.DataFrame.from_records(data)

        # 处理错误
        error = None
        if task_result_data["error"]:
            error = Exception(task_result_data["error"])

        return TaskResult(
            config=config,
            success=task_result_data["success"],
            data=data,
            error=error,
            retry_count=task_result_data.get("retry_count", 0),
        )
