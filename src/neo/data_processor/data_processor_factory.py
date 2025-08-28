"""数据处理器工厂

根据任务类型和配置选择合适的数据处理器。
"""

import logging
from typing import Dict, Any

from ..configs import get_config
from .interfaces import IDataProcessor

logger = logging.getLogger(__name__)


class DataProcessorFactory:
    """数据处理器工厂
    
    根据任务类型的更新策略配置，选择合适的数据处理器。
    """

    def __init__(self, container):
        """初始化工厂
        
        Args:
            container: 依赖注入容器
        """
        self.container = container
        self.config = get_config()

    def create_processor(self, task_type: str) -> IDataProcessor:
        """创建数据处理器
        
        Args:
            task_type: 任务类型
            
        Returns:
            IDataProcessor: 数据处理器实例
        """
        update_strategy = self._get_update_strategy(task_type)
        
        if update_strategy == "full_replace":
            logger.debug(f"为 {task_type} 创建全量替换数据处理器")
            return self.container.full_replace_data_processor()
        else:
            # 默认使用简单数据处理器（增量更新）
            logger.debug(f"为 {task_type} 创建简单数据处理器（增量更新）")
            return self.container.data_processor()

    def _get_update_strategy(self, task_type: str) -> str:
        """获取任务类型的更新策略
        
        Args:
            task_type: 任务类型
            
        Returns:
            str: 更新策略
        """
        try:
            # 从配置中获取更新策略
            download_tasks_config = self.config.download_tasks
            
            # 检查任务类型的配置
            if hasattr(download_tasks_config, task_type):
                task_config = getattr(download_tasks_config, task_type)
                if hasattr(task_config, 'update_strategy'):
                    strategy = task_config.update_strategy
                    logger.debug(f"任务 {task_type} 使用配置的更新策略: {strategy}")
                    return strategy
            
            # 默认策略：增量更新
            logger.debug(f"任务 {task_type} 使用默认更新策略: incremental")
            return "incremental"
            
        except Exception as e:
            logger.warning(f"获取 {task_type} 更新策略失败，使用默认策略: {e}")
            return "incremental"