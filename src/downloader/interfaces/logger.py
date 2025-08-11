"""日志接口定义

定义日志相关的Protocol接口。
"""

from typing import Protocol
import logging


class ILogger(Protocol):
    """日志接口协议"""
    
    def debug(self, message: str, *args, **kwargs) -> None:
        """记录调试信息"""
        ...
    
    def info(self, message: str, *args, **kwargs) -> None:
        """记录信息"""
        ...
    
    def warning(self, message: str, *args, **kwargs) -> None:
        """记录警告"""
        ...
    
    def error(self, message: str, *args, **kwargs) -> None:
        """记录错误"""
        ...


class StandardLogger(ILogger):
    """标准日志实现"""
    
    def __init__(self, logger):
        self._logger = logger
    
    def debug(self, message: str, *args, **kwargs) -> None:
        self._logger.debug(message, *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs) -> None:
        self._logger.info(message, *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs) -> None:
        self._logger.warning(message, *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs) -> None:
        self._logger.error(message, *args, **kwargs)


class LoggerFactory:
    """日志工厂"""
    
    @staticmethod
    def create_logger(name: str) -> ILogger:
        """创建日志实例"""
        logger = logging.getLogger(name)
        return StandardLogger(logger)