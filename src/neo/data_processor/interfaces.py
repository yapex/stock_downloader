"""数据处理器接口定义

定义数据处理相关的接口规范。
"""

from typing import Protocol, Callable, Optional, Awaitable, Union
import pandas as pd


class IDataProcessor(Protocol):
    """数据处理器接口

    专注于数据清洗、转换和验证，不处理业务逻辑。
    """

    def process(self, task_type: str, data: pd.DataFrame) -> bool:
        """处理任务结果

        Args:
            task_type: 任务类型字符串
            data: 要处理的数据

        Returns:
            bool: 处理是否成功
        """
        ...


class IDataBuffer(Protocol):
    """数据缓冲器接口

    提供数据缓冲、批量处理和异步刷新功能。
    支持同步和异步两种模式的操作。
    """

    def register_type(
        self,
        data_type: str,
        callback: Union[
            Callable[[str, pd.DataFrame], bool],
            Callable[[str, pd.DataFrame], Awaitable[bool]],
        ],
        max_size: int = 100,
    ) -> None:
        """注册数据类型和对应的回调函数

        Args:
            data_type: 数据类型标识
            callback: 数据处理回调函数（支持同步或异步）
            max_size: 缓冲区最大大小
        """
        ...

    def add(self, data_type: str, item: pd.DataFrame) -> Union[None, Awaitable[None]]:
        """添加数据到缓冲区

        Args:
            data_type: 数据类型标识
            item: 要添加的数据

        Returns:
            同步实现返回None，异步实现返回Awaitable[None]
        """
        ...

    def flush(self, data_type: Optional[str] = None) -> Union[bool, Awaitable[bool]]:
        """刷新缓冲区数据

        Args:
            data_type: 指定要刷新的数据类型，None表示刷新所有类型

        Returns:
            同步实现返回bool，异步实现返回Awaitable[bool]
        """
        ...

    def shutdown(self) -> Union[None, Awaitable[None]]:
        """关闭缓冲器，清理资源

        Returns:
            同步实现返回None，异步实现返回Awaitable[None]
        """
        ...
