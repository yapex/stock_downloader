"""数据获取器构建器模块

基于函数构建器模式，提供灵活的数据获取器构建功能。
支持配置驱动的任务模板和运行时参数覆盖。
现在支持从 stock_schema.toml 动态生成任务类型。
"""

import tushare as ts
from typing import Callable, Any, Optional
import pandas as pd
import logging
from threading import Lock
import os

from neo.helpers import normalize_stock_code
from neo.task_bus.types import TaskType, TaskTemplateRegistry

logger = logging.getLogger(__name__)


class TushareApiManager:
    """Tushare API 管理器（单例模式）"""

    _instance: Optional["TushareApiManager"] = None
    _lock = Lock()

    def __init__(self):
        if TushareApiManager._instance is not None:
            raise RuntimeError("TushareApiManager 是单例类，请使用 get_instance() 方法")
        self._initialize()

    @classmethod
    def get_instance(cls) -> "TushareApiManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_api_function(self, base_object: str, method_name: str):
        """获取 API 函数"""
        if base_object not in self.api_objects:
            raise ValueError(f"不支持的 API 对象: {base_object}")

        api_obj = self.api_objects[base_object]
        if not hasattr(api_obj, method_name):
            raise ValueError(f"API 对象 {base_object} 没有方法 {method_name}")

        return getattr(api_obj, method_name)

    def _initialize(self):
        """初始化 Tushare API"""
        ts.set_token(os.environ.get("TUSHARE_TOKEN"))
        self.pro = ts.pro_api()
        self.ts = ts
        self.api_objects = {"pro": self.pro, "ts": self.ts}


class FetcherBuilder:
    """数据获取器构建器"""

    def __init__(self):
        self.api_manager = TushareApiManager.get_instance()

    def build_by_task(
        self, task_type: TaskType, **kwargs: Any
    ) -> Callable[[], pd.DataFrame]:
        """构建指定股票代码的数据获取器

        Args:
            task_type: 任务类型
            **kwargs: 运行时参数，例如 symbol, start_date 等

        Returns:
            可执行的数据获取函数
        """
        # 获取任务模板配置
        try:
            template = TaskTemplateRegistry.get_template(task_type)
        except KeyError:
            raise ValueError(f"不支持的任务类型: {task_type}")

        # 标准化股票代码（如果提供了有效的股票代码）
        if 'symbol' in kwargs:
            symbol = kwargs.pop('symbol')
            # 只有当 symbol 不为空时才标准化
            if symbol:
                kwargs["ts_code"] = normalize_stock_code(symbol)

        # 合并参数：模板的固定参数 + 运行时的动态参数
        merged_params = {}
        if template.required_params:
            merged_params.update(template.required_params)
        merged_params.update(kwargs)

        # 获取 API 函数
        api_func = self.api_manager.get_api_function(
            template.base_object, template.api_method
        )

        def execute() -> pd.DataFrame:
            """执行数据获取"""
            try:
                result = api_func(**merged_params)
                logger.debug(
                    f"成功获取 {len(result)} 条记录 - 函数: {template.base_object}.{template.api_method}, 参数: {merged_params}"
                )
                return result
            except Exception as e:
                logger.error(
                    f"任务执行失败 - 函数: {template.base_object}.{template.api_method}, 参数: {merged_params}, 错误: {e}"
                )
                raise

        return execute


if __name__ == "__main__":
    # 最精简的用法示例
    task_type = TaskType.stock_daily
    # print(task_type)
    fetcher = FetcherBuilder().build_by_task(task_type, "600519")
    df = fetcher()
    print(df.head(1))
    fetcher = FetcherBuilder().build_by_task(task_type)
    df = fetcher()
    print(df["ts_code"].tail(3))
