# -*- coding: utf-8 -*-
"""
封装了数据下载器的核心业务逻辑。
"""
import logging
import time
from typing import List, Optional, Dict, Any

from .config import load_config
from .engine import DownloadEngine
from .fetcher import TushareFetcher
from .storage import DuckDBStorage


class DownloaderApp:
    """
    主应用程序类，封装了数据下载的核心业务逻辑。
    这个类将业务逻辑从 UI 层分离，使其更容易测试。
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def process_symbols_config(
        self, config: Dict[str, Any], symbols: Optional[List[str]] = None
    ) -> tuple[Dict[str, Any], bool]:
        """
        根据命令行参数调整配置中的股票符号。

        Args:
            config: 配置字典
            symbols: 命令行指定的股票符号列表

        Returns:
            (修改后的配置字典, 是否被命令行参数覆盖)
        """
        if not symbols:
            return config, False

        # 确保 config 中有 downloader 节点
        if "downloader" not in config:
            config["downloader"] = {}

        if len(symbols) == 1 and symbols[0].lower() == "all":
            self.logger.debug("命令行指定下载所有A股。")
            config["downloader"]["symbols"] = "all"
        else:
            self.logger.debug(f"命令行指定股票池: {list(symbols)}")
            config["downloader"]["symbols"] = list(symbols)

        return config, True

    def create_components(
        self, config: Dict[str, Any]
    ) -> tuple[TushareFetcher, DuckDBStorage]:
        """
        创建下载系统的核心组件。

        Args:
            config: 配置字典

        Returns:
            (fetcher, storage) 元组
        """
        fetcher = TushareFetcher()
        storage = DuckDBStorage(
            db_path=config.get("storage", {}).get("db_path", "data/stock.db")
        )
        return fetcher, storage

    def run_download(
        self,
        config_path: str = "config.yaml",
        group_name: str = "default",
        symbols: Optional[List[str]] = None,
        force: bool = False,
    ) -> bool:
        """
        执行��据下载任务。

        Args:
            config_path: 配置文件路径
            group_name: 要执行的组名
            symbols: 指定的股票符号列表
            force: 是否强制执行

        Returns:
            是否成功执行

        Raises:
            FileNotFoundError: 配置文件不存在
            ValueError: 配置参数错误
            Exception: 其他异常
        """
        self.logger.info(f"开始执行任务组: {group_name}")
        start_time = time.time()
        
        try:
            # 加载配置
            raw_config = load_config(config_path)
            
            # 提取指定的组配置
            if "groups" not in raw_config:
                raise ValueError("配置文件缺少 'groups' 节点")
            
            if group_name not in raw_config["groups"]:
                available_groups = list(raw_config["groups"].keys())
                raise ValueError(f"找不到组 '{group_name}'。可用的组: {available_groups}")
            
            group_config = raw_config["groups"][group_name]
            self.logger.info(f"使用组配置: {group_config.get('description', group_name)}")
            
            # 解析任务引用并转换���旧格式的配置结构
            task_definitions = raw_config.get("tasks", {})
            task_references = group_config.get("tasks", [])
            
            # 将任务引用转换为完整的任务配置
            resolved_tasks = []
            for task_ref in task_references:
                if isinstance(task_ref, str):
                    # 新格式：任务引用
                    if task_ref not in task_definitions:
                        raise ValueError(f"任务引用 '{task_ref}' 在配置中未定义")
                    
                    task_def = task_definitions[task_ref].copy()
                    # 设置默认值
                    task_def["enabled"] = True
                    task_def["update_strategy"] = task_def.get("update_strategy", "incremental")
                    
                    # stock_list 任务特殊处理：使用 cooldown 策略
                    if task_def.get("type") == "stock_list":
                        task_def["update_strategy"] = "cooldown"
                        task_def["update_interval_hours"] = task_def.get("update_interval_hours", 23)
                    
                    resolved_tasks.append(task_def)
                else:
                    # 旧格式：完整任务定义（向后兼容）
                    resolved_tasks.append(task_ref)
            
            config = {
                "storage": raw_config.get("storage", {}),
                "downloader": {
                    "symbols": group_config.get("symbols", []),
                    "max_concurrent_tasks": group_config.get("max_concurrent_tasks", 1)
                },
                "tasks": resolved_tasks
            }
            
            # 处理命令行股票参数覆盖
            config, symbols_overridden = self.process_symbols_config(config, symbols)

            # 创建组件
            fetcher, storage = self.create_components(config)
            engine = DownloadEngine(config, fetcher, storage, force_run=force, symbols_overridden=symbols_overridden, group_name=group_name)

            # 执行下载
            engine.run()
            
            # 获取执行统计
            stats = engine.get_execution_stats()
            self._log_execution_summary(stats)

            return True

        except (FileNotFoundError, ValueError) as e:
            self.logger.critical(f"程序启动失败: {e}")
            raise
        except Exception as e:
            self.logger.critical(f"程序主流程发生严重错误: {e}", exc_info=True)
            raise
        finally:
            elapsed_time = time.time() - start_time
            self.logger.info("全部任务已完成，耗时 %.2f 秒", elapsed_time)

    def _log_execution_summary(self, stats: Dict[str, Any]):
        """
        记录执行统计摘要
        
        Args:
            stats: 执行统计数据
        """
        total_symbols = stats.get('total_symbols', 0)
        failed_tasks = stats.get('failed_tasks', [])
        
        if total_symbols == 0:
            # 没有股票符号的任务（如更新股票列表）
            if failed_tasks:
                self.logger.info(f"执行统计: 任务失败 - {', '.join(failed_tasks)} (详细信息请查看 logs/downloader.log)")
            else:
                self.logger.info("执行统计: 所有任务成功完成")
        else:
            # 有股票符号的任务
            if failed_tasks:
                self.logger.info(f"执行统计: 处理 {total_symbols} 只股票，任务失败 - {', '.join(failed_tasks)} (详细信息请查看 logs/downloader.log)")
            else:
                self.logger.info(f"执行统计: 成功处理 {total_symbols} 只股票")
