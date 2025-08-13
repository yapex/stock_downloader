import tushare as ts
import toml
from box import Box
import pandas as pd
from pathlib import Path
import sys
from typing import Callable, List, Dict, Any, Optional


# --- 全局单例管理，确保 Tushare API 只初始化一次 ---
class TushareManager:
    _instance = None

    def __init__(self, token: str):
        if TushareManager._instance is not None:
            raise Exception("This is a singleton class. Use get_instance().")

        print("Initializing Tushare API...")
        ts.set_token(token)
        self.pro = ts.pro_api()
        self.api_objects = {"pro": self.pro, "ts": ts}
        TushareManager._instance = self

    @staticmethod
    def get_instance(token: str):
        if TushareManager._instance is None:
            TushareManager(token)
        return TushareManager._instance


# --- 辅助函数 ---
def _load_config(config_file: str = "config.toml") -> Box:
    """加载配置文件"""
    config_path = Path(config_file)
    if not config_path.is_file():
        raise FileNotFoundError(f"配置文件不存在: {config_file}")
    return Box(toml.load(config_path))


def _find_task_template(config: Box, task_name: str) -> Box:
    """查找任务模板"""
    for task in config.get("tasks", []):
        if task.get("name") == task_name:
            return task
    raise ValueError(f"未能在配置文件中找到名为 '{task_name}' 的任务。")


# --- 核心：函数构建器 ---
def build_downloader(
    task_name: str, ts_code: Optional[str] = None, **overrides: Any
) -> Callable[[], List[pd.DataFrame]]:
    """
    构建一个数据下载函数 (Function Builder)。

    此函数根据指定的任务模板和运行时参数，生成并返回一个可执行的下载器函数。

    Args:
        task_name (str): config.toml 中定义的任务名称。
        ts_codes (Optional[str], optional): 需要处理的股票代码。
                                                 如果提供，将为每个代码执行一次任务。
                                                 默认为 None。
        **overrides (Any): 运行时参数，用于覆盖 config.toml 中的同名参数。
                           例如: start_date='20240101'。

    Returns:
        Callable[[], List[pd.DataFrame]]:
            一个不接受任何参数的函数。当被调用时，它会执行实际的下载操作，
            并返回一个包含所有结果 DataFrame 的列表。
    """
    # 1. 加载配置并找到任务模板
    config = _load_config()
    task_template = _find_task_template(config, task_name)

    # 2. 初始化 Tushare API (通过单例管理器)
    manager = TushareManager.get_instance(config.tushare.token)
    api_objects = manager.api_objects

    # 3. 合并参数：运行时覆盖配置
    base_params = task_template.params.to_dict()
    base_params.update(overrides)  # 关键：运行时参数 **overrides 覆盖基础配置

    # 4. 定义并返回内部执行函数（闭包）
    def execute() -> List[pd.DataFrame]:
        """
        这个函数在被调用时才真正执行下载。
        它捕获了外层函数的上下文：task_template, base_params, ts_codes 等。
        """
        print(f"\n--- ▶️ 开始执行已构建的任务: '{task_name}' ---")

        # 确定API函数
        base_obj_name = task_template.get("base_object", "pro")
        api_func = getattr(api_objects[base_obj_name], task_template.api_name)

        if ts_code:
            # 迭代模式：为每个代码执行
            print(f"模式: 迭代 (共 {len(ts_code)} 个代码)")
            current_params = base_params.copy()
            current_params["ts_code"] = ts_code
            print(
                f"  [{i + 1}/{ts_code}] 调用: {base_obj_name}.{task_template.api_name}(**{current_params})"
            )
            try:
                df = api_func(**current_params)
            except Exception as e:
                print(f"    -> ❌ 失败: {e}")
        else:
            # 静态模式：执行一次
            print("模式: 静态")
            print(f"  调用: {base_obj_name}.{task_template.api_name}(**{base_params})")
            try:
                df = api_func(**base_params)
            except Exception as e:
                print(f"    -> ❌ 失败: {e}")

        print(f"--- ✅ 任务 '{task_name}' 执行完毕，共返回 {len(df)} 个结果。 ---")
        return df

    # 返回这个准备好的、可随时调用的函数
    return execute


# --- 使用示例 ---
if __name__ == "__main__":
    # 场景一：构建一个下载器，用于获取两只股票的日线，并覆盖 start_date
    print("--- 步骤 1: 构建下载器 downloader_1 (尚未执行) ---")
    downloader_1 = build_downloader(
        task_name="daily_kline",
        ts_code="600519.SH",
        start_date="20250110",  # 覆盖 config 中的 '20250101'
    )
    print("类型:", type(downloader_1))
    print("下载器 downloader_1 已创建，但数据下载尚未发生。")

    # 场景二：构建一个获取全市场股票列表的下载器
    print("\n--- 步骤 2: 构建下载器 downloader_2 (尚未执行) ---")
    downloader_2 = build_downloader(task_name="stock_list")
    print("下载器 downloader_2 已创建。")

    # ... 在程序的其他地方，或在满足某个条件后 ...

    # 步骤三：现在，真正执行下载
    print("\n\n--- 步骤 3: 决定执行已构建的下载器 ---")

    # 执行第一个下载器
    results_1 = downloader_1()
    print("\ndownloader_1 的执行结果:")
    for i, df in enumerate(results_1):
        if isinstance(df, pd.DataFrame):
            print(
                f"  结果 {i + 1} (来自 {df['ts_code'].iloc[0] if not df.empty else 'N/A'}):"
            )
            print(df.head().to_string())

    # 执行第二个下载器
    results_2 = downloader_2()
    print("\ndownloader_2 的执行结果:")
    if results_2 and isinstance(results_2, pd.DataFrame):
        print("  获取到股票列表:")
        print(results_2.head().to_string())
