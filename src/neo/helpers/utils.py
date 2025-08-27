import re
import logging
import warnings
from datetime import datetime, timedelta
from typing import Optional


def normalize_stock_code(code: str) -> str:
    """
    将股票代码标准化为 `代码.市场` 的格式，例如 `600519.SH`。

    - 支持纯数字、带市场前缀/后缀等多种格式，例如:
      - `600519` -> `600519.SH`
      - `SH600519` -> `600519.SH`
      - `600519SH` -> `600519.SH`
      - `sh600519` -> `600519.SH`
      - `000001.SZ` -> `000001.SZ`
    - 根据A股代码规则自动判断并添加 `.SH` (上海) 或 `.SZ` (深圳) 后缀。
    - 如果代码格式无法识别，则会抛出 ValueError。
    """
    if not isinstance(code, str):
        raise TypeError(f"股票代码必须是字符串，而不是 {type(code)}")

    # 提取6位数字代码
    match = re.search(r"(\d{6})", code)
    if not match:
        raise ValueError(f"无法从 '{code}' 中提取有效的6位股票代码")

    stock_number = match.group(1)

    # 根据前缀判断交易所
    if stock_number.startswith("6"):
        exchange = "SH"  # 上海证券交易所
    elif stock_number.startswith(("0", "3")):
        exchange = "SZ"  # 深圳证券交易所
    elif stock_number.startswith(("4", "8", "9")):
        exchange = "BJ"  # 北京证券交易所（新三板转板、原新三板精选层）
    else:
        raise ValueError(f"无法识别的股票代码前缀: {stock_number}")

    return f"{stock_number}.{exchange}"


def is_interval_greater_than_7_days(start_date: str, end_date: str) -> bool:
    """
    检查两个日期之间的间隔是否大于 7 天。

    Args:
        start_date (str): 起始日期，格式为 'YYYYMMDD'。
        end_date (str): 结束日期，格式为 'YYYYMMDD'。

    Returns:
        bool: 如果间隔大于 7 天，返回 True；否则返回 False。
    """
    # 将日期字符串转换为 datetime 对象
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    # 计算日期差值
    delta = end - start

    # 检查差值是否大于 7 天
    return delta.days > 7


# 全局标志，防止重复配置日志


# 移除了 _get_log_level 函数，因为 setLevel 可以直接接受字符串参数


def _setup_file_handler(log_type: str, log_level: str) -> None:
    """配置文件日志处理器"""
    import os
    from logging.handlers import TimedRotatingFileHandler

    # 确保logs目录存在
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 如果 log_type 为空，则不配置文件处理器
    if not log_type:
        return

    log_filename = f"{log_type}.log"

    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # 清除现有处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 创建文件处理器
    # file_handler = TimedRotatingFileHandler(
    #     filename=os.path.join(log_dir, log_filename),
    #     when="midnight",
    #     interval=1,
    #     backupCount=1,
    #     encoding="utf-8",
    # )

    # 创建文件处理器，使用 FileHandler 并设置模式为 'w' 以实现覆盖
    file_handler = logging.FileHandler(
        filename=os.path.join(log_dir, log_filename),
        mode="w",  # 设置模式为覆盖写入
        encoding="utf-8",
    )

    # 设置格式并添加处理器
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # 添加会话分隔符
    log_file_path = os.path.join(log_dir, log_filename)
    try:
        with open(log_file_path, "a", encoding="utf-8") as f:
            f.write("\n==== new start here ====\n")
    except Exception:
        raise ValueError(f"无法写入日志文件: {log_file_path}")


def _configure_third_party_loggers(log_level: str) -> None:
    """配置第三方库的日志级别"""
    # 屏蔽第三方库的噪音日志
    third_party_loggers = ["tushare", "sqlite3", "urllib3", "requests"]
    for logger_name in third_party_loggers:
        logging.getLogger(logger_name).setLevel(logging.CRITICAL)

    # 配置 Huey 日志
    huey_logger = logging.getLogger("huey")
    huey_logger.setLevel(log_level)
    huey_logger.propagate = True

    # 过滤 Huey 内部调试信息
    huey_internal_loggers = [
        "huey",
        "huey.consumer",
        "huey.consumer.Scheduler",
        "huey.consumer.Worker",
    ]
    for logger_name in huey_internal_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    # 屏蔽警告
    warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")
    warnings.filterwarnings("ignore", category=FutureWarning, module="tushare")


def setup_logging(log_type: str, log_level: str = "INFO"):
    """配置日志记录

    Args:
        log_type: 日志类型，将作为日志文件名的一部分 (e.g., 'download', 'consumer_fast')
        log_level: 日志级别，支持 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    """
    # 使用 log_type 来区分不同的日志文件
    _setup_file_handler(log_type, log_level.upper())
    _configure_third_party_loggers(log_level.upper())


def get_next_day_str(
    date_str: Optional[str], date_format: str = "%Y%m%d"
) -> Optional[str]:
    """计算给定日期的后一天

    Args:
        date_str: YYYYMMDD 格式的日期字符串
        date_format: 日期格式

    Returns:
        后一天的 YYYYMMDD 格式字符串，如果输入为 None 则返回 None
    """
    if date_str is None:
        return None

    try:
        current_date = datetime.strptime(date_str, date_format)
        next_day = current_date + timedelta(days=1)
        return next_day.strftime(date_format)
    except ValueError:
        # 如果日期格式不正确，可以返回 None 或抛出异常
        return None
