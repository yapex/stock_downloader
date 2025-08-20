import re
import logging
from datetime import datetime


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
_logging_configured = False


def setup_logging(logger: logging.Logger = None):
    """配置日志记录"""
    global _logging_configured

    # 如果已经配置过，直接返回
    if _logging_configured:
        return

    import logging
    import os
    from logging.handlers import TimedRotatingFileHandler

    # 确保logs目录存在
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # 清除根日志记录器现有的处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 创建文件处理器，每天轮换
    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(log_dir, "download.log"),
        when="midnight",
        interval=1,
        backupCount=1,  # 保留1天的日志
        encoding="utf-8",
    )

    # 设置日志格式
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)

    # 添加处理器到根日志记录器
    root_logger.addHandler(file_handler)

    # 在日志文件中添加会话分隔符，方便定位新的日志内容
    session_separator = f"\n{'=' * 80}\n"

    # 直接写入分隔符到文件，避免通过日志系统重复处理
    log_file_path = os.path.join("logs", "download.log")
    try:
        with open(log_file_path, "a", encoding="utf-8") as f:
            f.write(f"\n{session_separator}\n")
    except Exception:
        pass  # 如果写入失败，不影响程序运行

    # 屏蔽第三方模块的日志输出到控制台，只保留文件日志
    # 这样终端只会显示tqdm进度条信息
    logging.getLogger("tushare").setLevel(logging.CRITICAL)

    # 标记日志已配置
    _logging_configured = True
