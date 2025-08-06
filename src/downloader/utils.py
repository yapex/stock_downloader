import re
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
        # 记录无法识别的股票代码
        record_failed_task(
            "股票代码识别", 
            f"{stock_number}.Unknown", 
            f"无法识别的股票代码前缀: {stock_number}",
            "parameter"
        )
        raise ValueError(f"无法识别的股票代码前缀: {stock_number}")

    return f"{stock_number}.{exchange}"


def record_failed_task(task_name: str, entity_id: str, reason: str, error_category: str = "business"):
    """
    将下载失败的任务记录到日志文件。
    
    Args:
        task_name: 任务名称
        entity_id: 实体ID（如股票代码等）
        reason: 失败原因
        error_category: 错误分类，可选值：
            - "business": 正常业务错误
            - "test": 测试相关错误
            - "network": 网络连接错误
            - "parameter": 参数错误
            - "system": 系统错误
    """
    # 确定日志文件名
    log_file = "failed_tasks.log" if error_category == "business" else f"failed_tasks_{error_category}.log"
    
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()},{task_name},{entity_id},{reason},{error_category}\n")


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
