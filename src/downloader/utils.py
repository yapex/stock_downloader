import re
from datetime import datetime

# 用于跟踪日志文件是否已经在当前运行中被初始化（覆盖）
_log_files_initialized = set()


def get_table_name(data_type: str, entity_id: str) -> str:
    """
    根据数据类型和实体ID生成规范的表名。

    Args:
        data_type: 数据类型，如 'daily', 'daily_basic' 等
        entity_id: 实体ID，如股票代码或系统标识

    Returns:
        str: 规范化的表名

    Examples:
        >>> get_table_name('daily', '600519.SH')
        'daily_600519_SH'
        >>> get_table_name('system', 'stock_list_system')
        'sys_stock_list'
        >>> get_table_name('stock_list_basic', 'list_system')
        'sys_stock_list'
    当前业务表：
        stock_list, daily_qfq, daily_none, daily_basic, financial_income,
        financial_balance, financial_cashflow
    """
    if data_type.startswith("stock_list_") or entity_id == "list_system":
        # stock_list 相关的特殊处理
        return "sys_stock_list"
    elif data_type == "system" or entity_id == "system" or "_system" in entity_id:
        # 系统表使用简单命名
        return f"sys_{entity_id.replace('_system', '').replace('system', 'stock_list')}"
    else:
        # 股票数据表包含标准化的股票代码
        safe_entity_id = "".join(
            c if c.isalnum() else "_" for c in normalize_stock_code(entity_id)
        )
        return f"{data_type}_{safe_entity_id}"


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
            "parameter",
        )
        raise ValueError(f"无法识别的股票代码前缀: {stock_number}")

    return f"{stock_number}.{exchange}"


def record_failed_task(
    task_name: str, entity_id: str, reason: str, error_category: str = "business"
):
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
    # 确保logs目录存在
    import os

    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 确定日志文件名
    log_filename = (
        "failed_tasks.log"
        if error_category == "business"
        else f"failed_tasks_{error_category}.log"
    )
    log_file = os.path.join(log_dir, log_filename)

    # 确定写入模式：第一次写入覆盖，后续追加
    global _log_files_initialized
    if log_file not in _log_files_initialized:
        mode = "w"  # 覆盖模式
        _log_files_initialized.add(log_file)
    else:
        mode = "a"  # 追加模式

    with open(log_file, mode, encoding="utf-8") as f:
        f.write(
            f"{datetime.now().isoformat()},{task_name},{entity_id},{reason},{error_category}\n"
        )


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
