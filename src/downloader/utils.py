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
        exchange = "SH"
    elif stock_number.startswith(("0", "3")):
        exchange = "SZ"
    elif stock_number.startswith(("8", "9")):
        exchange = "BJ"
    else:
        raise ValueError(f"无法识别的股票代码前缀: {stock_number}")

    return f"{stock_number}.{exchange}"


def record_failed_task(task_name: str, entity_id: str, reason: str):
    """
    将下载失败的任务记录到日志文件。
    这是一个通用的工具函数。
    """
    with open("failed_tasks.log", "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()},{task_name},{entity_id},{reason}\n")
