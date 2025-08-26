"""日期工具函数"""

from datetime import datetime, timedelta
from typing import Optional


def get_next_day_str(date_str: Optional[str], date_format: str = "%Y%m%d") -> Optional[str]:
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
