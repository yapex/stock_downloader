from datetime import datetime


def record_failed_task(task_name: str, entity_id: str, reason: str):
    """
    将下载失败的任务记录到日志文件。
    这是一个通用的工具函数。
    """
    with open("failed_tasks.log", "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()},{task_name},{entity_id},{reason}\n")
