"""一次性手动触发元数据同步的脚本"""

import logging

# 必须先配置日志，才能看到 huey 任务中的日志输出
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

from neo.tasks.huey_tasks import sync_metadata

if __name__ == "__main__":
    print("即将手动执行一次元数据同步...")
    try:
        # 直接调用任务函数
        sync_metadata()
        print("元数据同步执行完毕。")
    except Exception as e:
        logging.error(f"手动执行元数据同步时发生错误: {e}")
