
# -*- coding: utf-8 -*-
"""
配置应用程序的日志系统。
"""
import logging
import sys
from tqdm import tqdm

class TqdmLoggingHandler(logging.StreamHandler):
    """与tqdm兼容的日志处理器"""

    def __init__(self):
        super().__init__()

    def emit(self, record):
        try:
            msg = self.format(record)
            # 使用 tqdm.write 来输出日志，这样不会打断进度条
            tqdm.write(msg, file=sys.stdout, end="\n")
        except Exception:
            self.handleError(record)

def setup_logging():
    """配置日志系统，错误信息只记录到文件，终端保持简洁"""
    root_logger = logging.getLogger()
    
    # 在清理之前，检查是否在测试环境中。如果是，保留pytest的日志处理器
    pytest_handlers = [h for h in root_logger.handlers if 'pytest' in str(type(h)).lower() or 'caplog' in str(type(h)).lower()]
    
    # 清理现有的 handlers（但保留pytest的处理器）
    for handler in root_logger.handlers[:]:
        if handler not in pytest_handlers:
            root_logger.removeHandler(handler)

    # 创建日志格式
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
    )
    console_formatter = logging.Formatter("%(message)s")  # 终端只显示消息内容

    # 文件处理器 - 记录所有级别的日志
    import os
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "downloader.log")
    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)

    # 控制台处理器 - 只显示INFO级别的关键信息
    console_handler = TqdmLoggingHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)
    
    # 添加过滤器，控制台只显示关键警告和错误，进度由 tqdm 管理
    class TerminalFilter(logging.Filter):
        def filter(self, record):
            # 只允许关键警告和错误在终端显示（WARNING 级别及以上）
            # 同时排除一些特定的调试/信息消息，让进度条独占终端
            if record.levelno >= logging.WARNING:
                return True
            
            # 允许一些关键的启动信息
            critical_messages = [
                "正在启动...",
                "程序启动失败",
                "程序主流程发生严重错误",
                "用户中断下载"
            ]
            return any(msg in record.getMessage() for msg in critical_messages)
    
    console_handler.addFilter(TerminalFilter())

    # 配置根日志记录器
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

