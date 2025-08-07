"""
死信处理命令行工具

提供命令行接口来查看、重新执行和管理死信任务。
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Optional

from .retry_policy import DeadLetterLogger, DeadLetterRecord
from .models import DownloadTask
from .queue_manager import MemoryQueueManager
from .producer_pool import ProducerPool
from .consumer_pool import ConsumerPool

logger = logging.getLogger(__name__)


class DeadLetterCLI:
    """死信处理命令行接口"""
    
    def __init__(self, log_path: str = "logs/dead_letter.jsonl"):
        self.dead_letter_logger = DeadLetterLogger(log_path)
    
    def list_dead_letters(self, 
                         limit: Optional[int] = None,
                         task_type: Optional[str] = None,
                         symbol_pattern: Optional[str] = None) -> None:
        """列出死信记录"""
        records = self.dead_letter_logger.read_dead_letters(
            limit=limit,
            task_type=task_type,
            symbol_pattern=symbol_pattern
        )
        
        if not records:
            print("没有找到死信记录")
            return
        
        print(f"\n找到 {len(records)} 条死信记录:\n")
        print(f"{'序号':<4} {'股票代码':<12} {'任务类型':<15} {'错误类型':<20} {'失败时间':<20} {'错误信息':<50}")
        print("-" * 120)
        
        for i, record in enumerate(records, 1):
            error_msg = record.error_message
            if len(error_msg) > 47:
                error_msg = error_msg[:47] + "..."
            
            print(f"{i:<4} {record.symbol:<12} {record.task_type:<15} "
                  f"{record.error_type:<20} {record.failed_at.strftime('%Y-%m-%d %H:%M'):<20} {error_msg:<50}")
    
    def show_statistics(self) -> None:
        """显示死信统计信息"""
        stats = self.dead_letter_logger.get_statistics()
        
        print(f"\n死信统计信息:")
        print(f"总记录数: {stats['total_count']}")
        
        if stats['total_count'] == 0:
            return
        
        print(f"\n按任务类型分布:")
        for task_type, count in stats['by_task_type'].items():
            print(f"  {task_type}: {count}")
        
        print(f"\n按错误类型分布:")
        for error_type, count in stats['by_error_type'].items():
            print(f"  {error_type}: {count}")
        
        print(f"\n最近失败记录:")
        for failure in stats['recent_failures'][:5]:
            print(f"  {failure['symbol']} ({failure['task_type']}) - "
                  f"{failure['error_type']}: {failure['error_message']}")
    
    def export_symbols(self, 
                      output_file: str,
                      task_type: Optional[str] = None,
                      unique: bool = True) -> None:
        """导出股票代码到文件"""
        records = self.dead_letter_logger.read_dead_letters(task_type=task_type)
        
        if not records:
            print("没有找到死信记录")
            return
        
        symbols = [record.symbol for record in records]
        
        if unique:
            symbols = list(set(symbols))
            symbols.sort()
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for symbol in symbols:
                f.write(f"{symbol}\n")
        
        print(f"已导出 {len(symbols)} 个股票代码到: {output_file}")
    
    async def retry_failed_tasks(self,
                               task_type: Optional[str] = None,
                               symbol_pattern: Optional[str] = None,
                               limit: Optional[int] = None,
                               dry_run: bool = False) -> None:
        """重试失败的任务"""
        records = self.dead_letter_logger.read_dead_letters(
            limit=limit,
            task_type=task_type,
            symbol_pattern=symbol_pattern
        )
        
        if not records:
            print("没有找到匹配的死信记录")
            return
        
        tasks = self.dead_letter_logger.convert_to_tasks(records)
        
        if dry_run:
            print(f"预演模式：将重试 {len(tasks)} 个任务:")
            for task in tasks:
                print(f"  {task.symbol} - {task.task_type.value}")
            return
        
        print(f"开始重试 {len(tasks)} 个失败任务...")
        
        # 创建队列管理器和处理池
        queue_manager = MemoryQueueManager()
        producer_pool = ProducerPool(
            max_producers=2,
            task_queue=queue_manager.task_queue,
            data_queue=queue_manager.data_queue
        )
        consumer_pool = ConsumerPool(
            max_consumers=1,
            data_queue=queue_manager.data_queue
        )
        
        try:
            # 启动处理组件
            await queue_manager.start()
            producer_pool.start()
            consumer_pool.start()
            
            # 提交任务
            for task in tasks:
                await queue_manager.put_task(task)
            
            # 等待处理完成
            print("任务已提交，等待处理完成...")
            
            # 简单等待，实际应用中可以更智能地监控进度
            await asyncio.sleep(10)  # 等待10秒让任务开始处理
            
            # 等待队列变空
            while queue_manager.task_queue_size > 0 or queue_manager.data_queue_size > 0:
                print(f"任务队列: {queue_manager.task_queue_size}, "
                      f"数据队列: {queue_manager.data_queue_size}")
                await asyncio.sleep(5)
            
            print("重试完成")
            
            # 归档处理过的记录
            processed_ids = [record.task_id for record in records]
            self.dead_letter_logger.archive_processed(processed_ids)
            print(f"已归档 {len(processed_ids)} 条处理过的死信记录")
            
        finally:
            # 清理资源
            consumer_pool.stop()
            producer_pool.stop()
            await queue_manager.stop()
    
    def clear_dead_letters(self, confirm: bool = False) -> None:
        """清空死信日志"""
        if not confirm:
            response = input("确定要清空所有死信记录吗？(y/N): ")
            if response.lower() != 'y':
                print("操作已取消")
                return
        
        log_path = self.dead_letter_logger.log_path
        if log_path.exists():
            log_path.unlink()
            print("死信日志已清空")
        else:
            print("死信日志文件不存在")


def main():
    """命令行入口函数"""
    parser = argparse.ArgumentParser(description="死信处理工具")
    parser.add_argument("--log-path", default="logs/dead_letter.jsonl",
                       help="死信日志文件路径")
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # list 命令
    list_parser = subparsers.add_parser('list', help='列出死信记录')
    list_parser.add_argument('--limit', type=int, help='最大显示数量')
    list_parser.add_argument('--task-type', help='过滤任务类型')
    list_parser.add_argument('--symbol', help='过滤股票代码模式')
    
    # stats 命令
    subparsers.add_parser('stats', help='显示统计信息')
    
    # export 命令
    export_parser = subparsers.add_parser('export', help='导出股票代码')
    export_parser.add_argument('output_file', help='输出文件路径')
    export_parser.add_argument('--task-type', help='过滤任务类型')
    export_parser.add_argument('--no-unique', action='store_true', 
                              help='不去重股票代码')
    
    # retry 命令
    retry_parser = subparsers.add_parser('retry', help='重试失败任务')
    retry_parser.add_argument('--task-type', help='过滤任务类型')
    retry_parser.add_argument('--symbol', help='过滤股票代码模式')
    retry_parser.add_argument('--limit', type=int, help='最大重试数量')
    retry_parser.add_argument('--dry-run', action='store_true', 
                             help='预演模式，不实际执行')
    
    # clear 命令
    clear_parser = subparsers.add_parser('clear', help='清空死信日志')
    clear_parser.add_argument('--yes', action='store_true', 
                             help='确认清空，跳过提示')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    cli = DeadLetterCLI(args.log_path)
    
    try:
        if args.command == 'list':
            cli.list_dead_letters(
                limit=args.limit,
                task_type=args.task_type,
                symbol_pattern=args.symbol
            )
        
        elif args.command == 'stats':
            cli.show_statistics()
        
        elif args.command == 'export':
            cli.export_symbols(
                output_file=args.output_file,
                task_type=args.task_type,
                unique=not args.no_unique
            )
        
        elif args.command == 'retry':
            asyncio.run(cli.retry_failed_tasks(
                task_type=args.task_type,
                symbol_pattern=args.symbol,
                limit=args.limit,
                dry_run=args.dry_run
            ))
        
        elif args.command == 'clear':
            cli.clear_dead_letters(confirm=args.yes)
    
    except KeyboardInterrupt:
        print("\n操作已中断")
        sys.exit(1)
    except Exception as e:
        print(f"执行出错: {e}")
        logger.exception("Unexpected error")
        sys.exit(1)


if __name__ == "__main__":
    main()
