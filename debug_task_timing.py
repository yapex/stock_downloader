#!/usr/bin/env python3

import asyncio
import logging
import time

# 配置详细日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from src.neo.helpers.app_service import ServiceFactory
from src.neo.task_bus.types import DownloadTaskConfig, TaskType

async def check_database():
    """检查数据库中是否有数据"""
    from src.neo.database.operator import DBOperator
    
    db_operator = DBOperator.create_default()
    try:
        # 检查股票基本信息表
        sql = "SELECT COUNT(*) as count FROM stock_basic"
        if callable(db_operator.conn):
            with db_operator.conn() as conn:
                result = conn.execute(sql).fetchone()
        else:
            result = db_operator.conn.execute(sql).fetchone()
            
        count = result[0] if result else 0
        print(f"📊 stock_basic表中的记录数: {count}")
        
        if count > 0:
            # 显示最新数据
            sql = "SELECT * FROM stock_basic LIMIT 1"
            if callable(db_operator.conn):
                with db_operator.conn() as conn:
                    result = conn.execute(sql).fetchone()
            else:
                result = db_operator.conn.execute(sql).fetchone()
                
            if result:
                # 转换为字典显示
                columns = ['ts_code', 'symbol', 'name', 'area', 'industry', 'market', 'list_date']  # 根据实际表结构调整
                row_dict = dict(zip(columns, result)) if len(result) >= len(columns) else dict(enumerate(result))
                print(f"📋 最新记录: {row_dict}")
            else:
                print("📋 没有记录")
        else:
            print("❌ 数据库中没有数据！")
    except Exception as e:
        print(f"❌ 查询数据库失败: {e}")
    finally:
        pass  # DBOperator 不需要异步关闭

def debug_task_timing():
    """调试任务执行时序的脚本"""
    print("🔍 调试任务执行时序...")
    
    # 创建测试任务
    tasks = [
        DownloadTaskConfig(
            task_type=TaskType.stock_basic,
            symbol="000001.SZ",
        )
    ]
    
    print(f"任务配置: task_type={tasks[0].task_type} (type: {type(tasks[0].task_type)})")
    
    # 创建应用服务
    app_service = ServiceFactory.create_app_service(with_progress=False)
    
    print("🚀 开始执行任务并监控时序...")
    start_time = time.time()
    
    try:
        # 运行任务
        app_service.run_downloader(tasks)
        
        end_time = time.time()
        print(f"⏰ 总执行时间: {end_time - start_time:.2f}秒")
        
        # 检查数据库中是否有数据
        asyncio.run(check_database())
            
    except Exception as e:
        print(f"❌ 执行过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_task_timing()
