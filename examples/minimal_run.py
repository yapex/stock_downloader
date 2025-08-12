#!/usr/bin/env python3
"""
最小化股票数据下载演示

展示如何使用 stock_downloader 完成基本的数据下载任务：
1. 下载股票列表
2. 下载指定股票的日K线数据
3. 查询和验证下载的数据

运行前请确保：
- 已设置 TUSHARE_TOKEN 环境变量
- 或在项目根目录创建 .env 文件并设置 TUSHARE_TOKEN
"""

import os
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from downloader.fetcher_factory import get_fetcher
from downloader.storage import PartitionedStorage
from downloader.tasks.daily import DailyTaskHandler
from downloader.tasks.stock_list import StockListTaskHandler


def check_environment():
    """检查运行环境"""
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        print("❌ 错误: 未找到 TUSHARE_TOKEN 环境变量")
        print("\n请按以下步骤设置:")
        print("1. 在项目根目录创建 .env 文件")
        print("2. 添加内容: TUSHARE_TOKEN=your_actual_token")
        print("3. 或者运行: export TUSHARE_TOKEN=your_actual_token")
        return False
    
    print(f"✅ Tushare Token: {token[:10]}...")
    return True


def main():
    """主演示函数"""
    print("🚀 股票数据下载最小化演示")
    print("=" * 50)
    
    # 检查环境
    if not check_environment():
        return
    
    # 设置数据存储路径
    data_dir = project_root / "data"
    data_dir.mkdir(exist_ok=True)
    db_path = data_dir / "minimal_example.db"
    
    print(f"📁 数据将保存到: {db_path}")
    
    try:
        # 初始化核心组件
        fetcher = get_fetcher(use_singleton=True)
        storage = PartitionedStorage(db_path)
        
        print("\n📊 步骤1: 下载股票列表")
        stock_handler = StockListTaskHandler(fetcher, storage)
        
        # 执行股票列表下载任务
        result = stock_handler.execute({})
        
        if result.success:
            print(f"✅ 成功下载 {result.records_count} 只股票信息")
            
            # 查询并显示部分股票信息
            stock_list = storage.query('stock_list', 'system')
            if not stock_list.empty:
                print("\n🔍 股票列表预览 (前5只):")
                for _, row in stock_list.head(5).iterrows():
                    print(f"  {row['ts_code']}: {row['name']} ({row['industry']})")
        else:
            print(f"❌ 股票列表下载失败: {result.error_message}")
            return
        
        print("\n📈 步骤2: 下载示例股票的日K线数据")
        daily_handler = DailyTaskHandler(fetcher, storage)
        
        # 选择几只代表性股票
        sample_stocks = ["000001.SZ", "600519.SH", "000858.SZ"]
        
        for stock_code in sample_stocks:
            print(f"\n📥 下载 {stock_code} 的日K线数据...")
            
            task_params = {
                'ts_code': stock_code,
                'start_date': '20241101',  # 最近一个月的数据
                'end_date': '20241130',
                'adjust': 'qfq'  # 前复权
            }
            
            result = daily_handler.execute(task_params)
            
            if result.success:
                print(f"✅ 成功下载 {result.records_count} 条日K线记录")
                
                # 查询并显示最近的数据
                daily_data = storage.query('daily', stock_code)
                if not daily_data.empty:
                    latest_data = daily_data.tail(3)
                    print("  📊 最近3个交易日数据:")
                    for _, row in latest_data.iterrows():
                        print(f"    {row['trade_date']}: "
                              f"开盘 {row['open']:.2f}, "
                              f"收盘 {row['close']:.2f}, "
                              f"成交量 {row['vol']:.0f}万手")
            else:
                print(f"❌ 下载失败: {result.error_message}")
        
        print("\n📋 步骤3: 数据库概览")
        
        # 显示数据库表信息
        tables = storage.list_tables()
        print(f"\n📚 数据库中的表 ({len(tables)}个):")
        
        for table in sorted(tables):
            # 获取表的记录数
            try:
                count = storage.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  📄 {table}: {count} 条记录")
            except Exception as e:
                print(f"  📄 {table}: 查询失败 ({e})")
        
        print("\n✅ 演示完成!")
        print(f"💾 数据已保存到: {db_path.resolve()}")
        print("📈 现在您可以使用数据分析工具连接该数据库进行分析")
        
        # 提供后续使用建议
        print("\n💡 后续使用建议:")
        print("1. 使用完整的命令行工具: dl --group default")
        print("2. 连接数据库进行分析: sqlite3 或 pandas.read_sql()")
        print("3. 查看 config.yaml 了解更多配置选项")
        
    except KeyboardInterrupt:
        print("\n\n⏹️  用户中断，程序退出")
    except Exception as e:
        print(f"\n❌ 运行出错: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        print("\n🔚 演示结束")


if __name__ == "__main__":
    main()
