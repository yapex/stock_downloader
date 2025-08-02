import logging
from dotenv import load_dotenv
from downloader.fetcher import TushareFetcher

def run_diagnostic():
    """
    一个独立的诊断脚本，用于精确测试 TushareFetcher 的行为。
    """
    # 1. 初始化
    print("--- 开始诊断 ---")
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    try:
        fetcher = TushareFetcher()
    except Exception as e:
        print(f"❌ Fetcher 初始化失败: {e}")
        return

    # 2. 定义测试参数
    ts_code = '600519.SH' # 我们用一个明确有数据的股票来测试
    start_date = '20230101'
    end_date = '20230110'

    print(f"\n--- 准备测试股票: {ts_code}, 日期范围: {start_date} -> {end_date} ---")

    # 3. 测试前复权 (qfq)
    print("\n[1. 测试前复权 (adjust='qfq')]")
    try:
        df_qfq = fetcher.fetch_daily_history(ts_code, start_date, end_date, adjust='qfq')
        
        if df_qfq is not None and not df_qfq.empty:
            print(f"  ✅ 成功获取到 {len(df_qfq)} 条前复权数据。")
            print("  --- 数据预览 (前3条) ---")
            print(df_qfq.head(3).to_string())
        elif df_qfq is not None:
            print("  ⚠️  获取成功，但返回了空的DataFrame。")
        else:
            print("  ❌ 获取失败，返回了 None。")
            
    except Exception as e:
        print(f"  ❌ 调用时发生异常: {e}")

    # 4. 测试不复权 (none)
    print("\n[2. 测试不复权 (adjust='none')]")
    try:
        df_none = fetcher.fetch_daily_history(ts_code, start_date, end_date, adjust='none')

        if df_none is not None and not df_none.empty:
            print(f"  ✅ 成功获取到 {len(df_none)} 条不复权数据。")
            print("  --- 数据预览 (前3条) ---")
            print(df_none.head(3).to_string())
        elif df_none is not None:
            print("  ⚠️  获取成功，但返回了空的DataFrame。")
        else:
            print("  ❌ 获取失败，返回了 None。")

    except Exception as e:
        print(f"  ❌ 调用时发生异常: {e}")
        
    print("\n--- 诊断结束 ---")


if __name__ == '__main__':
    run_diagnostic()