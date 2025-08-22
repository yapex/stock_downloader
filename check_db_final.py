#!/usr/bin/env python3

from src.neo.database.operator import DBOperator

def main():
    db = DBOperator()
    
    with db.conn() as conn:
        # 检查股票数量
        result = conn.execute('SELECT COUNT(*) as count FROM stock_basic').fetchone()
        print(f'数据库中股票数量: {result[0]}')
        
        # 显示前5只股票
        sample = conn.execute('SELECT ts_code, symbol, name FROM stock_basic LIMIT 5').fetchall()
        print('前5只股票:')
        for row in sample:
            print(f'{row[0]} - {row[1]} - {row[2]}')

if __name__ == '__main__':
    main()