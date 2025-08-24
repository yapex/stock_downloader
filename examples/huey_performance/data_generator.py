"""数据生成器"""
import asyncio
import random
import time
from typing import Dict, List, Any
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class StockData:
    """股票数据"""
    symbol: str
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int


class DataGenerator:
    """数据生成器"""
    
    def __init__(self, network_delay_min: float = 0.01, network_delay_max: float = 0.05):
        self.network_delay_min = network_delay_min
        self.network_delay_max = network_delay_max
        self._stock_symbols = [f"STOCK_{i:04d}" for i in range(1000)]
    
    def generate_stock_data(self, symbol: str, count: int) -> List[Dict[str, Any]]:
        """生成股票数据"""
        data = []
        base_price = random.uniform(10.0, 100.0)
        base_time = datetime.now() - timedelta(days=count)
        
        for i in range(count):
            price_change = random.uniform(-0.05, 0.05)
            base_price *= (1 + price_change)
            
            open_price = base_price
            high_price = base_price * random.uniform(1.0, 1.02)
            low_price = base_price * random.uniform(0.98, 1.0)
            close_price = base_price * random.uniform(0.99, 1.01)
            volume = random.randint(1000, 100000)
            
            data.append({
                'symbol': symbol,
                'timestamp': (base_time + timedelta(days=i)).isoformat(),
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': volume
            })
        
        return data
    
    def simulate_network_delay_sync(self) -> None:
        """模拟网络延迟（同步）"""
        delay = random.uniform(self.network_delay_min, self.network_delay_max)
        time.sleep(delay)
    
    async def simulate_network_delay_async(self) -> None:
        """模拟网络延迟（异步）"""
        delay = random.uniform(self.network_delay_min, self.network_delay_max)
        await asyncio.sleep(delay)
    
    def fetch_stock_data_sync(self, symbol: str, count: int) -> List[Dict[str, Any]]:
        """同步获取股票数据"""
        self.simulate_network_delay_sync()
        return self.generate_stock_data(symbol, count)
    
    async def fetch_stock_data_async(self, symbol: str, count: int) -> List[Dict[str, Any]]:
        """异步获取股票数据"""
        await self.simulate_network_delay_async()
        return self.generate_stock_data(symbol, count)
    
    def get_stock_symbols(self, count: int) -> List[str]:
        """获取股票代码列表"""
        return random.sample(self._stock_symbols, min(count, len(self._stock_symbols)))