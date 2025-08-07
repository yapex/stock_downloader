"""
用于集成测试的配置文件生成器
"""

def get_test_config():
    """生成用于测试的配置"""
    return {
        "database": {
            "path": "test_stock.db"
        },
        "downloader": {
            "symbols": ["000001.SZ", "600001.SH"],
            "max_producers": 1,
            "max_consumers": 1,
            "producer_queue_size": 10,
            "data_queue_size": 5,
            "retry_policy": {
                "max_attempts": 2,
                "base_delay": 0.1,
                "max_delay": 1.0,
                "backoff_factor": 2.0
            }
        },
        "consumer": {
            "batch_size": 10,
            "flush_interval": 1.0,
            "max_retries": 2
        },
        "fetcher": {
            "rate_limit": 200
        },
        "tasks": [
            {
                "name": "stock_list",
                "type": "stock_list",
                "enabled": True
            },
            {
                "name": "daily_data",
                "type": "daily",
                "enabled": True
            }
        ]
    }

def get_full_test_config():
    """生成包含所有任务类型的测试配置"""
    config = get_test_config()
    config["tasks"] = [
        {
            "name": "stock_list",
            "type": "stock_list",
            "enabled": True
        },
        {
            "name": "daily_data",
            "type": "daily",
            "enabled": True
        },
        {
            "name": "daily_basic",
            "type": "daily_basic", 
            "enabled": True
        }
    ]
    return config

def get_incremental_test_config():
    """生成增量下载测试配置"""
    config = get_test_config()
    config["downloader"]["force_run"] = False
    return config
