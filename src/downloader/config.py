# config.py
import tomllib
from pathlib import Path
from box import Box

# 直接读取配置文件并用 Box 包装，支持点号访问
_config_path = Path.cwd() / "config.toml"
with _config_path.open("rb") as f:
    _config_data = tomllib.load(f)

# 全局配置对象
_config = Box(_config_data)


def get_config():
    return _config


if __name__ == "__main__":
    print(get_config().tushare.token)
    print(get_config().database.path)
    print(get_config().database.type)
