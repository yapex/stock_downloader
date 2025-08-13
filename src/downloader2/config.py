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


def get():
    return _config


if __name__ == "__main__":
    print(get().tushare.token)
    print(get().database.path)
    print(get().database.type)
