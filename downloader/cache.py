import atexit
from diskcache import Cache
from pathlib import Path

CACHE_DIR = Path("./cache")
cache = Cache(str(CACHE_DIR), size_limit=int(1e9))
atexit.register(cache.close)
