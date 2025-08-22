from gevent import monkey

monkey.patch_all()
import gevent
from huey.contrib.mini import MiniHuey

# 使用 MiniHuey 实现单进程内异步任务执行
huey = MiniHuey("chain_tasks")
