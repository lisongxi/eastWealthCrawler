import gevent
from gevent import monkey
monkey.patch_all()

from datetime import datetime
import time


def foo(message, n):
    """
    Each thread will be passed the message, and n arguments
    in its initialization.
    """
    time.sleep(n)
    print(message)


threads = []
for i in range(100000):
    threads.append(gevent.spawn(foo, "Hello", 1))

start = datetime.now()  # 开始时间

gevent.joinall(threads)

end = datetime.now()  # 结束时间

print(f"总耗时{(end - start).seconds}秒")

# import time
#
# t1 = time.time()
# import gevent
# from gevent import monkey
#
# gevent.monkey.patch_all()
#
#
# def func(i):
#     time.sleep(1)
#     print(f'task {i} complete')
#
#
# tasks = [gevent.spawn(func, i) for i in range(10000)]
# gevent.wait(tasks)
# t2 = time.time()
# print(f'Time-consuming: {t2 - t1}')
