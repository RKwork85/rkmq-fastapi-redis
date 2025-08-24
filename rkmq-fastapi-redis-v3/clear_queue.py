from redis import Redis
from rq import Queue

redis_conn = Redis(host='localhost', port=6379, db=0,password='rkwork')
queue = Queue('default', connection=redis_conn)

# 清空队列中所有待执行任务
queue.empty()
print("✅ default 队列已清空（等待执行任务）")