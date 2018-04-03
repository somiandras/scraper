import os
import redis
from rq import Worker, Queue, Connection

# From https://devcenter.heroku.com/articles/python-rq

redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379')
conn = redis.from_url(redis_url)

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(Queue())
        worker.work()
