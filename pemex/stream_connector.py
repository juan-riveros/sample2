"""stream connector utilizing redis"""

import redis.asyncio as redis
import os

__pool = redis.ConnectionPool(
    host=os.getenv("REDIS_URI", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
)


def get_redis_connection():
    return redis.Redis(connection_pool=__pool, decode_responses=True, protocol=5)


__all__ = [
    "get_redis_connection",
]
