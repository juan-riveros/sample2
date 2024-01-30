import redis.asyncio as aredis
import redis.exceptions
import os
from pemex.logging import logging

from pemex.stream.base import Stream

class RedisStream(Stream):
    """RedisStream which handles reading and writing to a redis stream server"""
    def __init__(self, host:str = '127.0.0.1', port:str='6379', db:int=0, block:int=100, count:int=10, maxlen:int=int(10e6)):
        """
        Attributes:

        host(str): host of the redis server; defaults to 127.0.0.1
        port(str): port of the redis server; defaults to 6379
        db(int): db on the redis server; only exposed for weird setups, generally recommeneded to use different server isntances instead
        blocK(int): in milliseconds; how long to block while waiting on the next item; defaults to 100ms
        count(int): how many items to pull on a single call; defaults to 10
        """
        self.host = host
        self.port = port
        self.db = db
        self.__pool = aredis.ConnectionPool(
            host = host,
            port = port,
            db = db,
            decode_responses=True
        )
        self.block = block
        self.count = count
        self.maxlen = int(maxlen)

    async def close(self):
        await self.__pool.aclose()

    async def _xreadgroup(self, stream_name:str, consumer_group_name:str, consumer_name:str, block:int=100, count:int=10):
        conn = aredis.Redis(connection_pool=self.__pool, decode_responses=True,)
        resp = await conn.xreadgroup(
            groupname=consumer_group_name,
            consumername=consumer_name,
            streams={stream_name:">"},
            block=block,
            count=count,
        )
        if resp:
            logging.info(f'pulled {resp}')
        return resp
    
    async def ack(self, stream_name:str, consumer_group_name:str, *ids:int):
        conn = aredis.Redis(connection_pool=self.__pool, decode_responses=True)
        
        return await conn.xack(stream_name, groupname=consumer_group_name, *ids)

    async def read(self, stream_name:str, consumer_group_name:str, consumer_name:str):
        return await self._xreadgroup(stream_name, consumer_group_name, consumer_name, self.block, self.count)
        
    async def send(self, stream_name:str, message:dict):
        conn = aredis.Redis(connection_pool=self.__pool, decode_responses=True)
        return await conn.xadd(stream_name, message, maxlen=self.maxlen, approximate=True)
    
    async def create_consumer_group(self, stream_name: str, consumer_group_name:str):
        conn = aredis.Redis(connection_pool=self.__pool, decode_responses=True)
        try:
            resp =await conn.xgroup_create(stream_name, consumer_group_name, mkstream=True,)
        except redis.exceptions.ResponseError as err:
            if err.args[0] == "BUSYGROUP Consumer Group name already exists":
                return
            else:
                raise err

