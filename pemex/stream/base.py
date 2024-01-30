from abc import ABC, abstractmethod
from typing import TypeVar

StreamID = TypeVar
Message = TypeVar

class Stream(ABC):
    @abstractmethod
    async def ack(self, stream_name:str, consumer_group_name:str, *ids:int):
        raise NotImplementedError
    @abstractmethod
    async def read(self, stream_name:str, consumer_group_name:str, consumer_name:str) -> 'list[tuple[str,list[tuple[StreamID, Message]]]]':
        raise NotImplementedError
    @abstractmethod
    async def close(self):
        raise NotImplementedError
    @abstractmethod
    async def send(self, stream_name:str, message:dict):
        raise NotImplementedError
    @abstractmethod
    async def create_consumer_group(self, stream_name: str, consumer_group_name:str):
        raise NotImplementedError