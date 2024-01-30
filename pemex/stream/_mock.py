from typing import NamedTuple
from datetime import datetime
from pemex.stream.base import Message, StreamID
from .base import Stream

from uuid_extensions import uuid7

def infinite_seq():
    i = 0
    while True:
        yield i
        i+=1



class _Stream:
    def __init__(self):
        self.consumer_groups: dict[str, ConsumerGroup] = dict()
        self._stream = []
        self._messages = {}


class ConsumerGroup(NamedTuple):
    name:str
    pos: int

class MockStream(Stream):
    def __init__(self):
        self.streams: dict[str,_Stream] = dict()
        # self.consumer_groups = dict()
        self._seq = infinite_seq()

    async def ack(self, stream_name: str, consumer_group_name: str, *ids: int):
        stream = self.streams.setdefault(stream_name, _Stream())
        con_group = stream.consumer_groups.setdefault(consumer_group_name, ConsumerGroup(consumer_group_name, 0))
        

        for _id in ids:
            if _id > con_group.pos:
                con_group = ConsumerGroup(con_group.name, pos=_id)
            stream._messages.pop(_id)
            stream._stream.remove(_id)
        stream.consumer_groups[consumer_group_name] = con_group
        self.streams[stream_name] = stream

    
    async def read(self, stream_name: str, consumer_group_name: str, consumer_name: str) -> list[tuple[str, list[tuple[StreamID, Message]]]]:
        stream = self.streams.setdefault(stream_name, _Stream())
        con_group = stream.consumer_groups.setdefault(consumer_group_name, ConsumerGroup(consumer_group_name, 0))
        
        items = [(_id, stream._messages.get(_id)) for _id in stream._stream[con_group.pos:con_group.pos+10] if _id in stream._messages]
        return [(stream_name, items)] #type:ignore
    
    async def close(self):
        return 
    
    async def send(self, stream_name: str, message: dict):
        stream = self.streams.setdefault(stream_name, _Stream())
        _id = next(self._seq)
        stream._stream.append(_id)
        stream._messages[_id] = message
        return
    
    async def create_consumer_group(self, stream_name: str, consumer_group_name: str):
        stream = self.streams.setdefault(stream_name, _Stream())
        con_group = stream.consumer_groups.setdefault(consumer_group_name, ConsumerGroup(consumer_group_name, 0))