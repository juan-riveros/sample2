from abc import ABC, abstractmethod

class S3Connection(ABC):
    @abstractmethod
    async def get(self, bucket, key):
        raise NotImplementedError
    @abstractmethod
    async def put(self, bucket, key, contents):
        raise NotImplementedError
