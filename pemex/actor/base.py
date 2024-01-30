from abc import ABC, abstractmethod


class Actor(ABC):
    _consumergroupname: str
    @abstractmethod
    def stream_name(self) -> str:
        raise NotImplementedError
    
    @abstractmethod
    async def _run(self):
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        raise NotImplementedError
