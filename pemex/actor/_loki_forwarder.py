import asyncio
import json
from pathlib import Path
from datetime import datetime

from redis.asyncio import Redis
from uuid_extensions import uuid7
import httpx
from pemex.stream.base import Stream

from pemex.stream_connector import get_redis_connection
from pemex.actor.base import Actor


class LokiForwarder(Actor):
    _consumergroupname = "loki_forwarder"

    def __init__(
        self, stream:Stream, loki_uri: str, buffer_size: int = 500, 
    ):
        self.stream = stream
        self._stop_event = asyncio.Event()
        self.buffer_size: int = buffer_size
        self.consumername: str = uuid7().hex  # type: ignore
        self.loki_uri = loki_uri
        self.tls_context = httpx.create_ssl_context()
        

    def __init_async_client(self):
        args: dict = {"base_url": self.loki_uri}
        if getattr(self, "tls_context", None) is not None:
            args["verify"] = self.tls_context
        client = httpx.AsyncClient(**args)
        self.client = client

    def stream_name(self) -> str:
        return 'browser_visit'

    def define_tls_context(self, pem_path: Path):
        self.tls_context = httpx.create_ssl_context(verify=str(pem_path.absolute()))
        self.__init_async_client()

    def stop(self):
        self._stop_event.set()

    async def _run(self):
        """reads from redis stream, buffers messages, and then pushes to loki"""

        buffer = []
        while not self._stop_event.is_set():
            try:
                messages = await self.stream.read(self.stream_name(), self._consumergroupname, self.consumername)
                buffer.extend(messages)

                await asyncio.sleep(0)

                if len(buffer) > self.buffer_size:
                    msgs, buffer = (
                        buffer[: self.buffer_size],
                        buffer[self.buffer_size :],
                    )

                    await self.client.post(
                        "/loki/api/v1/push",
                        json={
                            "streams": [
                                {
                                    "stream": {"job": "browser"},
                                    "values": [
                                        [
                                            datetime.fromisoformat(
                                                msg.get("visited_on")
                                            ).timestamp(),
                                            json.dumps(msg),
                                        ]
                                        for msg in msgs
                                    ],
                                }
                            ]
                        },
                    )
                    await self.stream.ack(
                        self.stream_name(),
                        self._consumergroupname,
                        *(id for id, _ in msgs),
                    )

            except asyncio.CancelledError:
                break
        else:
            await self.client.aclose()
