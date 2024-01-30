import asyncio
from datetime import datetime
from hashlib import sha256
from typing import TYPE_CHECKING

from uuid_extensions import uuid7
import httpx

from pemex.actor import Actor
from pemex.stream import Stream
from pemex.logging import logging
from pemex.s3 import S3Connection

if TYPE_CHECKING:
    from redis.asyncio import Redis


class ArticleArchiver(Actor):
    _consumergroupname = "article_archiver"
    supported_domain = ("xkcd.com",)

    def __init__(self, stream:Stream, s3_conn:S3Connection, **kwargs):
        self._stop_event = asyncio.Event()
        self.stream: Stream = stream
        self.s3 = s3_conn
        self.consumername: str = uuid7().hex  # type: ignore

        self._background_tasks: list[asyncio.Task] = []

    def stream_name(self) -> str:
        return 'browser_visit'

    async def _process_page_content_article(
        self,
        url: str,
        domain: str,
        visited_on: str,
        page_content_uri: str,
        work_dict:dict|None=None
    ):
        visited_dt = datetime.fromisoformat(visited_on)
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(page_content_uri)
            resp.raise_for_status()

            # TODO: lxml.fromstring -> //[property*="og:"] -> get title, url, image
            #        -> write out to s3 & emit event for a work
            
            hash = sha256(resp.content, usedforsecurity=False).hexdigest()

            key = f'{domain}/{visited_dt.strftime("%Y%j-%H")}-{hash}.html'
            
            await self.s3.put('article', key=key, contents=resp.content)
            
            msg = {"url": url, "bucket": "article", "key":key}
            if work_dict is not None:
                msg['work'] = work_dict #type:ignore

            await self.stream.send('article_uploaded', msg)


    async def _process_article(self, url:str, domain:str, visited_on: str, work_dict:dict|None=None):
        visited_dt = datetime.fromisoformat(visited_on)
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            
            # TODO: lxml.fromstring -> //[property*="og:"] -> get title, url, image
            #        -> write out to s3 & emit event for a work

            hash = sha256(resp.content,usedforsecurity=False).hexdigest()

            key = f'{domain}/{visited_dt.strftime("%Y%j-%H")}-{hash}.html'
            await self.s3.put('article', key=key, contents=resp.content )
            msg = {"url": url, "bucket": "article", "key":key}
            if work_dict is not None:
                msg['work'] = work_dict #type:ignore
            await self.stream.send('article_uploaded', msg)

    async def _run(self):
        while not self._stop_event.is_set():
            try:
                resp = await self.stream.read(self.stream_name(), self._consumergroupname, self.consumername)
                for stream, msgs in resp:
                    logging.info(f'handling {stream=}')
                    for msg_id, msg in msgs:
                        logging.info(f'{msg=}')
                        match msg:
                            case {
                                "url": _,
                                "domain": domain,
                                "path": _,
                                "session_id": _,
                                "visited_on": _,
                            } if domain not in self.supported_domain:
                                logging.info(f'{domain=} not supported')
                                continue

                            case {
                                "url": url,
                                "domain": domain,
                                "path": path,
                                "session_id": session_id,
                                "visited_on": visited_on,
                                "work": work_dict,
                                "page_content_uri": pc_uri,
                            } if domain in self.supported_domain:
                                logging.info(f'page content uri detected; processing {url=}')
                                task = asyncio.create_task(
                                    self._process_page_content_article(
                                        url, domain, visited_on, pc_uri, work_dict=work_dict
                                    ),
                                )
                                task.add_done_callback(self.__remove_done_task)
                                self._background_tasks.append(task)
                            case {
                                "url": url,
                                "domain": domain,
                                "path": path,
                                "session_id": session_id,
                                "visited_on": visited_on,
                                "work": work_dict,
                            } if domain in self.supported_domain:
                                logging.info(f'no page content uri detected; processing {url=}')
                                task = asyncio.create_task(
                                    self._process_article(
                                        url, domain, visited_on, work_dict=work_dict
                                    )
                                )
                                task.add_done_callback(self.__remove_done_task)
                                self._background_tasks.append(task)
                            case {
                                "url": url,
                                "domain": domain,
                                "path": path,
                                "session_id": session_id,
                                "visited_on": visited_on,
                            } if domain in self.supported_domain:
                                logging.info(f'no page content uri detected; processing {url=}')
                                task = asyncio.create_task(
                                    self._process_article(
                                        url, domain, visited_on
                                    )
                                )
                                task.add_done_callback(self.__remove_done_task)
                                self._background_tasks.append(task)
                            case _:
                                logging.error('failed to find appropriate case')


            except asyncio.CancelledError:
                break
        await self.stream.close()

    def __remove_done_task(self, task):
        self._background_tasks.remove(task)

    def stop(self):
        self._stop_event.set()
