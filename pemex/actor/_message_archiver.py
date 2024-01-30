"""message archiver

Actor to save messages from the stream queue persistently.

"""

import sqlite3
import asyncio
from pathlib import Path
from datetime import datetime, UTC

from pemex.actor.base import Actor
from pemex.stream.base import Stream
from pemex.stream_connector import get_redis_connection

from redis.asyncio import Redis
from uuid_extensions import uuid7


def dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


SCHEMA = """
create table if not exists visit (
    id integer primary key,
    domain text not null,
    path text not null,
    visited_on DATETIME not null,
    session_id text not null
);
"""


class MessageArchiver(Actor):
    _consumergroupname = "message_archiver"

    def __init__(
        self,
        stream: Stream,
        base_path: Path,
    ):
        if not base_path.is_dir():
            raise NotADirectoryError(base_path)
        self.base_path = base_path
        self._stop_event = asyncio.Event()

        self.stream = stream
        self.consumername: str = uuid7().hex  # type: ignore

        self.__dt = datetime.now(tz=UTC)
        self.db = self.__init_db(base_path / f"{self.__dt.strftime('%Y-%j')}.db")

    def stream_name(self) -> str:
        return 'browser_visit'

    def __init_db(self, path: Path):
        """create a sqlite db"""
        if getattr(self, "db", None) is not None:
            self.db.close()
        self.db = sqlite3.connect(
            f"file:{path.absolute()}?cache=shared&foreign_keys=on&journal_mode=WAL&synchronous=normal&cache_size=-102400",
            check_same_thread=False,
            uri=True,
        )
        self.db.row_factory = dict_factory
        self.db.executescript(SCHEMA)

    async def _run(self):
        """reads from redis stream and writes messages into sqlite3 db"""

        while not self._stop_event.is_set():
            try:
                messages = await self.stream.read(self.stream_name(), self._consumergroupname, self.consumername)

                self.db.execute("begin")

                for msg_id, msg in messages:
                    match msg:
                        case {
                            "domain": domain,
                            "path": path,
                            "visited_on": visit_on,
                            "session_id": session_id,
                        }:
                            visit_dt = datetime.fromisoformat(visit_on)
                            if self.__dt.date() != visit_dt.date():
                                self.__init_db(
                                    self.base_path / f"{visit_dt.strftime('%Y-%j')}.db"
                                )
                                self.db.execute("begin")

                            self.db.execute(
                                "insert into visit (domain, path, visited_on, ) values (?, ?, ?, ?)",
                                (domain, path, visit_dt, session_id),
                            )
                            await self.stream.ack(
                                self.stream_name(), self._consumergroupname, msg_id #type: ignore
                            )
                        case _:
                            # invalid format, skipping message
                            continue

                self.db.execute("commit")

            except asyncio.CancelledError:
                break
        else:
            self.db.close()

    def stop(self):
        self._stop_event.set()
