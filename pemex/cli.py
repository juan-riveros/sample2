from argparse import ArgumentParser
import asyncio
import os
from typing import Type

from pemex.logging import logging

from pemex.actor import Actor, LokiForwarder, MessageArchiver, ArticleArchiver
from pemex.stream import RedisStream, Stream
from pemex.s3 import S3Connection, MinioConnection

async def amain(stream:Stream, actors:list[Type[Actor]], s3conn:S3Connection):
    _actors = []
    async with asyncio.TaskGroup() as tg:
        try:
            for actorT in actors:
                actor = actorT(stream=stream, s3_conn=s3conn)
                _actors.append(actor)
                await stream.create_consumer_group(actor.stream_name(), actor._consumergroupname)
                tg.create_task(actor._run())
        except asyncio.CancelledError:
            for actor in _actors:
                actor.stop()
            
        except KeyboardInterrupt:
            for actor in _actors:
                actor.stop()
            


def _main():
    __stream_choices = ['redis']
    __s3_choices = ['minio']
    parser = ArgumentParser()
    parser.add_argument('--article-archiver',action='store_true')
    parser.add_argument('--loki-forwarder', action='store_true')
    parser.add_argument('--message-archiver', action='store_true')
    parser.add_argument('--stream', dest='stream_type', choices=__stream_choices, required=True)
    parser.add_argument('--s3-conn', dest='s3_type', choices=__s3_choices, required=True)
    args = parser.parse_args()
    
    actors = []

    
    if args.article_archiver:
        actors.append(ArticleArchiver)
        
    if args.loki_forwarder:
        actors.append(LokiForwarder)
        
    if args.message_archiver:
        actors.append(MessageArchiver)

    match args.stream_type:
        case 'redis':
            stream = RedisStream(
                host=os.getenv('REDIS_HOST', '127.0.0.1'),
                port=os.getenv('REDIS_PORT', '6379'),
                db=0,
                block=int(os.getenv('REDIS_BLOCK_MS', 100)),
                count=int(os.getenv('REDIS_COUNT', 10)),
                maxlen=int(os.getenv('REDIS_MAXLEN', 10e6))

            )
        case _:
            logging.error(f"Invalid Stream supplied, supported stream choices are: {__stream_choices}")
            raise SystemExit(1)
        
    match args.s3_type:
        case 'minio':
            s3conn = MinioConnection(
                endpoint=os.getenv('MINIO_ENDPOINT', '127.0.0.1:9000'),
                access_key=os.getenv('MINIO_ACCESS_KEY', None),
                secret_key=os.getenv('MINIO_SECRET_KEY', None),
                secure=bool(os.getenv("MINIO_SECURE", False)),
                region=os.getenv('MINIO_REGION', ''),
                cert_check=bool(os.getenv('MINIO_CERT_CHECK', False))
            )
        case _:
            logging.error(f"Invalid S3 Connection supplied, supported S3 choices are: {__s3_choices}")
            raise SystemExit(1)
    try:
        asyncio.run(amain(stream, actors, s3conn,))
    except KeyboardInterrupt:
        pass
    except asyncio.CancelledError:
        pass
    
    



if __name__ == "__main__":
    _main()