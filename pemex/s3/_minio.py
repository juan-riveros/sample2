from io import BytesIO
from minio import Minio

from pemex import logging

from .base import S3Connection

class MinioConnection(S3Connection):
    def __init__(self, endpoint:str, access_key, secret_key, secure:bool, region:str, cert_check:bool):
        self.__client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region,
            cert_check=cert_check,
        )

    async def get(self, bucket, key):
        resp = self.__client.get_object(bucket, key,)
        if resp.status != 200:
            return None
        return resp.data

    async def put(self, bucket:str, key:str, contents:bytes, content_type:str|None=None):
        resp = self.__client.put_object(bucket, key, data=BytesIO(contents), length=len(contents), content_type='application/octet-stream' if content_type is None else content_type)
        logging.logging.info(f"{resp=}")
        