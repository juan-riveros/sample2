from .base import S3Connection
from ._minio import MinioConnection

__all__ = [
    "S3Connection",
    "MinioConnection"
]