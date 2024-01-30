from .base import S3Connection

class MockS3(S3Connection):
    def __init__(self):
        self.s3 = {}

    async def get(self, bucket, key):
        bucket = self.s3.setdefault(bucket, {})
        return bucket.get(key, None)
    
    async def put(self, bucket, key, contents):
        bucket = self.s3.setdefault(bucket, {})
        bucket[key] = contents
        return