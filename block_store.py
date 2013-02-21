import shutil

class BlockStore(object):
    def __init__(self):
        raise Error("Not implemented")

    def get(self, id, fp, progress_cb=None):
        raise Error("Not implemented")

    def put(self, id, fp, progress_cb=None):
        raise Error("Not implemented")

    def has(self, id):
        raise Error("Not implemented")

    def delete(self, id):
        raise Error("Not implemented")

import os
import os.path

class FileStore(BlockStore):
    def __init__(self, path):
        self.path = path

    def get(self, id, fp, progress_cb=None):
        fs = open(self.id_to_path(id), 'r')
        shutil.copyfileobj(fs, fp)
        fs.close()

    def put(self, id, fp, progress_cb=None):
        fd = open(self.id_to_path(id), 'w')
        shutil.copyfileobj(fp, fd)
        fd.close()

    def has(self, id):
        return os.path.exists(self.id_to_path(id))

    def delete(self, id):
        os.unlink(self.id_to_path(id))

    def id_to_path(self, id):
        return "%s/%s" % (self.path, id)

from boto.s3.connection import S3Connection
from boto.s3.connection import Location
from boto.s3.key import Key

class S3Store(BlockStore):
    def __init__(self, bucket, access_key=None, secret_key=None, location=None):
        self.conn = S3Connection(access_key, secret_key)
        if location != None:
            location = getattr(Location, location)
        else:
            location = Location.DEFAULT
        self.bucket = self.conn.create_bucket(bucket, location=location)

    def put(self, id, fp, progress_cb=None):
        k = Key(self.bucket)
        k.key = id
        k.set_contents_from_file(fp, cb=progress_cb)

    def get(self, id, fp, progress_cb=None):
        k = Key(self.bucket)
        k.key = id
        k.get_contents_to_file(fp, cb=progress_cb)

    def has(self, id):
        k = Key(self.bucket)
        k.key = id
        return k.exists()

    def delete(self, id):
        k = Key(self.bucket)
        k.key = id
        k.delete()
