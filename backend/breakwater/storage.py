import hashlib
import os
import tempfile
from pathlib import Path

ROOT = Path(os.getenv('OBJECT_ROOT','/tmp/breakwater-objects'))
MAX_UPLOAD = 50 * 1024 * 1024
MAX_ARTIFACT = 512 * 1024 * 1024


def path(key):
    if len(key)!=64 or any(c not in '0123456789abcdef' for c in key):
        raise ValueError('Invalid object key')
    return ROOT / key[:2] / key


def store_stream(stream,limit=MAX_UPLOAD):
    ROOT.mkdir(parents=True,exist_ok=True)
    h,size = hashlib.sha256(),0
    fd,name = tempfile.mkstemp(dir=ROOT)
    try:
        with os.fdopen(fd,'wb') as dest:
            while chunk := stream.read(1024*1024):
                size += len(chunk)
                if size>limit:
                    raise ValueError(f'File exceeds {limit//(1024*1024)} MiB storage limit')
                h.update(chunk)
                dest.write(chunk)
            dest.flush()
            os.fsync(dest.fileno())
        key = h.hexdigest()
        target = path(key)
        target.parent.mkdir(parents=True,exist_ok=True)
        try:
            os.link(name,target)  # Atomic create; never overwrite an existing object.
        except FileExistsError:
            pass
        with target.open('rb') as verify:
            if hashlib.file_digest(verify,'sha256').hexdigest()!=key:
                raise RuntimeError('Object-store integrity invariant violated')
        return key,size
    finally:
        Path(name).unlink(missing_ok=True)


def store_bytes(data):
    from io import BytesIO
    return store_stream(BytesIO(data),limit=MAX_ARTIFACT)[0]

