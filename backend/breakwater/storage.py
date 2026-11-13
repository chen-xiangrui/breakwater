import hashlib
import os
import tempfile
from pathlib import Path

# Backend containers share this directory through the objects Docker volume.
ROOT = Path(os.getenv('OBJECT_ROOT','/tmp/breakwater-objects'))
# Accept smaller source uploads and allow larger generated chunks/reports.
MAX_UPLOAD = 50 * 1024 * 1024
MAX_ARTIFACT = 512 * 1024 * 1024


def path(key):
    # Accept only SHA-256 keys, preventing arbitrary filenames or traversal outside ROOT.
    if len(key)!=64 or any(c not in '0123456789abcdef' for c in key):
        raise ValueError('Invalid object key')
    # Spread files across subdirectories using the first two checksum characters.
    return ROOT / key[:2] / key


def store_stream(stream,limit=MAX_UPLOAD):
    # Create a temporary file and compute its checksum while copying bounded buffers.
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
            # Flush Python and operating-system buffers before exposing the stored file.
            dest.flush()
            os.fsync(dest.fileno())
        # Use the content hash as the permanent key, allowing identical bytes to reuse storage.
        key = h.hexdigest()
        target = path(key)
        target.parent.mkdir(parents=True,exist_ok=True)
        try:
            os.link(name,target)  # Atomic create; never overwrite an existing object.
        except FileExistsError:
            pass
        # Re-read the permanent file to detect damaged or inconsistent stored bytes.
        with target.open('rb') as verify:
            if hashlib.file_digest(verify,'sha256').hexdigest()!=key:
                raise RuntimeError('Object-store integrity invariant violated')
        return key,size
    finally:
        # Remove the temporary name after success or failure; a linked permanent file survives.
        Path(name).unlink(missing_ok=True)


def store_bytes(data):
    # Adapt an in-memory byte sequence to the stream writer and return only its storage key.
    from io import BytesIO
    return store_stream(BytesIO(data),limit=MAX_ARTIFACT)[0]

