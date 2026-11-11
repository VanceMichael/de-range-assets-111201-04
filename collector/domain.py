import base64
import hashlib


class AssetGateway:
    def __init__(self, files):
        self.files = files

    def fetch(self, name, start=None, validator=None):
        item = self.files[name]
        raw = base64.b64decode(item['data'])
        headers = {'ETag': item['etag'], 'Accept-Ranges': 'bytes'}
        if start is None or item.get('ignore_range') or (validator and validator != item['etag']):
            return 200, headers, raw
        if start >= len(raw):
            return 416, dict(headers, **{'Content-Range': f'bytes */{len(raw)}'}), b''
        headers['Content-Range'] = f'bytes {start}-{len(raw)-1}/{len(raw)}'
        return 206, headers, raw[start:]


def verify(raw, size, digest):
    if len(raw) != size:
        raise ValueError('文件长度不符')
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError('文件摘要不符')
    return True


def collect(payload):
    source = AssetGateway(payload['files'])
    records = []
    for name, item in payload['files'].items():
        status, headers, raw = source.fetch(name)
        digest = hashlib.sha256(raw).hexdigest()
        verify(raw, item['size'], item['digest'])
        records.append(dict(key=name, size=len(raw), digest=digest, etag=headers['ETag']))
    return records
