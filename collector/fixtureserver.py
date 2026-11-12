"""本地合成资源响应服务器。

与旧的脚本回放工具不同，这里实现真实的 HTTP 范围语义：

* 正常情况下解析 ``Range`` 返回 206 + ``Content-Range`` + ``Accept-Ranges``；
* ``ignore_range`` 模式故意忽略 Range 回 200 全量；
* ``force_416`` 模式对范围请求回 416；
* ``If-Range`` 与当前实体不一致时按 RFC 回退为 200；
* 控制接口可在运行中换版本（同尺寸不同内容、新 ETag）或污染字节，
  用来演示实体变化与摘要不符。

仅监听回环地址，数据全部来自本地合成 fixture，不访问网络。
"""

import base64
import hashlib
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from .model import Manifest, Member

MODE_NORMAL = 'normal'
MODE_IGNORE_RANGE = 'ignore_range'
MODE_FORCE_416 = 'force_416'
MODE_CORRUPT = 'corrupt'


def load_seed(seed_path):
    """从 sample.json 读取初始实体字节与元数据。"""

    payload = json.loads(Path(seed_path).read_text(encoding='utf-8'))
    assets = {}
    for name, item in payload['files'].items():
        assets[name] = dict(
            data=base64.b64decode(item['data']),
            etag=item['etag'],
            size=item['size'],
            sha256=item['digest'],
        )
    return assets


class AssetServer:
    """可在运行中切换状态的合成资源服务器状态容器。"""

    def __init__(self, bundle, assets):
        self.bundle = bundle
        self.assets = dict(assets)
        self.generation = 1
        self.mode = MODE_NORMAL
        self.manifest_etag = '"m1"'
        self.serving_overrides = {}
        self.calls = []
        self.lock = threading.Lock()

    def manifest(self):
        members = tuple(
            Member(name=name, size=item['size'], sha256=item['sha256'],
                   etag=item['etag'], url=f'/files/{name}')
            for name, item in sorted(self.assets.items())
            if name not in self.serving_overrides
        )
        for name, item in sorted(self.serving_overrides.items()):
            original = self.assets[name]
            members += (Member(
                name=name, size=original['size'], sha256=original['sha256'],
                etag=original['etag'], url=f'/files/{name}'),)
        members = tuple(sorted(members, key=lambda item: item.name))
        return Manifest(bundle=self.bundle, generation=self.generation,
                        members=members, etag=self.manifest_etag)

    def apply_control(self, action):
        with self.lock:
            kind = action.get('action')
            if kind == 'set_mode':
                mode = action['mode']
                if mode not in (MODE_NORMAL, MODE_IGNORE_RANGE,
                                MODE_FORCE_416, MODE_CORRUPT):
                    raise ValueError(f'未知模式：{mode}')
                self.mode = mode
                return dict(mode=self.mode)
            if kind == 'override_serving_entity':
                # 清单保持原样，但文件实体换成新 ETag/新内容（同尺寸），
                # 用来演示续传时实体标识变化被拦截。
                name = action['name']
                if name not in self.assets:
                    raise KeyError(name)
                data = base64.b64decode(action['data'])
                if len(data) != self.assets[name]['size']:
                    raise ValueError('演示要求新版本与旧版本长度相同')
                self.serving_overrides[name] = dict(
                    data=data, etag=action['etag'], size=len(data),
                    sha256=hashlib.sha256(data).hexdigest())
                return dict(overridden=name, etag=action['etag'])
            if kind == 'clear_overrides':
                self.serving_overrides.clear()
                return dict(cleared=True)
            if kind == 'replace_asset':
                name = action['name']
                if name not in self.assets:
                    raise KeyError(name)
                data = base64.b64decode(action['data'])
                old = self.assets[name]
                if len(data) != old['size']:
                    raise ValueError('演示要求新版本与旧版本长度相同')
                self.assets[name] = dict(
                    data=data, etag=action['etag'], size=len(data),
                    sha256=hashlib.sha256(data).hexdigest())
                self.serving_overrides.pop(name, None)
                if not action.get('keep_generation'):
                    self.generation += 1
                    self.manifest_etag = f'"m{self.generation}"'
                return dict(generation=self.generation, etag=action['etag'])
            raise ValueError(f'未知控制动作：{kind}')


def parse_byte_range(header, total):
    """解析 bytes=start-end / bytes=start-，返回闭区间 (start, end)。"""

    if not header or not header.startswith('bytes='):
        return None
    spec = header[len('bytes='):].strip()
    if ',' in spec:
        spec = spec.split(',', 1)[0].strip()
    start_text, _, end_text = spec.partition('-')
    start = int(start_text)
    end = int(end_text) if end_text else total - 1
    if start < 0 or end < start:
        raise ValueError('非法 Range')
    return start, min(end, total - 1)


def make_handler(server_state):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, format_string, *arguments):
            return None

        def do_GET(self):
            path = urlparse(self.path).path
            with server_state.lock:
                server_state.calls.append(dict(
                    method='GET', path=path,
                    range=self.headers.get('Range'),
                    if_range=self.headers.get('If-Range')))
                mode = server_state.mode
            if path == '/manifest.json':
                self._serve_manifest()
            elif path.startswith('/files/'):
                self._serve_file(path[len('/files/'):], mode)
            else:
                self._send_json(404, dict(error='未找到资源'))

        def do_POST(self):
            path = urlparse(self.path).path
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length)
            if path != '/__control__':
                self._send_json(404, dict(error='未找到资源'))
                return
            try:
                action = json.loads(raw.decode('utf-8'))
                result = server_state.apply_control(action)
            except (ValueError, KeyError, json.JSONDecodeError) as error:
                self._send_json(400, dict(error=str(error)))
                return
            self._send_json(200, dict(ok=True, **result))

        def _serve_manifest(self):
            with server_state.lock:
                manifest = server_state.manifest()
                etag = server_state.manifest_etag
            if self.headers.get('If-None-Match') == etag:
                self.send_response(304)
                self.send_header('ETag', etag)
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            data = json.dumps(manifest.to_dict(), ensure_ascii=False).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('ETag', etag)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _serve_file(self, name, mode):
            with server_state.lock:
                asset = server_state.assets.get(name)
                if asset is None:
                    self._send_json(404, dict(error=f'未知文件：{name}'))
                    return
                override = server_state.serving_overrides.get(name)
                if override is not None:
                    data, etag = override['data'], override['etag']
                else:
                    data, etag = asset['data'], asset['etag']
                if mode == MODE_CORRUPT:
                    mutable = bytearray(data)
                    mutable[0] = (mutable[0] + 1) % 256
                    data = bytes(mutable)
                total = len(data)

            range_header = self.headers.get('Range')
            if_range = self.headers.get('If-Range')
            # If-Range 不匹配：按 RFC 忽略范围，回退为完整 200。
            range_stale = if_range is not None and if_range != etag
            if range_header and mode == MODE_FORCE_416:
                self.send_response(416)
                self.send_header('ETag', etag)
                self.send_header('Content-Range', f'bytes */{total}')
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            try:
                span = parse_byte_range(range_header, total)
            except ValueError as error:
                self._send_json(400, dict(error=str(error)))
                return
            if not range_header or mode == MODE_IGNORE_RANGE or range_stale or span is None:
                self._write_body(200, data, etag, content_range=None)
                return
            start, end = span
            if start >= total:
                self.send_response(416)
                self.send_header('ETag', etag)
                self.send_header('Content-Range', f'bytes */{total}')
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            self._write_body(206, data[start:end + 1], etag,
                             content_range=f'bytes {start}-{end}/{total}')

        def _write_body(self, status, body, etag, content_range):
            self.send_response(status)
            self.send_header('ETag', etag)
            self.send_header('Accept-Ranges', 'bytes')
            self.send_header('Content-Type', 'application/octet-stream')
            if content_range:
                self.send_header('Content-Range', content_range)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_json(self, status, payload):
            data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    return Handler


class FixtureHandle:
    def __init__(self, base, state, server, thread):
        self.base = base
        self.state = state
        self._server = server
        self._thread = thread

    def manifest_url(self):
        return self.base + '/manifest.json'

    def file_url(self, name):
        return self.base + '/files/' + name

    def control(self, action):
        from .transport import Client
        body = json.dumps(action).encode('utf-8')
        status, _, raw = Client().request(
            self.base + '/__control__', method='POST',
            headers={'Content-Type': 'application/json'}, body=body)
        if status != 200:
            raise RuntimeError(f'控制接口失败 {status}: {raw.decode()}')
        return json.loads(raw)

    def close(self):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


@contextmanager
def serve_fixtures(assets, bundle='survey-1'):
    state = AssetServer(bundle, assets)
    server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    handle = FixtureHandle(f'http://127.0.0.1:{server.server_port}',
                           state, server, thread)
    try:
        yield handle
    finally:
        handle.close()
