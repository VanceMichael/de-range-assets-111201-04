import base64
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class Headers:
    """大小写不敏感的响应头视图，统一 http.client 的驼峰键。"""

    def __init__(self, mapping):
        self._mapping = dict(mapping)
        self._lower = {str(key).lower(): value for key, value in self._mapping.items()}

    def get(self, name, default=None):
        return self._lower.get(str(name).lower(), default)

    def __getitem__(self, name):
        return self._lower[str(name).lower()]

    def __contains__(self, name):
        return str(name).lower() in self._lower

    def as_dict(self):
        return dict(self._mapping)


class StreamClient:
    """流式 HTTP 客户端：返回可读响应流，由调用方按需读取并关闭。

    4xx/5xx 不会在这里抛异常，而是作为正常响应返回，
    这样 416、200 等状态可以由下载协议层显式处理。
    """

    def __init__(self, timeout=30):
        self.timeout = timeout

    @contextmanager
    def open(self, url, headers=None):
        request = Request(url, headers=headers or {}, method='GET')
        try:
            response = urlopen(request, timeout=self.timeout)
        except HTTPError as error:
            response = error
        try:
            yield response.status, Headers(response.headers), response
        finally:
            response.close()


class Client:
    def __init__(self, timeout=5):
        self.timeout = timeout

    def request(self, url, method='GET', headers=None, body=None):
        request = Request(url, data=body, headers=headers or {}, method=method)
        try:
            response = urlopen(request, timeout=self.timeout)
        except HTTPError as error:
            response = error
        with response:
            return response.status, dict(response.headers), response.read()


@contextmanager
def replay_server(routes):
    positions = {}
    calls = []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format_string, *arguments):
            return None

        def do_GET(self):
            self.respond()

        def do_POST(self):
            self.respond()

        def respond(self):
            length = int(self.headers.get('Content-Length', '0'))
            body = self.rfile.read(length)
            with lock:
                calls.append(dict(method=self.command, path=self.path, headers=dict(self.headers), body=body.decode('utf-8', errors='replace')))
                replies = routes.get(self.path, [dict(status=404, body='未找到资源')])
                index = positions.get(self.path, 0)
                reply = replies[min(index, len(replies) - 1)]
                positions[self.path] = index + 1
            data = base64.b64decode(reply['base64']) if 'base64' in reply else reply.get('body', '').encode('utf-8')
            self.send_response(reply.get('status', 200))
            for name, value in reply.get('headers', {}).items():
                self.send_header(name, value)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}', calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
