"""真实 HTTP 后端 + fnOS Unix socket 链路的网关回归测试。"""
import importlib.util
import io
import json
import logging
from pathlib import Path
import socket
import sys
import tempfile
import threading
import unittest
from http.client import HTTPConnection, RemoteDisconnected
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
from urllib.parse import urlencode


class UnixConnection(HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost', timeout=3)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.conf = Path(self.tmp.name) / 'qBittorrent.conf'
        self.socket_path = str(Path(self.tmp.name) / 'q.sock')
        self.posts = 0
        self.accept_change = True
        owner = self

        class Backend(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def do_GET(self):
                body = str(self.server.server_port).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                self.rfile.read(int(self.headers.get('Content-Length', '0')))
                owner.posts += 1
                if self.path == '/drop':
                    self.close_connection = True
                    return
                self.send_response(204 if owner.accept_change else 400)
                if not owner.accept_change:
                    self.send_header('Content-Length', '0')
                self.end_headers()

            def log_message(self, *args):
                pass

        self.backends = [ThreadingHTTPServer(('127.0.0.1', 0), Backend) for _ in range(2)]
        for server in self.backends:
            threading.Thread(target=server.serve_forever, daemon=True).start()
        self.old, self.new = [s.server_port for s in self.backends]
        self.write_port(self.old)
        spec = importlib.util.spec_from_file_location('gateway', Path(__file__).resolve().parents[1] / 'app/bin/gateway-proxy.py')
        self.g = importlib.util.module_from_spec(spec)
        with patch.object(sys, 'argv', ['gateway', self.socket_path, '127.0.0.1', str(self.old), str(self.conf)]):
            spec.loader.exec_module(self.g)
        self.g.ProxyHandler._conn_pool = self.g.ConnectionPool('127.0.0.1', self.old)
        self.server = self.g.ThreadedUnixHTTPServer(self.socket_path, self.g.ProxyHandler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.clients = []

    def tearDown(self):
        for client in self.clients:
            client.close()
        self.server.shutdown()
        self.server.server_close()
        self.g.ProxyHandler._conn_pool.close_all()
        for backend in self.backends:
            backend.shutdown()
            backend.server_close()
        self.tmp.cleanup()

    def write_port(self, port):
        self.conf.write_text('[Preferences]\nWebUI\\Port=%s\n' % port)

    def client(self):
        client = UnixConnection(self.socket_path)
        self.clients.append(client)
        return client

    def request(self, client, method='GET', path='/api/v2/app/version', body=None):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'} if body else {}
        client.request(method, self.g.PREFIX + path, body, headers)
        response = client.getresponse()
        payload = response.read()
        return response, payload

    def test_live_switch_before_config_flush_and_after_flush(self):
        client = self.client()
        self.assertEqual(self.request(client)[1], str(self.old).encode())
        body = urlencode({'json': json.dumps({'web_ui_port': self.new})})
        response, _ = self.request(client, 'POST', '/api/v2/app/setPreferences', body)
        self.assertEqual(response.status, 204)
        self.assertIsNone(response.getheader('Transfer-Encoding'))
        self.assertEqual(self.request(client)[1], str(self.new).encode())
        self.write_port(self.new)
        self.assertEqual(self.request(client)[1], str(self.new).encode())
        self.write_port(self.old)
        self.assertEqual(self.request(client)[1], str(self.old).encode())

    def test_completed_connections_do_not_exhaust_worker_pool(self):
        # 保留超过 8 个客户端对象，验证已经完成响应的空闲连接不占线程。
        for _ in range(12):
            client = self.client()
            client.timeout = 1
            response, payload = self.request(client)
            self.assertEqual(response.status, 200)
            self.assertEqual(response.getheader('Connection'), 'close')
            self.assertEqual(payload, str(self.old).encode())

    def test_external_port_change_has_no_polling_delay(self):
        client = self.client()
        self.request(client)
        self.write_port(self.new)
        self.assertEqual(self.request(client)[1], str(self.new).encode())

    def test_rejected_change_does_not_change_target(self):
        self.accept_change = False
        client = self.client()
        body = urlencode({'json': json.dumps({'web_ui_port': self.new})})
        self.assertEqual(self.request(client, 'POST', '/api/v2/app/setPreferences', body)[0].status, 400)
        self.assertEqual(self.request(client)[1], str(self.old).encode())

    def test_invalid_config_keeps_last_valid_port(self):
        self.assertEqual(self.g.get_target_port(), self.old)
        for value in (0, 65536, 'invalid', '8082garbage'):
            self.write_port(value)
            self.assertEqual(self.g.get_target_port(), self.old)

    def test_inflight_old_connection_is_not_returned_to_pool(self):
        pool = self.g.ProxyHandler._conn_pool
        old = pool.acquire(self.old)
        old.request('GET', '/')
        old.getresponse().read()
        pool.ensure_port(self.new)
        pool.release(old)
        self.assertIsNone(old.sock)
        fresh = pool.acquire(self.new)
        self.assertEqual(fresh.port, self.new)
        fresh.close()

    def test_half_closed_idle_connection_is_discarded(self):
        pool = self.g.ProxyHandler._conn_pool
        stale = pool.acquire(self.old)
        local, peer = socket.socketpair()
        stale.sock = local
        pool.release(stale)
        peer.shutdown(socket.SHUT_WR)
        try:
            fresh = pool.acquire(self.old)
            self.assertIsNot(fresh, stale)
            self.assertIsNone(stale.sock)
            fresh.close()
        finally:
            peer.close()

    def test_mutation_is_not_replayed_after_lost_response(self):
        response, _ = self.request(self.client(), 'POST', '/drop', 'value=1')
        self.assertEqual(response.status, 502)
        self.assertEqual(self.posts, 1)

    def test_cancelled_cached_request_does_not_log_traceback(self):
        self.g._static_cache.set('GET:/assets/test.js', 200, [], b'hello')
        client = self.client()
        logs = io.StringIO()
        handler = logging.StreamHandler(logs)
        logging.getLogger().addHandler(handler)
        try:
            with patch.object(self.g.ProxyHandler, 'end_headers', side_effect=BrokenPipeError):
                client.request('GET', self.g.PREFIX + '/assets/test.js')
                with self.assertRaises(RemoteDisconnected):
                    client.getresponse()
            self.assertNotIn('Traceback', logs.getvalue())
            self.assertNotIn('unhandled', logs.getvalue())
        finally:
            logging.getLogger().removeHandler(handler)
        self.assertEqual(self.request(self.client())[0].status, 200)


if __name__ == '__main__':
    unittest.main()
