"""Installed native TCP fallback, with synthetic DNS and real loopback sockets."""
import os
import subprocess
import sys

import pytest

pytestmark = pytest.mark.skipif(sys.platform != 'linux', reason='Linux loader/socket qualification')

DNS_SHIM = r'''
#define _GNU_SOURCE
#include <dlfcn.h>
#include <netdb.h>
#include <string.h>
int getaddrinfo(const char *node, const char *service,
                const struct addrinfo *hints, struct addrinfo **result) {
    int (*resolve)(const char *, const char *, const struct addrinfo *, struct addrinfo **) =
        dlsym(RTLD_NEXT, "getaddrinfo");
    if (!node || strcmp(node, "dual-stack.invalid"))
        return resolve(node, service, hints, result);
    struct addrinfo *first = 0, *second = 0;
    int rc = resolve("127.0.0.1", service, hints, &first);
    if (rc) return rc;
    rc = resolve("::1", service, hints, &second);
    if (rc) { freeaddrinfo(first); return rc; }
    struct addrinfo *last = first;
    while (last->ai_next) last = last->ai_next;
    last->ai_next = second;
    *result = first;
    return 0;
}
'''

PROBE = r'''
import asyncio, base64, hashlib, socket
from nautilus_trader.core.nautilus_pyo3 import WebSocketClient, WebSocketConfig

async def main():
    backlog = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    backlog.bind(('127.0.0.1', 0))
    port = backlog.getsockname()[1]
    backlog.listen(0)
    occupied = socket.create_connection(('127.0.0.1', port), timeout=1)
    # Verify that the first route actually stalls; never certify a fixture
    # whose backlog capacity or platform behavior differs from this contract.
    proof = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    proof.settimeout(.1)
    try:
        proof.connect(('127.0.0.1', port))
        raise AssertionError('fixture did not stall first route')
    except TimeoutError:
        pass
    finally:
        proof.close()
    calls = []
    async def handshake(reader, writer):
        try:
            request = (await reader.readuntil(b'\r\n\r\n')).decode()
            headers = {key.lower(): value for key, value in (line.split(': ', 1) for line in request.split('\r\n')[1:] if ': ' in line)}
            assert headers['x-qualification'] == 'synthetic'
            key = headers['sec-websocket-key']
            accept = base64.b64encode(hashlib.sha1((key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
            writer.write(('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n').encode())
            await writer.drain()
            calls.append(request.split('\r\n')[0])
            await reader.read(1024)
        finally:
            writer.close()
            await writer.wait_closed()
    server_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
    server_socket.bind(('::1', port))
    server_socket.listen()
    server_socket.setblocking(False)
    server = await asyncio.start_server(handshake, sock=server_socket, limit=8192)
    try:
        for host in ['[::1]', 'dual-stack.invalid']:
            config = WebSocketConfig(url=f'ws://{host}:{port}/synthetic', headers=[('x-qualification', 'synthetic')])
            try:
                client = await asyncio.wait_for(WebSocketClient.connect(asyncio.get_running_loop(), config, lambda *_: None), 3)
            except TimeoutError as exc:
                raise AssertionError('stalled first route prevented reachable alternate handshake') from exc
            assert client.is_active()
            await asyncio.wait_for(client.disconnect(), 2)
        assert calls == ['GET /synthetic HTTP/1.1'] * 2
    finally:
        server.close()
        await server.wait_closed()
        occupied.close()
        backlog.close()
asyncio.run(main())
'''


def test_installed_websocket_skips_stalled_address_and_preserves_handshake(tmp_path):
    source = tmp_path / 'synthetic_dns.c'
    library = tmp_path / 'synthetic_dns.so'
    source.write_text(DNS_SHIM)
    subprocess.run(['cc', '-shared', '-fPIC', '-O2', str(source), '-ldl', '-o', str(library)],
                   check=True, capture_output=True, text=True, timeout=30)
    probe = tmp_path / 'probe.py'
    probe.write_text(PROBE)
    env = {k: v for k, v in os.environ.items() if k not in ('PYTHONPATH', 'LD_LIBRARY_PATH', 'LD_PRELOAD')}
    env['LD_PRELOAD'] = str(library)
    result = subprocess.run([sys.executable, '-I', str(probe)], cwd=tmp_path,
                            env=env, capture_output=True, text=True, timeout=12)
    assert result.returncode == 0, result.stdout + result.stderr
