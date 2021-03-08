import asyncio
from dataclasses import dataclass
from threading import Thread
import http.client
from typing import Dict

import tornado.ioloop
import tornado.netutil
import pytest

from .marshal import unmarshal_from_str
from .server import RPCServer, SERVER_AGENT


@dataclass(init=False)
class Response:
	status: int
	reason: str
	headers: Dict[str, str]
	body: str

	def __init__(self, resp):
		self.status = resp.status
		self.reason = resp.reason
		self.headers = resp.getheaders()
		self.headers = dict(self.headers)
		self.body = resp.read()


def http_request(port, method, endpoint, body=None, headers=None):
	conn = http.client.HTTPConnection('localhost', port, timeout=5.)
	try:
		conn.request(method, endpoint, body, headers or {})
		return Response(conn.getresponse())
	finally:
		conn.close()


def test_rpc_get(rpc_server):
	(port, obj) = rpc_server

	resp = http_request(port, 'GET', '/constant_value')

	assert resp.status == 200
	assert resp.headers['Server'] == SERVER_AGENT
	assert unmarshal_from_str(resp.body) == 5


class TestObj:
	constant_value = 5

	def fun(self):
		"""Doc string 1"""
		return 10

	def dict_function(self):
		"""Doc string 2"""
		return {'a': 5, 'b': 10}


@pytest.fixture
def rpc_server(scope="session"):

	obj = TestObj()
	server = RPCServer(obj)

	# listen on an empty port
	sockets = tornado.netutil.bind_sockets(None, 'localhost')
	assert len(sockets) == 1
	port = sockets[0].getsockname()[1]

	io_loop = asyncio.new_event_loop()

	def start_server():
		# set event loop
		asyncio.set_event_loop(io_loop)
		# assign sockets to the server
		server.server.add_sockets(sockets)
		# and start event loop
		tornado.ioloop.IOLoop.current().start()

	# run the server's event loop on a thread
	thread = Thread(target=start_server)
	thread.daemon = True
	thread.start()

	print(f"PyRPC server running on port {port}")

	yield (port, obj)

	# stop the server's event loop and wait for the thread to finish
	tornado.ioloop.IOLoop._ioloop_for_asyncio[io_loop].add_callback(io_loop.stop)
	thread.join(5.)
