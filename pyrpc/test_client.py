from socketserver import StreamRequestHandler, TCPServer
from io import BytesIO
from queue import Queue
from threading import Thread

import pytest

from .marshal import MARSHAL_VERSION_STR
from .client import RPCConnection, USER_AGENT


def test_netloc_parsing():
	c = RPCConnection("ssh://localhost:1023/")

	assert c.scheme == "ssh"
	assert c.host == "localhost"
	assert c.port == 1023
	assert c.netloc == f"{c.host}:{c.port}"
	assert c.base_path == "/"
	assert c.base_url == "ssh://localhost:1023/"

	c = RPCConnection("python.org:999")

	assert c.scheme == "http"
	assert c.host == "python.org"
	assert c.port == 999
	assert c.netloc == f"{c.host}:{c.port}"
	assert c.base_path == "/"
	assert c.base_url == "http://python.org:999/"

	c = RPCConnection("python.org/test/path/")

	assert c.scheme == "http"
	assert c.host == "python.org"
	assert c.port == 8888
	assert c.netloc == f"{c.host}:{c.port}"
	assert c.base_path == "/test/path/"
	assert c.base_url == "http://python.org:8888/test/path/"


class EchoHandler(StreamRequestHandler):
	def response(self):
		body = f'{{"v": "{MARSHAL_VERSION_STR}", "data": 5}}'
		return ("HTTP/1.1 200 OK\r\n"
		        f"Content-Length: {len(body)}\r\n\r\n"
		        f"{body}").encode('utf-8')

	def handle(self):
		# self.rfile: read stream
		# self.wfile: write stream
		req = BytesIO()
		body_len = None
		line = self.rfile.readline()
		while line == b'\r\n':
			# eat blank lines
			line = self.rfile.readline()

		while not line == b'\r\n':
			# find headers
			req.write(line)
			if line.startswith(b"Content-Length"):
				# handle content-length header
				body_len = int(line[15:].strip())
				print(f"got content-length {body_len}")
			line = self.rfile.readline()

		# if we have a body, read it
		if body_len is not None:
			req.write(self.rfile.read(body_len))

		# write response
		self.wfile.write(self.response())

		#print(req.getvalue())
		self.server.queue.put(req.getvalue())


@pytest.fixture(scope="function")
def echo_server():
	queue = Queue()

	with TCPServer(("localhost", 0), EchoHandler) as server:
		server.queue = queue
		thread = Thread(target=lambda: server.serve_forever())
		thread.daemon = True
		thread.start()
		host, port = server.server_address

		print(f"Started echo server on port {port}")

		yield (port, queue)

		server.shutdown()
		thread.join()


def test_client_get(echo_server):
	(port, queue) = echo_server

	c = RPCConnection("localhost", port)
	c.get("/")

	assert queue.get() == (
		"GET / HTTP/1.1\r\n"
		f"User-Agent: {USER_AGENT}\r\n"
		"Connection: close\r\n"
		f"Host: localhost:{port}\r\n"
		"Accept-Encoding: gzip\r\n"
	).encode('utf-8')
