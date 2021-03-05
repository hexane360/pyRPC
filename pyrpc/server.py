"""
Module containing the pyrpc `Server`.
"""

import inspect
import logging
import sys
import traceback
from typing import List, Sequence, Optional, Awaitable, Union

#from tornado import escape, gen
from tornado.httpserver import HTTPServer
from tornado.web import RequestHandler, Application, Finish, HTTPError
from tornado import escape
from tornado.httputil import responses


VERSION = 0.1


class RPCError(HTTPError):
	def __init__(self, status_code: int = 500,
	             *extra: Sequence[str],
	             reason: Optional[str] = None):
		self.status_code = status_code
		self.extra = extra
		self.reason = reason or responses.get(status_code, "Unknown")

		self.args = []
		self.log_message = '\n'.join(map(str, extra))

	def __str__(self) -> str:
		base = f"HTTP {self.status_code}: {self.reason}"
		if len(self.extra) > 0:
			return f"{base}:\n{self.log_message}"
		else:
			return base


def not_found_error(endpoint: str) -> RPCError:
	return RPCError(404, f"Endpoint '{endpoint}' not found.")


def call_error(endpoint: str, exc_info: Optional[str] = None) -> RPCError:
	if exc_info is None:
		return RPCError(400, f"Error calling '{endpoint}'")
	else:
		tb = traceback.format_exception(*exc_info)
		return RPCError(400, f"Error calling '{endpoint}':", *tb)


def not_callable_error(endpoint: str) -> RPCError:
	return RPCError(400, f"'{endpoint}' is not callable")


def signature_error() -> RPCError:
	return RPCError(400)


class RPCServer:
	def __init__(self, root, **kwargs):
		self.app = Application([
			(r".*", RPCHandler, dict(root=root))
		])
		self.server = HTTPServer(self.app, **kwargs)

	def listen(self, port: int, address: str = ""):
		self.server.listen(port, address)


class RPCHandler(RequestHandler):
	"""
	Class which handles an RPC request.
	"""

	def initialize(self, root):
		self.root = root

	def get(self):
		return self.get_endpoint(self.request.path)

	def post(self):
		logging.debug(f"POST body: {self.request.body}")

		if self.request.headers.get('Content-Type', 'application/x-json') != 'application/x-json':
			raise RPCError(415, "Expected application/x-json")

		try:
			body = escape.json_decode(self.request.body)
		except Exception as e:
			raise RPCError(400, f"Invalid JSON body: {e.msg}")

		if isinstance(body, dict):
			args = body.get('args', [])
			kwargs = body.get('kwargs', {})
		elif isinstance(body, list):
			args = body
			kwargs = {}
		else:
			args = [body]
			kwargs = {}

		return self.call_endpoint(self.request.path, *args, **kwargs)

	def get_endpoint(self, endpoint: str):
		components = endpoint.lstrip('/').split('/')

		if components == ['']:
			components = []

		obj = self.root
		for (i, component) in enumerate(components):
			if component == '__sig__':  # return function signature instead
				try:
					obj = inspect.signature(obj)
				except TypeError:
					raise not_found_error('/'.join(components[:i+1]))
			try:
				obj = getattr(self, component)
			except AttributeError:
				raise not_found_error('/'.join(components[:i+1]))
		return obj

	def call_endpoint(self, endpoint: str, *args, **kwargs):
		obj = self.get_endpoint(endpoint)
		if not callable(obj):
			raise not_callable_error(endpoint)
		logging.debug(f"Calling {obj.__name__}(*[{args}], {{**{kwargs}}})")
		# TODO check signature here
		try:
			obj(*args, **kwargs)
		except Exception:
			raise call_error(endpoint, sys.exc_info())

	def set_default_headers(self):
		self.set_header('Server', f"PyRPC/{VERSION}")

	def write_error(self, status_code: int, **kwargs):

		if 'exc_info' in kwargs:
			exception = kwargs['exc_info'][1]
		else:
			exception = HTTPError(status_code, reason=kwargs.get('reason'))

		self.set_header("Content-Type", "text/plain; charset=UTF-8")
		self.write(str(exception))

		self.finish()

	# def write(self, chunk: Union[str, bytes, dict]) -> None:
	# 	if self._finished:
	# 		raise RuntimeError("write after finish()")
	# 	if isinstance(chunk, dict):
	# 		chunk = escape.json_encode(chunk)
	# 		self.set_header("Content-Type", "application/json; charset=UTF-8")
	# 	chunk = escape.utf8(chunk)
	# 	self._write_buffer.append(chunk)


class TEMScript:
	constant_value = 5

	def fun(self):
		"""Doc string 1"""
		return 10

	def dict_function(self):
		"""Doc string 2"""
		return {'a': 5, 'b': 10}


if __name__ == '__main__':
	import tornado.ioloop

	logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
	server = RPCServer(TEMScript())
	server.listen(8888)
	tornado.ioloop.IOLoop.current().start()
