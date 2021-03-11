"""
Module containing the pyrpc `Server`.
"""

import inspect
import logging
#from os import urandom
import sys
import traceback
import json
from io import BytesIO
import weakref
from typing import List, Sequence, Optional, Awaitable, Union

#from tornado import escape, gen
from tornado.httpserver import HTTPServer
from tornado.web import RequestHandler, Application, Finish, HTTPError
from tornado import escape
from tornado.httputil import responses

from .util import encode_version, decode_version
from .marshal import marshal_to_str, unmarshal_from_str


PROTOCOL_VERSION = (0, 1)
PROTOCOL_VERSION_STR = encode_version(PROTOCOL_VERSION)

SERVER_AGENT = f"PyRPC/{PROTOCOL_VERSION_STR}"


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


class RPCServer(Application):
	def __init__(self, root):
		self.root = root
		self.refs = {}

		super().__init__([
			#(r"/id/.*", RefHandler, dict(root=root))
			(r".*", RPCHandler)
		])
		#self.server = HTTPServer(self.app, **kwargs)

	#def listen(self, port: int, address: str = ""):
	#	self.server.listen(port, address)

	def make_ref(self, obj):
		obj_id = f"{id(obj):x}"
		self.refs[obj_id] = weakref.ref(obj)
		return f"/id/{obj_id}/"


def encode_value(obj) -> bytes:
	output = BytesIO()
	json.dump(obj, output, ensure_ascii=False)


class RPCHandler(RequestHandler):
	"""
	Class which handles an RPC request.
	"""

	def initialize(self):
		pass

	def write_obj(self, obj):
		s = marshal_to_str(obj, self.application.make_ref)
		self.add_header('Content-Type', 'application/json; charset=UTF-8')
		self.write(s.encode('utf-8'))

	def get(self):
		logging.debug(f"GET {self.request.path}")
		self.write_obj(self.get_endpoint(self.request.path))
		return self.finish()

	def post(self):
		logging.debug(f"POST {self.request.path}\nbody:\n{self.request.body}")

		content_type = self.request.headers.get('Content-Type', 'application/json')
		content_type = content_type.split(';')[0].strip()
		if content_type not in ['application/json', 'application/x-json']:
			raise RPCError(415, "Expected application/json")

		try:
			body = unmarshal_from_str(self.request.body.decode('utf-8'))
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

		self.write_obj(self.call_endpoint(self.request.path, *args, **kwargs))
		return self.finish()

	def get_endpoint(self, endpoint: str):
		components = endpoint.strip('/').split('/')

		if components == ['']:
			components = []

		if len(components) > 0 and components[0] == 'id':
			if len(components) == 1:
				raise not_found_error("/id")
			logging.debug(f"Looking up reference '{components[1]}'")
			logging.debug(f"refs: {self.application.refs}")
			id = components[1]
			if id.lower() not in self.application.refs:
				raise not_found_error(f"/id/{id}")
			obj = self.application.refs[id.lower()]()
			if obj is None:
				raise not_found_error(f"/id/{id}")  # object has been deleted
			components = components[2:]  # todo this breaks error messages
		else:
			obj = self.application.root

		for (i, component) in enumerate(components):
			if component == '__sig__':  # return function signature instead
				try:
					obj = inspect.signature(obj)
				except TypeError:
					raise not_found_error('/'.join(components[:i+1]))
			try:
				obj = getattr(obj, component)
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
			return obj(*args, **kwargs)
		except Exception:
			raise call_error(endpoint, sys.exc_info())

	def set_default_headers(self):
		self.set_header('Server', SERVER_AGENT)

	def write_error(self, status_code: int, **kwargs):

		if 'exc_info' in kwargs:
			exception = kwargs['exc_info'][1]
		else:
			exception = HTTPError(status_code, reason=kwargs.get('reason'))

		self.set_header("Content-Type", "text/plain; charset=UTF-8")
		self.write(str(exception))

		self.finish()


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
