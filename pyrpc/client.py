
import logging
from urllib.parse import urlparse, urljoin
from functools import partial
from typing import Optional, Union, Any

from tornado.httputil import HTTPHeaders, split_host_and_port
from tornado.httpclient import HTTPClient, HTTPRequest, HTTPResponse, HTTPError

from .marshal import marshal_to_str, unmarshal_from_str
from .server import PROTOCOL_VERSION_STR

USER_AGENT = f"PyRPC/{PROTOCOL_VERSION_STR}"


class RPCConnection:
	def __init__(self, netloc: str, port: Optional[int] = None):

		if "//" in netloc and not urlparse(netloc).scheme == '':
			# parse as url
			url = urlparse(netloc)
			self.scheme = url.scheme
			self.host = url.hostname
			self.port = url.port
			self.base_path = url.path or "/"
		else:
			self.scheme = "http"
			(netloc, _, path) = netloc.partition("/")
			self.base_path = f"/{path}"
			(self.host, self.port) = split_host_and_port(netloc)

		# order of preference: passed parameter, parsed from url, default
		self.port = port or self.port or 8888
		self.netloc = f"{self.host}:{self.port}"
		self.base_url = f"{self.scheme}://{self.netloc}{self.base_path}"

		self._client = HTTPClient()

	def headers(self) -> HTTPHeaders:
		return HTTPHeaders({
			'User-Agent': USER_AGENT,
		})

	def make_request(self, method: str = "GET", endpoint: str = "/",
	                 body: Union[str, bytes, None] = None) -> HTTPRequest:
		url = urljoin(self.base_url, endpoint)
		logging.debug(f"{method} {url}")

		headers = self.headers()
		if body is not None:
			headers.add('Content-Type', 'application/json')

		return HTTPRequest(
			url,
			method,
			headers,
			body,
		)

	def fetch(self, request: HTTPRequest) -> HTTPResponse:
		body = "" if request.body is None else f" body: {request.body}"
		logging.debug(f"{request.method} {request.url}{body}")
		return self._client.fetch(request, raise_error=False)

	def get(self, endpoint: str = "/") -> Any:
		resp = self.fetch(self.make_request("GET", endpoint))

		try:
			body = resp.body.decode('utf-8')
		except UnicodeDecodeError:
			# skip error for now
			body = None

		if resp.code == 200:
			if body is None:
				# raise error now if we couldn't decode body
				raise UnicodeDecodeError("Couldn't decode response body.")
			print(f"raw body: {body}")
			return unmarshal_from_str(body, partial(make_node, self))
		if resp.code == 404:
			raise AttributeError(body)
		if resp.code == 400:
			raise TypeError(body)

		raise resp.error or HTTPError(resp.code, response=resp)

	def post(self, endpoint: str = "/", *args, **kwargs) -> Any:
		body = marshal_to_str({
			'args': args,
			'kwargs': kwargs,
		}).encode('utf-8')
		resp = self.fetch(self.make_request("POST", endpoint, body))

		try:
			body = resp.body.decode('utf-8')
		except UnicodeDecodeError:
			# skip error for now
			body = None

		if resp.code == 200:
			if body is None:
				raise UnicodeDecodeError("Couldn't decode response body.")
			return unmarshal_from_str(body, partial(make_node, self))
		if resp.code == 404:
			raise AttributeError(body)
		if resp.code == 400:
			raise TypeError(body)

		raise resp.error or HTTPError(resp.code, response=resp)


class RPCNode:
	def __init__(self, connection: RPCConnection, endpoint: str = "/"):
		# avoid calling our modified __setattr__
		object.__setattr__(self, 'connection', connection)
		object.__setattr__(self, 'endpoint', endpoint)

	def __getattr__(self, name):
		return self.connection.get(urljoin(self.endpoint, name))
		#return RPCNode(self.connection, urljoin(self.endpoint, name))

	def __setattr__(self, name, value):
		self.connection.put(urljoin(self.endpoint, name), value)

	def call(self, *args, **kwargs):
		return self.connection.post(self.endpoint, *args, **kwargs)

	def __call__(self, *args, **kwargs):
		return self.call(*args, **kwargs)

	def get(self):
		return self.connection.get(self.endpoint)

	def doc(self):
		return self.connection.get(urljoin(self.endpoint, "__doc__"))

	def signature(self):
		return self.connection.get(urljoin(self.endpoint, "__sig__"))

	#def __str__(self):
	#	return self.connection.post(urljoin(self.endpoint, '__str__'))

	def __repr__(self):
		return self.connection.post(urljoin(self.endpoint, '__str__'))


def make_node(connection: RPCConnection, url: str,
              cls: Optional[str] = None) -> RPCNode:
	return RPCNode(connection, url)
