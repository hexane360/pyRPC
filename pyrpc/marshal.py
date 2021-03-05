
from io import BytesIO, StringIO
import json
from typing import Sequence, Mapping, Iterator, Dict, Union, Tuple, Any
import numpy as np
import base64

JsonType = Union[Sequence, Mapping, bytes, str, int, float]

_missing = object()


VERSION = (0, 1)


def encode_version(v: Tuple) -> str:
	"""Encode a version tuple as a string."""
	return ".".join(map(str, v))


def decode_version(v: str) -> Tuple:
	"""Decode a version string as a tuple of ints."""
	return tuple(map(int, v.split('.')))


def map_values(fn, d: Mapping) -> Iterator:
	"""Map `fn` over the values of `d`"""
	return ((k, fn(v)) for (k, v) in d.items())


def wrapped(ty: str, data: Any, **kwargs) -> Dict:
	"""
	Return `data` wrapped with a type hint.
	`**kwargs` are passed through to the output dict.
	"""
	return {
		"type": ty,
		"data": data,
		**kwargs
	}


def marshal_ndarray(arr: np.ndarray) -> Dict:
	"""
	Marshal a numpy ndarray.

	This is done by encoding the ndarray in the npy binary
	format, and then base64 encoding the result.
	"""
	io = BytesIO()
	# use .npy v3.0
	np.lib.format.write_array(io, arr, (3, 0))

	# and then store the binary data as b64
	data = base64.b64encode(io.getvalue()).decode('ascii')

	return wrapped('ndarray', data, shape=arr.shape, size=arr.size)


def unmarshal_ndarray(obj: Dict) -> np.ndarray:
	"""
	Unmarshal a numpy ndarray.
	"""
	assert obj['type'] == 'ndarray'

	binary = base64.b64decode(obj['data'].encode('ascii'))
	io = BytesIO(binary)
	return np.lib.format.read_array(io)


def marshal_obj(obj: Any) -> JsonType:
	"""
	Marshal an object. This function usually shouldn't
	be called directly (use `marshal()` instead).

	The following scalar types are supported:
	    - int
	    - float (and types with a `__float__()` method)
	    - complex
	    - str
	    - bytes

	The following composite types are supported:
	    - list (and types with a `__iter__()` method)
	    - tuple
	    - dict (and types with a `.to_dict()` method. Keys must be scalar values)

	Subtypes of the above are supported, but they are usually unmarshalled as their parent type.
	This function is recursive, so collections are marshalled by first marshalling their members.
	"""

	# scalar types
	if isinstance(obj, (float, int, str)):
		# these types are natively supported by JSON
		return obj
	if isinstance(obj, complex):
		# marshal complex as (real, imag)
		return wrapped('complex', (obj.real, obj.imag))

	if isinstance(obj, np.ndarray):
		# numpy ndarrays are handled specially
		return marshal_ndarray(obj)

	if hasattr(obj, '__float__'):
		return float(obj)

	if isinstance(obj, bytes):
		# marshal bytes as base64 encoded
		return wrapped('bytes', base64.b64encode(obj).decode('ascii'))

	# composite types

	# Also supports types with a `to_dict` method
	if isinstance(obj, Mapping) or hasattr(obj, 'to_dict'):
		if hasattr(obj, 'to_dict'):
			obj = obj.to_dict()
		# dicts are wrapped so they're not mistaken for other values
		inner = dict(map_values(marshal_obj, obj))
		return wrapped('dict', inner)

	# __iter__ is marshalled as list as well
	if isinstance(obj, Sequence) or hasattr(obj, '__iter__'):
		# marshal inside values
		inner = list(map(marshal_obj, obj))
		if isinstance(obj, tuple):
			# tuples should be unmarshaled as tuples
			return wrapped('tuple', inner)
		return inner

	raise TypeError(f"Unsupported type {type(obj)}")


UNMARSHAL_MAP = {
	# just unmarshal inner dictionary
	'dict': lambda obj: dict(map_values(unmarshal_obj, obj['data'])),
	# unmarshal a list as a tuple
	'tuple': lambda obj: tuple(map(unmarshal_obj, obj['data'])),
	'ndarray': unmarshal_ndarray,
	# unmarshal complex from [real, imag]
	'complex': lambda obj: complex(*obj['data']),
	# bytes are base64 encoded
	'bytes': lambda obj: base64.b64decode(obj['data'].encode('ascii'))
}
"""
Map which takes a type annotation and dispatches to a function which
unmarshals objects of that type.
"""


def unmarshal_obj(obj: JsonType) -> Any:
	"""
	Unmarshal an object. This function usually shouldn't
	be called directly.
	"""
	if isinstance(obj, (int, float, str)):
		return obj
	if isinstance(obj, Sequence):
		return list(map(lambda v: unmarshal_obj(v), obj))

	if not isinstance(obj, Mapping):
		raise TypeError(f"Unknown json type {type(obj)}")

	ty = obj['type']
	if ty not in UNMARSHAL_MAP:
		raise ValueError("Unknown type annotation {ty}")

	# dispatch based on object type
	return UNMARSHAL_MAP[ty](obj)


def marshal(obj: Any) -> JsonType:
	"""
	Marshal an object, including version information.

	The following scalar types are supported:
	    - int
	    - float
	    - complex
	    - str
	    - bytes

	The following collection types are supported:
	    - list (and types with a `__iter__()` method)
	    - tuple
	    - dict (and types with a `.to_dict()` method. Keys must be scalar values)

	Subtypes of the above are supported, but they are usually unmarshalled as their parent type.
	This function is recursive, so collections are marshalled by first marshalling their members.
	"""
	return {
		'v': encode_version(VERSION),
		'data': marshal_obj(obj),
	}


def unmarshal(obj: Mapping) -> Any:
	"""
	Unmarshal an object, including version information. To unmarshal
	form a JSON string, use `unmarshal_from_string` instead.
	"""

	if not isinstance(obj, Mapping):
		raise TypeError(f"Expected a dict, got '{type(obj)}' instead.")
	try:
		version = decode_version(obj['v'])
		data = obj['data']
	except (KeyError, ValueError):
		raise ValueError("Could not decode protocol version info.")
	if not version == VERSION:
		raise ValueError(f"Unsupported protocol version '{obj['v']}'.")
	return unmarshal_obj(data)


def marshal_io(obj: Any, io):
	"""Marshal an object as JSON, and write it to a file object `io`."""
	json.dump(marshal(obj), io, ensure_ascii=False)


def marshal_to_str(obj: Any) -> str:
	"""Marshal an object as JSON, returning the serialized text."""
	output = StringIO()
	marshal_io(obj, output)
	return output.getvalue()


def unmarshal_io(io) -> Any:
	"""Unmarshal an object from an IO object `io`"""
	return unmarshal(json.load(io))


def unmarshal_from_str(s: str) -> Any:
	"""Unmarshal a JSON-encoded object from `bytes`"""
	return unmarshal(json.loads(s))
