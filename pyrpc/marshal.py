
from io import BytesIO, StringIO
import json
from typing import Sequence, Mapping, Set, Union
from typing import Dict, Any
import numpy as np
import base64
from functools import partial

from .util import encode_version, decode_version, map_values

JsonType = Union[Sequence, Mapping, bytes, str, int, float]

_missing = object()


MARSHAL_VERSION = (0, 1)
MARSHAL_VERSION_STR = encode_version(MARSHAL_VERSION)


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


def unmarshal_ndarray(obj: Dict, node_factory=None) -> np.ndarray:
	"""
	Unmarshal a numpy ndarray.
	"""
	assert obj['type'] == 'ndarray'

	binary = base64.b64decode(obj['data'].encode('ascii'))
	io = BytesIO(binary)
	return np.lib.format.read_array(io)


def marshal_obj(obj: Any, ref_factory=None) -> JsonType:
	"""
	Marshal an object. This function usually shouldn't
	be called directly (use `marshal()` instead).

	The following scalar types are supported:
	    - int
	    - float (and types with a `__float__()` method)
	    - complex
	    - str
	    - bytes
	    - NoneType

	The following composite types are supported:
	    - list (and types with a `__iter__()` method)
	    - tuple
	    - set (and types with a `.to_set()` method)
	    - dict (and types with a `.to_dict()` method. Keys must be scalar values)

	Subtypes of the above are supported, but they are usually unmarshalled as their parent type.
	This function is recursive, so collections are marshalled by first marshalling their members.
	"""

	# scalar types
	if isinstance(obj, (float, int, str, type(None))):
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

	recur = partial(marshal_obj, ref_factory=ref_factory)

	# Also supports types with a `to_dict` method
	if isinstance(obj, Mapping) or hasattr(obj, 'to_dict'):
		if hasattr(obj, 'to_dict'):
			obj = obj.to_dict()
		# dicts are wrapped so they're not mistaken for other values
		inner = dict(map_values(recur, obj))
		return wrapped('dict', inner)

	if isinstance(obj, Set) or hasattr(obj, 'to_set'):
		if hasattr(obj, 'to_set'):
			obj = obj.to_set()
		inner = list(map(recur, obj))
		return wrapped('set', inner)

	# __iter__ is marshalled as list as well
	if isinstance(obj, Sequence) or hasattr(obj, '__iter__'):
		# marshal inside values

		inner = list(map(recur, obj))
		if isinstance(obj, tuple):
			# tuples should be unmarshaled as tuples
			return wrapped('tuple', inner)
		return inner

	# marshal reference to object
	if ref_factory is None:
		raise TypeError(f"Unsupported type {type(obj)}")

	return {
		'type': 'ref',
		'url': ref_factory(obj),
		'class': type(obj).__name__,
	}


def unmarshal_ref(obj, node_factory=None):
	if node_factory is None:
		raise TypeError(f"Unsupported type {obj['class']}")
	return node_factory(url=obj['url'], cls=obj['class'])


def partial_unmarshal(node_factory=None):
	return partial(unmarshal_obj, node_factory=node_factory)


UNMARSHAL_MAP = {
	# just unmarshal inner dictionary
	'dict': lambda obj, node_f: dict(map_values(partial_unmarshal(node_f), obj['data'])),
	# re-interpret a list as a tuple
	'tuple': lambda obj, node_f: tuple(map(partial_unmarshal(node_f), obj['data'])),
	# re-interpret a list as a set
	'set': lambda obj, node_f: set(map(partial_unmarshal(node_f), obj['data'])),
	# ndarray unmarshal is done specially
	'ndarray': unmarshal_ndarray,
	# unmarshal complex from [real, imag]
	'complex': lambda obj, _: complex(*obj['data']),
	# bytes are base64 encoded
	'bytes': lambda obj, _: base64.b64decode(obj['data'].encode('ascii')),
	'ref': unmarshal_ref,
}
"""
Map which takes a type annotation and dispatches to a function which
unmarshals objects of that type.
"""


def unmarshal_obj(obj: JsonType, node_factory=None) -> Any:
	"""
	Unmarshal an object. This function usually shouldn't
	be called directly.
	"""
	if isinstance(obj, (int, float, str, type(None))):
		return obj
	if isinstance(obj, Sequence):
		return list(map(partial_unmarshal(node_factory), obj))

	if not isinstance(obj, Mapping):
		raise TypeError(f"Unknown json type {type(obj)}")

	ty = obj['type']
	if ty not in UNMARSHAL_MAP:
		raise ValueError("Unknown type annotation {ty}")

	# dispatch based on object type
	return UNMARSHAL_MAP[ty](obj, node_factory)


def marshal(obj: Any, ref_factory=None) -> JsonType:
	"""
	Marshal an object, including version information.

	The following scalar types are supported:
	    - int
	    - float
	    - complex
	    - str
	    - bytes
	    - NoneType

	The following collection types are supported:
	    - list (and types with a `__iter__()` method)
	    - tuple
	    - set
	    - dict (and types with a `.to_dict()` method. Keys must be scalar values)

	Subtypes of the above are supported, but they are usually unmarshalled as their parent type.
	This function is recursive, so collections are marshalled by first marshalling their members.
	"""
	return {
		'v': MARSHAL_VERSION_STR,
		'data': marshal_obj(obj, ref_factory),
	}


def unmarshal(obj: Mapping, node_factory=None) -> Any:
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
	if not version == MARSHAL_VERSION:
		raise ValueError(f"Unsupported protocol version '{obj['v']}'.")
	return unmarshal_obj(data, node_factory)


def marshal_io(obj: Any, io, ref_factory=None):
	"""Marshal an object as JSON, and write it to a file object `io`."""
	json.dump(marshal(obj, ref_factory), io, ensure_ascii=False)


def marshal_to_str(obj: Any, ref_factory=None) -> str:
	"""Marshal an object as JSON, returning the serialized text."""
	output = StringIO()
	marshal_io(obj, output, ref_factory)
	return output.getvalue()


def unmarshal_io(io, node_factory=None) -> Any:
	"""Unmarshal an object from an IO object `io`"""
	return unmarshal(json.load(io), node_factory)


def unmarshal_from_str(s: str, node_factory=None) -> Any:
	"""Unmarshal a JSON-encoded object from `bytes`"""
	return unmarshal(json.loads(s), node_factory)
