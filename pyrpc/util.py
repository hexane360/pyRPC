from typing import Tuple, Mapping, Iterator


def encode_version(v: Tuple) -> str:
	"""Encode a version tuple as a string."""
	return ".".join(map(str, v))


def decode_version(v: str) -> Tuple:
	"""Decode a version string as a tuple of ints."""
	return tuple(map(int, v.strip('.').split('.')))


def map_values(fn, d: Mapping) -> Iterator:
	"""Map `fn` over the values of `d`"""
	return ((k, fn(v)) for (k, v) in d.items())
