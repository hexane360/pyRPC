
from .util import encode_version, decode_version, map_values


def test_encode_version():
	assert encode_version((1, 2, 3)) == "1.2.3"
	assert encode_version((1,)) == "1"
	assert encode_version(()) == ""


def test_decode_version():
	assert decode_version("1.2.3") == (1, 2, 3)
	assert decode_version("1.") == (1,)
	assert decode_version(" 5 . 8 ") == (5, 8)


def test_map_values():
	assert dict(map_values(lambda v: v+1, {'k1': 1, 'k2': 2})) \
		== {'k1': 2, 'k2': 3}
