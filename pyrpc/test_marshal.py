import math
import re

import pytest
import numpy as np

from .marshal import marshal_to_str, unmarshal_from_str
from .marshal import marshal_obj
from .marshal import marshal, unmarshal
from .marshal import MARSHAL_VERSION_STR


TEST_ROUNDTRIP = {
	"int": 5,
	"float": 1./32.,
	"infinity": math.inf,
	"complex": complex(1., -1.),
	"str": "Test ⊗ String\0",
	"bytes": b"\0\5\10text\rs",
	"none": None,

	"collections": [1, 2, {'a': 5, 'b': [1, math.inf, 3]}],
	"set": set([1, 5, 8]),
}


@pytest.mark.parametrize("name,obj", TEST_ROUNDTRIP.items())
def test_roundtrip(name, obj):
	assert obj == unmarshal_from_str(marshal_to_str(obj))


def test_ndarray():
	arr = np.array(range(18), dtype=np.uint8)
	arr = arr.reshape((3, 3, 2))

	roundtrip = unmarshal_from_str(marshal_to_str(arr))

	assert np.array_equal(arr, roundtrip)
	assert arr.dtype == roundtrip.dtype
	assert arr.shape == roundtrip.shape

	assert marshal_obj(arr) == {
		'type': 'ndarray',
		'shape': (3, 3, 2),
		'size': 18,
		'data': "k05VTVBZAwB0AAAAeydkZXNjcic6ICd8dTEnLCAnZm9ydHJhb"
		        "l9vcmRlcic6IEZhbHNlLCAnc2hhcGUnOiAoMywgMywgMiksIH"
		        "0gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICA"
		        "gICAgICAgICAgICAgICAgIAoAAQIDBAUGBwgJCgsMDQ4PEBE="
	}


TEST_MARSHAL = {
	"dict": (
		{'a': 5, 'b': 10},
		{'type': 'dict', 'data': {'a': 5, 'b': 10}}
	),
	"nested_dict": (
		{'a': {'a': 5, 'b': 10}, 'd': 10},
		{'type': 'dict',
		 'data': {'a': {'type': 'dict',
		                'data': {'a': 5, 'b': 10}},
		          'd': 10}}
	),
}


@pytest.mark.parametrize("name,obj,expected", ((k, *v) for (k, v) in TEST_MARSHAL.items()))
def test_marshal(name, obj, expected):
	assert marshal_obj(obj) == expected


def test_version_marshal():
	assert marshal(5) == {
		'v': MARSHAL_VERSION_STR,
		'data': 5,
	}


def test_marshal_ref():
	class TestType():
		pass

	def make_ref(obj):
		return '/ref_url'

	assert marshal_obj(TestType(), make_ref) == {
		'type': 'ref', 'url': '/ref_url', 'class': 'TestType'
	}


def test_version_unmarshal():

	with pytest.raises(TypeError, match=re.escape("Expected a dict, got '<class 'int'>' instead.")):
		unmarshal(5)

	with pytest.raises(ValueError, match=re.escape("Could not decode protocol version info.")):
		unmarshal({
			'data': 5
		})

	with pytest.raises(ValueError, match=re.escape("Unsupported protocol version '0.0'")):
		unmarshal({
			'v': '0.0',
			'data': 5
		})

	assert unmarshal({'v': MARSHAL_VERSION_STR, 'data': 5}) == 5
