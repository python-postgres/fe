from ...types import JSONBOID


def jsonb_pack(x, typeio):
	jsonb = typeio.encode(x)
	return b'\x01' + jsonb


def jsonb_unpack(x, typeio):
	if x[0] != 1:
		raise ValueError('unexpected JSONB format version: {!r}'.format(x[0]))
	return typeio.decode(x[1:])


def _jsonb_io_factory(oid, typeio):
	_pack = lambda x: jsonb_pack(x, typeio)
	_unpack = lambda x: jsonb_unpack(x, typeio)

	return (_pack, _unpack, str)


oid_to_io = {
	JSONBOID: _jsonb_io_factory
}
