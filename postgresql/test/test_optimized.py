##
# test.test_optimized
##
import unittest
import struct
import sys
from ..port import optimized
from ..python.itertools import interlace

def pack_tuple(*data,
	packH = struct.Struct("!H").pack,
	packL = struct.Struct("!L").pack
):
	return packH(len(data)) + b''.join((
		packL(len(x)) + x if x is not None else b'\xff\xff\xff\xff'
		for x in data
	))

tuplemessages = (
	(b'D', pack_tuple(b'foo', b'bar')),
	(b'D', pack_tuple(b'foo', None, b'bar')),
	(b'N', b'fee'),
	(b'D', pack_tuple(b'foo', None, b'bar')),
	(b'D', pack_tuple(b'foo', b'bar')),
)

class test_optimized(unittest.TestCase):
	def test_consume_tuple_messages(self):
		ctm = optimized.consume_tuple_messages
		# expecting a tuple of pairs.
		self.failUnlessRaises(TypeError, ctm, [])
		self.failUnlessEqual(ctm(()), [])
		# Make sure that the slicing is working.
		self.failUnlessEqual(ctm(tuplemessages), [
			(b'foo', b'bar'),
			(b'foo', None, b'bar'),
		])
		# Not really checking consume here, but we are validating that
		# it's properly propagating exceptions.
		self.failUnlessRaises(ValueError, ctm, ((b'D', b'\xff\xff\xff\xfefoo'),))
		self.failUnlessRaises(ValueError, ctm, ((b'D', b'\x00\x00\x00\x04foo'),))

	def test_parse_tuple_message(self):
		ptm = optimized.parse_tuple_message
		self.failUnlessRaises(TypeError, ptm, "stringzor")
		self.failUnlessRaises(TypeError, ptm, 123)
		self.failUnlessRaises(ValueError, ptm, b'')
		self.failUnlessRaises(ValueError, ptm, b'0')

		notenoughdata = struct.pack('!H', 2)
		self.failUnlessRaises(ValueError, ptm, notenoughdata)

		wraparound = struct.pack('!HL', 2, 10) + (b'0' * 10) + struct.pack('!L', 0xFFFFFFFE)
		self.failUnlessRaises(ValueError, ptm, wraparound)

		oneatt_notenough = struct.pack('!HL', 2, 10) + (b'0' * 10) + struct.pack('!L', 15)
		self.failUnlessRaises(ValueError, ptm, oneatt_notenough)

		toomuchdata = struct.pack('!HL', 1, 3) + (b'0' * 10)
		self.failUnlessRaises(ValueError, ptm, toomuchdata)

		class faketup(tuple):
			def __new__(subtype, geeze):
				r = tuple.__new__(subtype, ())
				r.foo = geeze
				return r
		zerodata = struct.pack('!H', 0)
		r = ptm(zerodata)
		self.failUnlessRaises(AttributeError, getattr, r, 'foo')
		self.failUnlessRaises(AttributeError, setattr, r, 'foo', 'bar')
		self.failUnlessEqual(len(r), 0)

	def test_process_tuple(self):
		def funpass(procs, tup, col):
			pass
		pt = optimized.process_tuple
		# tuple() requirements
		self.failUnlessRaises(TypeError, pt, "foo", "bar", funpass)
		self.failUnlessRaises(TypeError, pt, (), "bar", funpass)
		self.failUnlessRaises(TypeError, pt, "foo", (), funpass)
		self.failUnlessRaises(TypeError, pt, (), ("foo",), funpass)

	def test_pack_tuple_data(self):
		pit = optimized.pack_tuple_data
		self.failUnlessEqual(pit((None,)), b'\xff\xff\xff\xff')
		self.failUnlessEqual(pit((None,)*2), b'\xff\xff\xff\xff'*2)
		self.failUnlessEqual(pit((None,)*3), b'\xff\xff\xff\xff'*3)
		self.failUnlessEqual(pit((None,b'foo')), b'\xff\xff\xff\xff\x00\x00\x00\x03foo')
		self.failUnlessEqual(pit((None,b'')), b'\xff\xff\xff\xff\x00\x00\x00\x00')
		self.failUnlessEqual(pit((None,b'',b'bar')), b'\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x03bar')
		self.failUnlessRaises(TypeError, pit, 1)
		self.failUnlessRaises(TypeError, pit, (1,))
		self.failUnlessRaises(TypeError, pit, ("",))

	def test_int2(self):
		d = b'\x00\x01'
		rd = b'\x01\x00'
		s = optimized.swap_int2_unpack(d)
		n = optimized.int2_unpack(d)
		sd = optimized.swap_int2_pack(1)
		nd = optimized.int2_pack(1)
		if sys.byteorder == 'little':
			self.failUnlessEqual(1, s)
			self.failUnlessEqual(256, n)
			self.failUnlessEqual(d, sd)
			self.failUnlessEqual(rd, nd)
		else:
			self.failUnlessEqual(1, n)
			self.failUnlessEqual(256, s)
			self.failUnlessEqual(d, nd)
			self.failUnlessEqual(rd, sd)
		self.failUnlessRaises(OverflowError, optimized.swap_int2_pack, 2**15)
		self.failUnlessRaises(OverflowError, optimized.int2_pack, 2**15)
		self.failUnlessRaises(OverflowError, optimized.swap_int2_pack, (-2**15)-1)
		self.failUnlessRaises(OverflowError, optimized.int2_pack, (-2**15)-1)

	def test_int4(self):
		d = b'\x00\x00\x00\x01'
		rd = b'\x01\x00\x00\x00'
		s = optimized.swap_int4_unpack(d)
		n = optimized.int4_unpack(d)
		sd = optimized.swap_int4_pack(1)
		nd = optimized.int4_pack(1)
		if sys.byteorder == 'little':
			self.failUnlessEqual(1, s)
			self.failUnlessEqual(16777216, n)
			self.failUnlessEqual(d, sd)
			self.failUnlessEqual(rd, nd)
		else:
			self.failUnlessEqual(1, n)
			self.failUnlessEqual(16777216, s)
			self.failUnlessEqual(d, nd)
			self.failUnlessEqual(rd, sd)
		self.failUnlessRaises(OverflowError, optimized.swap_int4_pack, 2**31)
		self.failUnlessRaises(OverflowError, optimized.int4_pack, 2**31)
		self.failUnlessRaises(OverflowError, optimized.swap_int4_pack, (-2**31)-1)
		self.failUnlessRaises(OverflowError, optimized.int4_pack, (-2**31)-1)

	def test_int8(self):
		d = b'\x00\x00\x00\x00\x00\x00\x00\x01'
		rd = b'\x01\x00\x00\x00\x00\x00\x00\x00'
		s = optimized.swap_int8_unpack(d)
		n = optimized.int8_unpack(d)
		sd = optimized.swap_int8_pack(1)
		nd = optimized.int8_pack(1)
		if sys.byteorder == 'little':
			self.failUnlessEqual(0x1, s)
			self.failUnlessEqual(0x100000000000000, n)
			self.failUnlessEqual(d, sd)
			self.failUnlessEqual(rd, nd)
		else:
			self.failUnlessEqual(0x1, n)
			self.failUnlessEqual(0x100000000000000, s)
			self.failUnlessEqual(d, nd)
			self.failUnlessEqual(rd, sd)
		self.failUnlessEqual(optimized.swap_int8_pack(-1), b'\xFF\xFF\xFF\xFF'*2)
		self.failUnlessEqual(optimized.int8_pack(-1), b'\xFF\xFF\xFF\xFF'*2)
		self.failUnlessRaises(OverflowError, optimized.swap_int8_pack, 2**63)
		self.failUnlessRaises(OverflowError, optimized.int8_pack, 2**63)
		self.failUnlessRaises(OverflowError, optimized.swap_int8_pack, (-2**63)-1)
		self.failUnlessRaises(OverflowError, optimized.int8_pack, (-2**63)-1)
		# edge I/O
		int8_max = ((2**63) - 1)
		int8_min = (-(2**63))
		swap_max = optimized.swap_int8_pack(int8_max)
		max = optimized.int8_pack(int8_max)
		swap_min = optimized.swap_int8_pack(int8_min)
		min = optimized.int8_pack(int8_min)
		self.failUnlessEqual(optimized.swap_int8_unpack(swap_max), int8_max)
		self.failUnlessEqual(optimized.int8_unpack(max), int8_max)
		self.failUnlessEqual(optimized.swap_int8_unpack(swap_min), int8_min)
		self.failUnlessEqual(optimized.int8_unpack(min), int8_min)

	def test_uint2(self):
		d = b'\x00\x01'
		rd = b'\x01\x00'
		s = optimized.swap_uint2_unpack(d)
		n = optimized.uint2_unpack(d)
		sd = optimized.swap_uint2_pack(1)
		nd = optimized.uint2_pack(1)
		if sys.byteorder == 'little':
			self.failUnlessEqual(1, s)
			self.failUnlessEqual(256, n)
			self.failUnlessEqual(d, sd)
			self.failUnlessEqual(rd, nd)
		else:
			self.failUnlessEqual(1, n)
			self.failUnlessEqual(256, s)
			self.failUnlessEqual(d, nd)
			self.failUnlessEqual(rd, sd)
		self.failUnlessRaises(OverflowError, optimized.swap_uint2_pack, -1)
		self.failUnlessRaises(OverflowError, optimized.uint2_pack, -1)
		self.failUnlessRaises(OverflowError, optimized.swap_uint2_pack, 2**16)
		self.failUnlessRaises(OverflowError, optimized.uint2_pack, 2**16)
		self.failUnlessEqual(optimized.uint2_pack(2**16-1), b'\xFF\xFF')
		self.failUnlessEqual(optimized.swap_uint2_pack(2**16-1), b'\xFF\xFF')

	def test_uint4(self):
		d = b'\x00\x00\x00\x01'
		rd = b'\x01\x00\x00\x00'
		s = optimized.swap_uint4_unpack(d)
		n = optimized.uint4_unpack(d)
		sd = optimized.swap_uint4_pack(1)
		nd = optimized.uint4_pack(1)
		if sys.byteorder == 'little':
			self.failUnlessEqual(1, s)
			self.failUnlessEqual(16777216, n)
			self.failUnlessEqual(d, sd)
			self.failUnlessEqual(rd, nd)
		else:
			self.failUnlessEqual(1, n)
			self.failUnlessEqual(16777216, s)
			self.failUnlessEqual(d, nd)
			self.failUnlessEqual(rd, sd)
		self.failUnlessRaises(OverflowError, optimized.swap_uint4_pack, -1)
		self.failUnlessRaises(OverflowError, optimized.uint4_pack, -1)
		self.failUnlessRaises(OverflowError, optimized.swap_uint4_pack, 2**32)
		self.failUnlessRaises(OverflowError, optimized.uint4_pack, 2**32)
		self.failUnlessEqual(optimized.uint4_pack(2**32-1), b'\xFF\xFF\xFF\xFF')
		self.failUnlessEqual(optimized.swap_uint4_pack(2**32-1), b'\xFF\xFF\xFF\xFF')

	def test_uint8(self):
		d = b'\x00\x00\x00\x00\x00\x00\x00\x01'
		rd = b'\x01\x00\x00\x00\x00\x00\x00\x00'
		s = optimized.swap_uint8_unpack(d)
		n = optimized.uint8_unpack(d)
		sd = optimized.swap_uint8_pack(1)
		nd = optimized.uint8_pack(1)
		if sys.byteorder == 'little':
			self.failUnlessEqual(0x1, s)
			self.failUnlessEqual(0x100000000000000, n)
			self.failUnlessEqual(d, sd)
			self.failUnlessEqual(rd, nd)
		else:
			self.failUnlessEqual(0x1, n)
			self.failUnlessEqual(0x100000000000000, s)
			self.failUnlessEqual(d, nd)
			self.failUnlessEqual(rd, sd)
		self.failUnlessRaises(OverflowError, optimized.swap_uint8_pack, -1)
		self.failUnlessRaises(OverflowError, optimized.uint8_pack, -1)
		self.failUnlessRaises(OverflowError, optimized.swap_uint8_pack, 2**64)
		self.failUnlessRaises(OverflowError, optimized.uint8_pack, 2**64)
		self.failUnlessEqual(optimized.uint8_pack((2**64)-1), b'\xFF\xFF\xFF\xFF'*2)
		self.failUnlessEqual(optimized.swap_uint8_pack((2**64)-1), b'\xFF\xFF\xFF\xFF'*2)

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
