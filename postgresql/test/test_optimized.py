##
# test.test_optimized
##
import unittest
import struct
import sys
from ..port import optimized

class test_optimized(unittest.TestCase):
	def test_parse_tuple_message(self):
		ptm = optimized.parse_tuple_message
		self.failUnlessRaises(TypeError, ptm, tuple, "stringzor")
		self.failUnlessRaises(TypeError, ptm, tuple, 123)
		self.failUnlessRaises(ValueError, ptm, tuple, b'')
		self.failUnlessRaises(ValueError, ptm, tuple, b'0')

		notenoughdata = struct.pack('!H', 2)
		self.failUnlessRaises(ValueError, ptm, tuple, notenoughdata)

		wraparound = struct.pack('!HL', 2, 10) + (b'0' * 10) + struct.pack('!L', 0xFFFFFFFE)
		self.failUnlessRaises(ValueError, ptm, tuple, wraparound)

		oneatt_notenough = struct.pack('!HL', 2, 10) + (b'0' * 10) + struct.pack('!L', 15)
		self.failUnlessRaises(ValueError, ptm, tuple, oneatt_notenough)

		toomuchdata = struct.pack('!HL', 1, 3) + (b'0' * 10)
		self.failUnlessRaises(ValueError, ptm, tuple, toomuchdata)

		class faketup(tuple):
			def __new__(subtype, geeze):
				r = tuple.__new__(subtype, ())
				r.foo = geeze
				return r
		zerodata = struct.pack('!H', 0)
		r = ptm(tuple, zerodata)
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

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
