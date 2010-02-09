##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import struct
from .functools import Composition as compose

null_sequence = b'\xff\xff\xff\xff'

# Always to and from network order.
# Create a pair, (pack, unpack) for the given `struct` format.'
def mk_pack(x):
	s = struct.Struct('!' + x)
	if len(x) > 1:
		return (lambda y: s.pack(*y), s.unpack_from)
	else:
		return (s.pack, lambda y: s.unpack_from(y)[0])

byte_pack, byte_unpack = lambda x: bytes((x,)), lambda x: x[0]
double_pack, double_unpack = mk_pack("d")
float_pack, float_unpack = mk_pack("f")
dd_pack, dd_unpack = mk_pack("dd")
ddd_pack, ddd_unpack = mk_pack("ddd")
dddd_pack, dddd_unpack = mk_pack("dddd")
LH_pack, LH_unpack = mk_pack("LH")
lH_pack, lH_unpack = mk_pack("lH")
llL_pack, llL_unpack = mk_pack("llL")
qll_pack, qll_unpack = mk_pack("qll")
dll_pack, dll_unpack = mk_pack("dll")

dl_pack, dl_unpack = mk_pack("dl")
ql_pack, ql_unpack = mk_pack("ql")

hhhh_pack, hhhh_unpack = mk_pack("hhhh")

longlong_pack, longlong_unpack = mk_pack("q")
ulonglong_pack, ulonglong_unpack = mk_pack("Q")

# Optimizations for int2, int4, and int8.
try:
	from ..port import optimized as opt
	from sys import byteorder as bo
	if bo == 'little':
		short_unpack = opt.swap_int2_unpack
		short_pack = opt.swap_int2_pack
		ushort_unpack = opt.swap_uint2_unpack
		ushort_pack = opt.swap_uint2_pack
		long_unpack = opt.swap_int4_unpack
		long_pack = opt.swap_int4_pack
		ulong_unpack = opt.swap_uint4_unpack
		ulong_pack = opt.swap_uint4_pack

		if hasattr(opt, 'uint8_pack'):
			longlong_unpack = opt.swap_int8_unpack
			longlong_pack = opt.swap_int8_pack
			ulonglong_unpack = opt.swap_uint8_unpack
			ulonglong_pack = opt.swap_uint8_pack
	elif bo == 'big':
		short_unpack = opt.int2_unpack
		short_pack = opt.int2_pack
		ushort_unpack = opt.uint2_unpack
		ushort_pack = opt.uint2_pack
		long_unpack = opt.int4_unpack
		long_pack = opt.int4_pack
		ulong_unpack = opt.uint4_unpack
		ulong_pack = opt.uint4_pack

		if hasattr(opt, 'uint8_pack'):
			longlong_unpack = opt.int8_unpack
			longlong_pack = opt.int8_pack
			ulonglong_unpack = opt.uint8_unpack
			ulonglong_pack = opt.uint8_pack
	del bo, opt
except ImportError:
	short_pack, short_unpack = mk_pack("h")
	ushort_pack, ushort_unpack = mk_pack("H")
	long_pack, long_unpack = mk_pack("l")
	ulong_pack, ulong_unpack = mk_pack("L")
