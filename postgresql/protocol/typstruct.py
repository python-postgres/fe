##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
PostgreSQL data type protocol--packing and unpacking functions.

This module provides functions that pack and unpack many standard PostgreSQL
types. It also associates those functions with their corresponding type Oids.

The name of the function describes what type the function is intended to be used
on. Normally, the fucntions return a structured form of the serialized data to
be used as a parameter to the creation of a higher level instance. In
particular, most of the functions that deal with time return a pair for
representing the relative offset: (seconds, microseconds). For times, this
provides an abstraction for quad-word based times used by some configurations of
PostgreSQL.
"""
import math
import struct
from operator import itemgetter
from .. import types as pg_types
from ..python.functools import Composition as compose

null_sequence = b'\xff\xff\xff\xff'

# Always to and from network order.
def mk_pack(x):
	'Create a pair, (pack, unpack) for the given `struct` format.'
	s = struct.Struct('!' + x)
	if len(x) > 1:
		def pack_apply(data):
			return s.pack(*data)
		return (pack_apply, s.unpack)
	else:
		return (s.pack, compose((s.unpack, itemgetter(0))))

def mktimetuple(ts):
	'make a pair of (seconds, microseconds) out of the given double'
	seconds = math.floor(ts)
	return (int(seconds), int(1000000 * (ts - seconds)))

def mktimetuple64(ts):
	'make a pair of (seconds, microseconds) out of the given long'
	seconds = ts // 1000000
	return (seconds, ts - (seconds * 1000000))

def mktime(seconds_ms):
	'make a double out of the pair of (seconds, microseconds)'
	return float(seconds_ms[0]) + (seconds_ms[1] / 1000000.0)

def mktime64(seconds_ms):
	'make an integer out of the pair of (seconds, microseconds)'
	return seconds_ms[0] * 1000000 + seconds_ms[1]

data_to_bool = {b'\x01' : True, b'\x00' : False}
bool_to_data = {True : b'\x01', False : b'\x00'}

bool_pack = bool_to_data.__getitem__
bool_unpack = data_to_bool.__getitem__

bitdata_to_bool = {b'\x00\x00\x00\x01\x01' : True, b'\x00\x00\x00\x01\x00' : False}
bool_to_bitdata = {True : b'\x00\x00\x00\x01\x01', False : b'\x00\x00\x00\x01\x00'}

bit_pack = bool_to_bitdata.__getitem__
bit_unpack = bitdata_to_bool.__getitem__


longlong_pack, longlong_unpack = mk_pack("q")
long_pack, long_unpack = mk_pack("l")
ulong_pack, ulong_unpack = mk_pack("L")
byte_pack, byte_unpack = lambda x: bytes((x,)), lambda x: x[0]
short_pack, short_unpack = mk_pack("h")
ushort_pack, ushort_unpack = mk_pack("H")
double_pack, double_unpack = mk_pack("d")
float_pack, float_unpack = mk_pack("f")
dd_pack, dd_unpack = mk_pack("dd")
ddd_pack, ddd_unpack = mk_pack("ddd")
dddd_pack, dddd_unpack = mk_pack("dddd")
LH_pack, LH_unpack = mk_pack("LH")
llL_pack, llL_unpack = mk_pack("llL")
qll_pack, qll_unpack = mk_pack("qll")
dll_pack, dll_unpack = mk_pack("dll")
BBBB_pack, BBBB_unpack = mk_pack("BBBB")

dl_pack, dl_unpack = mk_pack("dl")
ql_pack, ql_unpack = mk_pack("ql")

hhhh_pack, hhhh_unpack = mk_pack("hhhh")

# Optimizations for int2 and int4.
try:
	from sys import byteorder as bo
	if bo == 'little':
		from .optimized import swap_int2_unpack as short_unpack, swap_int2_pack as short_pack
		from .optimized import swap_int4_unpack as long_unpack, swap_int4_pack as long_pack
		from .optimized import swap_uint2_unpack as ushort_unpack, swap_uint2_pack as ushort_pack
		from .optimized import swap_uint4_unpack as ulong_unpack, swap_uint4_pack as ulong_pack
	elif bo == 'big':
		from .optimized import int2_unpack as short_unpack, int2_pack as short_pack
		from .optimized import int4_unpack as long_unpack, int4_pack as long_pack
		from .optimized import uint2_unpack as ushort_unpack, uint2_pack as ushort_pack
		from .optimized import uint4_unpack as ulong_unpack, uint4_pack as ulong_pack
	del bo
except ImportError:
	pass

int2_pack, int2_unpack = short_pack, short_unpack
int4_pack, int4_unpack = long_pack, long_unpack
int8_pack, int8_unpack = longlong_pack, longlong_unpack

oid_pack = cid_pack = xid_pack = ulong_pack
oid_unpack = cid_unpack = xid_unpack = ulong_unpack

tid_pack, tid_unpack = LH_pack, LH_unpack

# geometry types
point_pack, point_unpack = dd_pack, dd_unpack
circle_pack, circle_unpack = ddd_pack, ddd_unpack
lseg_pack = box_pack = dddd_pack
lseg_unpack = box_unpack = dddd_unpack

def numeric_pack(data):
	(header, numbers) = data
	return hhhh_pack(header) + struct.pack("!%dh"%(len(numbers),), *numbers)

def numeric_unpack(data):
	header = hhhh_unpack(data[:8])
	return (header, struct.unpack("!8x%dh"%((len(data)-8) // 2,), data))

def path_pack(data):
	"""
	Given a sequence of point data, pack it into a path's serialized form.

		[px1, py1, px2, py2, ...]

	Must be an even number of numbers.
	"""
	return struct.pack("!l%dd" %(len(data),), len(data), *data)

def path_unpack(data):
	"""
	Unpack a path's serialized form into a sequence of point data:

		[px1, py1, px2, py2, ...]

	Should be an even number of numbers.
	"""
	npoints = long_unpack(data[:4])
	points = struct.unpack("!4x%dd" %(npoints,), data)
	return points
polygon_pack, polygon_unpack = path_pack, path_unpack

# time types
date_pack, date_unpack = long_pack, long_unpack

# takes a pair, (seconds, microseconds)
time_pack = compose((mktime, double_pack))
time_unpack = compose((double_unpack, mktimetuple))

def interval_pack(m_d_timetup):
	"""
	Given a triple, (month, day, (seconds, microseconds)), serialize it for
	transport.
	"""
	(month, day, timetup) = m_d_timetup
	return dll_pack((mktime(timetup), day, month))

def interval_unpack(data):
	"""
	Given a serialized interval, '{month}{day}{time}', yield the triple:

		(month, day, (seconds, microseconds))
	"""
	tim, day, month = dll_unpack(data)
	return (month, day, mktimetuple(tim))

def interval_noday_pack(month_day_timetup):
	"""
	Given a triple, (month, day, (seconds, microseconds)), return the serialized
	form that does not have an individual day component.

	There is no day component, so if day is non-zero, it will be converted to
	seconds and subsequently added to the seconds.
	"""
	(month, day, timetup) = month_day_timetup
	if day:
		timetup = (timetup[0] + (day * 24 * 60 * 60), timetup[1])
	return dl_pack((mktime(timetup), month))

def interval_noday_unpack(data):
	"""
	Given a serialized interval without a day component, return the triple:

		(month, day(always zero), (seconds, microseconds))
	"""
	tim, month = dl_unpack(data)
	return (month, 0, mktimetuple(tim))

time64_pack = compose((mktime64, longlong_pack))
time64_unpack = compose((longlong_unpack, mktimetuple64))

def interval64_pack(m_d_timetup):
	"""
	Given a triple, (month, day, (seconds, microseconds)), return the serialized
	data using a quad-word for the (seconds, microseconds) tuple.
	"""
	(month, day, timetup) = m_d_timetup
	return qll_pack((mktime64(timetup), day, month))

def interval64_unpack(data):
	"""
	Unpack an interval containing a quad-word into a triple:

		(month, day, (seconds, microseconds))
	"""
	tim, day, month = qll_unpack(data)
	return (month, day, mktimetuple64(tim))

def interval64_noday_pack(m_d_timetup):
	"""
	Pack an interval without a day component and using a quad-word for second
	representation.

	There is no day component, so if day is non-zero, it will be converted to
	seconds and subsequently added to the seconds.
	"""
	(month, day, timetup) = m_d_timetup
	if day:
		timetup = (timetup[0] + (day * 24 * 60 * 60), timetup[1])
	return ql_pack((mktime64(timetup), month))

def interval64_noday_unpack(data):
	"""
	Unpack a ``noday`` quad-word based interval. Returns a triple:

		(month, day(always zero), (seconds, microseconds))
	"""
	tim, month = ql_unpack(data)
	return (month, 0, mktimetuple64(tim))

def timetz_pack(timetup_tz):
	"""
	Pack a time; offset from beginning of the day and timezone offset.

	Given a pair, ((seconds, microseconds), timezone_offset), pack it into its
	serialized form: "!dl".
	"""
	(timetup, tz_offset) = timetup_tz
	return dl_pack((mktime(timetup), tz_offset))

def timetz_unpack(data):
	"""
	Given serialized time data, unpack it into a pair:

	    ((seconds, microseconds), timezone_offset).
	"""
	ts, tz = dl_unpack(data)
	return (mktimetuple(ts), tz)

def timetz64_pack(timetup_tz):
	"""
	Pack a time; offset from beginning of the day and timezone offset.

	Given a pair, ((seconds, microseconds), timezone_offset), pack it into its
	serialized form using a long long: "!ql".
	"""
	(timetup, tz_offset) = timetup_tz
	return ql_pack((mktime64(timetup), tz_offset))

def timetz64_unpack(data):
	"""
	Given "long long" serialized time data, "ql", unpack it into a pair:
	
	    ((seconds, microseconds), timezone_offset)
	"""
	ts, tz = ql_unpack(data)
	return (mktimetuple64(ts), tz)

# oidvectors are 128 bytes, so pack the number of Oids in self
# and justify that to 128 by padding with \x00.
def oidvector_pack(seq):
	"""
	Given a sequence of Oids, pack them into the serialized form.

	An oidvector is a type used by the PostgreSQL catalog.
	"""
	return struct.pack("!%dL"%(len(seq),), *seq).ljust(128, '\x00')

def oidvector_unpack(data):
	"""
	Given a serialized oidvector(32 longs), unpack it into a list of unsigned integers.

	An int2vector is a type used by the PostgreSQL catalog.
	"""
	return struct.unpack("!32L", data)

def int2vector_pack(seq):
	"""
	Given a sequence of integers, pack them into the serialized form.

	An int2vector is a type used by the PostgreSQL catalog.
	"""
	return struct.pack("!%dh"%(len(seq),), *seq).ljust(64, '\x00')

def int2vector_unpack(data):
	"""
	Given a serialized int2vector, unpack it into a list of integers.

	An int2vector is a type used by the PostgreSQL catalog.
	"""
	return struct.unpack("!32h", data)

def varbit_pack(bits_data):
	r"""
	Given a pair, serialize the varbit.

	# (number of bits, data)
	>>> varbit_pack((1, '\x00'))
	b'\x00\x00\x00\x01\x00'
	"""
	return long_pack(bits_data[0]) + bits_data[1]

def varbit_unpack(data):
	"""
	Given ``varbit`` data, unpack it into a pair:

		(bits, data)
	
	Where 
	"""
	return long_unpack(data[0:4]), data[4:]

def net_pack(family_mask_data):
	"""
	Given a triple, yield the serialized form for transport.

	Prepends the ``family``, ``mask`` and implicit ``is_cidr`` fields.

	Supports cidr and inet types.
	"""
	(family, mask, data) = family_mask_data

	return b''.join((
		byte_pack(family),
		byte_pack(mask),
		b'\x01',
		byte_pack(len(data)),
		data
	))

def net_unpack(data):
	"""
	Given serialized cidr data, return a tuple:

		(family, mask, data)
	"""
	family, mask, is_cidr, size = BBBB_unpack(data[:4])

	rd = data[4:]
	if len(rd) != size:
		raise ValueError("invalid size parameter")

	return (family, mask, rd)

def record_unpack(data):
	"""
	Given serialized record data, return a tuple of tuples of type Oids and
	attributes.
	"""
	columns = long_unpack(data[0:4])
	offset = 4

	for x in range(columns):
		typid = oid_unpack(data[offset:offset+4])
		offset += 4

		if data[offset:offset+4] == null_sequence:
			att = None
			offset += 4
		else:
			size = long_unpack(data[offset:offset+4])
			offset += 4
			att = data[offset:offset + size]
			if size < -1 or len(att) != size:
				raise ValueError("insufficient data left in message")
			offset += size
		yield (typid, att)

	if len(data) - offset != 0:
		raise ValueError("extra data, %d octets, at end of record" %(len(data),))

def record_pack(seq):
	"""
	pack a record given an iterable of (type_oid, data) pairs.
	"""
	return long_pack(len(seq)) + b''.join([
		# typid + (null_seq or data)
		oid_pack(x) + (y is None and null_sequence or (long_pack(len(y)) + y))
		for x, y in seq
	])

def elements_pack(elements):
	"""
	Pack the elements for containment within a serialized array.

	This is used by array_pack.
	"""
	for x in elements:
		if x is None:
			yield null_sequence
		else:
			yield long_pack(len(x))
			yield x

def array_pack(array_data):
	"""
	Pack a raw array. A raw array consists of flags, type oid, sequence of lower
	and upper bounds, and an iterable of already serialized element data:

		(0, element type oid, (lower bounds, upper bounds, ...), iterable of element_data)
	
	The lower bounds and upper bounds specifies boundaries of the dimension. So the length
	of the boundaries sequence is two times the number of dimensions that the array has.

	array_pack((flags, type_id, lower_upper_bounds, element_data))

	The format of ``lower_upper_bounds`` is a sequence of lower bounds and upper
	bounds. First lower then upper inlined within the sequence:

		[lower, upper, lower, upper]
	
	The above array `dlb` has two dimensions. The lower and upper bounds of the
	first dimension is defined by the first two elements in the sequence. The
	second dimension is then defined by the last two elements in the sequence.
	"""
	(flags, typid, dlb, elements) = array_data
	header = llL_pack((len(dlb) // 2, flags, typid))
	return header + \
		struct.pack("!%dl" %(len(dlb),), *dlb) + \
		b''.join(elements_pack(elements))

def elements_unpack(data, offset):
	"""
	Unpack the serialized elements of an array into a list.

	This is used by array_unpack.
	"""
	data_len = len(data)
	while offset < data_len:
		lend = data[offset:offset+4]
		offset += 4
		if lend == null_sequence:
			yield None
		else:
			sizeof_el = long_unpack(lend)
			yield data[offset:offset+sizeof_el]
			offset += sizeof_el

def array_unpack(data):
	"""
	Given a serialized array, unpack it into a tuple:

		(flags, typid, (lower bounds, upper bounds, ...), [elements])
	"""
	ndim, flags, typid = llL_unpack(data[0:12])
	if ndim < 0:
		raise ValueError("invalid number of dimensions: %d" %(ndim,))
	# "ndim" number of pairs of longs
	end = 4 * 2 * ndim + 12
	# Dimension Bounds
	dlb = struct.unpack("!%dl"%(2 * ndim,), data[12:end])
	return (flags, typid, dlb, elements_unpack(data, end))


def return_arg(arg):
	return arg
literal = (return_arg, return_arg)
del return_arg

oid_to_io = {
	pg_types.RECORDOID : (record_pack, record_unpack),
	pg_types.ANYARRAYOID : (array_pack, array_unpack),

	pg_types.BOOLOID : (bool_pack, bool_unpack),
	pg_types.BITOID : (bit_pack, bit_unpack),
	pg_types.VARBITOID : (varbit_pack, varbit_unpack),

	pg_types.BYTEAOID : (bytes, bytes),
	pg_types.CHAROID : literal,

#	pg_types.MACADDROID : literal,
	pg_types.INETOID : (net_pack, net_unpack),
	pg_types.CIDROID : (net_pack, net_unpack),

	pg_types.DATEOID : (date_pack, date_unpack),
	pg_types.ABSTIMEOID : (long_pack, long_unpack),

	pg_types.INT2OID : (int2_pack, int2_unpack),
	pg_types.INT4OID : (int4_pack, int4_unpack),
	pg_types.INT8OID : (int8_pack, int8_unpack),
	pg_types.NUMERICOID : (numeric_pack, numeric_unpack),

	pg_types.OIDOID : (oid_pack, oid_unpack),
	pg_types.XIDOID : (xid_pack, xid_unpack),
	pg_types.CIDOID : (cid_pack, cid_unpack),
	pg_types.TIDOID : (tid_pack, tid_unpack),

	pg_types.FLOAT4OID : (float_pack, float_unpack),
	pg_types.FLOAT8OID : (double_pack, double_unpack),

	pg_types.POINTOID : (point_pack, point_unpack),
	pg_types.LSEGOID : (lseg_pack, lseg_unpack),
	pg_types.BOXOID : (box_pack, box_unpack),
	pg_types.CIRCLEOID : (circle_pack, circle_unpack),
	pg_types.PATHOID : (path_pack, path_unpack),
	pg_types.POLYGONOID : (polygon_pack, polygon_unpack),

	#pg_types.ACLITEMOID : (aclitem_pack, aclitem_unpack),
	#pg_types.LINEOID : (line_pack, line_unpack),
	#pg_types.CASHOID : (cash_pack, cash_unpack),
}

time_io = {
	pg_types.TIMEOID : (time_pack, time_unpack),
	pg_types.TIMETZOID : (timetz_pack, timetz_unpack),
	pg_types.TIMESTAMPOID : (time_pack, time_unpack),
	pg_types.TIMESTAMPTZOID : (time_pack, time_unpack),
	pg_types.INTERVALOID : (interval_pack, interval_unpack)
}

time_noday_io = {
	pg_types.TIMEOID : (time_pack, time_unpack),
	pg_types.TIMETZOID : (timetz_pack, timetz_unpack),
	pg_types.TIMESTAMPOID : (time_pack, time_unpack),
	pg_types.TIMESTAMPTZOID : (time_pack, time_unpack),
	pg_types.INTERVALOID : (interval_noday_pack, interval_noday_unpack)
}

time64_io = {
	pg_types.TIMEOID : (time64_pack, time64_unpack),
	pg_types.TIMETZOID : (timetz64_pack, timetz64_unpack),
	pg_types.TIMESTAMPOID : (time64_pack, time64_unpack),
	pg_types.TIMESTAMPTZOID : (time64_pack, time64_unpack),
	pg_types.INTERVALOID : (interval64_pack, interval64_unpack)
}

time64_noday_io = {
	pg_types.TIMEOID : (time64_pack, time64_unpack),
	pg_types.TIMETZOID : (timetz64_pack, timetz64_unpack),
	pg_types.TIMESTAMPOID : (time64_pack, time64_unpack),
	pg_types.TIMESTAMPTZOID : (time64_pack, time64_unpack),
	pg_types.INTERVALOID : (interval64_noday_pack, interval64_noday_unpack)
}
