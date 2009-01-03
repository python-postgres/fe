##
# copyright 2008, pg/python project.
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

null_sequence = b'\xff\xff\xff\xff'

def mk_pack(x):
	'Create a pair, (pack, unpack) for the given `struct` format.'
	s = struct.Struct('!' + x)
	if len(x) > 1:
		return lambda y: s.pack(*y), s.unpack
	else:
		return s.pack, lambda y: s.unpack(y)[0]

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
byte_pack, byte_unpack = mk_pack("B")
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

def time_pack(timetup):
	"""
	Given a pair, (seconds, microseconds), serialize it into a double.
	"""
	return double_pack(mktime(timetup))

def time_unpack(data):
	"""
	Given a serialized double, return the pai (seconds, microseconds).
	"""
	return mktimetuple(double_unpack(data))

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

def time64_pack(timetup):
	"""
	Pack a (seconds, microseconds) tuple into a quad-word.
	"""
	return longlong_pack(mktime64(timetup))

def time64_unpack(data):
	"""
	Given a quad-word, unpack it into a pair:

		(seconds, microseconds)
	"""
	return mktimetuple64(longlong_unpack(data))

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

def cidr_pack(family_mask_data):
	"""
	Given a triple, yield the serialized form for transport.

	Prepends the ``family``, ``mask`` and implicit ``is_cidr`` fields.
	"""
	(family, mask, data) = family_mask_data
	return b'%s%s'b'\x01'b'%s%s' %(chr(family), chr(mask), chr(len(data)), data)
inet_pack = cidr_pack

def cidr_unpack(data):
	"""
	Given serialized cidr data, return a tuple:

		(family, mask, data)
	"""
	family, mask, is_cidr, size = BBBB_unpack(data[:4])

	rd = data[4:]
	if len(rd) != size:
		raise ValueError("invalid size parameter")

	return (family, mask, rd)
inet_unpack = cidr_unpack

def record_unpack(data):
	"""
	Given serialized record data, return a tuple of tuples of type Oids and
	attributes.
	"""
	columns = long_unpack(data[0:4])
	offset = 4

	for x in xrange(columns):
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
				# XXX: raise 22P03
				raise ValueError("insufficient data left in message")
			offset += size
		yield (typid, att)

	if len(data) - offset != 0:
		raise ValueError("extra data, %d octets, at end of record" %(len(data),))

def record_pack(seq):
	"""
	pack a record given an iterable of (type_oid, data) pairs.
	"""
	return long_pack(len(seq)) + ''.join([
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
		''.join(elements_pack(elements))

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
