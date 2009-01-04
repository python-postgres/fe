##
# copyright 2007, pg/python project.
# http://python.projects.postgresql.org
##
"""
PostgreSQL data type protocol--packing and unpacking functions.

This module provides functions that pack and unpack many standard PostgreSQL
types. These functions are completely unassociated with normally corresponding
type Oids; that mapping is handled by the `postgresql.protocol.typical.oidmaps`
module and the `postgresql.protocol.typical.stdio` module(The latter doesn't
normally map to these functions, rather a higher level transformation that uses
these functions).

The name of the function describes what type the function is intended to be used
on. Normally, the fucntions return a structured form of the serialized data to
be used as a parameter to the creation of a higher level instance. In
particular, most of the functions that deal with time return a pair for
representing the relative offset: (seconds, microseconds). For times, this
provides an abstraction for quad-word based times used by some configurations of
PostgreSQL.

Oid -> I/O
==========

Map PostgreSQL type Oids to routines that pack and unpack raw data.

 oid_to_io:
  The primary map.

 time_io
  Floating-point based time I/O.

 time_noday_io
  Floating-point based time I/O with noday-intervals.

 time64_io
  long-long based time I/O.

 time64_noday_io
  long-long based time I/O with noday-intervals.

"""
import postgresql.types as pg_type
from datetime import tzinfo
from . import typstruct as ts
from .element3 import StringFormat, BinaryFormat

class FixedOffset(tzinfo):
	def __init__(self, offset):
		self._offset = offset

	def utcoffset(self):
		return self._offset

def return_arg(arg):
	return arg

literal = (return_arg, return_arg)

oid_to_io = {
	#pg_type.RECORDOID : (record_pack, record_unpack),
	#pg_type.ANYARRAYOID : (array_pack, array_unpack),

	pg_type.BOOLOID : (ts.bool_pack, ts.bool_unpack),
	pg_type.BITOID : (ts.bit_pack, ts.bit_unpack),
	pg_type.VARBITOID : (ts.varbit_pack, ts.varbit_unpack),

	pg_type.BYTEAOID : (bytes, bytes),
	pg_type.CHAROID : literal,
	pg_type.MACADDROID : literal,

	pg_type.INETOID : (ts.cidr_pack, ts.cidr_unpack),
	pg_type.CIDROID : (ts.cidr_pack, ts.cidr_unpack),

	pg_type.DATEOID : (ts.date_pack, ts.date_unpack),
	pg_type.ABSTIMEOID : (ts.long_pack, ts.long_unpack),

	pg_type.INT2OID : (ts.int2_pack, ts.int2_unpack),
	pg_type.INT4OID : (ts.int4_pack, ts.int4_unpack),
	pg_type.INT8OID : (ts.int8_pack, ts.int8_unpack),
	pg_type.NUMERICOID : literal,

	pg_type.OIDOID : (ts.oid_pack, ts.oid_unpack),
	pg_type.XIDOID : (ts.xid_pack, ts.xid_unpack),
	pg_type.CIDOID : (ts.cid_pack, ts.cid_unpack),
	pg_type.TIDOID : (ts.tid_pack, ts.tid_unpack),

	pg_type.FLOAT4OID : (ts.float_pack, ts.float_unpack),
	pg_type.FLOAT8OID : (ts.double_pack, ts.double_unpack),

	pg_type.POINTOID : (ts.point_pack, ts.point_unpack),
	pg_type.LSEGOID : (ts.lseg_pack, ts.lseg_unpack),
	pg_type.BOXOID : (ts.box_pack, ts.box_unpack),
	pg_type.CIRCLEOID : (ts.circle_pack, ts.circle_unpack),
	pg_type.PATHOID : (ts.path_pack, ts.path_unpack),
	pg_type.POLYGONOID : (ts.polygon_pack, ts.polygon_unpack),

	#pg_type.ACLITEMOID : (aclitem_pack, aclitem_unpack),
	#pg_type.LINEOID : (line_pack, line_unpack),
	#pg_type.CASHOID : (cash_pack, cash_unpack),
}

time_io = {
	pg_type.TIMEOID : (ts.time_pack, ts.time_unpack),
	pg_type.TIMETZOID : (ts.timetz_pack, ts.timetz_unpack),
	pg_type.TIMESTAMPOID : (ts.time_pack, ts.time_unpack),
	pg_type.TIMESTAMPTZOID : (ts.time_pack, ts.time_unpack),
	pg_type.INTERVALOID : (ts.interval_pack, ts.interval_unpack)
}

time_noday_io = {
	pg_type.TIMEOID : (ts.time_pack, ts.time_unpack),
	pg_type.TIMETZOID : (ts.timetz_pack, ts.timetz_unpack),
	pg_type.TIMESTAMPOID : (ts.time_pack, ts.time_unpack),
	pg_type.TIMESTAMPTZOID : (ts.time_pack, ts.time_unpack),
	pg_type.INTERVALOID : (ts.interval_noday_pack, ts.interval_noday_unpack)
}

time64_io = {
	pg_type.TIMEOID : (ts.time64_pack, ts.time64_unpack),
	pg_type.TIMETZOID : (ts.timetz64_pack, ts.timetz64_unpack),
	pg_type.TIMESTAMPOID : (ts.time64_pack, ts.time64_unpack),
	pg_type.TIMESTAMPTZOID : (ts.time64_pack, ts.time64_unpack),
	pg_type.INTERVALOID : (ts.interval64_pack, ts.interval64_unpack)
}

time64_noday_io = {
	pg_type.TIMEOID : (ts.time64_pack, ts.time64_unpack),
	pg_type.TIMETZOID : (ts.timetz64_pack, ts.timetz64_unpack),
	pg_type.TIMESTAMPOID : (ts.time64_pack, ts.time64_unpack),
	pg_type.TIMESTAMPTZOID : (ts.time64_pack, ts.time64_unpack),
	pg_type.INTERVALOID : (ts.interval64_noday_pack, ts.interval64_noday_unpack)
}

class Row(tuple):
	"Name addressable items tuple; mapping and sequence"
	def __init__(subtype, iter, attmap = {}):
		rob = tuple.__new__(subtype, attr)
		rob.attmap = attmap
		return rob

	def attindex(self, k):
		return self.attmap.get(k)

	def __getitem__(self, i):
		if type(i) is int or type(i) is long:
			return tuple.__getitem__(self, i)
		idx = self.attmap[i]
		return tuple.__getitem__(self, idx)

	def get(self, i):
		if type(i) is int or type(i) is long:
			l = len(self)
			if -l < i < l:
				return tuple.__getitem__(self, i)
		else:
			idx = self.attmap.get(i)
			if idx is not None:
				return tuple.__getitem__(self, idx)
		return None

	def has_key(self, k):
		return self.attmap.has_key(k)

	def keys(self):
		return self.attmap.keys()

	def values(self):
		return self

	def items(self):
		for k, v in self.attmap.iteritems():
			yield k, tuple.__getitem__(self, v)

def array_typio(pack_element, unpack_element, typoid, hasbin_input, hasbin_output):
	"""
	create an array's typio pair
	"""
	if hasbin_input:
		def pack_array_elements(a):
			for x in a:
				if x is None:
					yield None
				else:
					yield pack_element(x)

		def pack_an_array(data):
			if not type(data) is pg_types.array:
				data = pg_types.array(data, seqtypes = (list, tuple))
			dlb = []
			for x in data.dimensions:
				dlb.append(x)
				dlb.append(1)
			return array_pack((
				0, typoid, dlb,
				pack_array_elements(data.elements)
			))
	else:
		pack_an_array = None

	if hasbin_output:
		def unpack_array_elements(a):
			for x in a:
				if x is None:
					yield None
				else:
					yield unpack(x)

		def unpack_an_array(data):
			flags, typoid, dlb, elements = array_unpack(data)
			dim = []
			for x in range(0, len(dlb), 2):
				dim.append(dlb[x] - (dlb[x+1] or 1) + 1)
			return pg_types.array(
				tuple(unpack_array_elements(elements)),
				dimensions = dim
			)
	else:
		unpack_an_array = None

	return (pack_an_array, unpack_an_array)

def attmap_from_pqdesc(desc):
	"""
	create a dictionary from a pq desc that maps attribute names
	to their index
	"""
	return {
		desc[x][0] : x for x in range(len(desc))
	}

# Postgres always sends object data in row form, so
# make the fundamental tranformation routines work on a sequence.
def row_unpack(seq, typio, decode):
	'Transform object data into an object using the associated IO routines'
	for x in range(len(typio)):
		io = typio[x]
		ob = seq[x]
		if ob is None:
			yield None
		elif io is None:
			# StringFormat
			yield decode(ob)
		else:
			# BinaryFormat
			yield io(ob)

def row_pack(seq, typio, encode):
	'Transform objects into object data using the associated IO routines'
	for x in range(len(typio)):
		io = typio[x]
		ob = seq[x]
		if ob is None:
			yield (StringFormat, None)
		elif io is None or (not io is bytes and type(ob) is str):
			# StringFormat
			yield (StringFormat, encode(ob))
		else:
			# BinaryFormat
			yield (BinaryFormat, io(ob))

def composite_typio(self, ci):
	"""
	create the typio pair for a given composite type
	"""
	attmap = {}
	rtypi = []
	rtypo = []
	rtypids = []
	i = 0

	for x in ci:
		attmap[x[1]] = i
		typid = x[0]
		rtypids.append(typid)
		pack, unpack = self.lookup(typid)
		rtypi.append(pack or self.encode)
		rtypo.append(unpack or self.decode)
		i += 1
	rtypi = tuple(rtypi)
	rtypo = tuple(rtypo)
	rtypids = tuple(rtypids)
	del i, pack, unpack

	def transform_record(obj_xf, data):
		i = -1
		for x in data:
			i += 1
			if x is None:
				yield None
			else:
				tio = obj_xf[i]
				yield tio(x)

	def unpack_a_record(data):
		return Row(
			transform_record(
				rtypo,
				[x[1] for x in record_unpack(data)]
			),
			attmap
		)

	def pack_a_record(data):
		return record_pack(
			zip(rtypids, transform_record(rtypi, data))
		)

	return (pack_a_record, unpack_a_record)

class TypeIO(object):
	"""
	A class that manages I/O for a given configuration. Normally, a connection
	would create an instance, and configure it based upon the version and
	configuration of PostgreSQL that it is connected to.
	"""

	def __init__(self,
		version_info : "tuple representing version of postgresql",
		lookup_type_meta : "given a typoid, lookup the type metadata",
		integer_datetimes : "setting from pg_settings" = False,
		noday_times : "bool(): if none, determine from version" = None,
		encoding = 'utf-8',
		timezone = 'utc',
	):
		self.lookup_type_meta = lookup_type_meta
		self.integer_datetimes = integer_datetimes
		self.version_info = version_info
		if noday_times is None:
			self.noday_times = self.version_info[:2] <= (8,0)
		else:
			noday_times = bool(noday_times)

		self._cache = {
			pg_types.RECORDOID : (
				record_pack,
				lambda r: tuple([
					self.lookup_type_meta(typoid)[1](data)
					for typoid, data in record_unpack(r)
				]),
			),
			pg_types.ANYARRAYOID : (
				array_pack,
				lambda a: anyarray_unpack(
					lambda x: (
						self.lookup(x)[1] or self.decode
					), a
				)
			),
			# Encoded character strings
			pg_types.NAMEOID : (None, None),
			pg_types.VARCHAROID : (None, None),
			pg_types.TEXTOID : (None, None),
		}

		if integer_datetimes is True:
			if self.noday_times:
				self._time_io = time64_io_noday
			else:
				self._time_io = time64_io
		else:
			if self.noday_times:
				self._time_io = time_io_noday
			else:
				self._time_io = time_io

	def encode(self, string_data):
		return self._encode(string_data)[0]

	def decode(self, bytes_data):
		return self._decode(bytes_data)[0]

	def set_encoding(self, value):
		self.encoding = value
		enc = pg_enc_aliases.postgres_to_python.get(value, value)
		ci = codecs.lookup(enc)
		self._encode = ci[0]
		self._decode = ci[1]

	def _pack_timestamptz(self, dt):
		if dt.tzinfo:
			# _pack_timestamp ignores the tzinfo,
			# so we can just adjust to 
			return self._ts_pack(
				(dt - dt.tzinfo.utcoffset(dt)).replace(tzinfo = None)
			)
		else:
			# If no timezone is specified, assume UTC.
			return self._ts_pack(dt)

	def _unpack_timestamptz(self, data):
		return self.tzinfo.fromutc(self._ts_unpack(data))

	def set_timezone(self, tzname, offset):
		self.timezone = value
		self.tzinfo = FixedOffset(tzname, value)
		self._cache.update(self._time_io)
		# Used by _(un)?pack_timestamptz
		self._ts_pack, self._ts_unpack = self._cache.lookup(pg_types.TIMESTAMPOID)
		self.typio[pg_types.TIMESTAMPTZOID] = (
			self._pack_timestamptz,
			self._unpack_timestamptz,
		)

	def resolve(self, typoid, recursed = False):
		"lookup a type's IO routines from a given typoid"
		typoid = int(typoid)
		if typio is None:
			ti = self.cquery(TypeLookup, title = 'lookup_type').first(typoid)
			if ti is not None:
				typname, typtype, typlen, typelem, typrelid, \
					ae_typid, ae_hasbin_input, ae_hasbin_output = ti
				if typrelid:
					# Composite/Complex/Row Type
					typio = composite_typio(self, typrelid)
					self.typio[typoid] = typio
				elif ae_typid is not None:
					# Array Type
					#
					# Defensive coding here,
					# Array type, be careful to avoid infinite recursion
					# in cases of a corrupt or malicious catalog/user.
					if recursed is False:
						te = self._typio(int(typelem), recursed = True) or \
							(None, None)
						typio = array_typio(
							self, typelem, te, ae_hasbin_input, ae_hasbin_output
						)
						self.typio[typoid] = typio
					else:
						raise TypeError(
							"array type element(%d) is an array type" %(typoid,)
						)
				else:
					# Unknown type, check for typname entry for UDTs.
					typio = self.typio.get(typname) or \
						self.connector._typio(typname)
			else:
				warnings.warn("type %d does not exist in pg_type" %(typoid,))

		return typio

	def resolve_descriptor(self, desc, index):
		'create a sequence of I/O routines from a pq descriptor'
		return [
			(self.resolve(x[3]) or (None, None))[index] for x in desc
		]

def anyarray_unpack_elements(a, unpack):
	'generator for yielding None if x is None or unpack(x)'
	for x in a:
		if x is None:
			yield None
		else:
			yield unpack(x)

def anyarray_unpack(unpack, data):
	'unpack the array, normalize the lower bounds and return a pg_types.array'
	flags, typoid, dlb, elements = array_unpack(data)
	dim = []
	for x in range(0, len(dlb), 2):
		dim.append(dlb[x] - (dlb[x+1] or 1) + 1)
	return pg_types.array(
		tuple(anyarray_unpack_elements(elements, unpack(typoid))),
		dimensions = dim
	)

def _mktuple(x, unpackers, decode):
	'given a PQ tuple, return a map tuple'
	return Row(
		row_unpack(
			x, self._output_io, self.connection._decode
		),
		self._output_attmap
	)
