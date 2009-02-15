##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
PostgreSQL type I/O tools--packing and unpacking functions.

This module provides functions that pack and unpack many standard PostgreSQL
types. 

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
import codecs
from abc import ABCMeta, abstractmethod

from decimal import Decimal
import datetime

try:
	import xml.etree.cElementTree as etree
except ImportError:
	import xml.etree.ElementTree as etree

from .. import types as pg_types
from ..encodings import aliases as pg_enc_aliases
from . import typstruct as ts
from .element3 import StringFormat, BinaryFormat

pg_epoch_datetime = datetime.datetime(2000, 1, 1)
pg_epoch_date = pg_epoch_datetime.date()
pg_date_offset = pg_epoch_date.toordinal()
## Difference between PostgreSQL epoch and Unix epoch.
## Used to convert a PostgreSQL ordinal to an ordinal usable by datetime
pg_time_days = (pg_date_offset - datetime.date(1970, 1, 1).toordinal())

class FixedOffset(datetime.tzinfo):
	def __init__(self, offset, tzname = None):
		self._tzname = tzname
		self._offset = datetime.timedelta(0, offset)
		self._dst = datetime.timedelta(0)

	def utcoffset(self, offset_from):
		return self._offset

	def tzname(self):
		return self._tzname

	def dst(self, arg):
		return self._dst

	def __repr__(self):
		return "{path}.{name}({off}{tzname})".format(
			path = type(self).__module__,
			name = type(self).__name__,
			off = repr(self._offset.days * 24 * 60 * 60 + self._offset.seconds),
			tzname = (
				", tzname = {tzname!r}".format(tzname = self._tzname) \
				if self._tzname is not None else ""
			)
		)
UTC = FixedOffset(0, tzname = 'UTC')

class Row(tuple):
	"Name addressable items tuple; mapping and sequence"
	def __new__(subtype, iter, attmap = {}):
		rob = tuple.__new__(subtype, iter)
		rob.attmap = attmap
		return rob

	def attindex(self, k):
		return self.attmap.get(k)

	def __getitem__(self, i):
		if type(i) is int:
			return tuple.__getitem__(self, i)
		idx = self.attmap[i]
		return tuple.__getitem__(self, idx)

	def get(self, i):
		if type(i) is int:
			l = len(self)
			if -l < i < l:
				return tuple.__getitem__(self, i)
		else:
			idx = self.attmap.get(i)
			if idx is not None:
				return tuple.__getitem__(self, idx)
		return None

	def __contains__(self, k):
		return k in self.attmap

	def keys(self):
		return self.attmap.keys()

	def values(self):
		return self

	def items(self):
		for k, v in self.attmap.iteritems():
			yield k, tuple.__getitem__(self, v)

##
# High level type I/O routines.
##

def date_pack(x):
	return ts.date_pack(x.toordinal() - pg_date_offset)

def date_unpack(x):
	return datetime.date.fromordinal(pg_date_offset + ts.date_unpack(x))

def timestamp_pack(x):
	"""
	Create a (seconds, microseconds) pair from a `datetime.datetime` instance.
	"""
	d = (x - pg_epoch_datetime)
	return (d.days * 24 * 60 * 60 + d.seconds, d.microseconds)

def timestamp_unpack(seconds):
	"""
	Create a `datetime.datetime` instance from a (seconds, microseconds) pair.
	"""
	return pg_epoch_datetime + datetime.timedelta(
		seconds = seconds[0], microseconds = seconds[1]
	)

def time_pack(x):
	"""
	Create a (seconds, microseconds) pair from a `datetime.time` instance.
	"""
	return (
		(x.hour * 60 * 60) + (x.minute * 60) + x.second,
		x.microsecond
	)

def time_unpack(seconds_ms):
	"""
	Create a `datetime.time` instance from a (seconds, microseconds) pair.
	Seconds being offset from epoch.
	"""
	seconds, ms = seconds_ms
	minutes, sec = divmod(seconds, 60)
	hours, min = divmod(minutes, 60)
	return datetime.time(hours, min, sec, ms)

def interval_pack(x):
	"""
	Create a (months, days, (seconds, microseconds)) tuple from a
	`datetime.timedelta` instance.
	"""
	return (
		0, x.days,
		(x.seconds, x.microseconds)
	)

def interval_unpack(mds):
	"""
	Given a (months, days, (seconds, microseconds)) tuple, create a
	`datetime.timedelta` instance.
	"""
	months, days, seconds_ms = mds
	sec, ms = seconds_ms
	return datetime.timedelta(
		days = days + (months * 30),
		seconds = sec, microseconds = ms
	)

def timetz_pack(x):
	"""
	Create a ((seconds, microseconds), timezone) tuple from a `datetime.time`
	instance.
	"""
	return (time_pack(x), x.utcoffset())

def timetz_unpack(tstz):
	"""
	Create a `datetime.time` instance from a ((seconds, microseconds), timezone)
	tuple.
	"""
	t = time_unpack(tstz[0])
	return t.replace(tzinfo = FixedOffset(tstz[1]))

datetimemap = {
	pg_types.INTERVALOID : (interval_pack, interval_unpack),
	pg_types.TIMEOID : (time_pack, time_unpack),
	pg_types.TIMESTAMPOID : (time_pack, time_unpack),
}

time_io = {
	pg_types.TIMEOID : (
		lambda x: ts.time_pack(time_pack(x)),
		lambda x: time_unpack(ts.time_unpack(x))
	),
	pg_types.TIMETZOID : (
		lambda x: ts.timetz_pack(timetz_pack(x)),
		lambda x: timetz_unpack(ts.timetz_unpack(x))
	),
	pg_types.TIMESTAMPOID : (
		lambda x: ts.time_pack(timestamp_pack(x)),
		lambda x: timestamp_unpack(ts.time_unpack(x))
	),
	pg_types.TIMESTAMPTZOID : (
		lambda x: ts.time_pack(timestamp_pack(x)),
		lambda x: timestamp_unpack(ts.time_unpack(x))
	),
	pg_types.INTERVALOID : (
		lambda x: ts.interval_pack(interval_pack(x)),
		lambda x: interval_unpack(ts.interval_unpack(x))
	),
}
time_io_noday = time_io.copy()
time_io_noday[pg_types.INTERVALOID] = (
	lambda x: ts.interval_noday_pack(interval_pack(x)),
	lambda x: interval_unpack(ts.interval_noday_unpack(x))
)

time64_io = {
	pg_types.TIMEOID : (
		lambda x: ts.time64_pack(time_pack(x)),
		lambda x: time_unpack(ts.time64_unpack(x))
	),
	pg_types.TIMETZOID : (
		lambda x: ts.timetz64_pack(timetz_pack(x)),
		lambda x: timetz_unpack(ts.timetz64_unpack(x))
	),
	pg_types.TIMESTAMPOID : (
		lambda x: ts.time64_pack(timestamp_pack(x)),
		lambda x: timestamp_unpack(ts.time64_unpack(x))
	),
	pg_types.TIMESTAMPTZOID : (
		lambda x: ts.time64_pack(timestamp_pack(x)),
		lambda x: timestamp_unpack(ts.time64_unpack(x))
	),
	pg_types.INTERVALOID : (
		lambda x: ts.interval64_pack(interval_pack(x)),
		lambda x: interval_unpack(ts.interval64_unpack(x))
	),
}
time64_io_noday = time64_io.copy()
time64_io_noday[pg_types.INTERVALOID] = (
	lambda x: ts.interval64_noday_pack(interval_pack(x)),
	lambda x: interval_unpack(ts.interval64_noday_unpack(x))
)

def circle_unpack(x):
	"""
	Given raw circle data, (x, y, radius), make a circle instance using
	`postgresql.types.circle`.
	"""
	return pg_types.circle(((x[0], x[1]), x[2]))

def two_pair(x):
	'Make a pair of pairs out of a sequence of four objects'
	return ((x[0], x[1]), (x[2], x[3]))

def varbit_pack(x):
	return ts.varbit_pack((x.bits, x.data))
def varbit_unpack(x):
	return pg_types.varbit.from_bits(*ts.varbit_unpack(x))
bitio = (varbit_pack, varbit_unpack)

point_pack = ts.point_pack
def point_unpack(x):
	return pg_types.point(ts.point_unpack(x))	

def box_pack(x):
	return ts.box_pack((x[0][0], x[0][1], x[1][0], x[1][1]))
def box_unpack(x):
	return pg_types.box(two_pair(ts.box_unpack(x)))

def lseg_pack(x):
	return ts.lseg_pack((x[0][0], x[0][1], x[1][0], x[1][1]))
def lseg_unpack(x):
	return pg_types.lseg(two_pair(ts.lseg_unpack(x)))

def circle_pack(x):
	lambda x: ts.circle_pack((x[0][0], x[0][1], x[1])),
def circle_unpack(x):
	lambda x: circle_unpack(ts.circle_unpack(x))

# Map type oids to a (pack, unpack) pair.
oid_to_io = {
	pg_types.NUMERICOID : (None, Decimal),

	pg_types.DATEOID : (date_pack, date_unpack),

	pg_types.VARBITOID : bitio,
	pg_types.BITOID : bitio,

	pg_types.POINTOID : (point_pack, point_unpack),
	pg_types.BOXOID : (box_pack, box_unpack),
	pg_types.LSEGOID : (lseg_pack, lseg_unpack),
	pg_types.CIRCLEOID : (circle_pack, circle_unpack),
}

def anyarray_unpack_elements(elements, unpack):
	'generator for yielding None if x is None or unpack(x)'
	for x in elements:
		if x is None:
			yield None
		else:
			yield unpack(x)

def anyarray_unpack(unpack, data):
	'unpack the array, normalize the lower bounds and return a pg_types.Array'
	flags, typoid, dlb, elements = ts.array_unpack(data)
	dim = []
	for x in range(0, len(dlb), 2):
		dim.append(dlb[x] - (dlb[x+1] or 1) + 1)
	return pg_types.Array(
		tuple(anyarray_unpack_elements(elements, unpack(typoid))),
		dimensions = dim
	)
anyarray_pack = ts.array_pack

def array_typio(
	pack_element, unpack_element,
	typoid, hasbin_input, hasbin_output
):
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
			if not type(data) is pg_types.Array:
				data = pg_types.Array(data, seqtypes = (list,))
			dlb = []
			for x in data.dimensions:
				dlb.append(x)
				dlb.append(1)
			return ts.array_pack((
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
					yield unpack_element(x)

		def unpack_an_array(data):
			flags, typoid, dlb, elements = ts.array_unpack(data)
			dim = []
			for x in range(0, len(dlb), 2):
				dim.append(dlb[x] - (dlb[x+1] or 1) + 1)
			return pg_types.Array(
				tuple(unpack_array_elements(elements)),
				dimensions = dim
			)
	else:
		unpack_an_array = None

	return (pack_an_array, unpack_an_array)

def transform_record(obj_xf, raw_columns, io):
	i = -1
	for x in raw_columns:
		i += 1
		if x is None:
			yield None
		else:
			yield obj_xf[io][i](x)

def composite_typio(
	cio : "sequence (pack,unpack) tuples corresponding to the",
	attmap : "mapping of column name to index number",
	typids : "sequence of type Oids; index must correspond to the composite's"
):
	"""
	create the typio pair for the composite type metadata passed in.
	"""
	def unpack_a_record(data):
		return Row(
			transform_record(
				cio,
				[x[1] for x in ts.record_unpack(data)],
				1
			),
			attmap
		)

	def pack_a_record(data):
		return ts.record_pack(
			zip(typids, transform_record(cio, data, 0))
		)

	return (pack_a_record, unpack_a_record)

# PostgreSQL always sends object data in row form, so
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
			yield None
		elif io is None:
			# StringFormat
			yield encode(ob)
		else:
			# BinaryFormat
			yield io(ob)

class TypeIO(object, metaclass = ABCMeta):
	"""
	A class that manages I/O for a given configuration. Normally, a connection
	would create an instance, and configure it based upon the version and
	configuration of PostgreSQL that it is connected to.
	"""

	@abstractmethod
	def lookup_type_info(self, typid):
		"""
		"""

	@abstractmethod
	def lookup_composite_type_info(self, typid):
		"""
		"""

	def select_time_io(self, 
		version_info : "postgresql.version.split(settings['server_version'])",
		integer_datetimes : "bool(settings['integer_datetimes'])",
		noday_intervals : "bool(): if none, determine from `version_info`" = None,
	):
		self.integer_datetimes = integer_datetimes
		self.version_info = version_info
		if noday_intervals is None:
			# 8.0 and lower use no-day binary times
			self.noday_intervals = self.version_info[:2] <= (8,0)
		else:
			self.noday_intervals = bool(noday_intervals)

		if integer_datetimes is True:
			if self.noday_intervals:
				self._time_io = time64_io_noday
			else:
				self._time_io = time64_io
		else:
			if self.noday_intervals:
				self._time_io = time_io_noday
			else:
				self._time_io = time_io
		self._ts_pack, self._ts_unpack = self._time_io[pg_types.TIMESTAMPOID]

	def encode(self, string_data):
		return self._encode(string_data)[0]

	def decode(self, bytes_data):
		return self._decode(bytes_data)[0]

	def resolve_pack(self, typid):
		return self.resolve(typid)[0] or self.encode

	def resolve_unpack(self, typid):
		return self.resolve(typid)[1] or self.decode

	def record_unpack(self, rdata):
		return tuple([
			self.resolve_unpack(typid)(data)
			for (typid, data) in ts.record_unpack(rdata)
		])

	def anyarray_unpack(self, adata):
		return anyarray_unpack(self.resolve_unpack, adata)

	def xml_pack(self, xml):
		return self._encode(etree.tostring(xml))[0]

	def xml_unpack(self, xmldata):
		return pg_types.etree.XML(self._decode(xmldata)[0])

	def attribute_map(self, pq_descriptor):
		return {
			self._decode(k)[0] : v
			for k, v in pq_descriptor.attribute_map.items()
		}

	def __init__(self):
		self.encoding = None
		self.tzinfo = None
		self._time_io = ()
		self._cache = {
			pg_types.RECORDOID : (
				ts.record_pack,
				self.record_unpack,
			),
			pg_types.ANYARRAYOID : (
				anyarray_pack,
				self.anyarray_unpack,
			),
			# Encoded character strings
			pg_types.NAMEOID : (None, None),
			pg_types.VARCHAROID : (None, None),
			pg_types.TEXTOID : (None, None),
			pg_types.TIMESTAMPTZOID : (
				self._pack_timestamptz,
				self._unpack_timestamptz,
			),
			pg_types.XMLOID : (
				self.xml_pack, self.xml_unpack
			)
		}

	def set_encoding(self, value):
		self.encoding = value.lower()
		enc = pg_enc_aliases.postgres_to_python.get(value, value)
		ci = codecs.lookup(enc)
		self._encode = ci[0]
		self._decode = ci[1]

	def _pack_timestamptz(self, dt):
		if dt.tzinfo:
			return self._ts_pack(
				(dt - dt.tzinfo.utcoffset(dt)).replace(tzinfo = None)
			)
		else:
			# If no timezone is specified, assume UTC.
			return self._ts_pack(dt)

	def _unpack_timestamptz(self, data):
		dt = self._ts_unpack(data)
		dt = dt.replace(tzinfo = UTC)
		return dt

	def set_timezone(self, offset, tzname):
		self.tzinfo = FixedOffset(offset, tzname = tzname)

	def resolve_descriptor(self, desc, index):
		'create a sequence of I/O routines from a pq descriptor'
		return [
			(self.resolve(x[3]) or (None, None))[index] for x in desc
		]

	def resolve(
		self,
		typid : "The Oid of the type to resolve pack and unpack routines for.",
		from_resolution_of : \
		"Sequence of typid's used to identify infinite recursion" = ()
	):
		"lookup a type's IO routines from a given typid"
		if from_resolution_of and typid in from_resolution_of:
			raise TypeError(
				"type, %d, is already being looked up: %r" %(
					typid, from_resolution_of
				)
			)
		typid = int(typid)

		typio = None
		for x in (self._cache, self._time_io, oid_to_io, ts.oid_to_io):
			if typid in x:
				typio = x[typid]
				break
		if typio is None:
			# Lookup the type information for the typid as it's not cached.
			##
			ti = self.lookup_type_info(typid)
			if ti is not None:
				typname, typtype, typlen, typelem, typrelid, \
					ae_typid, ae_hasbin_input, ae_hasbin_output = ti
				if typrelid:
					# Composite/Complex/Row Type
					#
					# So the attribute name map, the column I/O, and type Oids are
					# needed.
					attmap = {}
					cio = []
					typids = []
					i = 0
					for x in self.lookup_composite_type_info(typrelid):
						attmap[x[1]] = i
						typids.append(x[0])
						cio.append(self.resolve(
							x[0],
							list(from_resolution_of) + [typid]
						))
						i += 1

					self._cache[typid] = typio = composite_typio(
						cio, typids, attmap
					)
				elif ae_typid is not None:
					# Array Type
					te = self.resolve(
						int(typelem),
						from_resolution_of = list(from_resolution_of) + [typid]
					) or (None, None)
					typio = array_typio(
						te[0] or self.encode,
						te[1] or self.decode,
						typelem,
						ae_hasbin_input,
						ae_hasbin_output
					)
					self._cache[typid] = typio
				else:
					self._cache[typid] = typio = (None, None)
			else:
				# Throw warning about type without entry in pg_type?
				typio = (None, None)
		return typio
