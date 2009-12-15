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
  The primary map for higher level types.

 time_io
  Floating-point based time I/O.

 time_noday_io
  Floating-point based time I/O with noday-intervals.

 time64_io
  long-long based time I/O.

 time64_noday_io
  long-long based time I/O with noday-intervals.
"""
import warnings
import codecs
from ..encodings import aliases as pg_enc_aliases
from .. import exceptions as pg_exc

from operator import itemgetter, add, sub, mul, methodcaller
get0 = itemgetter(0)
get1 = itemgetter(1)

from itertools import chain, starmap, repeat, groupby, cycle, islice, count
from functools import partial

from abc import ABCMeta, abstractmethod

from decimal import Decimal, DecimalTuple
import datetime

from ..exceptions import TypeConversionWarning
from ..python.datetime import UTC, FixedOffset

from ..python.functools import Composition as compose

from .. import types as pg_types
from .. import string as pg_str
from . import typstruct as ts
from .element3 import StringFormat, BinaryFormat

pg_epoch_datetime = datetime.datetime(2000, 1, 1)
pg_epoch_date = pg_epoch_datetime.date()
pg_date_offset = pg_epoch_date.toordinal()
## Difference between PostgreSQL epoch and Unix epoch.
## Used to convert a PostgreSQL ordinal to an ordinal usable by datetime
pg_time_days = (pg_date_offset - datetime.date(1970, 1, 1).toordinal())

##
# High level type I/O routines.
##
toordinal = methodcaller("toordinal")
convert_to_utc = methodcaller('astimezone', UTC)
remove_tzinfo = methodcaller('replace', tzinfo = None)
set_as_utc = methodcaller('replace', tzinfo = UTC)

date_pack = compose((
	toordinal,
	partial(add, -pg_date_offset),
	ts.date_pack,
))
date_unpack = compose((
	ts.date_unpack,
	partial(add, pg_date_offset),
	datetime.date.fromordinal
))

seconds_in_day = 24 * 60 * 60
seconds_in_hour = 60 * 60

def timestamp_pack(x):
	"""
	Create a (seconds, microseconds) pair from a `datetime.datetime` instance.
	"""
	d = (x - pg_epoch_datetime)
	return ((d.days * seconds_in_day) + d.seconds, d.microseconds)

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
		(x.hour * seconds_in_hour) + (x.minute * 60) + x.second,
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
		0, x.days, (x.seconds, x.microseconds)
	)

def interval_unpack(mds):
	"""
	Given a (months, days, (seconds, microseconds)) tuple, create a
	`datetime.timedelta` instance.
	"""
	months, days, seconds_ms = mds
	if months != 0:
		w = pg_exc.TypeConversionWarning(
			"datetime.timedelta cannot represent relative intervals",
			details = {
				''
				'hint': 'An interval was unpacked with a non-zero "month" field.'
			},
			source = 'DRIVER'
		)
		warnings.warn(w)
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
	td = x.tzinfo.utcoffset(x)
	seconds = (td.days * seconds_in_day + td.seconds)
	return (time_pack(x), seconds)

def timetz_unpack(tstz):
	"""
	Create a `datetime.time` instance from a ((seconds, microseconds), timezone)
	tuple.
	"""
	t = time_unpack(tstz[0])
	return t.replace(tzinfo = FixedOffset(tstz[1]))

time_io = {
	pg_types.TIMEOID : (
		compose((time_pack, ts.time_pack)),
		compose((ts.time_unpack, time_unpack)),
	),
	pg_types.TIMETZOID : (
		compose((timetz_pack, ts.timetz_pack)),
		compose((ts.timetz_unpack, timetz_unpack)),
	),
	pg_types.TIMESTAMPOID : (
		compose((timestamp_pack, ts.time_pack)),
		compose((ts.time_unpack, timestamp_unpack)),
	),
	pg_types.TIMESTAMPTZOID : (
		compose((convert_to_utc, remove_tzinfo, timestamp_pack, ts.time_pack)),
		compose((ts.time_unpack, timestamp_unpack, set_as_utc)),
	),
	pg_types.INTERVALOID : (
		compose((interval_pack, ts.interval_pack)),
		compose((ts.interval_unpack, interval_unpack)),
	),
}
time_io_noday = time_io.copy()
time_io_noday[pg_types.INTERVALOID] = (
	compose((interval_pack, ts.interval_noday_pack)),
	compose((ts.interval_noday_unpack, interval_unpack)),
)

time64_io = {
	pg_types.TIMEOID : (
		compose((time_pack, ts.time64_pack)),
		compose((ts.time64_unpack, time_unpack)),
	),
	pg_types.TIMETZOID : (
		compose((timetz_pack, ts.timetz64_pack)),
		compose((ts.timetz64_unpack, timetz_unpack)),
	),
	pg_types.TIMESTAMPOID : (
		compose((timestamp_pack, ts.time64_pack)),
		compose((ts.time64_unpack, timestamp_unpack)),
	),
	pg_types.TIMESTAMPTZOID : (
		compose((convert_to_utc, remove_tzinfo, timestamp_pack, ts.time64_pack)),
		compose((ts.time64_unpack, timestamp_unpack, set_as_utc)),
	),
	pg_types.INTERVALOID : (
		compose((interval_pack, ts.interval64_pack)),
		compose((ts.interval64_unpack, interval_unpack)),
	),
}
time64_io_noday = time64_io.copy()
time64_io_noday[pg_types.INTERVALOID] = (
	compose((interval_pack, ts.interval64_noday_pack)),
	compose((ts.interval64_noday_unpack, interval_unpack)),
)

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
box_unpack = compose((
	ts.box_unpack,
	two_pair,
	pg_types.box,
))

def lseg_pack(x):
	return ts.lseg_pack((x[0][0], x[0][1], x[1][0], x[1][1]))
lseg_unpack = compose((
	ts.lseg_unpack,
	two_pair,
	pg_types.lseg
))

def circle_pack(x):
	return ts.circle_pack((x[0][0], x[0][1], x[1]))
def circle_unpack(x):
	x = ts.circle_unpack(x)
	return pg_types.circle(((x[0], x[1]), x[2]))

##
# numeric is represented using:
#  ndigits, the number of *numeric* digits.
#  weight, the *numeric* digits "left" of the decimal point
#  sign, negativity. see `numeric_signs` below
#  dscale, *display* precision. used to identify exponent.
#
# NOTE: A numeric digit is actually four digits in the representation.
#
# Python's Decimal consists of:
#  sign, negativity.
#  digits, sequence of int()'s
#  exponent, digits that fall to the right of the decimal point
numeric_negative = 16384

def numeric_pack(x,
	numeric_digit_length : "number of decimal digits in a numeric digit" = 4
):
	if not isinstance(x, Decimal):
		x = Decimal(x)
	x = x.as_tuple()
	if x.exponent == 'F':
		raise ValueError("numeric does not support infinite values")

	# normalize trailing zeros (truncate em')
	# this is important in order to get the weight and padding correct
	# and to avoid packing superfluous data which will make pg angry.
	trailing_zeros = 0
	weight = 0
	if x.exponent < 0:
		# only attempt to truncate if there are digits after the point,
		##
		for i in range(-1, max(-len(x.digits), x.exponent)-1, -1):
			if x.digits[i] != 0:
				break
			trailing_zeros += 1
		# truncate trailing zeros right of the decimal point
		# this *is* the case as exponent < 0.
		if trailing_zeros:
			digits = x.digits[:-trailing_zeros]
		else:
			digits = x.digits
			# the entire exponent is just trailing zeros(zero-weight).
		rdigits = -(x.exponent + trailing_zeros)
		ldigits = len(digits) - rdigits
		rpad = rdigits % numeric_digit_length
		if rpad:
			rpad = numeric_digit_length - rpad
	else:
		# Need the weight to be divisible by four,
		# so append zeros onto digits until it is.
		r = (x.exponent % numeric_digit_length)
		if x.exponent and r:
			digits = x.digits + ((0,) * r)
			weight = (x.exponent - r)
		else:
			digits = x.digits
			weight = x.exponent
		# The exponent is not evenly divisible by four, so
		# the weight can't simple be x.exponent as it doesn't
		# match the size of the numeric digit.
		ldigits = len(digits)
		# no fractional quantity.
		rdigits = 0
		rpad = 0

	lpad = ldigits % numeric_digit_length
	if lpad:
		lpad = numeric_digit_length - lpad
	weight += (ldigits + lpad)

	digit_groups = map(
		get1,
		groupby(
			zip(
				# group by numeric digit size
				# every four digits make up a numeric digit
				cycle((0,) * numeric_digit_length + (1,) * numeric_digit_length),

				# multiply each digit appropriately
				# for the eventual sum() into a numeric digit
				starmap(
					mul,
					zip(
						# pad with leading zeros to make
						# the cardinality of the digit sequence
						# to be evenly divisible by four,
						# the numeric digit size.
						chain(
							repeat(0, lpad),
							digits,
							repeat(0, rpad),
						),
						cycle([10**x for x in range(numeric_digit_length-1, -1, -1)]),
					)
				),
			),
			get0,
		),
	)
	return ts.numeric_pack((
		(
			(ldigits + rdigits + lpad + rpad) // numeric_digit_length, # ndigits
			(weight // numeric_digit_length) - 1, # numeric weight
			numeric_negative if x.sign == 1 else x.sign, # sign
			- x.exponent if x.exponent < 0 else 0, # dscale
		),
		list(map(sum, ([get1(y) for y in x] for x in digit_groups))),
	))

def numeric_convert_digits(d):
	i = iter(d)
	for x in str(next(i)):
		# no leading zeros
		yield int(x)
	# leading digit should not include zeros
	for y in i:
		for x in str(y).rjust(4, '0'):
			yield int(x)

numeric_signs = {
	numeric_negative : 1,
}

def numeric_unpack(x):
	header, digits = ts.numeric_unpack(x)
	npad = (header[3] - ((header[0] - (header[1] + 1)) * 4))
	return Decimal(
		DecimalTuple(
			sign = numeric_signs.get(header[2], header[2]),
			digits = chain(
				numeric_convert_digits(digits),
				(0,) * npad
			) if npad >= 0 else list(
				numeric_convert_digits(digits)
			)[:npad],
			exponent = -header[3]
		)
	)

# Map type oids to a (pack, unpack) pair.
oid_to_io = {
	pg_types.NUMERICOID : (numeric_pack, numeric_unpack),

	pg_types.DATEOID : (date_pack, date_unpack),

	pg_types.VARBITOID : bitio,
	pg_types.BITOID : bitio,

	pg_types.POINTOID : (point_pack, point_unpack),
	pg_types.BOXOID : (box_pack, box_unpack),
	pg_types.LSEGOID : (lseg_pack, lseg_unpack),
	pg_types.CIRCLEOID : (circle_pack, circle_unpack),
}

oid_to_io[pg_types.CIDROID] = (None, None)
oid_to_io[pg_types.INETOID] = (None, None)

def process_tuple(procs, tup, exception_handler):
	"""
	Call each item in `procs` with the corresponding
	item in `tup` returning the result as `type`.

	If an item in `tup` is `None`, don't process it.

	If a give transformation failes, call the given exception_handler which
	*should* raise a postgresql.exceptions.TypeIOError [with context].
	"""
	i = len(procs)
	if len(tup) != i:
		raise TypeError(
			"inconsistent items, %d processors and %d items in row" %(
				i, len(tup)
			)
		)
	r = [None] * i
	try:
		for i in range(i):
			ob = tup[i]
			if ob is None:
				continue
			r[i] = procs[i](ob)
	except Exception:
		# relying on python to imply [from current]
		exception_handler(procs, tup, i)
		raise RuntimeError("process_tuple exception handler failed to raise")
	return r

def process_chunk(procs, tupc, fail):
	return [
		process_tuple(procs, x, fail) for x in tupc
	]

try:
	# C implementation of the tuple processors.
	from .optimized import process_tuple, process_chunk
except ImportError:
	pass

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
				data = pg_types.Array(data)
			dlb = []
			for x in data.dimensions:
				dlb.append(x)
				dlb.append(1)
			return ts.array_pack((
				0, typoid, dlb,
				pack_array_elements(data.elements)
			))
	else:
		# signals string formatting
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
		# signals string formatting
		unpack_an_array = None

	return (pack_an_array, unpack_an_array)

def composite_typio(
	cio : "sequence (pack,unpack) tuples corresponding to the",
	typids : "sequence of type Oids; index must correspond to the composite's",
	attmap : "mapping of column name to index number",
	typnames : "sequence of sql type names in order",
	attnames : "sequence of attribute names in order",
	composite_name : "the name of the composite type",
):
	"""
	create the typio pair for the composite type metadata passed in.
	"""
	fpack = tuple(map(get0, cio))
	funpack = tuple(map(get1, cio))

	def raise_pack_tuple_error(procs, tup, itemnum):
		data = repr(tup[itemnum])
		if len(data) > 80:
			# Be sure not to fill screen with noise.
			data = data[:75] + ' ...'
		raise pg_exc.ColumnError(
			"failed to pack attribute %d, %s::%s, of composite %s for transfer" %(
				itemnum,
				attnames[itemnum],
				typnames[itemnum],
				composite_name,
			),
			details = {
				'context': data,
				'position' : str(itemnum)
			},
		)

	def raise_unpack_tuple_error(procs, tup, itemnum):
		data = repr(tup[itemnum])
		if len(data) > 80:
			# Be sure not to fill screen with noise.
			data = data[:75] + ' ...'
		raise pg_exc.ColumnError(
			"failed to unpack attribute %d, %s::%s, of composite %s from wire data" %(
				itemnum,
				attnames[itemnum],
				typnames[itemnum],
				composite_name,
			),
			details = {
				'context': data,
				'position' : str(itemnum),
			},
		)

	def unpack_a_record(data):
		data = tuple([x[1] for x in ts.record_unpack(data)])
		return pg_types.Row.from_sequence(
			attmap,
			process_tuple(funpack, data, raise_unpack_tuple_error),
		)

	sorted_atts = sorted(attmap.items(), key = get1)
	def pack_a_record(data):
		if isinstance(data, dict):
			data = [data.get(k) for k,_ in sorted_atts]
		return ts.record_pack(
			tuple(zip(
				typids,
				process_tuple(fpack, tuple(data), raise_pack_tuple_error)
			))
		)
	return (pack_a_record, unpack_a_record)

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

	def encode(self, string_data):
		return self._encode(string_data)[0]

	def decode(self, bytes_data):
		return self._decode(bytes_data)[0]

	def decodes(self, iter):
		"""
		Decode the items in the iterable from the configured encoding.
		"""
		return map(compose((self._decode, get0)), iter)

	def encodes(self, iter):
		"""
		Encode the items in the iterable in the configured encoding.
		"""
		return map(compose((self._encode, get0)), iter)

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
		if isinstance(xml, str):
			# if it's a string, encode and return.
			return self._encode(xml)[0]
		elif isinstance(xml, tuple):
			# if it's a tuple, encode and return the joined items.
			return b''.join((
				self._encode(
					x if isinstance(x, str) else pg_types.etree.tostring(x)
				)[0] for x in xml
			))
		return self._encode(pg_types.etree.tostring(xml))[0]

	def xml_unpack(self, xmldata):
		xml_or_frag = self._decode(xmldata)[0]
		try:
			return pg_types.etree.XML(xml_or_frag)
		except Exception:
			# try it again, but return the sequence of children.
			return tuple(pg_types.etree.XML('<x>' + xml_or_frag + '</x>'))

	def attribute_map(self, pq_descriptor):
		return zip(self.decodes(pq_descriptor.keys()), count())

	def __init__(self):
		self.encoding = None
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
			pg_types.ACLITEMOID : (None, None), # No binary functions.
			pg_types.NAMEOID : (None, None),
			pg_types.BPCHAROID : (None, None),
			pg_types.VARCHAROID : (None, None),
			pg_types.CSTRINGOID : (None, None),
			pg_types.TEXTOID : (None, None),
			pg_types.REGPROCEDUREOID : (None, None),
			pg_types.REGTYPEOID : (None, None),
			pg_types.REGPROCOID : (None, None),

			pg_types.XMLOID : (
				self.xml_pack, self.xml_unpack
			),
		}
		self.typmeta = {}

	def sql_type_from_oid(self, oid):
		if oid in self.typmeta:
			nsp, name, *_ = self.typmeta[oid]
			return pg_str.quote_ident(nsp) + '.' + pg_str.quote_ident(name)
		return 'pg_catalog.' + pg_types.oid_to_name.get(oid)

	def type_from_oid(self, oid):
		typ = pg_types.oid_to_type.get(oid)
		if typ is None:
			if oid in self.typmeta:
				tm = self.typmeta[oid]
				if tm[2]:
					# composite/row
					typ = tuple
				elif tm[3]:
					# array
					typ = list
		return typ

	def set_encoding(self, value):
		self.encoding = value.lower().strip()
		enc = pg_enc_aliases.get_python_name(self.encoding)
		ci = codecs.lookup(enc or self.encoding)
		self._encode = ci[0]
		self._decode = ci[1]

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
				typnamespace, typname, typtype, typlen, typelem, typrelid, \
					ae_typid, ae_hasbin_input, ae_hasbin_output = ti
				self.typmeta[typid] = (
					typnamespace, typname, typrelid, int(typelem) if ae_typid else None
				)
				if typrelid:
					# Composite/Complex/Row Type
					#
					# So the attribute name map, the column I/O, and type Oids are
					# needed.
					attmap = {}
					cio = []
					typids = []
					attnames = []
					i = 0
					for x in self.lookup_composite_type_info(typrelid):
						attmap[x[1]] = i
						attnames.append(x[1])
						typids.append(x[0])
						pack, unpack = self.resolve(
							x[0], list(from_resolution_of) + [typid]
						)
						cio.append((pack or self.encode, unpack or self.decode))
						i += 1
					self._cache[typid] = typio = composite_typio(
						cio, typids, attmap, list(
							map(self.sql_type_from_oid, typids)
						), attnames,
						pg_str.quote_ident(typnamespace) + '.' + \
						pg_str.quote_ident(typname),
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
