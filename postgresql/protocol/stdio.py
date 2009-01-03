##
# copyright 2007, pg/python project.
# http://python.projects.postgresql.org
##
"""
Typical PostgreSQL type I/O routines

I/O maps for converting wire data into standard Python objects.
"""
import datetime
import socket
from decimal import Decimal
from netaddr.address import CIDR, Addr, EUI

import postgresql.types as pg_type
import postgresql.protocol.typio as pg_typio

pg_epoch_datetime = datetime.datetime(2000, 1, 1)
pg_epoch_date = pg_epoch_datetime.date()
pg_date_offset = pg_epoch_date.toordinal()
## Difference between Postgres epoch and Unix epoch.
## Used to convert a Postgres ordinal to an ordinal usable by datetime
pg_time_days = (pg_date_offset - datetime.date(1970, 1, 1).toordinal())

date_io = (
	lambda x: pg_typio.date_pack(x.toordinal() - pg_date_offset),
	lambda x: datetime.date.fromordinal(pg_date_offset + pg_typio.date_unpack(x))
)

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

##
# FIXME: Don't ignore time zone.
def timetz_pack(x):
	"""
	Create a ((seconds, microseconds), timezone) tuple from a `datetime.time`
	instance.
	"""
	return (time_pack(x), 0)

def timetz_unpack(tstz):
	"""
	Create a `datetime.time` instance from a ((seconds, microseconds), timezone)
	tuple.
	"""
	return time_unpack(tstz[0])

datetimemap = {
	pg_type.INTERVALOID : (interval_pack, interval_unpack),
	pg_type.TIMEOID : (time_pack, time_unpack),
	pg_type.TIMESTAMPOID : (time_pack, time_unpack),
}

time_io = {
	pg_type.TIMEOID : (
		lambda x: pg_typio.time_pack(time_pack(x)),
		lambda x: time_unpack(pg_typio.time_unpack(x))
	),
	pg_type.TIMETZOID : (
		lambda x: pg_typio.timetz_pack(timetz_pack(x)),
		lambda x: timetz_unpack(pg_typio.timetz_unpack(x))
	),
	pg_type.TIMESTAMPOID : (
		lambda x: pg_typio.time_pack(timestamp_pack(x)),
		lambda x: timestamp_unpack(pg_typio.time_unpack(x))
	),
	pg_type.TIMESTAMPTZOID : (
		lambda x: pg_typio.time_pack(timestamp_pack(x)),
		lambda x: timestamp_unpack(pg_typio.time_unpack(x))
	),
	pg_type.INTERVALOID : (
		lambda x: pg_typio.interval_pack(interval_pack(x)),
		lambda x: interval_unpack(pg_typio.interval_unpack(x))
	),
}
time_io_noday = time_io.copy()
time_io_noday[pg_type.INTERVALOID] = (
	lambda x: pg_typio.interval_noday_pack(interval_pack(x)),
	lambda x: interval_unpack(pg_typio.interval_noday_unpack(x))
)

time64_io = {
	pg_type.TIMEOID : (
		lambda x: pg_typio.time64_pack(time_pack(x)),
		lambda x: time_unpack(pg_typio.time64_unpack(x))
	),
	pg_type.TIMETZOID : (
		lambda x: pg_typio.timetz64_pack(timetz_pack(x)),
		lambda x: timetz_unpack(pg_typio.timetz64_unpack(x))
	),
	pg_type.TIMESTAMPOID : (
		lambda x: pg_typio.time64_pack(timestamp_pack(x)),
		lambda x: timestamp_unpack(pg_typio.time64_unpack(x))
	),
	pg_type.TIMESTAMPTZOID : (
		lambda x: pg_typio.time64_pack(timestamp_pack(x)),
		lambda x: timestamp_unpack(pg_typio.time64_unpack(x))
	),
	pg_type.INTERVALOID : (
		lambda x: pg_typio.interval64_pack(interval_pack(x)),
		lambda x: interval_unpack(pg_typio.interval64_unpack(x))
	),
}
time64_io_noday = time64_io.copy()
time64_io_noday[pg_type.INTERVALOID] = (
	lambda x: pg_typio.interval64_noday_pack(interval_pack(x)),
	lambda x: interval_unpack(pg_typio.interval64_noday_unpack(x))
)

def circle_unpack(x):
	"""
	Given raw circle data, (x, y, radius), make a circle instance using
	`postgresql.types.circle`.
	"""
	return pg_type.circle(((x[0], x[1]), x[2]))

def inet_pack(x):
	d = socket.inet_pton(
		x.addr_type == 4 and socket.AF_INET or socket.AF_INET6,
		str(x)
	)
	return pg_typio.cidr_pack((x.addr_type == 4 and 2 or 3, x.prefixlen(), d))
def inet_unpack(x):
	"""
	Given serialized inet data, make a `netaddr.address.Addr` instance.
	"""
	fam, mask, data = pg_typio.cidr_unpack(x)
	d = socket.inet_ntop(
		fam == 2 and socket.AF_INET or socket.AF_INET6, data
	)
	# cidr will determine family from len of data.
	return Addr('%s' %(d,))

def cidr_pack(x):
	# INET family is 2, INET6 family is 3.
	addr, mask = str(x).split('/', 1)
	d = socket.inet_pton(
		x.addr_type == 4 and socket.AF_INET or socket.AF_INET6, addr
	)
	return pg_typio.cidr_pack((x.addr_type == 4 and 2 or 3, x.prefixlen(), d))
def cidr_unpack(x):
	"""
	Given serialized cidr data, make a `netaddr.address.CIDR` instance.
	"""
	fam, mask, data = pg_typio.cidr_unpack(x)
	d = socket.inet_ntop(
		fam == 2 and socket.AF_INET or socket.AF_INET6, data
	)
	return CIDR('%s/%d' %(d, mask))

def macaddr_pack(x):
	return ''.join([
		chr(int(y, base = 16)) for y in str(x).split('-')
	])
def macaddr_unpack(x):
	return EUI(':'.join([('%.2x' %(ord(y),)) for y in x]))

def two_pair(x):
	'Make a pair of pairs out of a sequence of four objects'
	return ((x[0], x[1]), (x[2], x[3]))

def varbit_pack(x):
	return pg_typio.varbit_pack((x.bits, x.data))
def varbit_unpack(x):
	return pg_type.varbit.from_bits(*pg_typio.varbit_unpack(x))
bitio = (varbit_pack, varbit_unpack)

def point_unpack(x):
	return pg_type.point(pg_typio.point_unpack(x))	

# Map type oids to a (pack, unpack) pair.
stdio = {
	pg_type.NUMERICOID : (str, Decimal),

	pg_type.DATEOID : date_io,
	pg_type.VARBITOID : bitio,
	pg_type.BITOID : bitio,

	pg_type.INETOID : (
		inet_pack,
		inet_unpack,
	),

	pg_type.CIDROID : (
		cidr_pack,
		cidr_unpack,
	),

	pg_type.MACADDROID : (
		macaddr_pack,
		macaddr_unpack,
	),

	pg_type.POINTOID : (
		pg_typio.point_pack,
		point_unpack,
	),

	pg_type.BOXOID : (
		lambda x: pg_typio.box_pack((x[0][0], x[0][1], x[1][0], x[1][1])),
		lambda x: pg_type.box(two_pair(pg_typio.box_unpack(x)))
	),

	pg_type.LSEGOID : (
		lambda x: pg_typio.lseg_pack((x[0][0], x[0][1], x[1][0], x[1][1])),
		lambda x: pg_type.lseg(two_pair(pg_typio.lseg_unpack(x)))
	),

	pg_type.CIRCLEOID : (
		lambda x: pg_typio.circle_pack((x[0][0], x[0][1], x[1])),
		lambda x: circle_unpack(pg_typio.circle_unpack(x))
	),
}
