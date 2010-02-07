##
# datetime - timewise
#
# I/O routines for date, time, timetz, timestamp, timestamptz, and interval.
# Supported by the datetime module.
##
"""
 time_io
  Floating-point based time I/O.

 time_noday_io
  Floating-point based time I/O with noday-intervals.

 time64_io
  long-long based time I/O.

 time64_noday_io
  long-long based time I/O with noday-intervals.
"""
import datetime
import warnings
from functools import partial
from operator import methodcaller, add

from ...python.datetime import UTC, FixedOffset
from ...python.functools import Composition as compose
from ...exceptions import TypeConversionWarning

from .. import \
	DATEOID, INTERVALOID, \
	TIMEOID, TIMETZOID, \
	TIMESTAMPOID, TIMESTAMPTZOID

from . import lib

oid_to_type = {
	DATEOID: datetime.date,
	TIMESTAMPOID: datetime.datetime,
	TIMESTAMPTZOID: datetime.datetime,
	TIMEOID: datetime.time,
	TIMETZOID: datetime.time,

	# XXX: datetime.timedelta doesn't support months.
	INTERVALOID: datetime.timedelta,
}

pg_epoch_datetime = datetime.datetime(2000, 1, 1)
pg_epoch_date = pg_epoch_datetime.date()
pg_date_offset = pg_epoch_date.toordinal()

## Difference between PostgreSQL epoch and Unix epoch.
## Used to convert a PostgreSQL ordinal to an ordinal usable by datetime
pg_time_days = (pg_date_offset - datetime.date(1970, 1, 1).toordinal())

toordinal = methodcaller("toordinal")
convert_to_utc = methodcaller('astimezone', UTC)
remove_tzinfo = methodcaller('replace', tzinfo = None)
set_as_utc = methodcaller('replace', tzinfo = UTC)

date_pack = compose((
	toordinal, partial(add, -pg_date_offset), lib.date_pack,
))
date_unpack = compose((
	lib.date_unpack, partial(add, pg_date_offset), datetime.date.fromordinal
))

seconds_in_day = 24 * 60 * 60
seconds_in_hour = 60 * 60

def timestamp_pack(x):
	"""
	Create a (seconds, microseconds) pair from a `datetime.datetime` instance.
	"""
	d = (x - pg_epoch_datetime)
	return ((d.days * seconds_in_day) + d.seconds, d.microseconds)

def timestamp_unpack(seconds, timedelta = datetime.timedelta):
	"""
	Create a `datetime.datetime` instance from a (seconds, microseconds) pair.
	"""
	return pg_epoch_datetime + timedelta(
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

def time_unpack(seconds_ms, time = datetime.time):
	"""
	Create a `datetime.time` instance from a (seconds, microseconds) pair.
	Seconds being offset from epoch.
	"""
	seconds, ms = seconds_ms
	minutes, sec = divmod(seconds, 60)
	hours, min = divmod(minutes, 60)
	return time(hours, min, sec, ms)

def interval_pack(x):
	"""
	Create a (months, days, (seconds, microseconds)) tuple from a
	`datetime.timedelta` instance.
	"""
	return (
		0, x.days, (x.seconds, x.microseconds)
	)

def interval_unpack(mds, timedelta = datetime.timedelta):
	"""
	Given a (months, days, (seconds, microseconds)) tuple, create a
	`datetime.timedelta` instance.
	"""
	months, days, seconds_ms = mds
	if months != 0:
		w = pg_exc.TypeConversionWarning(
			"datetime.timedelta cannot represent relative intervals",
			details = {
				'hint': 'An interval was unpacked with a non-zero "month" field.'
			},
			source = 'DRIVER'
		)
		warnings.warn(w)
	sec, ms = seconds_ms
	return timedelta(
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

FloatTimes = False
IntTimes = True
NoDay = True
WithDay = False

id_to_io = {
	(FloatTimes, TIMEOID) : (
		compose((time_pack, lib.time_pack)),
		compose((lib.time_unpack, time_unpack)),
		datetime.time
	),
	(FloatTimes, TIMETZOID) : (
		compose((timetz_pack, lib.timetz_pack)),
		compose((lib.timetz_unpack, timetz_unpack)),
		datetime.time
	),
	(FloatTimes, TIMESTAMPOID) : (
		compose((timestamp_pack, lib.time_pack)),
		compose((lib.time_unpack, timestamp_unpack)),
		datetime.datetime
	),
	(FloatTimes, TIMESTAMPTZOID) : (
		compose((convert_to_utc, remove_tzinfo, timestamp_pack, lib.time_pack)),
		compose((lib.time_unpack, timestamp_unpack, set_as_utc)),
		datetime.datetime
	),
	(FloatTimes, WithDay, INTERVALOID): (
		compose((interval_pack, lib.interval_pack)),
		compose((lib.interval_unpack, interval_unpack)),
		datetime.timedelta
	),
	(FloatTimes, NoDay, INTERVALOID): (
		compose((interval_pack, lib.interval_noday_pack)),
		compose((lib.interval_noday_unpack, interval_unpack)),
		datetime.timedelta
	),

	(IntTimes, TIMEOID) : (
		compose((time_pack, lib.time64_pack)),
		compose((lib.time64_unpack, time_unpack)),
		datetime.time
	),
	(IntTimes, TIMETZOID) : (
		compose((timetz_pack, lib.timetz64_pack)),
		compose((lib.timetz64_unpack, timetz_unpack)),
		datetime.time
	),
	(IntTimes, TIMESTAMPOID) : (
		compose((timestamp_pack, lib.time64_pack)),
		compose((lib.time64_unpack, timestamp_unpack)),
		datetime.datetime
	),
	(IntTimes, TIMESTAMPTZOID) : (
		compose((convert_to_utc, remove_tzinfo, timestamp_pack, lib.time64_pack)),
		compose((lib.time64_unpack, timestamp_unpack, set_as_utc)),
		datetime.datetime
	),
	(IntTimes, WithDay, INTERVALOID) : (
		compose((interval_pack, lib.interval64_pack)),
		compose((lib.interval64_unpack, interval_unpack)),
		datetime.timedelta
	),
	(IntTimes, NoDay, INTERVALOID) : (
		compose((interval_pack, lib.interval64_noday_pack)),
		compose((lib.interval64_noday_unpack, interval_unpack)),
		datetime.timedelta
	),
}

##
# Identify whether it's IntTimes or FloatTimes
def time_type(typio):
	idt = typio.database.settings.get('integer_datetimes', None)
	if idt is None:
		# assume its absence means its on after 9.0
		return bool(typio.database.version_info >= (9,0))
	elif idt.__class__ is bool:
		return idt
	else:
		return (idt.lower() in ('on', 'true', 't', True))

def select_format(oid, typio, get = id_to_io.__getitem__):
	return get((time_type(typio), oid))

def select_day_format(oid, typio, get = id_to_io.__getitem__):
	return get((time_type(typio), typio.database.version_info <= (8,0), oid))

oid_to_io = {
	DATEOID : (date_pack, date_unpack, datetime.date),
	TIMEOID : select_format,
	TIMETZOID : select_format,
	TIMESTAMPOID : select_format,
	TIMESTAMPTZOID : select_format,
	INTERVALOID : select_day_format,
}
