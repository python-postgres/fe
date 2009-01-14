##
# copyright 2009, pg/python project.
# http://python.projects.postgresql.org
##
def date_pack(x):
	return ts.date_pack(x.toordinal() - pg_date_offset)

def date_unpack(x):
	return datetime.date.fromordinal(pg_date_offset + pg_typio.date_unpack(x))

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
	t.tzinfo = FixedOffset(tstz[1])

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
		lambda x: pg_typio.timetz_pack(timetz_pack(x)),
		lambda x: timetz_unpack(pg_typio.timetz_unpack(x))
	),
	pg_types.TIMESTAMPOID : (
		lambda x: pg_typio.time_pack(timestamp_pack(x)),
		lambda x: timestamp_unpack(pg_typio.time_unpack(x))
	),
	pg_types.TIMESTAMPTZOID : (
		lambda x: pg_typio.time_pack(timestamp_pack(x)),
		lambda x: timestamp_unpack(pg_typio.time_unpack(x))
	),
	pg_types.INTERVALOID : (
		lambda x: pg_typio.interval_pack(interval_pack(x)),
		lambda x: interval_unpack(pg_typio.interval_unpack(x))
	),
}
time_io_noday = time_io.copy()
time_io_noday[pg_types.INTERVALOID] = (
	lambda x: pg_typio.interval_noday_pack(interval_pack(x)),
	lambda x: interval_unpack(pg_typio.interval_noday_unpack(x))
)

time64_io = {
	pg_types.TIMEOID : (
		lambda x: pg_typio.time64_pack(time_pack(x)),
		lambda x: time_unpack(pg_typio.time64_unpack(x))
	),
	pg_types.TIMETZOID : (
		lambda x: pg_typio.timetz64_pack(timetz_pack(x)),
		lambda x: timetz_unpack(pg_typio.timetz64_unpack(x))
	),
	pg_types.TIMESTAMPOID : (
		lambda x: pg_typio.time64_pack(timestamp_pack(x)),
		lambda x: timestamp_unpack(pg_typio.time64_unpack(x))
	),
	pg_types.TIMESTAMPTZOID : (
		lambda x: pg_typio.time64_pack(timestamp_pack(x)),
		lambda x: timestamp_unpack(pg_typio.time64_unpack(x))
	),
	pg_types.INTERVALOID : (
		lambda x: pg_typio.interval64_pack(interval_pack(x)),
		lambda x: interval_unpack(pg_typio.interval64_unpack(x))
	),
}
time64_io_noday = time64_io.copy()
time64_io_noday[pg_types.INTERVALOID] = (
	lambda x: pg_typio.interval64_noday_pack(interval_pack(x)),
	lambda x: interval_unpack(pg_typio.interval64_noday_unpack(x))
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
	lambda x: pg_typio.circle_pack((x[0][0], x[0][1], x[1])),
def circle_unpack(x):
	lambda x: circle_unpack(pg_typio.circle_unpack(x))

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
