##
# copyright 2007, pg/python project.
# http://python.projects.postgresql.org
##
"""
DB-API 2.0 conforming interface on postgresql.driver.pgapi.
"""
threadsafety = 1
paramstyle = 'pyformat'
apilevel = '2.0'

import postgresql.driver.pgapi as pg_driver
import postgresql.types as pg_type
import postgresql.strings as pg_str
import datetime, time

from postgresql.exceptions import \
	Error, DataError, InternalError, IntegrityError, \
	SEARVError as ProgrammingError, \
	IRError as OperationalError, \
	Warning
class InterfaceError(Error):
	pass
DatabaseError = Error
class NotSupportedError(DatabaseError):
	pass

STRING = str
BINARY = bytes
NUMBER = int
DATETIME = datetime.datetime
ROWID = int

Binary = BINARY
Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
DateFromTicks = lambda x: Date(*time.localtime(x)[:3])
TimeFromTicks = lambda x: Time(*time.localtime(x)[3:6])
TimestampFromTicks = lambda x: Timestamp(*time.localtime(x)[:7])

def dbapi_type(typid):
	if typid in (
		pg_type.TEXTOID,
		pg_type.CHAROID,
		pg_type.VARCHAROID,
		pg_type.NAMEOID,
		pg_type.CSTRINGOID,
	):
		return STRING
	elif typid == pg_type.BYTEAOID:
		return BINARY
	elif typid in (pg_type.INT8OID, pg_type.INT2OID, pg_type.INT4OID):
		return NUMBER
	elif typid in (pg_type.TIMESTAMPOID, pg_type.TIMESTAMPTZOID):
		return DATETIME
	elif typid == pg_type.OIDOID:
		return ROWID

def convert_keyword_parameters(nseq, seq):
	"""
	Given a sequence of keywords, `nseq`, yield each mapping object in `seq` as a
	tuple whose objects are the values of the keys specified in `nseq` in an
	order consistent with that in `nseq`
	"""
	for x in seq:
		yield [x[y] for y in nseq]

class Cursor(object):
	rowcount = -1
	arraysize = 1
	description = None

	def __init__(self, C):
		self.pg_api_c = C
		self.description = ()
		self.__portals = []

	def setinputsizes(self, sizes):
		pass

	def setoutputsize(self, sizes, columns = None):
		pass
	
	def callproc(self, proname, args):
		p = self.pg_api_c.query("SELECT %s(%s)" %(
			proname, ','.join([
				'$%d' %(x,) for x in range(1, len(args) + 1)
			])
		))
		self.__portals.insert(0, p(*args))
		return args

	def fetchone(self):
		try:
			return self._portal.next()
		except StopIteration:
			return None

	def next(self):
		return self._portal.next()
	def __iter__(self):
		return self

	def fetchmany(self, arraysize = None):
		return self._portal.read(arraysize or self.arraysize or 1)

	def fetchall(self):
		return self._portal.read()

	def nextset(self):
		del self._portal
		return len(self.__portals) or None

	def _mkquery(self, query, parameters):
		parameters = list(dict(parameters).items())
		pnmap = {}
		plist = []
		nseq = []
		for x in range(len(parameters)):
			pnmap[parameters[x][0]] = '$' + str(x + 1)
			plist.append(parameters[x][1])
			nseq.append(parameters[x][0])
		# Substitute %(key)s with the $x positional parameter number
		rqparts = []
		for qpart in pg_str.split(query):
			if type(qpart) is type(()):
				rqparts.append(qpart)
			else:
				rqparts.append(qpart % pnmap)
		q = self.pg_api_c.query(pg_str.unsplit(rqparts))
		return q, nseq, plist

	def execute(self, query, parameters = None):
		if parameters:
			q, nseq, plist = self._mkquery(query, parameters)
			r = q(*plist)
		else:
			q = self.pg_api_c.query(query)
			r = q()
		if q.output is not None and len(q.output) > 0:
			# name, relationId, columnNumber, typeId, typlen, typmod, format
			self.description = tuple([
				(x[0], dbapi_type(x[3]),
					None, None, None, None, None)
				for x in q.output
			])
			self.__portals.insert(0, r)
		else:
			self.description = None
			if self.__portals:
				del self._portal

	def executemany(self, query, pseq):
		if pseq:
			q, nseq, _ = self._mkquery(query, pseq[0])
			q.load(convert_keyword_parameters(nseq, pseq))

	def close(self):
		self.description = None
		ps = self.__portals
		if self.__portals is not None:
			self.__portals = None
			for p in ps: p.close()

	# Describe the "real" cursor as a "portal".
	# This should keep ambiguous terminology out of the picture.
	def _portal():
		def fget(self):
			if self.__portals is None:
				raise Error("access on closed cursor")
			try:
				p = self.__portals[0]
			except IndexError:
				raise InterfaceError("no portal on stack")
			return p
		def fdel(self):
			try:
				del self.__portals[0]
			except IndexError:
				raise InterfaceError("no portal on stack")
		return locals()
	_portal = property(**_portal())

class Connection(object):
	"""
	DB-API 2.0 connection implementation for PG-API connection objects.
	"""
	from postgresql.exceptions import \
		Error, DataError, InternalError, IntegrityError, \
		SEARVError as ProgrammingError, \
		IRError as OperationalError, \
		Warning
	InterfaceError = InterfaceError
	DatabaseError = DatabaseError
	NotSupportedError = NotSupportedError

	def __init__(self, connection):
		self.pg_api = connection
		self.pg_api.xact.start()

	def close(self):
		if self.pg_api.closed:
			raise Error("connection already closed")
		self.pg_api.close()

	def cursor(self):
		return Cursor(self.pg_api)

	def commit(self):
		self.pg_api.xact.checkpoint()

	def rollback(self):
		self.pg_api.xact.restart()

_connectors = {}
def connect(**kw):
	kwi = kw.items()
	kwi.sort()
	kwi = tuple(kwi)
	con = _connectors.get(kwi)
	if not con:
		con = _connectors[kwi] = pg_driver.connector(**kw)
	return Connection(con())
