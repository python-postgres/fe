##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
DB-API 2.0 conforming interface using postgresql.driver.
"""
threadsafety = 1
paramstyle = 'pyformat'
apilevel = '2.0'

from operator import itemgetter
import re
import postgresql.driver as pg_driver
import postgresql.types as pg_type
import postgresql.string as pg_str
import datetime, time

find_parameters = re.compile(r'%\(([^)]+)\)s')

from postgresql.exceptions import \
	Error, DataError, InternalError, \
	ICVError as IntegrityError, \
	SEARVError as ProgrammingError, \
	IRError as OperationalError, \
	DriverError as InterfaceError, \
	Warning
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
	Given a sequence of keywords, `nseq`, yield each mapping object in `seq`
	as a tuple whose objects are the values of the keys specified in `nseq` in
	an order consistent with that in `nseq`
	"""
	for x in seq:
		yield [x[y] for y in nseq]

class Cursor(object):
	rowcount = -1
	arraysize = 1
	description = None

	def __init__(self, C):
		self.connection = C
		self.database = C.database
		self.description = ()
		self.__portals = []

	def setinputsizes(self, sizes):
		pass

	def setoutputsize(self, sizes, columns = None):
		pass

	def callproc(self, proname, args):
		p = self.database.prepare("SELECT %s(%s)" %(
			proname, ','.join([
				'$%d' %(x,) for x in range(1, len(args) + 1)
			])
		))
		self.__portals.insert(0, p(*args))
		return args

	def fetchone(self):
		try:
			return next(self._portal)
		except StopIteration:
			return None

	def __next__(self):
		return next(self._portal)
	def __iter__(self):
		return self

	def fetchmany(self, arraysize = None):
		return self._portal.read(arraysize or self.arraysize or 1)

	def fetchall(self):
		return self._portal.read()

	def nextset(self):
		del self._portal
		return len(self.__portals) or None

	def execute(self, query, parameters = None):
		if parameters:
			parameters = list(parameters.items())
			pnmap = {}
			plist = []
			for x in range(len(parameters)):
				pnmap[parameters[x][0]] = '$' + str(x + 1)
				plist.append(parameters[x][1])
			# Substitute %(key)s with the $x positional parameter number
			rqparts = []
			for qpart in pg_str.split(query):
				if type(qpart) is type(()):
					# quoted section
					rqparts.append(qpart)
				else:
					rqparts.append(qpart % pnmap)
			q = self.database.prepare(pg_str.unsplit(rqparts))
			r = q(*plist)
		else:
			q = self.database.prepare(query)
			r = q()

		if q._output is not None and len(q._output) > 0:
			# name, relationId, columnNumber, typeId, typlen, typmod, format
			self.description = tuple([
				(self.database.typio.decode(x[0]), dbapi_type(x[3]),
				None, None, None, None, None)
				for x in q._output
			])
			self.__portals.insert(0, r)
		else:
			self.description = None
			if self.__portals:
				del self._portal
		return self

	def _convert_query(self, string, map):
		rqparts = []
		for qpart in pg_str.split(string):
			if type(qpart) is type(()):
				rqparts.append(qpart)
			else:
				rqparts.append(qpart % map)
		return pg_str.unsplit(rqparts)

	def _statement_params(self, string):
		map = {}
		param_num = 1
		for qpart in pg_str.split(string):
			if type(qpart) is not type(()):
				for x in find_parameters.finditer(qpart):
					pname = x.group(1)
					if pname not in map:
						map[pname] = param_num
						param_num += 1
		return map

	def executemany(self, query, param_iter):
		mapseq = list(self._statement_params(query).items())
		realquery = self._convert_query(query, {
			k : '$' + str(v) for k,v in mapseq
		})
		mapseq.sort(key = itemgetter(1))
		nseq = [x[0] for x in mapseq]
		q = self.database.prepare(realquery)
		q.prepare()
		if q._input is not None:
			q.load(convert_keyword_parameters(nseq, param_iter))
		else:
			q.load(param_iter)
		return self

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
		Error, DataError, InternalError, \
		ICVError as IntegrityError, \
		SEARVError as ProgrammingError, \
		IRError as OperationalError, \
		DriverError as InterfaceError, \
		Warning
	DatabaseError = DatabaseError
	NotSupportedError = NotSupportedError

	def __init__(self, connection):
		self.database = connection
		self.database.xact.start()

	def close(self):
		if self.database.closed:
			raise Error("connection already closed")
		self.database.close()

	def cursor(self):
		return Cursor(self)

	def commit(self):
		self.database.xact.commit()
		self.database.xact.start()

	def rollback(self):
		self.database.xact.abort()
		self.database.xact.start()

def connect(**kw):
	"""
	Create a DB-API connection using the given parameters.
	"""
	db = pg_driver.connect(**kw)
	dbapi = Connection(db)
	return dbapi
