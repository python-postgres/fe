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
from functools import partial
import datetime
import time
import re

from .. import driver as pg_driver
from .. import types as pg_type
from .. import string as pg_str

##
# Basically, is it a mapping, or is it a sequence?
# If findall()'s first index is 's', it's a sequence.
# If it starts with '(', it's mapping.
# The pain here is due to a need to recognize any %% escapes.
parameters_re = re.compile(
	r'(?:%%)+|%(s|[(][^)]*[)]s)'
)
def percent_parameters(sql):
	# filter any %% matches(empty strings).
	return [
		x for x in parameters_re.findall(sql) if x
	]

def convert_keywords(keys, mapping):
	return [
		mapping[k] for k in keys
	]

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
		self.__portals.insert(0, p._cursor(*args))
		return args

	def fetchone(self):
		try:
			return next(self._portal)
		except StopIteration:
			return None

	def __next__(self):
		return next(self._portal)
	next = __next__
	def __iter__(self):
		return self

	def fetchmany(self, arraysize = None):
		return self._portal.read(arraysize or self.arraysize or 1)

	def fetchall(self):
		return self._portal.read()

	def nextset(self):
		del self._portal
		return len(self.__portals) or None

	def _convert_query(self, string):
		parts = list(pg_str.split(string))
		style = None
		count = 0
		keys = []
		kmap = {}
		transformer = tuple
		rparts = []
		for part in parts:
			if type(part) is type(()):
				# skip quoted portions
				rparts.append(part)
			else:
				r = percent_parameters(part)
				pcount = 0
				for x in r:
					if x == 's':
						pcount += 1
					else:
						x = x[1:-2]
						if x not in keys:
							kmap[x] = '$' + str(len(keys) + 1)
							keys.append(x)
				if r:
					if pcount:
						# format
						params = tuple([
							'$' + str(i+1) for i in range(count, count + pcount)
						])
						count += pcount
						rparts.append(part % params)
					else:
						# pyformat
						rparts.append(part % kmap)
				else:
					# no parameters identified in string
					rparts.append(part)

		if keys:
			if count:
				raise TypeError(
					"keyword parameters and positional parameters used in query"
				)
			transformer = partial(convert_keywords, keys)
			count = len(keys)

		return (pg_str.unsplit(rparts) if rparts else string, transformer, count)

	def execute(self, statement, parameters = ()):
		sql, pxf, nparams = self._convert_query(statement)
		if nparams != -1 and len(parameters) != nparams:
			raise TypeError(
				"statement require %d parameters, given %d" %(
					nparams, len(parameters)
				)
			)
		ps = self.database.prepare(sql)
		c = ps._cursor(*pxf(parameters))
		if ps._output is not None and len(ps._output) > 0:
			# name, relationId, columnNumber, typeId, typlen, typmod, format
			self.description = tuple([
				(self.database.typio.decode(x[0]), dbapi_type(x[3]),
				None, None, None, None, None)
				for x in ps._output
			])
			self.__portals.insert(0, c)
		else:
			self.description = None
			if self.__portals:
				del self._portal
		return self

	def executemany(self, statement, parameters):
		sql, pxf, nparams = self._convert_query(statement)
		ps = self.database.prepare(sql)
		if ps._input is not None:
			ps.load(map(pxf, parameters))
		else:
			ps.load(parameters)
		return self

	def close(self):
		self.description = None
		ps = self.__portals
		if self.__portals is not None:
			self.__portals = None
			for p in ps: p.close()

	# Describe the "real" cursor as a "portal".
	# This should keep ambiguous terminology out of adaptor.
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

	def autocommit_set(self, val):
		if val:
			# already in autocommit mode.
			if self._xact is None:
				return
			self._xact.rollback()
			self._xact = None
		else:
			if self._xact is not None:
				return
			self._xact = self.database.xact()
			self._xact.start()

	def autocommit_get(self):
		return self._xact is None

	def autocommit_del(self):
		self.autocommit = False

	autocommit = property(
		fget = autocommit_get,
		fset = autocommit_set,
		fdel = autocommit_del,
	)
	del autocommit_set, autocommit_get, autocommit_del

	def __init__(self, connection):
		self.database = connection
		self._xact = self.database.xact()
		self._xact.start()

	def close(self):
		if self.database.closed:
			err = Error(
				"connection already closed",
				source = 'DRIVER',
			)
			self.database.ife_descend(err)
			err.raise_exception()
		self.database.close()

	def cursor(self):
		return Cursor(self)

	def commit(self):
		if self._xact is None:
			err = InterfaceError(
				"commit on connection in autocommit mode",
				source = 'DRIVER',
				details = {
					'hint': 'The "autocommit" property on the connection was set to True.'
				}
			)
			self.database.ife_descend(err)
			err.raise_exception()
		self._xact.commit()
		self._xact = self.database.xact()
		self._xact.start()

	def rollback(self):
		if self._xact is None:
			err = InterfaceError(
				"rollback on connection in autocommit mode",
				source = 'DRIVER',
				details = {
					'hint': 'The "autocommit" property on the connection was set to True.'
				}
			)
			self.database.ife_descend(err)
			err.raise_exception()
		self._xact.rollback()
		self._xact = self.database.xact()
		self._xact.start()

def connect(**kw):
	"""
	Create a DB-API connection using the given parameters.
	"""
	db = pg_driver.connect(**kw)
	dbapi = Connection(db)
	return dbapi
