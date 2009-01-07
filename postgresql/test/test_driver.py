##
# copyright 2008, pg/python project.
# http://python.projects.postgresql.org
##
import sys
import os
import unittest
import threading
import time
import datetime

import postgresql.exceptions as pg_exc
import postgresql.protocol.client3 as c3

type_samples = (
	('smallint', (
			((1 << 16) / 2) - 1, - ((1 << 16) / 2),
			-1, 0, 1,
		),
	),
	('int', (
			((1 << 32) / 2) - 1, - ((1 << 32) / 2),
			-1, 0, 1,
		),
	),
	('bigint', (
			((1 << 64) / 2) - 1, - ((1 << 64) / 2),
			-1, 0, 1,
		),
	),
	('bytea', (
			''.join([chr(x) for x in range(256)]),
			''.join([chr(x) for x in range(255, -1, -1)]),
		),
	),
	('smallint[]', (
			(123,321,-123,-321),
		),
	),
	('int[]', (
			(123,321,-123,-321),
		),
	),
	('bigint[]', (
			(0xFFFFFFFFFFFF, -0xFFFFFFFFFFFF),
		),
	),
	('varchar[]', (
			("foo", "bar",),
			("foo", "bar",),
		),
	),
	('timestamp', (
			datetime.datetime(2000,1,1,5,25,10),
			datetime.datetime(500,1,1,5,25,10),
		),
	)
)


class test_pgapi(unittest.TestCase):
	"test features intended to be specific to tracenull"
	def testInterrupt(self):
		# The point is to test that pg.interrupt() works.
		#
		# To do this, we start a thread that waits for a protocol transaction
		# to be started. Using pg_sleep() we can block and hold the transaction
		# object until the thread identifies it as existing. Once this happens,
		# a pg.interrupt() is called from the thread, and the main part of the
		# program is notified of the completion by the appendage of `None` or
		# `sys.exc_info()` to `rl`.
		def sendint(l):
			try:
				while pg._xact is None:
					time.sleep(0.05)
					if time.time() - b > 5:
						self.fail("times up(5s), and it doesn't look like the thread even ran")
				while pg._xact.state[0] is c3.Sending:
					time.sleep(0.05)
				pg.interrupt()
			except:
				l.append(sys.exc_info())
			else:
				l.append(None)
		rl = []
		threading.start_new_thread(sendint, (rl,))
		self.failUnlessRaises(pg_exc.QueryCanceledError,
			execute, "SELECT pg_sleep(5)")
		b = time.time()
		while not rl:
			time.sleep(0.1)
			if time.time() - b > 5:
				self.fail("times up(5s), and it looks like the thread never finished")
		if rl[0] is not None:
			e, v, tb = rl[0]
			raise v

	def testLookupProcByName(self):
		execute(
			"CREATE OR REPLACE FUNCTION public.foo() RETURNS INT LANGUAGE SQL AS 'SELECT 1'"
		)
		settings['search_path'] = 'public'
		f = proc('foo()')
		f2 = proc('public.foo()')
		self.failUnless(f.oid == f2.oid,
			"function lookup incongruence(%r != %r)" %(f, f2)
		)

	def testLookupProcById(self):
		pass

	def testProcExecution(self):
		ver = proc("version()")
		ver()
		execute(
			"CREATE OR REPLACE FUNCTION ifoo(int) RETURNS int LANGUAGE SQL AS 'select $1'"
		)
		ifoo = proc('ifoo(int)')
		self.failUnless(ifoo(1) == 1)
		self.failUnless(ifoo(None) is None)

	def testNULL(self):
		# Directly commpare (SELECT NULL) is None
		self.failUnless(
			query("SELECT NULL")().next()[0] is None,
			"SELECT NULL did not return None"
		)
		# Indirectly compare (select NULL) is None
		self.failUnless(
			query("SELECT $1::text")(None).next()[0] is None,
			"[SELECT $1::text](None) did not return None "
		)

	def testBool(self):
		fst, snd = query("SELECT true, false")().next()
		self.failUnless(fst is True)
		self.failUnless(snd is False)

	def testSelect(self):
		self.failUnless(
			query('')() == None,
			'Empty query did not return None'
		)
		# Test SELECT 1.
		s1 = query("SELECT 1")
		p = s1()
		tup = p.next()
		self.failUnless(tup[0] == 1)

		for tup in s1:
			self.failUnless(tup[0] == 1)

	def testDDL(self):
		execute("CREATE TEMP TABLE t(i int)")
		try:
			insert_t = query("INSERT INTO t VALUES ($1)")
			delete_t = query("DELETE FROM t WHERE i = $1")
			delete_all_t = query("DELETE FROM t")
			update_t = query("UPDATE t SET i = $2 WHERE i = $1")
			self.failUnlessEqual(insert_t(1).count(), 1)
			self.failUnlessEqual(delete_t(1).count(), 1)
			self.failUnlessEqual(insert_t(2).count(), 1)
			self.failUnlessEqual(insert_t(2).count(), 1)
			self.failUnlessEqual(delete_t(2).count(), 2)

			self.failUnlessEqual(insert_t(3).count(), 1)
			self.failUnlessEqual(insert_t(3).count(), 1)
			self.failUnlessEqual(insert_t(3).count(), 1)
			self.failUnlessEqual(delete_all_t().count(), 3)

			self.failUnlessEqual(update_t(1, 2).count(), 0)
			self.failUnlessEqual(insert_t(1).count(), 1)
			self.failUnlessEqual(update_t(1, 2).count(), 1)
			self.failUnlessEqual(delete_t(1).count(), 0)
			self.failUnlessEqual(delete_t(2).count(), 1)
		finally:
			execute("DROP TABLE t")

	def testBatchDDL(self):
		execute("CREATE TEMP TABLE t(i int)")
		try:
			insert_t = query("INSERT INTO t VALUES ($1)")
			delete_t = query("DELETE FROM t WHERE i = $1")
			delete_all_t = query("DELETE FROM t")
			update_t = query("UPDATE t SET i = $2 WHERE i = $1")
			mset = (
				(2,), (2,), (3,), (4,), (5,),
			)
			insert_t << mset
			self.failUnlessEqual(mset, tuple([
				tuple(x) for x in query(
					"SELECT * FROM t ORDER BY 1 ASC"
				)
			]))
		finally:
			execute("DROP TABLE t")

	def testTypes(self):
		'test basic object I/O--input must equal output'
		for (typname, sample_data) in type_samples:
			pb = query("SELECT $1::" + typname)
			for sample in sample_data:
				rsample = pb.first(sample)
				self.failUnless(
					rsample == sample,
					"failed to return %s object data as-is; gave %r, received %r" %(
						typname, sample, rsample
					)
				)

	def testSyntaxError(self):
		self.failUnlessRaises(
			pg_exc.SyntaxError,
			query, "SELEKT 1",
		)

	def testInvalidSchemaError(self):
		self.failUnlessRaises(
			pg_exc.InvalidSchemaName,
			query, "SELECT * FROM sdkfldasjfdskljZknvson.foo"
		)

	def testUndefinedTableError(self):
		self.failUnlessRaises(
			pg_exc.UndefinedTableError,
			query, "SELECT * FROM public.lkansdkvsndlvksdvnlsdkvnsdlvk"
		)

	def testUndefinedColumnError(self):
		self.failUnlessRaises(
			pg_exc.UndefinedColumnError,
			query, "SELECT x____ysldvndsnkv FROM information_schema.tables"
		)

	def testSEARVError_avgInWhere(self):
		self.failUnlessRaises(
			pg_exc.SEARVError,
			query, "SELECT 1 WHERE avg(1) = 1"
		)

	def testSEARVError_groupByAgg(self):
		self.failUnlessRaises(
			pg_exc.SEARVError,
			query, "SELECT 1 GROUP BY avg(1)"
		)

	def testDatatypeMismatchError(self):
		self.failUnlessRaises(
			pg_exc.DatatypeMismatchError,
			query, "SELECT 1 WHERE 1"
		)

	def testUndefinedObjectError(self):
		try:
			self.failUnlessRaises(
				pg_exc.UndefinedObjectError,
				query, "CREATE TABLE lksvdnvsdlksnv(i intt___t)"
			)
		except:
			# newer versions throw the exception on execution
			self.failUnlessRaises(
				pg_exc.UndefinedObjectError,
				query("CREATE TABLE lksvdnvsdlksnv(i intt___t)")
			)

	def testZeroDivisionError(self):
		self.failUnlessRaises(
			pg_exc.ZeroDivisionError,
			query("SELECT 1/i FROM (select 0 as i) AS g(i)").first,
		)

	def testTransaction(self):
		with xact:
			execute("CREATE TEMP TABLE withfoo(i int)")
		query("SELECT * FROM withfoo")

		execute("DROP TABLE withfoo")
		with xact:
			execute("CREATE TEMP TABLE withfoo(i int)")
			raise pg_exc.AbortTransaction
		self.failUnlessRaises(
			pg_exc.UndefinedTableError,
			query, "SELECT * FROM withfoo"
		)

		class SomeError(StandardError):
			pass
		try:
			with xact:
				execute("CREATE TABLE withfoo (i int)")
				raise SomeError
		except SomeError:
			pass
		self.failUnlessRaises(
			pg_exc.UndefinedTableError,
			query, "SELECT * FROM withfoo"
		)

	def testConfiguredTransaction(self):
		if 'gid' in xact.prepared:
			xact.rollback_prepared('gid')
		with xact('gid'):
			pass
		with xact:
			pass
		xact.rollback_prepared('gid')

# Log: dbapi20.py
# Revision 1.10  2003/10/09 03:14:14  zenzen
# Add test for DB API 2.0 optional extension, where database exceptions
# are exposed as attributes on the Connection object.
#
# Revision 1.9  2003/08/13 01:16:36  zenzen
# Minor tweak from Stefan Fleiter
#
# Revision 1.8  2003/04/10 00:13:25  zenzen
# Changes, as per suggestions by M.-A. Lemburg
# - Add a table prefix, to ensure namespace collisions can always be avoided
#
# Revision 1.7  2003/02/26 23:33:37  zenzen
# Break out DDL into helper functions, as per request by David Rushby
#
# Revision 1.6  2003/02/21 03:04:33  zenzen
# Stuff from Henrik Ekelund:
#     added test_None
#     added test_nextset & hooks
#
# Revision 1.5  2003/02/17 22:08:43  zenzen
# Implement suggestions and code from Henrik Eklund - test that cursor.arraysize
# defaults to 1 & generic cursor.callproc test added
#
# Revision 1.4  2003/02/15 00:16:33  zenzen
# Changes, as per suggestions and bug reports by M.-A. Lemburg,
# Matthew T. Kromer, Federico Di Gregorio and Daniel Dittmar
# - Class renamed
# - Now a subclass of TestCase, to avoid requiring the driver stub
#   to use multiple inheritance
# - Reversed the polarity of buggy test in test_description
# - Test exception heirarchy correctly
# - self.populate is now self._populate(), so if a driver stub
#   overrides self.ddl1 this change propogates
# - VARCHAR columns now have a width, which will hopefully make the
#   DDL even more portible (this will be reversed if it causes more problems)
# - cursor.rowcount being checked after various execute and fetchXXX methods
# - Check for fetchall and fetchmany returning empty lists after results
#   are exhausted (already checking for empty lists if select retrieved
#   nothing
# - Fix bugs in test_setoutputsize_basic and test_setinputsizes
#
class test_dbapi20(unittest.TestCase):
	'''
	Test a database self.driver for DB API 2.0 compatibility.
	This implementation tests Gadfly, but the TestCase
	is structured so that other self.drivers can subclass this 
	test case to ensure compiliance with the DB-API. It is 
	expected that this TestCase may be expanded in the future
	if ambiguities or edge conditions are discovered.

	The 'Optional Extensions' are not yet being tested.

	self.drivers should subclass this test, overriding setUp, tearDown,
	self.driver, connect_args and connect_kw_args. Class specification
	should be as follows:

	import dbapi20 
	class mytest(dbapi20.DatabaseAPI20Test):
		[...] 

	__rcs_id__  = 'Id: dbapi20.py,v 1.10 2003/10/09 03:14:14 zenzen Exp'
	__version__ = 'Revision: 1.10'
	__author__ = 'Stuart Bishop <zen@shangri-la.dropbear.id.au>'
	'''

	import postgresql.driver.dbapi20 as driver
	table_prefix = 'dbapi20test_' # If you need to specify a prefix for tables

	booze_name = table_prefix + 'booze'
	ddl1 = 'create temp table %s (name varchar(20))' % booze_name
	ddl2 = 'create temp table %sbarflys (name varchar(20))' % table_prefix
	xddl1 = 'drop table %sbooze' % table_prefix
	xddl2 = 'drop table %sbarflys' % table_prefix

	lowerfunc = 'lower' # Name of stored procedure to convert string->lowercase

	# Some drivers may need to override these helpers, for example adding
	# a 'commit' after the execute.
	def executeDDL1(self,cursor):
		cursor.execute(self.ddl1)

	def executeDDL2(self,cursor):
		cursor.execute(self.ddl2)

	def tearDown(self):
		con = self._connect()
		try:
			cur = con.cursor()
			for ddl in (self.xddl1,self.xddl2):
				try: 
					cur.execute(ddl)
					con.commit()
				except self.driver.Error: 
					# Assume table didn't exist. Other tests will check if
					# execute is busted.
					pass
		finally:
			con.close()

	def _connect(self):
		return self.driver.Connection(pg.connector())

	def test_connect(self):
		con = self._connect()
		con.close()

	def test_apilevel(self):
		try:
			# Must exist
			apilevel = self.driver.apilevel
			# Must equal 2.0
			self.assertEqual(apilevel,'2.0')
		except AttributeError:
			self.fail("Driver doesn't define apilevel")

	def test_threadsafety(self):
		try:
			# Must exist
			threadsafety = self.driver.threadsafety
			# Must be a valid value
			self.failUnless(threadsafety in (0,1,2,3))
		except AttributeError:
			self.fail("Driver doesn't define threadsafety")

	def test_paramstyle(self):
		try:
			# Must exist
			paramstyle = self.driver.paramstyle
			# Must be a valid value
			self.failUnless(paramstyle in (
				'qmark','numeric','named','format','pyformat'
				))
		except AttributeError:
			self.fail("Driver doesn't define paramstyle")

	def test_Exceptions(self):
		# Make sure required exceptions exist, and are in the
		# defined heirarchy.
		self.failUnless(
			issubclass(self.driver.InterfaceError,self.driver.Error)
			)
		self.failUnless(
			issubclass(self.driver.DatabaseError,self.driver.Error)
			)
		self.failUnless(
			issubclass(self.driver.OperationalError,self.driver.Error)
			)
		self.failUnless(
			issubclass(self.driver.IntegrityError,self.driver.Error)
			)
		self.failUnless(
			issubclass(self.driver.InternalError,self.driver.Error)
			)
		self.failUnless(
			issubclass(self.driver.ProgrammingError,self.driver.Error)
			)
		self.failUnless(
			issubclass(self.driver.NotSupportedError,self.driver.Error)
			)

	def test_ExceptionsAsConnectionAttributes(self):
		# OPTIONAL EXTENSION
		# Test for the optional DB API 2.0 extension, where the exceptions
		# are exposed as attributes on the Connection object
		# I figure this optional extension will be implemented by any
		# driver author who is using this test suite, so it is enabled
		# by default.
		con = self._connect()
		drv = self.driver
		self.failUnless(con.Warning is drv.Warning)
		self.failUnless(con.Error is drv.Error)
		self.failUnless(con.InterfaceError is drv.InterfaceError)
		self.failUnless(con.DatabaseError is drv.DatabaseError)
		self.failUnless(con.OperationalError is drv.OperationalError)
		self.failUnless(con.IntegrityError is drv.IntegrityError)
		self.failUnless(con.InternalError is drv.InternalError)
		self.failUnless(con.ProgrammingError is drv.ProgrammingError)
		self.failUnless(con.NotSupportedError is drv.NotSupportedError)

	def test_commit(self):
		con = self._connect()
		try:
			# Commit must work, even if it doesn't do anything
			con.commit()
		finally:
			con.close()

	def test_rollback(self):
		con = self._connect()
		# If rollback is defined, it should either work or throw
		# the documented exception
		if hasattr(con,'rollback'):
			try:
				con.rollback()
			except self.driver.NotSupportedError:
				pass

	def test_cursor(self):
		con = self._connect()
		try:
			cur = con.cursor()
		finally:
			con.close()

	def test_cursor_isolation(self):
		con = self._connect()
		try:
			# Make sure cursors created from the same connection have
			# the documented transaction isolation level
			cur1 = con.cursor()
			cur2 = con.cursor()
			self.executeDDL1(cur1)
			cur1.execute("insert into %sbooze values ('Victoria Bitter')" % (
				self.table_prefix
				))
			cur2.execute("select name from %sbooze" % self.table_prefix)
			booze = cur2.fetchall()
			self.assertEqual(len(booze),1)
			self.assertEqual(len(booze[0]),1)
			self.assertEqual(booze[0][0],'Victoria Bitter')
		finally:
			con.close()

	def test_description(self):
		con = self._connect()
		try:
			cur = con.cursor()
			self.executeDDL1(cur)
			self.assertEqual(cur.description,None,
				'cursor.description should be none after executing a '
				'statement that can return no rows (such as DDL)'
				)
			cur.execute('select name from %sbooze' % self.table_prefix)
			self.assertEqual(len(cur.description),1,
				'cursor.description describes too many columns'
				)
			self.assertEqual(len(cur.description[0]),7,
				'cursor.description[x] tuples must have 7 elements'
				)
			self.assertEqual(cur.description[0][0].lower(),'name',
				'cursor.description[x][0] must return column name'
				)
			self.assertEqual(cur.description[0][1],self.driver.STRING,
				'cursor.description[x][1] must return column type. Got %r'
					% cur.description[0][1]
				)

			# Make sure self.description gets reset
			self.executeDDL2(cur)
			self.assertEqual(cur.description,None,
				'cursor.description not being set to None when executing '
				'no-result statements (eg. DDL)'
				)
		finally:
			con.close()

	def test_rowcount(self):
		con = self._connect()
		try:
			cur = con.cursor()
			self.executeDDL1(cur)
			self.assertEqual(cur.rowcount,-1,
				'cursor.rowcount should be -1 after executing no-result '
				'statements'
				)
			cur.execute("insert into %sbooze values ('Victoria Bitter')" % (
				self.table_prefix
				))
			self.failUnless(cur.rowcount in (-1,1),
				'cursor.rowcount should == number or rows inserted, or '
				'set to -1 after executing an insert statement'
				)
			cur.execute("select name from %sbooze" % self.table_prefix)
			self.failUnless(cur.rowcount in (-1,1),
				'cursor.rowcount should == number of rows returned, or '
				'set to -1 after executing a select statement'
				)
			self.executeDDL2(cur)
			self.assertEqual(cur.rowcount,-1,
				'cursor.rowcount not being reset to -1 after executing '
				'no-result statements'
				)
		finally:
			con.close()

	lower_func = 'lower'
	def test_callproc(self):
		con = self._connect()
		try:
			cur = con.cursor()
			if self.lower_func and hasattr(cur,'callproc'):
				r = cur.callproc(self.lower_func,('FOO',))
				self.assertEqual(len(r),1)
				self.assertEqual(r[0],'FOO')
				r = cur.fetchall()
				self.assertEqual(len(r),1,'callproc produced no result set')
				self.assertEqual(len(r[0]),1,
					'callproc produced invalid result set'
					)
				self.assertEqual(r[0][0],'foo',
					'callproc produced invalid results'
					)
		finally:
			con.close()

	def test_close(self):
		con = self._connect()
		try:
			cur = con.cursor()
		finally:
			con.close()

		# cursor.execute should raise an Error if called after connection
		# closed
		self.assertRaises(self.driver.Error,self.executeDDL1,cur)

		# connection.commit should raise an Error if called after connection'
		# closed.'
		self.assertRaises(self.driver.Error,con.commit)

		# connection.close should raise an Error if called more than once
		self.assertRaises(self.driver.Error,con.close)

	def test_execute(self):
		con = self._connect()
		try:
			cur = con.cursor()
			self._paraminsert(cur)
		finally:
			con.close()

	def _paraminsert(self,cur):
		self.executeDDL1(cur)
		cur.execute("insert into %sbooze values ('Victoria Bitter')" % (
			self.table_prefix
			))
		self.failUnless(cur.rowcount in (-1,1))

		if self.driver.paramstyle == 'qmark':
			cur.execute(
				'insert into %sbooze values (?)' % self.table_prefix,
				("Cooper's",)
				)
		elif self.driver.paramstyle == 'numeric':
			cur.execute(
				'insert into %sbooze values (:1)' % self.table_prefix,
				("Cooper's",)
				)
		elif self.driver.paramstyle == 'named':
			cur.execute(
				'insert into %sbooze values (:beer)' % self.table_prefix, 
				{'beer':"Cooper's"}
				)
		elif self.driver.paramstyle == 'format':
			cur.execute(
				'insert into %sbooze values (%%s)' % self.table_prefix,
				("Cooper's",)
				)
		elif self.driver.paramstyle == 'pyformat':
			cur.execute(
				'insert into %sbooze values (%%(beer)s)' % self.table_prefix,
				{'beer':"Cooper's"}
				)
		else:
			self.fail('Invalid paramstyle')
		self.failUnless(cur.rowcount in (-1,1))

		cur.execute('select name from %sbooze' % self.table_prefix)
		res = cur.fetchall()
		self.assertEqual(len(res),2,'cursor.fetchall returned too few rows')
		beers = [res[0][0],res[1][0]]
		beers.sort()
		self.assertEqual(beers[0],"Cooper's",
			'cursor.fetchall retrieved incorrect data, or data inserted '
			'incorrectly'
			)
		self.assertEqual(beers[1],"Victoria Bitter",
			'cursor.fetchall retrieved incorrect data, or data inserted '
			'incorrectly'
			)

	def test_executemany(self):
		con = self._connect()
		try:
			cur = con.cursor()
			self.executeDDL1(cur)
			largs = [ ("Cooper's",) , ("Boag's",) ]
			margs = [ {'beer': "Cooper's"}, {'beer': "Boag's"} ]
			if self.driver.paramstyle == 'qmark':
				cur.executemany(
					'insert into %sbooze values (?)' % self.table_prefix,
					largs
					)
			elif self.driver.paramstyle == 'numeric':
				cur.executemany(
					'insert into %sbooze values (:1)' % self.table_prefix,
					largs
					)
			elif self.driver.paramstyle == 'named':
				cur.executemany(
					'insert into %sbooze values (:beer)' % self.table_prefix,
					margs
					)
			elif self.driver.paramstyle == 'format':
				cur.executemany(
					'insert into %sbooze values (%%s)' % self.table_prefix,
					largs
					)
			elif self.driver.paramstyle == 'pyformat':
				cur.executemany(
					'insert into %sbooze values (%%(beer)s)' % (
						self.table_prefix
						),
					margs
					)
			else:
				self.fail('Unknown paramstyle')
			self.failUnless(cur.rowcount in (-1,2),
				'insert using cursor.executemany set cursor.rowcount to '
				'incorrect value %r' % cur.rowcount
				)
			cur.execute('select name from %sbooze' % self.table_prefix)
			res = cur.fetchall()
			self.assertEqual(len(res),2,
				'cursor.fetchall retrieved incorrect number of rows'
				)
			beers = [res[0][0],res[1][0]]
			beers.sort()
			self.assertEqual(beers[0],"Boag's",'incorrect data retrieved')
			self.assertEqual(beers[1],"Cooper's",'incorrect data retrieved')
		finally:
			con.close()

	def test_fetchone(self):
		con = self._connect()
		try:
			cur = con.cursor()

			# cursor.fetchone should raise an Error if called before
			# executing a select-type query
			self.assertRaises(self.driver.Error,cur.fetchone)

			# cursor.fetchone should raise an Error if called after
			# executing a query that cannnot return rows
			self.executeDDL1(cur)
			self.assertRaises(self.driver.Error,cur.fetchone)

			cur.execute('select name from %sbooze' % self.table_prefix)
			self.assertEqual(cur.fetchone(),None,
				'cursor.fetchone should return None if a query retrieves '
				'no rows'
				)
			self.failUnless(cur.rowcount in (-1,0))

			# cursor.fetchone should raise an Error if called after
			# executing a query that cannnot return rows
			cur.execute("insert into %sbooze values ('Victoria Bitter')" % (
				self.table_prefix
				))
			self.assertRaises(self.driver.Error,cur.fetchone)

			cur.execute('select name from %sbooze' % self.table_prefix)
			r = cur.fetchone()
			self.assertEqual(len(r),1,
				'cursor.fetchone should have retrieved a single row'
				)
			self.assertEqual(r[0],'Victoria Bitter',
				'cursor.fetchone retrieved incorrect data'
				)
			self.assertEqual(cur.fetchone(),None,
				'cursor.fetchone should return None if no more rows available'
				)
			self.failUnless(cur.rowcount in (-1,1))
		finally:
			con.close()

	samples = [
		'Carlton Cold',
		'Carlton Draft',
		'Mountain Goat',
		'Redback',
		'Victoria Bitter',
		'XXXX'
		]

	def _populate(self):
		''' Return a list of sql commands to setup the DB for the fetch
			tests.
		'''
		populate = [
			"insert into %sbooze values ('%s')" % (self.table_prefix,s) 
				for s in self.samples
			]
		return populate

	def test_fetchmany(self):
		con = self._connect()
		try:
			cur = con.cursor()

			# cursor.fetchmany should raise an Error if called without
			#issuing a query
			self.assertRaises(self.driver.Error,cur.fetchmany,4)

			self.executeDDL1(cur)
			for sql in self._populate():
				cur.execute(sql)

			cur.execute('select name from %sbooze' % self.table_prefix)
			r = cur.fetchmany()
			self.assertEqual(len(r),1,
				'cursor.fetchmany retrieved incorrect number of rows, '
				'default of arraysize is one.'
				)
			cur.arraysize=10
			r = cur.fetchmany(3) # Should get 3 rows
			self.assertEqual(len(r),3,
				'cursor.fetchmany retrieved incorrect number of rows'
				)
			r = cur.fetchmany(4) # Should get 2 more
			self.assertEqual(len(r),2,
				'cursor.fetchmany retrieved incorrect number of rows'
				)
			r = cur.fetchmany(4) # Should be an empty sequence
			self.assertEqual(len(r),0,
				'cursor.fetchmany should return an empty sequence after '
				'results are exhausted'
			)
			self.failUnless(cur.rowcount in (-1,6))

			# Same as above, using cursor.arraysize
			cur.arraysize=4
			cur.execute('select name from %sbooze' % self.table_prefix)
			r = cur.fetchmany() # Should get 4 rows
			self.assertEqual(len(r),4,
				'cursor.arraysize not being honoured by fetchmany'
				)
			r = cur.fetchmany() # Should get 2 more
			self.assertEqual(len(r),2)
			r = cur.fetchmany() # Should be an empty sequence
			self.assertEqual(len(r),0)
			self.failUnless(cur.rowcount in (-1,6))

			cur.arraysize=6
			cur.execute('select name from %sbooze' % self.table_prefix)
			rows = cur.fetchmany() # Should get all rows
			self.failUnless(cur.rowcount in (-1,6))
			self.assertEqual(len(rows),6)
			self.assertEqual(len(rows),6)
			rows = [r[0] for r in rows]
			rows.sort()
		  
			# Make sure we get the right data back out
			for i in range(0,6):
				self.assertEqual(rows[i],self.samples[i],
					'incorrect data retrieved by cursor.fetchmany'
					)

			rows = cur.fetchmany() # Should return an empty list
			self.assertEqual(len(rows),0,
				'cursor.fetchmany should return an empty sequence if '
				'called after the whole result set has been fetched'
				)
			self.failUnless(cur.rowcount in (-1,6))

			self.executeDDL2(cur)
			cur.execute('select name from %sbarflys' % self.table_prefix)
			r = cur.fetchmany() # Should get empty sequence
			self.assertEqual(len(r),0,
				'cursor.fetchmany should return an empty sequence if '
				'query retrieved no rows'
				)
			self.failUnless(cur.rowcount in (-1,0))

		finally:
			con.close()

	def test_fetchall(self):
		con = self._connect()
		try:
			cur = con.cursor()
			# cursor.fetchall should raise an Error if called
			# without executing a query that may return rows (such
			# as a select)
			self.assertRaises(self.driver.Error, cur.fetchall)

			self.executeDDL1(cur)
			for sql in self._populate():
				cur.execute(sql)

			# cursor.fetchall should raise an Error if called
			# after executing a a statement that cannot return rows
			self.assertRaises(self.driver.Error,cur.fetchall)

			cur.execute('select name from %sbooze' % self.table_prefix)
			rows = cur.fetchall()
			self.failUnless(cur.rowcount in (-1,len(self.samples)))
			self.assertEqual(len(rows),len(self.samples),
				'cursor.fetchall did not retrieve all rows'
				)
			rows = [r[0] for r in rows]
			rows.sort()
			for i in range(0,len(self.samples)):
				self.assertEqual(rows[i],self.samples[i],
				'cursor.fetchall retrieved incorrect rows'
				)
			rows = cur.fetchall()
			self.assertEqual(
				len(rows),0,
				'cursor.fetchall should return an empty list if called '
				'after the whole result set has been fetched'
				)
			self.failUnless(cur.rowcount in (-1,len(self.samples)))

			self.executeDDL2(cur)
			cur.execute('select name from %sbarflys' % self.table_prefix)
			rows = cur.fetchall()
			self.failUnless(cur.rowcount in (-1,0))
			self.assertEqual(len(rows),0,
				'cursor.fetchall should return an empty list if '
				'a select query returns no rows'
				)
			
		finally:
			con.close()
	
	def test_mixedfetch(self):
		con = self._connect()
		try:
			cur = con.cursor()
			self.executeDDL1(cur)
			for sql in self._populate():
				cur.execute(sql)

			cur.execute('select name from %sbooze' % self.table_prefix)
			rows1  = cur.fetchone()
			rows23 = cur.fetchmany(2)
			rows4  = cur.fetchone()
			rows56 = cur.fetchall()
			self.failUnless(cur.rowcount in (-1,6))
			self.assertEqual(len(rows23),2,
				'fetchmany returned incorrect number of rows'
				)
			self.assertEqual(len(rows56),2,
				'fetchall returned incorrect number of rows'
				)

			rows = [rows1[0]]
			rows.extend([rows23[0][0],rows23[1][0]])
			rows.append(rows4[0])
			rows.extend([rows56[0][0],rows56[1][0]])
			rows.sort()
			for i in range(0,len(self.samples)):
				self.assertEqual(rows[i],self.samples[i],
					'incorrect data retrieved or inserted'
					)
		finally:
			con.close()

	def help_nextset_setUp(self,cur):
		'''
		Should create a procedure called deleteme
		that returns two result sets, first the 
		number of rows in booze then "name from booze"
		'''
		cur.execute('select name from ' + self.booze_name)
		cur.execute('select count(*) from ' + self.booze_name)

	def help_nextset_tearDown(self,cur):
		'If cleaning up is needed after nextSetTest'
		pass

	def test_nextset(self):
		con = self._connect()
		try:
			cur = con.cursor()
			if not hasattr(cur,'nextset'):
				return

			try:
				self.executeDDL1(cur)
				sql=self._populate()
				for sql in self._populate():
					cur.execute(sql)

				self.help_nextset_setUp(cur)

				numberofrows=cur.fetchone()
				assert numberofrows[0]== len(self.samples)
				assert cur.nextset()
				names=cur.fetchall()
				assert len(names) == len(self.samples)
				s=cur.nextset()
				assert s == None,'No more return sets, should return None'
			finally:
				self.help_nextset_tearDown(cur)

		finally:
			con.close()

	def test_arraysize(self):
		# Not much here - rest of the tests for this are in test_fetchmany
		con = self._connect()
		try:
			cur = con.cursor()
			self.failUnless(hasattr(cur,'arraysize'),
				'cursor.arraysize must be defined'
				)
		finally:
			con.close()

	def test_setinputsizes(self):
		con = self._connect()
		try:
			cur = con.cursor()
			cur.setinputsizes( (25,) )
			self._paraminsert(cur) # Make sure cursor still works
		finally:
			con.close()

	def test_setoutputsize_basic(self):
		# Basic test is to make sure setoutputsize doesn't blow up
		con = self._connect()
		try:
			cur = con.cursor()
			cur.setoutputsize(1000)
			cur.setoutputsize(2000,0)
			self._paraminsert(cur) # Make sure the cursor still works
		finally:
			con.close()

	def test_setoutputsize(self):
		# Real test for setoutputsize is driver dependant
		pass

	def test_None(self):
		con = self._connect()
		try:
			cur = con.cursor()
			self.executeDDL1(cur)
			cur.execute('insert into %sbooze values (NULL)' % self.table_prefix)
			cur.execute('select name from %sbooze' % self.table_prefix)
			r = cur.fetchall()
			self.assertEqual(len(r),1)
			self.assertEqual(len(r[0]),1)
			self.assertEqual(r[0][0],None,'NULL value not returned as None')
		finally:
			con.close()

	def test_Date(self):
		d1 = self.driver.Date(2002,12,25)
		d2 = self.driver.DateFromTicks(time.mktime((2002,12,25,0,0,0,0,0,0)))
		# Can we assume this? API doesn't specify, but it seems implied
		# self.assertEqual(str(d1),str(d2))

	def test_Time(self):
		t1 = self.driver.Time(13,45,30)
		t2 = self.driver.TimeFromTicks(time.mktime((2001,1,1,13,45,30,0,0,0)))
		# Can we assume this? API doesn't specify, but it seems implied
		# self.assertEqual(str(t1),str(t2))

	def test_Timestamp(self):
		t1 = self.driver.Timestamp(2002,12,25,13,45,30)
		t2 = self.driver.TimestampFromTicks(
			time.mktime((2002,12,25,13,45,30,0,0,0))
			)
		# Can we assume this? API doesn't specify, but it seems implied
		# self.assertEqual(str(t1),str(t2))

	def test_Binary(self):
		b = self.driver.Binary('Something')
		b = self.driver.Binary('')

	def test_STRING(self):
		self.failUnless(hasattr(self.driver,'STRING'),
			'module.STRING must be defined'
			)

	def test_BINARY(self):
		self.failUnless(hasattr(self.driver,'BINARY'),
			'module.BINARY must be defined.'
			)

	def test_NUMBER(self):
		self.failUnless(hasattr(self.driver,'NUMBER'),
			'module.NUMBER must be defined.'
			)

	def test_DATETIME(self):
		self.failUnless(hasattr(self.driver,'DATETIME'),
			'module.DATETIME must be defined.'
			)

	def test_ROWID(self):
		self.failUnless(hasattr(self.driver,'ROWID'),
			'module.ROWID must be defined.'
			)

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)