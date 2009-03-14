##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import sys
import os
import unittest
import threading
import time
import datetime
import decimal
from itertools import chain
from operator import itemgetter

import postgresql.types as pg_types
import postgresql.exceptions as pg_exc
import postgresql.protocol.client3 as c3

import postgresql.unittest as pg_unittest

type_samples = (
	('smallint', (
			((1 << 16) // 2) - 1, - ((1 << 16) // 2),
			-1, 0, 1,
		),
	),
	('int', (
			((1 << 32) // 2) - 1, - ((1 << 32) // 2),
			-1, 0, 1,
		),
	),
	('bigint', (
			((1 << 64) // 2) - 1, - ((1 << 64) // 2),
			-1, 0, 1,
		),
	),
	('numeric', (
			-(2**64),
			2**64,
			-(2**128),
			2**128,
			-1, 0, 1,
			decimal.Decimal("0.00000000000000"),
			decimal.Decimal("1.00000000000000"),
			decimal.Decimal("-1.00000000000000"),
			decimal.Decimal("-2.00000000000000"),
			decimal.Decimal("1000000000000000.00000000000000"),
			decimal.Decimal("-0.00000000000000"),
			decimal.Decimal(1234),
			decimal.Decimal(-1234),
			decimal.Decimal("1234000000.00088883231"),
			decimal.Decimal(str(1234.00088883231)),
			decimal.Decimal("3123.23111"),
			decimal.Decimal("-3123000000.23111"),
			decimal.Decimal("3123.2311100000"),
			decimal.Decimal("-03123.0023111"),
			decimal.Decimal("3123.23111"),
			decimal.Decimal("3123.23111"),
			decimal.Decimal("10000.23111"),
			decimal.Decimal("100000.23111"),
			decimal.Decimal("1000000.23111"),
			decimal.Decimal("10000000.23111"),
			decimal.Decimal("100000000.23111"),
			decimal.Decimal("1000000000.23111"),
			decimal.Decimal("1000000000.3111"),
			decimal.Decimal("1000000000.111"),
			decimal.Decimal("1000000000.11"),
			decimal.Decimal("100000000.0"),
			decimal.Decimal("10000000.0"),
			decimal.Decimal("1000000.0"),
			decimal.Decimal("100000.0"),
			decimal.Decimal("10000.0"),
			decimal.Decimal("1000.0"),
			decimal.Decimal("100.0"),
			decimal.Decimal("100"),
			decimal.Decimal("100.1"),
			decimal.Decimal("100.12"),
			decimal.Decimal("100.123"),
			decimal.Decimal("100.1234"),
			decimal.Decimal("100.12345"),
			decimal.Decimal("100.123456"),
			decimal.Decimal("100.1234567"),
			decimal.Decimal("100.12345679"),
			decimal.Decimal("100.123456790"),
			decimal.Decimal("100.123456790000000000000000"),
			decimal.Decimal("1.0"),
			decimal.Decimal("0.0"),
			decimal.Decimal("-1.0"),
			decimal.Decimal("1.0E-1000"),
			decimal.Decimal("1.0E1000"),
			decimal.Decimal("1.0E10000"),
			decimal.Decimal("1.0E-10000"),
			decimal.Decimal("1.0E15000"),
			decimal.Decimal("1.0E-15000"),
			decimal.Decimal("1.0E-16382"),
			decimal.Decimal("1.0E32767"),
			decimal.Decimal("0.000000000000000000000000001"),
			decimal.Decimal("0.000000000000010000000000001"),
			decimal.Decimal("0.00000000000000000000000001"),
			decimal.Decimal("0.00000000100000000000000001"),
			decimal.Decimal("0.0000000000000000000000001"),
			decimal.Decimal("0.000000000000000000000001"),
			decimal.Decimal("0.00000000000000000000001"),
			decimal.Decimal("0.0000000000000000000001"),
			decimal.Decimal("0.000000000000000000001"),
			decimal.Decimal("0.00000000000000000001"),
			decimal.Decimal("0.0000000000000000001"),
			decimal.Decimal("0.000000000000000001"),
			decimal.Decimal("0.00000000000000001"),
			decimal.Decimal("0.0000000000000001"),
			decimal.Decimal("0.000000000000001"),
			decimal.Decimal("0.00000000000001"),
			decimal.Decimal("0.0000000000001"),
			decimal.Decimal("0.000000000001"),
			decimal.Decimal("0.00000000001"),
			decimal.Decimal("0.0000000001"),
			decimal.Decimal("0.000000001"),
			decimal.Decimal("0.00000001"),
			decimal.Decimal("0.0000001"),
			decimal.Decimal("0.000001"),
			decimal.Decimal("0.00001"),
			decimal.Decimal("0.0001"),
			decimal.Decimal("0.001"),
			decimal.Decimal("0.01"),
			decimal.Decimal("0.1"),
			# these require some weight transfer
		),
	),
	('bytea', (
			bytes(range(256)),
			bytes(range(255, -1, -1)),
		),
	),
	('smallint[]', (
			[123,321,-123,-321],
			[],
		),
	),
	('int[]', [
			[123,321,-123,-321],
			[[1],[2]],
			[],
		],
	),
	('bigint[]', [
			[
				0,
				1,
				-1,
				0xFFFFFFFFFFFF,
				-0xFFFFFFFFFFFF,
				((1 << 64) // 2) - 1,
				- ((1 << 64) // 2),
			],
			[],
		],
	),
	('varchar[]', [
			["foo", "bar",],
			["foo", "bar",],
			[],
		],
	),
	('timestamp', [
			datetime.datetime(2000,1,1,5,25,10),
			datetime.datetime(500,1,1,5,25,10),
		],
	)
)

class test_driver(pg_unittest.TestCaseWithCluster):
	"""
	postgresql.driver *interface* tests.
	"""
	def testInterrupt(self):
		def pg_sleep(l):
			try:
				self.db.execute("SELECT pg_sleep(5)")
			except Exception:
				l.append(sys.exc_info())
			else:
				l.append(None)
				return
		rl = []
		t = threading.Thread(target = pg_sleep, args = (rl,))
		t.start()
		time.sleep(0.2)
		while t.is_alive():
			self.db.interrupt()
			time.sleep(0.1)

		def raise_exc(l):
			if l[0] is not None:
				e, v, tb = rl[0]
				raise v
		self.failUnlessRaises(pg_exc.QueryCanceledError, raise_exc, rl)

	def testStatementParameters(self):
		# too few and takes one
		ps = self.db.prepare("select $1::integer")
		self.failUnlessRaises(TypeError, ps)

		# too many and takes one
		self.failUnlessRaises(TypeError, ps, 1, 2)

		# too many and takes none
		ps = self.db.prepare("select 1")
		self.failUnlessRaises(TypeError, ps, 1)

		# too many and takes some
		ps = self.db.prepare("select $1::int, $2::text")
		self.failUnlessRaises(TypeError, ps, 1, "foo", "bar")

	def testCopyToSTDOUT(self):
		with self.db.xact():
			self.db.execute("CREATE TABLE foo (i int)")
			foo = self.db.prepare('insert into foo values ($1)')
			foo.load(((x,) for x in range(500)))

			copy_foo = self.db.prepare('copy foo to stdout')
			foo_content = set(copy_foo)
			expected = set((str(i).encode('ascii') + b'\n' for i in range(500)))
			self.failUnlessEqual(expected, foo_content)
			self.db.execute("DROP TABLE foo")

	def testCopyFromSTDIN(self):
		with self.db.xact():
			self.db.execute("CREATE TABLE foo (i int)")
			foo = self.db.prepare('copy foo from stdin')
			foo.load((str(i).encode('ascii') + b'\n' for i in range(200)))
			foo_content = list((
				x for (x,) in self.db.prepare('select * from foo order by 1 ASC')
			))
			self.failUnlessEqual(foo_content, list(range(200)))
			self.db.execute("DROP TABLE foo")

	def testLookupProcByName(self):
		self.db.execute(
			"CREATE OR REPLACE FUNCTION public.foo() RETURNS INT LANGUAGE SQL AS 'SELECT 1'"
		)
		self.db.settings['search_path'] = 'public'
		f = self.db.proc('foo()')
		f2 = self.db.proc('public.foo()')
		self.failUnless(f.oid == f2.oid,
			"function lookup incongruence(%r != %r)" %(f, f2)
		)

	def testLookupProcById(self):
		gsoid = self.db.prepare(
			"select oid from pg_proc where proname = 'generate_series' limit 1"
		).first()
		gs = self.db.proc(gsoid)
		self.failUnlessEqual(
			list(gs(1, 100)), list(range(1, 101))
		)

	def testProcExecution(self):
		ver = self.db.proc("version()")
		ver()
		self.db.execute(
			"CREATE OR REPLACE FUNCTION ifoo(int) RETURNS int LANGUAGE SQL AS 'select $1'"
		)
		ifoo = self.db.proc('ifoo(int)')
		self.failUnless(ifoo(1) == 1)
		self.failUnless(ifoo(None) is None)

	def testProcExecutionInXact(self):
		with self.db.xact():
			self.testProcExecution()

	def testNULL(self):
		# Directly commpare (SELECT NULL) is None
		self.failUnless(
			next(self.db.prepare("SELECT NULL")())[0] is None,
			"SELECT NULL did not return None"
		)
		# Indirectly compare (select NULL) is None
		self.failUnless(
			next(self.db.prepare("SELECT $1::text")(None))[0] is None,
			"[SELECT $1::text](None) did not return None "
		)

	def testBool(self):
		fst, snd = self.db.prepare("SELECT true, false").first()
		self.failUnless(fst is True)
		self.failUnless(snd is False)

	def testSelect(self):
		#self.failUnlessEqual(
		#	self.db.prepare('')().command(),
		#	None,
		#	'Empty statement has command?'
		#)
		# Test SELECT 1.
		s1 = self.db.prepare("SELECT 1 as name")
		p = s1()
		tup = next(p)
		self.failUnless(tup[0] == 1)

		for tup in s1:
			self.failUnlessEqual(tup[0], 1)

		for tup in s1:
			self.failUnlessEqual(tup["name"], 1)

	def testSelectInXact(self):
		with self.db.xact():
			self.testSelect()

	def testChunking(self):
		gs = self.db.prepare("SELECT i FROM generate_series(1, 10000) AS g(i)")
		self.failUnlessEqual(
			list((x[0] for x in chain(*list((gs().chunks))))),
			list(range(1, 10001))
		)
		# exercise ``for x in chunks: dst.load(x)``
		try:
			with self.db.connector() as db2:
				db2.prepare(
					"""
					CREATE TABLE chunking AS
					SELECT i::text AS t, i::int AS i
					FROM generate_series(1, 10000) g(i);
					"""
				)()
				read_chunking = self.db.prepare('select * FROM chunking')
				write_chunking = db2.prepare('insert into chunking values ($1, $2)')
				out = read_chunking()
				out.chunksize = 256
				for rows in out.chunks:
					write_chunking.load(rows)
				self.failUnlessEqual(
					self.db.prepare('select count(*) FROM chunking').first(),
					20000
				)
				self.failUnlessEqual(
					self.db.prepare('select count(DISTINCT i) FROM chunking').first(),
					10000
				)
		finally:
			try:
				self.db.execute('DROP TABLE chunking')
			except:
				pass

	def testDDL(self):
		self.db.execute("CREATE TEMP TABLE t(i int)")
		try:
			insert_t = self.db.prepare("INSERT INTO t VALUES ($1)")
			delete_t = self.db.prepare("DELETE FROM t WHERE i = $1")
			delete_all_t = self.db.prepare("DELETE FROM t")
			update_t = self.db.prepare("UPDATE t SET i = $2 WHERE i = $1")
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
			self.db.execute("DROP TABLE t")

	def testDDLInXact(self):
		with self.db.xact():
			self.testDDL()

	def testBatchDDL(self):
		self.db.execute("CREATE TEMP TABLE t(i int)")
		try:
			insert_t = self.db.prepare("INSERT INTO t VALUES ($1)")
			delete_t = self.db.prepare("DELETE FROM t WHERE i = $1")
			delete_all_t = self.db.prepare("DELETE FROM t")
			update_t = self.db.prepare("UPDATE t SET i = $2 WHERE i = $1")
			mset = (
				(2,), (2,), (3,), (4,), (5,),
			)
			insert_t.load(mset)
			self.failUnlessEqual(mset, tuple([
				tuple(x) for x in self.db.prepare(
					"SELECT * FROM t ORDER BY 1 ASC"
				)
			]))
		finally:
			self.db.execute("DROP TABLE t")

	def testBatchDDLInXact(self):
		with self.db.xact():
			self.testBatchDDL()

	def testTypes(self):
		'test basic object I/O--input must equal output'
		for (typname, sample_data) in type_samples:
			pb = self.db.prepare(
				"SELECT $1::" + typname + ", $1::" + typname + "::text"
			)
			for sample in sample_data:
				rsample, tsample = pb.first(sample)
				if isinstance(rsample, pg_types.Array):
					rsample = rsample.nest()
				self.failUnless(
					rsample == sample,
					"failed to return %s object data as-is; gave %r, received %r(%r::text)" %(
						typname, sample, rsample, tsample
					)
				)

	def testSyntaxError(self):
		self.failUnlessRaises(
			pg_exc.SyntaxError,
			self.db.prepare("SELEKT 1")
		)

	def testSchemaNameError(self):
		self.failUnlessRaises(
			pg_exc.SchemaNameError,
			self.db.prepare("SELECT * FROM sdkfldasjfdskljZknvson.foo")
		)

	def testUndefinedTableError(self):
		self.failUnlessRaises(
			pg_exc.UndefinedTableError,
			self.db.prepare("SELECT * FROM public.lkansdkvsndlvksdvnlsdkvnsdlvk")
		)

	def testUndefinedColumnError(self):
		self.failUnlessRaises(
			pg_exc.UndefinedColumnError,
			self.db.prepare("SELECT x____ysldvndsnkv FROM information_schema.tables")
		)

	def testSEARVError_avgInWhere(self):
		self.failUnlessRaises(
			pg_exc.SEARVError,
			self.db.prepare("SELECT 1 WHERE avg(1) = 1")
		)

	def testSEARVError_groupByAgg(self):
		self.failUnlessRaises(
			pg_exc.SEARVError,
			self.db.prepare("SELECT 1 GROUP BY avg(1)")
		)

	def testTypeMismatchError(self):
		self.failUnlessRaises(
			pg_exc.TypeMismatchError,
			self.db.prepare("SELECT 1 WHERE 1")
		)

	def testUndefinedObjectError(self):
		try:
			self.failUnlessRaises(
				pg_exc.UndefinedObjectError,
				self.db.prepare("CREATE TABLE lksvdnvsdlksnv(i intt___t)")
			)
		except:
			# newer versions throw the exception on execution
			self.failUnlessRaises(
				pg_exc.UndefinedObjectError,
				self.db.prepare("CREATE TABLE lksvdnvsdlksnv(i intt___t)")
			)

	def testZeroDivisionError(self):
		self.failUnlessRaises(
			pg_exc.ZeroDivisionError,
			self.db.prepare("SELECT 1/i FROM (select 0 as i) AS g(i)").first,
		)

	def testTransactionCommit(self):
		with self.db.xact():
			self.db.execute("CREATE TEMP TABLE withfoo(i int)")
		self.db.prepare("SELECT * FROM withfoo")

		self.db.execute("DROP TABLE withfoo")
		self.failUnlessRaises(
			pg_exc.UndefinedTableError,
			self.db.prepare("SELECT * FROM withfoo")
		)

	def testTransactionAbort(self):
		class SomeError(Exception):
			pass
		try:
			with self.db.xact():
				self.db.execute("CREATE TABLE withfoo (i int)")
				raise SomeError
		except SomeError:
			pass
		self.failUnlessRaises(
			pg_exc.UndefinedTableError,
			self.db.prepare("SELECT * FROM withfoo")
		)

	def testPreparedTransactionCommit(self):
		with self.db.xact(gid = 'commit_gid') as x:
			with x:
				self.db.execute("create table commit_gidtable as select 'foo'::text as t;")
			self.failUnlessRaises(pg_exc.UndefinedTableError,
				self.db.prepare("select * from commit_gidtable")
			)
		self.failUnlessEqual(
			self.db.prepare("select * FROM commit_gidtable").first(),
			'foo',
		)

	def testUnPreparedTransactionCommit(self):
		x = self.db.xact(gid = 'never_prepared')
		x.start()
		self.failUnlessRaises(pg_exc.OperationError, x.commit)
		self.failUnlessRaises(pg_exc.OperationError, x.commit)

	def testPreparedTransactionRollback(self):
		x = self.db.xact(gid = 'rollback_gid')
		with x:
			self.db.execute("create table gidtable as select 'foo'::text as t;")
		x.rollback()
		self.failUnlessRaises(pg_exc.UndefinedTableError,
			self.db.prepare("select * from gidtable")
		)

	def testPreparedTransactionRecovery(self):
		x = self.db.xact(gid='recover dis')
		with x:
			self.db.execute("create table distable (i int);")
		del x
		x = self.db.xact(gid='recover dis')
		x.recover()
		x.commit()
		self.db.execute("drop table distable;")

	def testPreparedTransactionRecoveryAbort(self):
		x = self.db.xact(gid='recover dis abort')
		with x:
			self.db.execute("create table distableabort (i int);")
		del x
		x = self.db.xact(gid='recover dis abort')
		x.recover()
		x.rollback()
		self.failUnlessRaises(pg_exc.UndefinedTableError,
			self.db.prepare("select * from distableabort")
		)

	def testPreparedTransactionFailedRecovery(self):
		x = self.db.xact(gid="NO XACT HERE")
		self.failUnlessRaises(
			pg_exc.UndefinedObjectError,
			x.recover
		)

	def testSerializeable(self):
		with self.db.connector() as db2:
			db2.execute("create table some_darn_table (i int);")
			try:
				with self.db.xact(isolation = 'serializable'):
					self.db.execute('insert into some_darn_table values (123);')
					# db2 is in autocommit..
					db2.execute('insert into some_darn_table values (321);')
					self.failIfEqual(
						list(self.db.prepare('select * from some_darn_table')),
						list(db2.prepare('select * from some_darn_table')),
					)
			finally:
				# cleanup
				db2.execute("drop table some_darn_table;")

	def testReadOnly(self):
		class something(Exception):
			pass
		try:
			with self.db.xact(mode = 'read only'):
				self.failUnlessRaises(
					pg_exc.ReadOnlyTransactionError,
					self.db.execute, 
					"create table ieeee(i int)"
				)
				raise something("yeah, it raised.")
			self.fail("should have been passed by exception")
		except something:
			pass

	def testFailedTransactionBlock(self):
		try:
			with self.db.xact():
				try:
					self.db.execute("selekt 1;")
				except pg_exc.SyntaxError:
					pass
			self.fail("__exit__ didn't identify failed transaction")
		except pg_exc.InFailedTransactionError as err:
			self.failUnlessEqual(err.source, 'DRIVER')

	def testFailedSubtransactionBlock(self):
		with self.db.xact():
			try:
				with self.db.xact():
					try:
						self.db.execute("selekt 1;")
					except pg_exc.SyntaxError:
						pass
				self.fail("__exit__ didn't identify failed transaction")
			except pg_exc.InFailedTransactionError as err:
				# driver should have released/aborted instead
				self.failUnlessEqual(err.source, 'DRIVER')

	def testCloseInSubTransactionBlock(self):
		try:
			with self.db.xact():
				self.db.close()
			self.fail("transaction __exit__ didn't identify cause ConnectionDoesNotExistError")
		except pg_exc.ConnectionDoesNotExistError:
			pass

	def testCloseInSubTransactionBlock(self):
		try:
			with self.db.xact():
				with self.db.xact():
					self.db.close()
				self.fail("transaction __exit__ didn't identify cause ConnectionDoesNotExistError")
			self.fail("transaction __exit__ didn't identify cause ConnectionDoesNotExistError")
		except pg_exc.ConnectionDoesNotExistError:
			pass

	def testSettingsCM(self):
		orig = self.db.settings['search_path']
		with self.db.settings(search_path='public'):
			self.failUnlessEqual(self.db.settings['search_path'], 'public')
		self.failUnlessEqual(self.db.settings['search_path'], orig)

	def testSettingsReset(self):
		# <3 search_path
		cur = self.db.settings['search_path']
		self.db.settings['search_path'] = 'pg_catalog'
		del self.db.settings['search_path']
		self.failUnlessEqual(self.db.settings['search_path'], cur)

	def testSettingsCount(self):
		self.failUnlessEqual(
			len(self.db.settings), self.db.prepare('select count(*) from pg_settings').first()
		)

	def testSettingsGet(self):
		self.failUnlessEqual(
			self.db.settings['search_path'], self.db.settings.get('search_path')
		)
		self.failUnlessEqual(None, self.db.settings.get(' $*0293 vksnd'))

	def testSettingsGetSet(self):
		sub = self.db.settings.getset(
			('search_path', 'default_statistics_target')
		)
		self.failUnlessEqual(self.db.settings['search_path'], sub['search_path'])
		self.failUnlessEqual(self.db.settings['default_statistics_target'], sub['default_statistics_target'])

	def testSettings(self):
		'general access tests'
		d = dict(self.db.settings)
		d = dict(self.db.settings.items())
		k = list(self.db.settings.keys())
		v = list(self.db.settings.values())
		self.failUnlessEqual(len(k), len(d))
		self.failUnlessEqual(len(k), len(v))
		for x in k:
			self.failUnless(d[x] in v)
		all = list(self.db.settings.getset(k).items())
		all.sort(key=itemgetter(0))
		dall = list(d.items())
		dall.sort(key=itemgetter(0))
		self.failUnlessEqual(dall, all)

if __name__ == '__main__':
	unittest.main()
