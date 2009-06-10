##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import sys
import os
import unittest
import gc
import threading
import time
import datetime
import decimal
from itertools import chain, islice
from operator import itemgetter

from ..python.datetime import FixedOffset
from .. import types as pg_types
from .. import exceptions as pg_exc
from .. import unittest as pg_unittest
from .. import lib as pg_lib

type_samples = [
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
			b'\x00\x00',
			b'foo',
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
			datetime.datetime(3000,5,20,5,30,10),
			datetime.datetime(2000,1,1,5,25,10),
			datetime.datetime(500,1,1,5,25,10),
			datetime.datetime(250,1,1,5,25,10),
		],
	),
	('date', [
			datetime.date(3000,5,20),
			datetime.date(2000,1,1),
			datetime.date(500,1,1),
			datetime.date(1,1,1),
		],
	),
	('time', [
			datetime.time(12,15,20),
			datetime.time(0,1,1),
			datetime.time(23,59,59),
		],
	),
	('timestamptz', [
			# It's converted to UTC. When it comes back out, it will be in UTC
			# again. The datetime comparison will take the tzinfo into account.
			datetime.datetime(1990,5,12,10,10,0, tzinfo=FixedOffset(4000)),
			datetime.datetime(1982,5,18,10,10,0, tzinfo=FixedOffset(6000)),
			datetime.datetime(1950,1,1,10,10,0, tzinfo=FixedOffset(7000)),
			datetime.datetime(1800,1,1,10,10,0, tzinfo=FixedOffset(2000)),
			datetime.datetime(2400,1,1,10,10,0, tzinfo=FixedOffset(2000)),
		],
	),
	('timetz', [
			datetime.time(10,10,0, tzinfo=FixedOffset(4000)),
			datetime.time(10,10,0, tzinfo=FixedOffset(6000)),
			datetime.time(10,10,0, tzinfo=FixedOffset(7000)),
			datetime.time(10,10,0, tzinfo=FixedOffset(2000)),
		],
	),
	('interval', [
			datetime.timedelta(40, 10, 1234),
			datetime.timedelta(0, 0),
			datetime.timedelta(-100, 0),
			datetime.timedelta(-100, -400),
		],
	),
	('point', [
			(10, 1234),
			(-1, -1),
			(0, 0),
			(1, 1),
			(-100, 0),
			(-100, -400),
			(-100.02314, -400.930425),
			(0xFFFF, 1.3124243),
		],
	),
	('lseg', [
			((0,0),(0,0)),
			((10,5),(18,293)),
			((55,5),(10,293)),
			((-1,-1),(-1,-1)),
			((-100,0.00231),(50,45.42132)),
			((0.123,0.00231),(50,45.42132)),
		],
	),
	('circle', [
			((0,0),0),
			((0,0),1),
			((0,0),1.0011),
			((1,1),1.0011),
			((-1,-1),1.0011),
			((1,-1),1.0011),
			((-1,1),1.0011),
		],
	),
	('box', [
			((0,0),(0,0)),
			((-1,-1),(-1,-1)),
			((1,1),(-1,-1)),
			((10,1),(-1,-1)),
			((100.2312,45.1232),(-123.023,-1423.82342)),
		],
	),
	('bit', [
			pg_types.bit('1'),
			pg_types.bit('0'),
			None,
		],
	),
	('varbit', [
			pg_types.varbit('1'),
			pg_types.varbit('0'),
			pg_types.varbit('10'),
			pg_types.varbit('11'),
			pg_types.varbit('00'),
			pg_types.varbit('001'),
			pg_types.varbit('101'),
			pg_types.varbit('111'),
			pg_types.varbit('0010'),
			pg_types.varbit('1010'),
			pg_types.varbit('1010'),
			pg_types.varbit('01010101011111011010110101010101111'),
			pg_types.varbit('010111101111'),
		],
	),
]

if False:
	# When an implementation does make it,
	# re-enable these tests.
	type_samples.append((
		'inet', [
			IPAddress4('255.255.255.255'),
			IPAddress4('127.0.0.1'),
			IPAddress4('10.0.0.1'),
			IPAddress4('0.0.0.0'),
			IPAddress6('::1'),
			IPAddress6('ffff' + ':ffff'*7),
			IPAddress6('fe80::1'),
			IPAddress6('fe80::1'),
			IPAddress6('0::0'),
		],
	))
	type_samples.append((
		'cidr', [
			IPNetwork4('255.255.255.255/32'),
			IPNetwork4('127.0.0.0/8'),
			IPNetwork4('127.1.0.0/16'),
			IPNetwork4('10.0.0.0/32'),
			IPNetwork4('0.0.0.0/0'),
			IPNetwork6('ffff' + ':ffff'*7 + '/128'),
			IPNetwork6('::1/128'),
			IPNetwork6('fe80::1/128'),
			IPNetwork6('fe80::0/64'),
			IPNetwork6('fe80::0/16'),
			IPNetwork6('0::0/0'),
		],
	))

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

	def testClones(self):
		self.db.execute('create table _can_clone_see_this (i int);')
		try:
			with self.db.clone() as db2:
				self.failUnlessEqual(db2.prepare('select 1').first(), 1)
				self.failUnlessEqual(db2.prepare(
						"select count(*) FROM information_schema.tables " \
						"where table_name = '_can_clone_see_this'"
					).first(), 1
				)
		finally:
			self.db.execute('drop table _can_clone_see_this')
		# check already open
		db = self.db.clone()
		self.failUnlessEqual(db.prepare('select 1').first(), 1)
		db.close()

		ps = self.db.prepare('select 1')
		ps2 = ps.clone()
		self.failUnlessEqual(ps2.first(), ps.first())
		ps2.close()
		c = ps.declare()
		c2 = c.clone()
		self.failUnlessEqual(c.read(), c2.read())

	def testItsClosed(self):
		ps = self.db.prepare("SELECT 1")
		# If scroll is False it will pre-fetch, and no error will be thrown.
		c = ps.declare()
		#
		c.close()
		self.failUnlessRaises(pg_exc.CursorNameError, c.read)
		self.failUnlessEqual(ps.first(), 1)
		#
		ps.close()
		self.failUnlessRaises(pg_exc.StatementNameError, ps.first)
		#
		self.db.close()
		self.failUnlessRaises(
			pg_exc.ConnectionDoesNotExistError,
			self.db.execute, "foo"
		)
		# No errors, it's already closed.
		ps.close()
		c.close()
		self.db.close()

	def testGarbage(self):
		ps = self.db.prepare('select 1')
		sid = ps.statement_id
		ci = ps.chunks()
		ci_id = ci.cursor_id
		c = ps.declare()
		cid = c.cursor_id
		# make sure there are no remaining xact references..
		self.db._pq_complete()
		# ci and c both hold references to ps, so they must
		# be removed before we can observe the effects __del__
		del c
		gc.collect()
		self.failUnless(self.db.typio.encode(cid) in self.db.pq.garbage_cursors)
		del ci
		gc.collect()
		self.failUnless(self.db.typio.encode(ci_id) in self.db.pq.garbage_cursors)
		del ps
		gc.collect()
		self.failUnless(self.db.typio.encode(sid) in self.db.pq.garbage_statements)

	def testStatementCall(self):
		ps = self.db.prepare("SELECT 1")
		r = ps()
		self.failUnless(isinstance(r, list))
		self.failUnlessEqual(ps(), [(1,)])
		ps = self.db.prepare("SELECT 1, 2")
		self.failUnlessEqual(ps(), [(1,2)])
		ps = self.db.prepare("SELECT 1, 2 UNION ALL SELECT 3, 4")
		self.failUnlessEqual(ps(), [(1,2),(3,4)])

	def testStatementRowsPersistence(self):
		# validate that rows' cursor will persist beyond a transaction.
		ps = self.db.prepare("SELECT i FROM generate_series($1::int, $2::int) AS g(i)")
		# create the iterator inside the transaction
		rows = ps.rows(0, 10000-1)
		ps(0,1)
		# validate the first half.
		self.failUnlessEqual(
			list(islice(map(itemgetter(0), rows), 5000)),
			list(range(5000))
		)
		ps(0,1)
		# and the second half.
		self.failUnlessEqual(
			list(map(itemgetter(0), rows)),
			list(range(5000, 10000))
		)

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

	def testStatementAndCursorMetadata(self):
		ps = self.db.prepare("SELECT $1::integer AS my_int_column")
		self.failUnlessEqual(tuple(ps.column_names), ('my_int_column',))
		self.failUnlessEqual(tuple(ps.sql_column_types), ('INTEGER',))
		self.failUnlessEqual(tuple(ps.sql_parameter_types), ('INTEGER',))
		self.failUnlessEqual(tuple(ps.pg_parameter_types), (pg_types.INT4OID,))
		self.failUnlessEqual(tuple(ps.parameter_types), (int,))
		self.failUnlessEqual(tuple(ps.column_types), (int,))
		c = ps.declare(15)
		self.failUnlessEqual(tuple(c.column_names), ('my_int_column',))
		self.failUnlessEqual(tuple(c.sql_column_types), ('INTEGER',))
		self.failUnlessEqual(tuple(c.column_types), (int,))

		ps = self.db.prepare("SELECT $1::text AS my_text_column")
		self.failUnlessEqual(tuple(ps.column_names), ('my_text_column',))
		self.failUnlessEqual(tuple(ps.sql_column_types), ('pg_catalog.text',))
		self.failUnlessEqual(tuple(ps.sql_parameter_types), ('pg_catalog.text',))
		self.failUnlessEqual(tuple(ps.pg_parameter_types), (pg_types.TEXTOID,))
		self.failUnlessEqual(tuple(ps.column_types), (str,))
		self.failUnlessEqual(tuple(ps.parameter_types), (str,))
		c = ps.declare('textdata')
		self.failUnlessEqual(tuple(c.column_names), ('my_text_column',))
		self.failUnlessEqual(tuple(c.sql_column_types), ('pg_catalog.text',))
		self.failUnlessEqual(tuple(c.pg_column_types), (pg_types.TEXTOID,))
		self.failUnlessEqual(tuple(c.column_types), (str,))

		ps = self.db.prepare("SELECT $1::text AS my_column1, $2::varchar AS my_column2")
		self.failUnlessEqual(tuple(ps.column_names), ('my_column1','my_column2'))
		self.failUnlessEqual(tuple(ps.sql_column_types), ('pg_catalog.text', 'CHARACTER VARYING'))
		self.failUnlessEqual(tuple(ps.sql_parameter_types), ('pg_catalog.text', 'CHARACTER VARYING'))
		self.failUnlessEqual(tuple(ps.pg_parameter_types), (pg_types.TEXTOID, pg_types.VARCHAROID))
		self.failUnlessEqual(tuple(ps.pg_column_types), (pg_types.TEXTOID, pg_types.VARCHAROID))
		self.failUnlessEqual(tuple(ps.parameter_types), (str,str))
		self.failUnlessEqual(tuple(ps.column_types), (str,str))
		c = ps.declare('textdata', 'varchardata')
		self.failUnlessEqual(tuple(c.column_names), ('my_column1','my_column2'))
		self.failUnlessEqual(tuple(c.sql_column_types), ('pg_catalog.text', 'CHARACTER VARYING'))
		self.failUnlessEqual(tuple(c.pg_column_types), (pg_types.TEXTOID, pg_types.VARCHAROID))
		self.failUnlessEqual(tuple(c.column_types), (str,str))

		self.db.execute("CREATE TYPE public.myudt AS (i int)")
		myudt_oid = self.db.prepare("select oid from pg_type WHERE typname='myudt'").first()
		ps = self.db.prepare("SELECT $1::text AS my_column1, $2::varchar AS my_column2, $3::public.myudt AS my_column3")
		self.failUnlessEqual(tuple(ps.column_names), ('my_column1','my_column2', 'my_column3'))
		self.failUnlessEqual(tuple(ps.sql_column_types), ('pg_catalog.text', 'CHARACTER VARYING', 'public.myudt'))
		self.failUnlessEqual(tuple(ps.sql_parameter_types), ('pg_catalog.text', 'CHARACTER VARYING', 'public.myudt'))
		self.failUnlessEqual(tuple(ps.pg_column_types), (
			pg_types.TEXTOID, pg_types.VARCHAROID, myudt_oid)
		)
		self.failUnlessEqual(tuple(ps.pg_parameter_types), (
			pg_types.TEXTOID, pg_types.VARCHAROID, myudt_oid)
		)
		self.failUnlessEqual(tuple(ps.parameter_types), (str,str,tuple))
		self.failUnlessEqual(tuple(ps.column_types), (str,str,tuple))
		c = ps.declare('textdata', 'varchardata', (123,))
		self.failUnlessEqual(tuple(c.column_names), ('my_column1','my_column2', 'my_column3'))
		self.failUnlessEqual(tuple(c.sql_column_types), ('pg_catalog.text', 'CHARACTER VARYING', 'public.myudt'))
		self.failUnlessEqual(tuple(c.pg_column_types), (
			pg_types.TEXTOID, pg_types.VARCHAROID, myudt_oid
		))
		self.failUnlessEqual(tuple(c.column_types), (str,str,tuple))

	def testRowInterface(self):
		data = (1, '0', decimal.Decimal('0.00'), datetime.datetime(1982,5,18,12,30,0))
		ps = self.db.prepare(
			"SELECT 1::int2 AS col0, " \
			"'0'::text AS col1, 0.00::numeric as col2, " \
			"'1982-05-18 12:30:00'::timestamp as col3;"
		)
		row = ps.first()
		self.failUnlessEqual(tuple(row), data)

		self.failUnless(1 in row)
		self.failUnless('0' in row)
		self.failUnless(decimal.Decimal('0.00') in row)
		self.failUnless(datetime.datetime(1982,5,18,12,30,0) in row)

		self.failUnlessEqual(
			tuple(row.column_names),
			tuple(['col' + str(i) for i in range(4)])
		)
		self.failUnlessEqual(
			(row["col0"], row["col1"], row["col2"], row["col3"]),
			(row[0], row[1], row[2], row[3]),
		)
		self.failUnlessEqual(
			(row["col0"], row["col1"], row["col2"], row["col3"]),
			(row[0], row[1], row[2], row[3]),
		)
		keys = list(row.keys())
		cnames = list(ps.column_names)
		cnames.sort()
		keys.sort()
		self.failUnlessEqual(keys, cnames)
		self.failUnlessEqual(list(row.values()), list(data))
		self.failUnlessEqual(list(row.items()), list(zip(ps.column_names, data)))

		row_d = dict(row)
		for x in ps.column_names:
			self.failUnlessEqual(row_d[x], row[x])
		for x in row_d.keys():
			self.failUnlessEqual(row.get(x), row_d[x])

		row_t = tuple(row)
		self.failUnlessEqual(row_t, row)

		# transform
		crow = row.transform(col0 = str)
		self.failUnlessEqual(type(crow[0]), str)
		crow = row.transform(str)
		self.failUnlessEqual(type(crow[0]), str)
		crow = row.transform(str, int)
		self.failUnlessEqual(type(crow[0]), str)
		self.failUnlessEqual(type(crow[1]), int)
		# None = no transformation
		crow = row.transform(None, int)
		self.failUnlessEqual(type(crow[0]), int)
		self.failUnlessEqual(type(crow[1]), int)
		# and a combination
		crow = row.transform(str, col1 = int, col3 = str)
		self.failUnlessEqual(type(crow[0]), str)
		self.failUnlessEqual(type(crow[1]), int)
		self.failUnlessEqual(type(crow[3]), str)

		for i in range(4):
			self.failUnlessEqual(i, row.index_from_key('col' + str(i)))
			self.failUnlessEqual('col' + str(i), row.key_from_index(i))

	def testStatementFromId(self):
		self.db.execute("PREPARE foo AS SELECT 1 AS colname;")
		ps = self.db.statement_from_id('foo')
		self.failUnlessEqual(ps.first(), 1)
		self.failUnlessEqual(ps(), [(1,)])
		self.failUnlessEqual(list(ps), [(1,)])
		self.failUnlessEqual(tuple(ps.column_names), ('colname',))

	def testCursorFromId(self):
		self.db.execute("DECLARE foo CURSOR WITH HOLD FOR SELECT 1")
		c = self.db.cursor_from_id('foo')
		self.failUnlessEqual(c.read(), [(1,)])
		self.db.execute(
			"DECLARE bar SCROLL CURSOR WITH HOLD FOR SELECT i FROM generate_series(0, 99) AS g(i)"
		)
		c = self.db.cursor_from_id('bar')
		c.seek(50)
		self.failUnlessEqual([x for x, in c.read(10)], list(range(50,60)))
		c.seek(0,2)
		self.failUnlessEqual(c.read(), [])
		c.seek(0)
		self.failUnlessEqual([x for x, in c.read()], list(range(100)))

	def testCopyToSTDOUT(self):
		with self.db.xact():
			self.db.execute("CREATE TABLE foo (i int)")
			foo = self.db.prepare('insert into foo values ($1)')
			foo.load_rows(((x,) for x in range(500)))

			copy_foo = self.db.prepare('copy foo to stdout')
			foo_content = set(copy_foo)
			expected = set((str(i).encode('ascii') + b'\n' for i in range(500)))
			self.failUnlessEqual(expected, foo_content)
			self.failUnlessEqual(expected, set(copy_foo()))
			self.failUnlessEqual(expected, set(chain.from_iterable(copy_foo.chunks())))
			self.failUnlessEqual(expected, set(copy_foo.rows()))
			self.db.execute("DROP TABLE foo")

	def testCopyFromSTDIN(self):
		with self.db.xact():
			self.db.execute("CREATE TABLE foo (i int)")
			foo = self.db.prepare('copy foo from stdin')
			foo.load_rows((str(i).encode('ascii') + b'\n' for i in range(200)))
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
		self.failUnlessEqual(ifoo(1), 1)
		self.failUnlessEqual(ifoo(None), None)
		self.db.execute(
			"CREATE OR REPLACE FUNCTION ifoo(varchar) RETURNS text LANGUAGE SQL AS 'select $1'"
		)
		ifoo = self.db.proc('ifoo(varchar)')
		self.failUnlessEqual(ifoo('1'), '1')
		self.failUnlessEqual(ifoo(None), None)
		self.db.execute(
			"CREATE OR REPLACE FUNCTION ifoo(varchar,int) RETURNS text LANGUAGE SQL AS 'select ($1::int + $2)::varchar'"
		)
		ifoo = self.db.proc('ifoo(varchar,int)')
		self.failUnlessEqual(ifoo('1',1), '2')
		self.failUnlessEqual(ifoo(None,1), None)
		self.failUnlessEqual(ifoo('1',None), None)
		self.failUnlessEqual(ifoo('2',2), '4')

	def testProcExecutionInXact(self):
		with self.db.xact():
			self.testProcExecution()

	def testNULL(self):
		# Directly commpare (SELECT NULL) is None
		self.failUnless(
			self.db.prepare("SELECT NULL")()[0][0] is None,
			"SELECT NULL did not return None"
		)
		# Indirectly compare (select NULL) is None
		self.failUnless(
			self.db.prepare("SELECT $1::text")(None)[0][0] is None,
			"[SELECT $1::text](None) did not return None"
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
		tup = p[0]
		self.failUnless(tup[0] == 1)

		for tup in s1:
			self.failUnlessEqual(tup[0], 1)

		for tup in s1:
			self.failUnlessEqual(tup["name"], 1)

	def testSelectInXact(self):
		with self.db.xact():
			self.testSelect()

	def testCursorRead(self):
		ps = self.db.prepare("SELECT i FROM generate_series(0, (2^8)::int - 1) AS g(i)")
		c = ps.declare()
		self.failUnlessEqual(c.read(0), [])
		self.failUnlessEqual(c.read(0), [])
		self.failUnlessEqual(c.read(1), [(0,)])
		self.failUnlessEqual(c.read(1), [(1,)])
		self.failUnlessEqual(c.read(2), [(2,), (3,)])
		self.failUnlessEqual(c.read(2), [(4,), (5,)])
		self.failUnlessEqual(c.read(3), [(6,), (7,), (8,)])
		self.failUnlessEqual(c.read(4), [(9,), (10,), (11,), (12,)])
		self.failUnlessEqual(c.read(4), [(13,), (14,), (15,), (16,)])
		self.failUnlessEqual(c.read(5), [(17,), (18,), (19,), (20,), (21,)])
		self.failUnlessEqual(c.read(0), [])
		self.failUnlessEqual(c.read(6), [(22,),(23,),(24,),(25,),(26,),(27,)])
		r = [-1]
		i = 4
		v = 28
		maxv = 2**8
		while r:
			i = i * 2
			r = [x for x, in c.read(i)]
			top = min(maxv, v + i)
			self.failUnlessEqual(r, list(range(v, top)))
			v = top

	def testCursorReadInXact(self):
		with self.db.xact():
			self.testCursorRead()

	def testScroll(self, direction = True):
		# Use a large row-set.
		imin = 0
		imax = 2**16
		if direction:
			ps = self.db.prepare("SELECT i FROM generate_series(0, (2^16)::int) AS g(i)")
		else:
			ps = self.db.prepare("SELECT i FROM generate_series((2^16)::int, 0, -1) AS g(i)")
		c = ps.declare()
		c.direction = direction
		if not direction:
			c.seek(0)

		self.failUnlessEqual([x for x, in c.read(10)], list(range(10)))
		# bit strange to me, but i've watched the fetch backwards -jwp 2009
		self.failUnlessEqual([x for x, in c.read(10, 'BACKWARD')], list(range(8, -1, -1)))
		c.seek(0, 2)
		self.failUnlessEqual([x for x, in c.read(10, 'BACKWARD')], list(range(imax, imax-10, -1)))

		# move to end
		c.seek(0, 2)
		self.failUnlessEqual([x for x, in c.read(100, 'BACKWARD')], list(range(imax, imax-100, -1)))
		# move backwards, relative
		c.seek(-100, 1)
		self.failUnlessEqual([x for x, in c.read(100, 'BACKWARD')], list(range(imax-200, imax-300, -1)))

		# move abs, again
		c.seek(14000)
		self.failUnlessEqual([x for x, in c.read(100)], list(range(14000, 14100)))
		# move forwards, relative
		c.seek(100, 1)
		self.failUnlessEqual([x for x, in c.read(100)], list(range(14200, 14300)))
		# move abs, again
		c.seek(24000)
		self.failUnlessEqual([x for x, in c.read(200)], list(range(24000, 24200)))
		# move to end and then back some
		c.seek(20, 2)
		self.failUnlessEqual([x for x, in c.read(200, 'BACKWARD')], list(range(imax-20, imax-20-200, -1)))

		c.seek(0, 2)
		c.seek(-10, 1)
		r1 = c.read(10)
		c.seek(10, 2)
		self.failUnlessEqual(r1, c.read(10))

	def testScrollBackwards(self):
		self.testScroll(direction = False)

	def testWithHold(self):
		with self.db.xact():
			ps = self.db.prepare("SELECT 1")
			c = ps.declare()
			cid = c.cursor_id
		self.failUnlessEqual(c.read()[0][0], 1)
		# make sure it's not cheating
		self.failUnlessEqual(c.cursor_id, cid)
		# check grabs beyond the default chunksize.
		with self.db.xact():
			ps = self.db.prepare("SELECT i FROM generate_series(0, 99) as g(i)")
			c = ps.declare()
			cid = c.cursor_id
		self.failUnlessEqual([x for x, in c.read()], list(range(100)))
		# make sure it's not cheating
		self.failUnlessEqual(c.cursor_id, cid)

	def testLoadRows(self):
		gs = self.db.prepare("SELECT i FROM generate_series(1, 10000) AS g(i)")
		self.failUnlessEqual(
			list((x[0] for x in gs.rows())),
			list(range(1, 10001))
		)
		# exercise ``for x in chunks: dst.load(x)``
		with self.db.connector() as db2:
			db2.execute(
				"""
				CREATE TABLE chunking AS
				SELECT i::text AS t, i::int AS i
				FROM generate_series(1, 10000) g(i);
				"""
			)
			read = self.db.prepare('select * FROM chunking').rows()
			write = db2.prepare('insert into chunking values ($1, $2)').load_rows
			with db2.xact():
				write(read)
			del read, write

			self.failUnlessEqual(
				self.db.prepare('select count(*) FROM chunking').first(),
				20000
			)
			self.failUnlessEqual(
				self.db.prepare('select count(DISTINCT i) FROM chunking').first(),
				10000
			)
		self.db.execute('DROP TABLE chunking')

	def testLoadRowsInXact(self):
		with self.db.xact():
			self.testLoadRows()

	def testLoadChunk(self):
		gs = self.db.prepare("SELECT i FROM generate_series(1, 10000) AS g(i)")
		self.failUnlessEqual(
			list((x[0] for x in chain.from_iterable(gs.chunks()))),
			list(range(1, 10001))
		)
		# exercise ``for x in chunks: dst.load_chunks(x)``
		with self.db.connector() as db2:
			db2.execute(
				"""
				CREATE TABLE chunking AS
				SELECT i::text AS t, i::int AS i
				FROM generate_series(1, 10000) g(i);
				"""
			)
			read = self.db.prepare('select * FROM chunking').chunks()
			write = db2.prepare('insert into chunking values ($1, $2)').load_chunks
			with db2.xact():
				write(read)
			del read, write

			self.failUnlessEqual(
				self.db.prepare('select count(*) FROM chunking').first(),
				20000
			)
			self.failUnlessEqual(
				self.db.prepare('select count(DISTINCT i) FROM chunking').first(),
				10000
			)
		self.db.execute('DROP TABLE chunking')

	def testLoadChunkInXact(self):
		with self.db.xact():
			self.testLoadChunk()

	def testSimpleDML(self):
		self.db.execute("CREATE TEMP TABLE emp(emp_name text, emp_age int)")
		try:
			mkemp = self.db.prepare("INSERT INTO emp VALUES ($1, $2)")
			del_all_emp = self.db.prepare("DELETE FROM emp")
			command, count = mkemp('john', 35)
			self.failUnlessEqual(command, 'INSERT')
			self.failUnlessEqual(count, 1)
			command, count = mkemp('jane', 31)
			self.failUnlessEqual(command, 'INSERT')
			self.failUnlessEqual(count, 1)
			command, count = del_all_emp()
			self.failUnlessEqual(command, 'DELETE')
			self.failUnlessEqual(count, 2)
		finally:
			self.db.execute("DROP TABLE emp")

	def testDML(self):
		self.db.execute("CREATE TEMP TABLE t(i int)")
		try:
			insert_t = self.db.prepare("INSERT INTO t VALUES ($1)")
			delete_t = self.db.prepare("DELETE FROM t WHERE i = $1")
			delete_all_t = self.db.prepare("DELETE FROM t")
			update_t = self.db.prepare("UPDATE t SET i = $2 WHERE i = $1")
			self.failUnlessEqual(insert_t(1)[1], 1)
			self.failUnlessEqual(delete_t(1)[1], 1)
			self.failUnlessEqual(insert_t(2)[1], 1)
			self.failUnlessEqual(insert_t(2)[1], 1)
			self.failUnlessEqual(delete_t(2)[1], 2)

			self.failUnlessEqual(insert_t(3)[1], 1)
			self.failUnlessEqual(insert_t(3)[1], 1)
			self.failUnlessEqual(insert_t(3)[1], 1)
			self.failUnlessEqual(delete_all_t()[1], 3)

			self.failUnlessEqual(update_t(1, 2)[1], 0)
			self.failUnlessEqual(insert_t(1)[1], 1)
			self.failUnlessEqual(update_t(1, 2)[1], 1)
			self.failUnlessEqual(delete_t(1)[1], 0)
			self.failUnlessEqual(delete_t(2)[1], 1)
		finally:
			self.db.execute("DROP TABLE t")

	def testDMLInXact(self):
		with self.db.xact():
			self.testDML()

	def testBatchDML(self):
		self.db.execute("CREATE TEMP TABLE t(i int)")
		try:
			insert_t = self.db.prepare("INSERT INTO t VALUES ($1)")
			delete_t = self.db.prepare("DELETE FROM t WHERE i = $1")
			delete_all_t = self.db.prepare("DELETE FROM t")
			update_t = self.db.prepare("UPDATE t SET i = $2 WHERE i = $1")
			mset = (
				(2,), (2,), (3,), (4,), (5,),
			)
			insert_t.load_rows(mset)
			content = self.db.prepare("SELECT * FROM t ORDER BY 1 ASC")
			self.failUnlessEqual(mset, tuple(content()))
		finally:
			self.db.execute("DROP TABLE t")

	def testBatchDMLInXact(self):
		with self.db.xact():
			self.testBatchDML()

	def testTypes(self):
		'test basic object I/O--input must equal output'
		for (typname, sample_data) in type_samples:
			pb = self.db.prepare(
				"SELECT $1::" + typname
			)
			for sample in sample_data:
				rsample = list(pb.rows(sample))[0][0]
				if isinstance(rsample, pg_types.Array):
					rsample = rsample.nest()
				self.failUnless(
					rsample == sample,
					"failed to return %s object data as-is; gave %r, received %r" %(
						typname, sample, rsample
					)
				)

	def testXML(self):
		try:
			xml = self.db.prepare('select $1::xml')
			textxml = self.db.prepare('select $1::text::xml')
			r = textxml.first('<foo/>')
		except (pg_exc.FeatureError, pg_exc.UndefinedObjectError):
			return
		foo = pg_types.etree.XML('<foo/>')
		bar = pg_types.etree.XML('<bar/>')
		tostr = pg_types.etree.tostring
		self.failUnlessEqual(tostr(xml.first(foo)), tostr(foo))
		self.failUnlessEqual(tostr(xml.first(bar)), tostr(bar))
		self.failUnlessEqual(tostr(textxml.first('<foo/>')), tostr(foo))
		self.failUnlessEqual(tostr(textxml.first('<foo/>')), tostr(foo))
		self.failUnlessEqual(tostr(xml.first(pg_types.etree.XML('<foo/>'))), tostr(foo))
		self.failUnlessEqual(tostr(textxml.first('<foo/>')), tostr(foo))
		# test fragments
		self.failUnlessEqual(
			tuple(
				tostr(x) for x in xml.first('<foo/><bar/>')
			), (tostr(foo), tostr(bar))
		)
		self.failUnlessEqual(
			tuple(
				tostr(x) for x in textxml.first('<foo/><bar/>')
			),
			(tostr(foo), tostr(bar))
		)
		# mixed text and etree.
		self.failUnlessEqual(
			tuple(
				tostr(x) for x in xml.first((
					'<foo/>', bar,
				))
			),
			(tostr(foo), tostr(bar))
		)
		self.failUnlessEqual(
			tuple(
				tostr(x) for x in xml.first((
					'<foo/>', bar,
				))
			),
			(tostr(foo), tostr(bar))
		)

	def testTypeIOError(self):
		original = dict(self.db.typio._cache)
		ps = self.db.prepare('SELECT $1::numeric')
		self.failUnlessRaises(pg_exc.ParameterError, ps, 'foo')
		try:
			self.db.execute('CREATE type test_tuple_error AS (n numeric);')
			ps = self.db.prepare('SELECT $1::test_tuple_error AS the_column')
			self.failUnlessRaises(pg_exc.ParameterError, ps, ('foo',))
			try:
				ps(('foo',))
			except pg_exc.ParameterError as err:
				# 'foo' is not a valid Decimal.
				# Expecting a double TupleError here, one from the composite pack
				# and one from the row pack.
				self.failUnless(isinstance(err.__context__, pg_exc.ColumnError))
				self.failUnlessEqual(int(err.details['position']), 0)
				# attribute number that the failure occurred on
				self.failUnlessEqual(int(err.__context__.details['position']), 0)
			else:
				self.fail("failed to raise TupleError")

			# testing tuple error reception is a bit more difficult.
			# to do this, we need to immitate failure as we can't rely that any
			# causable failure will always exist.
			class ThisError(Exception):
				pass
			def raise_ThisError(arg):
				raise ThisError(arg)
			pack, unpack = self.db.typio.resolve(pg_types.NUMERICOID)
			# remove any existing knowledge about "test_tuple_error"
			self.db.typio._cache = original
			self.db.typio._cache[pg_types.NUMERICOID] = (pack, raise_ThisError)
			# Now, numeric_unpack will always raise "ThisError".
			ps = self.db.prepare('SELECT $1::numeric as col')
			self.failUnlessRaises(
				pg_exc.ColumnError, ps, decimal.Decimal("101")
			)
			try:
				ps(decimal.Decimal("101"))
			except pg_exc.ColumnError as err:
				self.failUnless(isinstance(err.__context__, ThisError))
				# might be too inquisitive....
				self.failUnlessEqual(int(err.details['position']), 0)
				self.failUnless('numeric' in err.message)
				self.failUnless('col' in err.message)
			else:
				self.fail("failed to raise TupleError from reception")
			ps = self.db.prepare('SELECT $1::test_tuple_error AS tte')
			try:
				ps((decimal.Decimal("101"),))
			except pg_exc.ColumnError as err:
				self.failUnless(isinstance(err.__context__, pg_exc.ColumnError))
				self.failUnless(isinstance(err.__context__.__context__, ThisError))
				# might be too inquisitive....
				self.failUnlessEqual(int(err.details['position']), 0)
				self.failUnlessEqual(int(err.__context__.details['position']), 0)
				self.failUnless('test_tuple_error' in err.message)
			else:
				self.fail("failed to raise TupleError from reception")
		finally:
			self.db.execute('drop type test_tuple_error;')

	def testSyntaxError(self):
		try:
			self.db.prepare("SELEKT 1")()
		except pg_exc.SyntaxError:
			return
		self.fail("SyntaxError was not raised")

	def testSchemaNameError(self):
		try:
			self.db.prepare("SELECT * FROM sdkfldasjfdskljZknvson.foo")()
		except pg_exc.SchemaNameError:
			return
		self.fail("SchemaNameError was not raised")

	def testUndefinedTableError(self):
		try:
			self.db.prepare("SELECT * FROM public.lkansdkvsndlvksdvnlsdkvnsdlvk")()
		except pg_exc.UndefinedTableError:
			return
		self.fail("UndefinedTableError was not raised")

	def testUndefinedColumnError(self):
		try:
			self.db.prepare("SELECT x____ysldvndsnkv FROM information_schema.tables")()
		except pg_exc.UndefinedColumnError:
			return
		self.fail("UndefinedColumnError was not raised")

	def testSEARVError_avgInWhere(self):
		try:
			self.db.prepare("SELECT 1 WHERE avg(1) = 1")()
		except pg_exc.SEARVError:
			return
		self.fail("SEARVError was not raised")

	def testSEARVError_groupByAgg(self):
		try:
			self.db.prepare("SELECT 1 GROUP BY avg(1)")()
		except pg_exc.SEARVError:
			return
		self.fail("SEARVError was not raised")

	def testTypeMismatchError(self):
		try:
			self.db.prepare("SELECT 1 WHERE 1")()
		except pg_exc.TypeMismatchError:
			return
		self.fail("TypeMismatchError was not raised")

	def testUndefinedObjectError(self):
		try:
			self.failUnlessRaises(
				pg_exc.UndefinedObjectError,
				self.db.prepare, "CREATE TABLE lksvdnvsdlksnv(i intt___t)"
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
			self.db.execute, "SELECT * FROM withfoo"
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
			self.db.execute, "SELECT * FROM withfoo"
		)

	def testPreparedTransactionCommit(self):
		with self.db.xact(gid='commit_gid') as x:
			self.db.execute("create table commit_gidtable as select 'foo'::text as t;")
			x.prepare()
			# not committed yet, so it better fail.
			self.failUnlessRaises(pg_exc.UndefinedTableError,
				self.db.execute, "select * from commit_gidtable"
			)
		# now it's committed.
		self.failUnlessEqual(
			self.db.prepare("select * FROM commit_gidtable").first(),
			'foo',
		)
		self.db.execute('drop table commit_gidtable;')

	def testWithUnpreparedTransaction(self):
		try:
			with self.db.xact(gid='not-gonna-prepare-it') as x:
				pass
		except pg_exc.ActiveTransactionError:
			# *must* be okay to query again.
			self.failUnlessEqual(self.db.prepare('select 1').first(), 1)
		else:
			self.fail("commit with gid succeeded unprepared..")

	def testWithPreparedException(self):
		class TheFailure(Exception):
			pass
		try:
			with self.db.xact(gid='yeah,weprepare') as x:
				x.prepare()
				raise TheFailure()
		except TheFailure as err:
			# __exit__ should have issued ROLLBACK PREPARED, so let's find out.
			# *must* be okay to query again.
			self.failUnlessEqual(self.db.prepare('select 1').first(), 1)
			x = self.db.xact(gid='yeah,weprepare')
			self.failUnlessRaises(pg_exc.UndefinedObjectError, x.recover)
		else:
			self.fail("failure exception was not raised")

	def testUnPreparedTransactionCommit(self):
		x = self.db.xact(gid='never_prepared')
		x.start()
		self.failUnlessRaises(pg_exc.ActiveTransactionError, x.commit)
		self.failUnlessRaises(pg_exc.InFailedTransactionError, x.commit)

	def testPreparedTransactionRollback(self):
		x = self.db.xact(gid='rollback_gid')
		x.start()
		self.db.execute("create table gidtable as select 'foo'::text as t;")
		x.prepare()
		x.rollback()
		self.failUnlessRaises(
			pg_exc.UndefinedTableError,
			self.db.execute, "select * from gidtable"
		)

	def testPreparedTransactionRecovery(self):
		x = self.db.xact(gid='recover dis')
		x.start()
		self.db.execute("create table distable (i int);")
		x.prepare()
		del x
		x = self.db.xact(gid='recover dis')
		x.recover()
		x.commit()
		self.db.execute("drop table distable;")

	def testPreparedTransactionRecoveryAbort(self):
		x = self.db.xact(gid='recover dis abort')
		x.start()
		self.db.execute("create table distableabort (i int);")
		x.prepare()
		del x
		x = self.db.xact(gid='recover dis abort')
		x.recover()
		x.rollback()
		self.failUnlessRaises(
			pg_exc.UndefinedTableError,
			self.db.execute, "select * from distableabort"
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
			self.failUnlessEqual(err.source, 'CLIENT')

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
				self.failUnlessEqual(err.source, 'CLIENT')

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
