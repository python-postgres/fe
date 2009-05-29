##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import sys
import os
import unittest
import tempfile

from .. import exceptions as pg_exc
from .. import unittest as pg_unittest
from .. import lib as pg_lib
from .. import sys as pg_sys

ilf = """
preface

[sym]
select 1
[sym_ref]
*[sym]
[sym_ref_trail]
*[sym] WHERE FALSE
[sym_first::first]
select 1


[sym_rows::rows]
select 1

[sym_chunks::chunks]
select 1

[sym_declare::declare]
select 1

[sym_const:const:first]
select 1
[sym_const_rows:const:rows]
select 1
[sym_const_chunks:const:chunks]
select 1
[sym_const_column:const:column]
select 1
[sym_const_ddl:const:]
create temp table sym_const_dll (i int);

[sym_preload:preload:first]
select 1

[sym_proc:proc]
test_ilf_proc(int)

[sym_srf_proc:proc]
test_ilf_srf_proc(int)
"""

class test_lib(pg_unittest.TestCaseWithCluster):
	# NOTE: Module libraries are implicitly tested
	# in postgresql.test.test_driver; much functionality
	# depends on the `sys` library.
	def _testILF(self, lib):
		self.failUnless('preface' in lib.preface)
		self.db.execute("CREATE OR REPLACE FUNCTION test_ilf_proc(int) RETURNS int language sql as 'select $1';")
		self.db.execute("CREATE OR REPLACE FUNCTION test_ilf_srf_proc(int) RETURNS SETOF int language sql as 'select $1';")
		b = pg_lib.Binding(self.db, lib)
		self.failUnlessEqual(b.sym_ref(), [(1,)])
		self.failUnlessEqual(b.sym_ref_trail(), [])
		self.failUnlessEqual(b.sym(), [(1,)])
		self.failUnlessEqual(b.sym_first(), 1)
		self.failUnlessEqual(list(b.sym_rows()), [(1,)])
		self.failUnlessEqual([list(x) for x in b.sym_chunks()], [[(1,)]])
		c = b.sym_declare()
		self.failUnlessEqual(c.read(), [(1,)])
		c.seek(0)
		self.failUnlessEqual(c.read(), [(1,)])
		self.failUnlessEqual(b.sym_const, 1)
		self.failUnlessEqual(b.sym_const_column, [1])
		self.failUnlessEqual(b.sym_const_rows, [(1,)])
		self.failUnlessEqual(b.sym_const_chunks, [[(1,)]])
		self.failUnlessEqual(b.sym_const_ddl, ('CREATE', None))
		self.failUnlessEqual(b.sym_preload(), 1)
		# now stored procs
		self.failUnlessEqual(b.sym_proc(2,), 2)
		self.failUnlessEqual(list(b.sym_srf_proc(2,)), [2])
		self.failUnlessRaises(AttributeError, getattr, b, 'LIES')

	def testILF_from_lines(self):
		lib = pg_lib.ILF.from_lines([l + '\n' for l in ilf.splitlines()])
		self._testILF(lib)

	def testILF_from_file(self):
		with tempfile.NamedTemporaryFile('w', encoding = 'utf-8') as f:
			f.write(ilf)
			f.flush()
			lib = pg_lib.ILF.open(f.name, encoding = 'utf-8')
			self._testILF(lib)

	def testLoad(self):
		# gotta test it in the cwd...
		pid = os.getpid()
		frag = 'temp' + str(pid)
		fn = 'lib' + frag + '.sql'
		try:
			with open(fn, 'w') as f:
				f.write("[foo]\nSELECT 1")
			pg_sys.libpath.insert(0, os.path.curdir)
			l = pg_lib.load(frag)
			b = pg_lib.Binding(self.db, l)
			self.failUnlessEqual(b.foo(), [(1,)])
		finally:
			os.remove(fn)
	
	def testCategory(self):
		lib = pg_lib.ILF.from_lines([l + '\n' for l in ilf.splitlines()])
		# XXX: evil, careful..
		lib._name = 'name'
		c = pg_lib.Category(lib)
		c(self.db)
		self.failUnlessEqual(self.db.name.sym_first(), 1)
		c = pg_lib.Category(renamed = lib)
		c(self.db)
		self.failUnlessEqual(self.db.renamed.sym_first(), 1)

	def testCategoryAliases(self):
		lib = pg_lib.ILF.from_lines([l + '\n' for l in ilf.splitlines()])
		# XXX: evil, careful..
		lib._name = 'name'
		c = pg_lib.Category(lib, renamed = lib)
		c(self.db)
		self.failUnlessEqual(self.db.name.sym_first(), 1)
		self.failUnlessEqual(self.db.renamed.sym_first(), 1)

if __name__ == '__main__':
	unittest.main()
