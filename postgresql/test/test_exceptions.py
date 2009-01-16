##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import unittest
import postgresql.exceptions as pg_exc

class test_exceptions(unittest.TestCase):
	def test_error_lookup(self):
		self.failUnless(
			pg_exc.ErrorLookup('00000') == pg_exc.Error
		)
		self.failUnless(
			pg_exc.ErrorLookup('XX000') == pg_exc.InternalError
		)
		self.failUnless(
			pg_exc.ErrorLookup('42000') == pg_exc.SEARVError
		)
		self.failUnless(
			pg_exc.ErrorLookup('08P01') == pg_exc.ProtocolError
		)

	def test_warning_lookup(self):
		self.failUnless(
			pg_exc.WarningLookup('01000') == pg_exc.Warning
		)
		self.failUnless(
			pg_exc.WarningLookup('02000') == pg_exc.NoDataWarning
		)
		self.failUnless(
			pg_exc.WarningLookup('01P01') == pg_exc.DeprecationWarning
		)

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
