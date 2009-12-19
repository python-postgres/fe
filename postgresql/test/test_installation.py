##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import sys
import os
import unittest
from .. import installation as ins

class test_installation(unittest.TestCase):
	"""
	Most of this is exercised by TestCaseWithCluster, but do some
	explicit checks up front to help find any specific issues that
	do not naturally occur.
	"""
	def test_parse_configure_options(self):
		# Check expectations.
		self.failUnlessEqual(
			list(ins.parse_configure_options("")), [],
		)
		self.failUnlessEqual(
			list(ins.parse_configure_options("  ")), [],
		)
		self.failUnlessEqual(
			list(ins.parse_configure_options("--foo --bar")),
			[('foo',True),('bar',True)]
		)
		self.failUnlessEqual(
			list(ins.parse_configure_options("'--foo' '--bar'")),
			[('foo',True),('bar',True)]
		)
		self.failUnlessEqual(
			list(ins.parse_configure_options("'--foo=A properly isolated string' '--bar'")),
			[('foo','A properly isolated string'),('bar',True)]
		)
		# hope they don't ever use backslash escapes.
		# This is pretty dirty, but it doesn't seem well defined anyways.
		self.failUnlessEqual(
			list(ins.parse_configure_options("'--foo=A ''properly'' isolated string' '--bar'")),
			[('foo',"A 'properly' isolated string"),('bar',True)]
		)
		# handle some simple variations, but it's 
		self.failUnlessEqual(
			list(ins.parse_configure_options("'--foo' \"--bar\"")),
			[('foo',True),('bar',True)]
		)
		# Show the failure.
		try:
			self.failUnlessEqual(
				list(ins.parse_configure_options("'--foo' \"--bar=/A dir/file\"")),
				[('foo',True),('bar','/A dir/file')]
			)
		except AssertionError:
			pass
		else:
			self.fail("did not detect induced failure")

	def test_minimum(self):
		'version info'
		# Installation only "needs" the version information
		i = ins.Installation({'version' : 'PostgreSQL 2.2.3'})
		self.failUnlessEqual(
			i.version, 'PostgreSQL 2.2.3'
		)
		self.failUnlessEqual(
			i.version_info, (2,2,3,'final',0)
		)
		self.failUnlessEqual(i.postgres, None)
		self.failUnlessEqual(i.postmaster, None)

	def test_exec(self):
		# check the executable
		i = ins.pg_config_dictionary(
			sys.executable, '-m', __package__ + '.support', 'pg_config')
		# automatically lowers the key
		self.failUnlessEqual(i['foo'], 'BaR')
		self.failUnlessEqual(i['feh'], 'YEAH')
		self.failUnlessEqual(i['version'], 'NAY')

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
