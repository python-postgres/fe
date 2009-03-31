##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import unittest
import socket
import errno
from itertools import chain
from operator import methodcaller

from ..python import functools
from ..python import itertools
from ..python.socket import find_available_port

class test_itertools(unittest.TestCase):
	def testInterlace(self):
		i1 = range(0, 100, 4)
		i2 = range(1, 100, 4)
		i3 = range(2, 100, 4)
		i4 = range(3, 100, 4)
		self.failUnlessEqual(
			list(itertools.interlace(i1, i2, i3, i4)),
			list(range(100))
		)

class test_functools(unittest.TestCase):
	def testComposition(self):
		compose = functools.Composition
		simple = compose((int, str))
		self.failUnlessEqual("100", simple("100"))
		timesfour_fourtimes = compose((methodcaller('__mul__', 4),)*4)
		self.failUnlessEqual(4*(4*4*4*4), timesfour_fourtimes(4))

	def testRSetAttr(self):
		class anob(object):
			pass
		ob = anob()
		self.failUnlessRaises(AttributeError, getattr, ob, 'foo')
		rob = functools.rsetattr('foo', 'bar', ob)
		self.failUnless(rob is ob)
		self.failUnless(rob.foo is ob.foo)
		self.failUnless(rob.foo == 'bar')

class test_socket(unittest.TestCase):
	def testFindAvailable(self):
		# the port is randomly generated, so make a few trials before
		# determining success.
		for i in range(100):
			portnum = find_available_port()
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			try:
				s.connect(('localhost', portnum))
			except socket.error as err:
				self.failUnlessEqual(err.errno, errno.ECONNREFUSED)
			else:
				self.fail("got a connection to an available port: " + str(portnum))

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
