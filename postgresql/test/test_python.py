##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import unittest
import socket
import errno
from itertools import chain
from operator import methodcaller
from contextlib import contextmanager

from ..python.contextlib import *
from ..python import functools
from ..python import itertools
from ..python.socket import find_available_port
from ..python import element

class Ele(element.Element):
	_e_label = property(
		lambda x: getattr(x, 'label', 'ELEMENT')
	)
	_e_factors = ('ancestor', 'secondary')
	secondary = None

	def __init__(self, s = None):
		self.ancestor = s

	def __str__(self):
		return 'STRDATA'

	def _e_metas(self):
		yield ('first', getattr(self, 'first', 'firstv'))
		yield ('second', getattr(self, 'second', 'secondv'))

class test_element(unittest.TestCase):
	def test_primary_factor(self):
		x = Ele()
		# no factors
		self.failUnlessEqual(element.prime_factor(object()), None)
		self.failUnlessEqual(element.prime_factor(x), ('ancestor', None))
		y = Ele(x)
		self.failUnlessEqual(element.prime_factor(y), ('ancestor', x))

	def test_primary_factors(self):
		x = Ele()
		x.ancestor = x
		self.failUnlessRaises(
			element.RecursiveFactor, list, element.prime_factors(x)
		)
		y = Ele(x)
		x.ancestor = y
		self.failUnlessRaises(
			element.RecursiveFactor, list, element.prime_factors(y)
		)
		self.failUnlessRaises(
			element.RecursiveFactor, list, element.prime_factors(x)
		)
		x.ancestor = None
		z = Ele(y)
		self.failUnlessEqual(list(element.prime_factors(z)), [
			('ancestor', y),
			('ancestor', x),
			('ancestor', None),
		])

	def test_format_element(self):
		# Considering that this is subject to change, frequently,
		# I/O equality tests are inappropriate.
		# Rather, a hierarchy will be defined, and the existence
		# of certain pieces of information in the string will be validated.
		x = Ele()
		y = Ele()
		z = Ele()
		alt1 = Ele()
		alt2 = Ele()
		alt1.first = 'alt1-first'
		alt1.second = 'alt1-second'
		alt2.first = 'alt2-first'
		alt2.second = 'alt2-second'
		altprime = Ele()
		altprime.first = 'alt2-ancestor'
		alt2.ancestor = altprime
		z.ancestor = y
		y.ancestor = x
		z.secondary = alt1
		y.secondary = alt2
		x.first = 'unique1'
		y.first = 'unique2'
		x.second = 'unique3'
		z.second = 'unique4'
		y.label = 'DIFF'
		data = element.format_element(z)
		self.failUnless(x.first in data)
		self.failUnless(y.first in data)
		self.failUnless(x.second in data)
		self.failUnless(z.second in data)
		self.failUnless('DIFF' in data)
		self.failUnless('alt1-first' in data)
		self.failUnless('alt2-first' in data)
		self.failUnless('alt1-second' in data)
		self.failUnless('alt2-second' in data)
		self.failUnless('alt2-ancestor' in data)
		x.ancestor = z
		self.failUnlessRaises(element.RecursiveFactor, element.format_element, z)

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
		nothing = compose(())
		self.failUnlessEqual(nothing("100"), "100")
		self.failUnlessEqual(nothing(100), 100)
		self.failUnlessEqual(nothing(None), None)

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

class contextlib(unittest.TestCase):
	def testNoCM(self):
		with NoCM as foo:
			pass
		self.failUnlessEqual(foo, None)
		self.failUnlessEqual(NoCM(), NoCM)
		# has no state, may be used repeatedly
		with NoCM as foo:
			pass
		self.failUnlessEqual(foo, None)

	def testNested(self):
		class SomeExc(Exception):
			pass
		@contextmanager
		def just_one():
			yield 1
		@contextmanager
		def just_two():
			try:
				yield 2
			except:
				print("wtf")
		@contextmanager
		def raise_exc():
			raise SomeExc("foo")
			yield 1
		@contextmanager
		def finally_exc():
			try:
				yield 1
			finally:
				raise SomeExc("foo")
		@contextmanager
		def suppress():
			try:
				yield None
			except SomeExc:
				pass
		@contextmanager
		def expect_and_raise(exc, rexc):
			try:
				yield None
			except exc:
				raise rexc("bleh")

		with Nested() as foo:
			pass
		self.failUnlessEqual(foo, ())
		try:
			with Nested() as foo:
				self.failUnlessEqual(foo, ())
				raise SomeExc("bar")
		except SomeExc:
			pass
		else:
			self.fail("empty Nested did not raise exception")

		with Nested(just_one()) as foo:
			pass
		self.failUnlessEqual(foo, (1,))

		with Nested(just_one(), just_one()) as foo:
			pass
		self.failUnlessEqual(foo, (1,1))

		# NoCM won't raise a RuntimeError on re-use.
		N=Nested(NoCM)
		with N:
			pass
		try:
			with N:
				pass
		except RuntimeError:
			pass
		else:
			self.fail("exhausted Nested() failed to raise RuntimeError")

		# unsuppressed exceptions on entry
		try:
			with Nested(raise_exc()):
				pass
		except SomeExc:
			pass
		else:
			self.fail("nested didn't raise SomeExc")
		try:
			with Nested(just_one(), raise_exc()):
				pass
		except SomeExc:
			pass
		else:
			self.fail("nested didn't raise SomeExc")

		# suppressed SomeExc during entry--disallowed.
		try:
			with Nested(suppress(), raise_exc()):
				pass
		except RuntimeError:
			pass
		else:
			self.fail("nested didn't raise RuntimeError on suppressed partial entry")

		# suppress should stop it, and that's okay because we're already
		# inside the block.
		with Nested(suppress(), finally_exc()):
			raise SomeExc("foo")

		# This test case validates that the context is being
		# properly set.
		class ThisExc(Exception):
			pass
		try:
			with Nested(expect_and_raise(ThisExc, SomeExc)):
				raise ThisExc("FOO")
		except SomeExc as e:
			self.failUnlessEqual(type(e.__context__), ThisExc)
		else:
			self.fail("failed to raise exception")

		class ThatExc(Exception):
			pass
		try:
			with Nested(expect_and_raise(ThisExc, SomeExc), expect_and_raise(ThatExc, ThisExc)):
				raise ThatExc("BAFOON")
		except SomeExc as e:
			# ThatExc -> ThisExc -> SomeExc
			self.failUnlessEqual(type(e.__cause__), ThisExc)
			self.failUnlessEqual(type(e.__cause__.__cause__), ThatExc)

		try:
			with Nested(just_one()) as foo:
				self.failUnlessEqual(foo, (1,))
				raise SomeExc("inside the block")
		except SomeExc as e:
			pass
		else:
			self.fail("Nested didn't pass up exception raised in block")

		# Slightly more complex suppression.
		with Nested(just_two(), suppress(), expect_and_raise(ThisExc, SomeExc), expect_and_raise(ThatExc, ThisExc)) as foo:
			raise ThatExc("MOOF")
		self.failUnlessEqual(foo[0], 2)

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
