# -*- encoding: utf-8 -*-
# $Id: test.py,v 1.2 2008/01/29 04:25:27 jwp Exp $
##
# copyright 2004-2005, pg/python project.
# http://python.projects.postgresql.org
##
import unittest
import postgresql.encodings.bytea

class test_bytea_codec(unittest.TestCase):
	def testEncoding(self):
		for x in xrange(255):
			c = chr(x)
			b = c.encode('bytea')
			if c == '\\':
				c = '\\\\'
			if c != b and oct(x).lstrip('0').rjust(3, '0') != b[1:]:
				self.fail(
					"bytea encoding failed at %d; encoded %r to %r" %(x, c, b,)
				)

	def testDecoding(self):
		self.failUnless('bytea'.decode('bytea') == 'bytea')
		self.failUnless('\\\\'.decode('bytea') == '\\')
		self.failUnlessRaises(ValueError, '\\'.decode, 'bytea')
		self.failUnlessRaises(ValueError, 'foo\\'.decode, 'bytea')
		self.failUnlessRaises(ValueError, r'foo\0'.decode, 'bytea')
		self.failUnlessRaises(ValueError, r'foo\00'.decode, 'bytea')
		self.failUnlessRaises(ValueError, r'\f'.decode, 'bytea')
		self.failUnlessRaises(ValueError, r'\800'.decode, 'bytea')
		self.failUnlessRaises(ValueError, r'\7f0'.decode, 'bytea')
		for x in xrange(255):
			seq = ('\\' + oct(x).lstrip('0').rjust(3, '0'))
			dx = ord(seq.decode('bytea'))
			if dx != x:
				self.fail(
					"generated sequence failed to map back; current is %d, " \
					"rendered %r, transformed to %d" %(x, seq, dx)
				)

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
