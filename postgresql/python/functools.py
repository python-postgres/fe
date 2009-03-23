##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
additional functools
"""
from .decorlib import method
try:
	from .optimized import compose
except ImportError:
	pass

class Composition(tuple):
	'simple compositions'
	def __call__(self, r):
		for x in self:
			r = x(r)
		return r
	try:
		__call__ = method(compose)
	except NameError:
		pass
