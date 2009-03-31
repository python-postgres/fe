##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
additional functools
"""
from .decorlib import method

def rsetattr(attr, val, ob):
	"""
	setattr() and return `ob`. Different order used to allow easier partial
	usage.
	"""
	setattr(ob, attr, val)
	return ob

try:
	from .optimized import rsetattr
except ImportError:
	pass

class Composition(tuple):
	'simple compositions'
	def __call__(self, r):
		for x in self:
			r = x(r)
		return r

	try:
		from .optimized import compose
		__call__ = method(compose)
		del compose
	except ImportError:
		pass
