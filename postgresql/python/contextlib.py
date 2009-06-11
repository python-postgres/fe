##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import sys

class NoCM(object):
	"""
	CM that does nothing. Useful for parameterized CMs.
	"""
	__slots__ = ()

	def __new__(typ):
		return typ

	@staticmethod
	def __enter__():
		pass

	@classmethod
	def __context__(typ):
		return typ

	@staticmethod
	def __exit__(typ, val, tb):
		pass

class Nested(object):
	"""
	cause they deprecated it in 3.1

	Implemented with a class instead of a contextlib.contextmanager. A generator
	CM is probably a better choice as it would likely make it easier to properly
	preserve __context__.

	WARNING: Uses __cause__ to reference exception contexts when possible.
	"""
	__slots__ = ('cm', 'exits')

	def __init__(self, *cm):
		self.cm = tuple([
			x.__context__() if hasattr(x, '__context__') else x for x in cm
		])

	def __enter__(self):
		if hasattr(self, 'exits'):
			raise RuntimeError("context manager already ran")
		self.exits = []
		r = []
		try:
			for x in self.cm:
				r.append(x.__enter__())
				self.exits.append(x.__exit__)
		except:
			if self.__exit__(*sys.exc_info()):
				raise RuntimeError("cannot suppress exceptions raised during entry")
			raise
		return tuple(r)

	def __exit__(self, typ, val, tb):
		# if there are no exits, there's nothing to be done.
		oval = val
		for x in reversed(self.exits):
			try:
				if x(typ, val, tb):
					typ = val = tb = None
			except:
				newtyp, newval, newtb = sys.exc_info()
				if newval.__cause__ is None:
					newval.__cause__ = newval.__context__
				typ = newtyp
				val = newval
				tb = newtb
		self.exits = ()
		if val is not None and val is not oval:
			raise val
		return val is None

	def __context__(self):
		return self
