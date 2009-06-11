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
