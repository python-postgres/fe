##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
PostgreSQL library system functions and data.
"""
import sys
import os
from .python.element import format_element

def _msghook__(msg):
	"""
	Built-in message hook. DON'T TOUCH!
	"""
	if sys.stderr and not sys.stderr.closed:
		try:
			sys.stderr.write(format_element(msg) + os.linesep)
		except Exception:
			try:
				sys.excepthook(*sys.exc_info())
			except Exception:
				# gasp.
				pass

def msghook(msg):
	"""
	Message hook pointing to _msghook__.

	Overload if you like. All untrapped messages raised by
	driver connections come here to be printed to stderr.
	"""
	return _msghook__(msg)

def reset_msghook(with_func = msghook):
	'restore the original msghook function'
	global msghook
	msghook = with_func
