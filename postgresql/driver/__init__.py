##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Driver package for connecting to PostgreSQL via a data stream(sockets).
"""
from . import pgapi as implementation
__all__ = ['Connector', 'Connection', 'connect', 'implementation']

Connector = implementation.Connector
Connection = implementation.Connection

_connectors = {}
def connect(**kw):
	"""
	Create a PG-API connection using the given parameters.

	See the `Connecting` section in the documentation for more information about
	suitable parameters.
	"""
	id = set(kw.items())
	c = _connectors.get(id)
	if c is None:
		c = Connector(**kw)
		_connectors[id] = c
	return c()
