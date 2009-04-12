##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Protocol version 3.0 client and tools.
"""
import os
from traceback import format_exception_only

class ConnectionAttempt(object):
	"""
	When a PQv3 connection attempt is made, but not successfully established, a
	ConnectionAttempt can be used to document the attempt in order to provide
	detailed information about the failure.

	Instances of this class should be used with
	`postgresql.exceptions.ClientCannotConnectError`s.

	Properties:

	 ssl_negotiation
	  True if SSL was attempted
	  False if connection attempt was made without SSL
	  None if SSL was attempted, but backend signal
	"""
	exception_string = staticmethod(format_exception_only)
	__slots__ = (
		'ssl_negotiation',
		'socket_creator',
		'exception',
	)

	def __init__(self,
		ssl_negotiation,
		socket_creator,
		exception,
	):
		self.ssl_negotiation = ssl_negotiation
		self.socket_creator = socket_creator
		self.exception = exception

	def __str__(self):
		if self.ssl_negotiation is True:
			ssl = 'SSL'
		elif self.ssl_negotiation is False:
			ssl = 'NOSSL'
		elif self.ssl_negotiation is None:
			ssl = 'SSL then NOSSL'
		else:
			ssl = '<unexpected ssl_negotiation configuration>'
		excstr = ''.join(self.exception_string(type(self.exception), self.exception))
		return str(self.socket_creator) \
			+ ' -> (' + ssl + ')' \
			+ os.linesep + excstr.strip()
