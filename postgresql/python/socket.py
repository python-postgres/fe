##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
socket tools
"""
import random
import socket
import math
import errno

__all__ = ['find_available_port', 'SocketCreator']

class SocketCreator(object):
	"""
	Object used to create a socket and connect it.

	This is, more or less, a specialized partial() for socket creation.
	"""
	def __call__(self, timeout = None):
		s = socket.socket(*self.socket_create)
		s.settimeout(float(timeout) if timeout is not None else None)
		s.connect(self.socket_connect)
		s.settimeout(None)
		return s

	def __init__(self,
		socket_create : "positional parameters given to socket.socket()",
		socket_connect : "parameter given to socket.connect()",
	):
		self.socket_create = socket_create
		self.socket_connect = socket_connect

	def __str__(self):
		return 'socket' + repr(self.socket_connect)

def find_available_port(
	interface : "attempt to bind to interface" = 'localhost',
	address_family : "address family to use (default: AF_INET)" = socket.AF_INET,
	limit : "Number tries to make before giving up" = 1024,
	port_range = (6600, 56600)
) -> (int, None):
	"""
	Find an available port on the given interface for the given address family.

	Returns a port number that was successfully bound to or `None` if the attempt
	limit was reached.
	"""
	i = 0
	while i < limit:
		i += 1
		port = (
			math.floor(
				random.random() * (port_range[1] - port_range[0])
			) + port_range[0]
		)
		s = socket.socket(address_family, socket.SOCK_STREAM,)
		try:
			s.bind(('localhost', port))
		except socket.error as e:
			if e.errno in (errno.EACCES, errno.EADDRINUSE, errno.EINTR):
				# try again
				continue
		finally:
			s.close()
		break
	else:
		port = None

	return port
