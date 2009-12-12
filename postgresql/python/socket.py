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
import ssl

__all__ = ['find_available_port', 'SocketFactory']

class SocketFactory(object):
	"""
	Object used to create a socket and connect it.

	This is, more or less, a specialized partial() for socket creation.

	Additionally, it provides methods and attributes for abstracting
	exception management on socket operation.
	"""
	fatal_exception_messages = {
		errno.ECONNRESET : 'server explicitly closed the connection',
		errno.EPIPE : 'broken connection detected on send',
		errno.ECONNREFUSED : 'server refused connection',
		errno.EHOSTUNREACH : 'server is not reachable',
	}
	timeout_exception = socket.timeout
	fatal_exception = socket.error
	try_again_exception = socket.error

	def timed_out(self, err) -> bool:
		return type(err) is self.timeout_exception

	def try_again(self, err) -> bool:
		"""
		Does the error indicate that the operation should be
		tried again?
		"""
		return getattr(err, 'errno', 0) == errno.EINTR

	def connection_refused(self, err) -> bool:
		"""
		Does the error indicate that the connection was explicitly
		refused by the server?
		"""
		return getattr(err, 'errno', 0) == errno.ECONNREFUSED

	@classmethod
	def fatal_exception_message(typ, err) -> (str, None):
		"""
		If the exception was fatal to the connection,
		what message should be given to the user?
		"""
		return typ.fatal_exception_messages.get(err.errno)

	def secure(self, socket : socket.socket) -> ssl.SSLSocket:
		"secure a socket with SSL"
		if self.socket_secure is not None:
			return ssl.wrap_socket(socket, **self.socket_secure)
		else:
			return ssl.wrap_socket(socket)

	def __call__(self, timeout = None):
		s = socket.socket(*self.socket_create)
		s.settimeout(float(timeout) if timeout is not None else None)
		s.connect(self.socket_connect)
		s.settimeout(None)
		return s

	def __init__(self,
		socket_create : "positional parameters given to socket.socket()",
		socket_connect : "parameter given to socket.connect()",
		socket_secure : "keywords given to ssl.wrap_socket" = None,
	):
		self.socket_create = socket_create
		self.socket_connect = socket_connect
		self.socket_secure = socket_secure

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

	Returns a port number that was successfully bound to or `None` if the
	attempt limit was reached.
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
