##
# .python.socket - additional tools for working with sockets
##
import sys
import os
import random
import socket
import errno

__all__ = ['find_available_port', 'SocketFactory']

class SocketFactory(object):
	"""
	Object used to create a socket and connect it.

	This is, more or less, a specialized partial() for socket creation.

	Additionally, it provides methods and attributes for abstracting
	exception management on socket operation.
	"""

	timeout_exception = socket.timeout
	fatal_exception = socket.error
	try_again_exception = socket.error

	def timed_out(self, err) -> bool:
		return err.__class__ is self.timeout_exception

	@staticmethod
	def try_again(err, codes = (errno.EAGAIN, errno.EINTR, errno.EWOULDBLOCK, errno.ETIMEDOUT)) -> bool:
		"""
		Does the error indicate that the operation should be tried again?

		More importantly, the connection is *not* dead.
		"""
		errno = getattr(err, 'errno', None)
		if errno is None:
			return False
		return errno in codes

	@classmethod
	def fatal_exception_message(typ, err) -> (str, None):
		"""
		If the exception was fatal to the connection,
		what message should be given to the user?
		"""
		if typ.try_again(err):
			return None
		return getattr(err, 'strerror', '<strerror not present>')

	@property
	def _security_context(self):
		if self._security_context_ii is None:
			from ssl import SSLContext, PROTOCOL_TLS_CLIENT
			ctx = self._security_context_ii = SSLContext(PROTOCOL_TLS_CLIENT)
			ctx.check_hostname = False

			cf = self.socket_secure.get('certfile')
			kf = self.socket_secure.get('keyfile')
			if cf is not None:
				self._security_context_ii.load_cert_chain(cf, keyfile=kf)

			ca = self.socket_secure.get('ca_certs')
			if ca is not None:
				self._security_context_ii.load_verify_locations(ca)

		return self._security_context_ii

	def secure(self, socket: socket.socket):
		"""
		Secure a socket with SSL.
		"""
		return self._security_context.wrap_socket(socket)

	def __call__(self, timeout = None):
		s = socket.socket(*self.socket_create)
		try:
			s.settimeout(float(timeout) if timeout is not None else None)
			s.connect(self.socket_connect)
			s.settimeout(None)
		except Exception:
			s.close()
			raise
		return s

	def __init__(self,
		socket_create,
		socket_connect,
		socket_secure = None,
		socket_security_context = None
	):
		self._security_context_ii = socket_security_context
		self.socket_create = socket_create
		self.socket_connect = socket_connect
		self.socket_secure = socket_secure or {}

	def __str__(self):
		return 'socket' + repr(self.socket_connect)

def find_available_port(
	interface = 'localhost',
	address_family = socket.AF_INET,
):
	"""
	Find an available port on the given interface for the given address family.
	"""

	port = None
	s = socket.socket(address_family, socket.SOCK_STREAM,)
	try:
		s.bind(('localhost', 0))
		port = s.getsockname()[1]
	finally:
		s.close()

	return port
