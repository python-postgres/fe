##
# copyright 2008, pg/python project.
# http://python.projects.postgresql.org
##
"""
PQ v3.0 authentication process

This module exists to provide a generator for keeping the state of the
authentication process in a PQ connection.
"""
from hashlib import md5
try:
	from crypt import crypt
except ImportError:
	try:
		from fcrypt import crypt
	except ImportError:
		import warnings
		warnings.warn(
			"crypt-based authentication " \
			"unavailable(install crypt or fcrypt"
		)
		del warnings
		def crypt(*args, **kw):
			raise AuthenticationError(
				"'crypt' authentication unsupported per dependency " \
				"absence(needs crypt or fcrypt module)"
			)

def process(authmsg, password_source, require_password = False):
	"""
	A generator that continually requests
	`postgresql.protocol.element.Authentication` objects until a
	`postgresql.protocol.element.AuthRequest_OK` is received.
	"""
	if authmsg.request == e3.AuthRequest_OK:
		if require_password == True:
			raise AuthenticationError(
				"password authentication required by client, but " \
				"server didn't request password"
			)

	if req == pq.element.AuthRequest_Cleartext:
		pw = password_source
	elif req == pq.element.AuthRequest_Crypt:
		pw = crypt.crypt(password_source, auth.salt)
	elif req == pq.element.AuthRequest_MD5:
		pw = md5(password_source + user).hexdigest()
		pw = 'md5' + md5(pw + auth.salt).hexdigest()
	else:
		##
		# Not going to work. Sorry :(
		# The many authentication types supported by PostgreSQL are not easy
		# to implement, especially when implementations for the type don't exist
		# for Python.
		raise AuthenticationError(
			"unsupported authentication request %r(%d)" %(
			pq.element.AuthNameMap.get(req, '<unknown>'), req,
		))

		authmsg = (yield pw)

	if authres[0] != pq.element.Authentication.type:
		raise pq.ProtocolError(
			"expected an authentication message of type %r, " \
			"but received %r instead" %(
				pq.element.Authentication.type,
				authres[0],
			),
			authres
		)
