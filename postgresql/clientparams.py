##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Client connection parameters dictionary constructor.

This module provides a means for tying in client connection parameters into a
single dictionary from multiple sources: optparse, environment variables,
pgpass, pg_service.conf, etc.
"""
import sys
import os
import postgresql.pgpass as pgpass

try:
	from getpass import getuser, getpass
except ImportError:
	getpass = raw_input
	def getuser():
		return 'postgres'

default_host = 'localhost'
default_port = 5432
pg_home_passfile = '.pgpass'
pg_home_directory = '.postgresql'
pg_appdata_directory = 'postgresql'
pg_appdata_passfile = 'pgpass.conf'

def defaults(getuser = getuser, environ = {}, params = None):
	"""
	Create a client parameters dictionary with all the default values.
	"""
	user = getuser()
	if sys.platform == 'win32':
		appdata = environ.get('APPDATA')
		if appdata:
			pgdata = os.path.join(appdata, pg_appdata_directory)
			passfile = os.path.join(pgdata, pg_appdata_passfile)
	else:
		userdir = os.path.expanduser('~' + user)
		passfile = os.path.join(userdir, pg_home_passfile)
		pgdata = os.path.join(pg_home_directory)

	# if they exist, they appear in the returned parameters
	files = [
		('sslcrtfile', os.path.join(pgdata, 'postgresql.crt')),
		('sslkeyfile', os.path.join(pgdata, 'postgresql.key')),
		('sslrootcrtfile', os.path.join(pgdata, 'root.crt')),
		('sslrootcrlfile', os.path.join(pgdata, 'root.crl')),
		('pgpassfile', passfile),
	]

	params = params or {}
	params.update({
		'user' : user,
		'host' : default_host,
		'port' : default_port,
	})
	params.update([
		x for x in files if os.path.exists(x[1])
	])
	return params

socket_providers = ('pipe', 'socket', 'unix', 'host', 'process',)
def merge_params(d, od):
	"""
	merge the second mapping onto the first using special rules
	to keep the integrity of the parameters dictionary.
	(split path, remove socket_providers when one is being merged)
	"""
	# Remove existing socket provider as it has been superceded.
	nd = dict(od)
	for k in nd:
		if k in socket_providers:
			for x in socket_providers:
				d.pop(x, None)
			break
	# However, let port persist in case host is specified in another merge.

	# Remove conflicts based on the positional priority(first one wins)
	win = None
	for x in socket_providers:
		if x in nd:
			if win is None:
				win = x
			else:
				del nd[x]

	# filter out the non-standard merges
	d.setdefault('settings', {}).update(dict(nd.pop('settings', {})))
	d['path'] = list(nd.pop('path', ())) + list(d.get('path', ()))
	dbname = nd.pop('dbname', None)
	if dbname is not None:
		d.setdefault('database', dbname)

	d.update([
		kv for kv in nd.items() if kv[1] is not None
	])

def resolve_password(d):
	"""
	Given a dictionary containing 'password' and 'pgpassfile' keys
	"""
	if d.get('password') is None:
		# No password? Look in the pgpassfile.
		passfile = d.get('pgpassfile')
		if passfile is not None:
			d['password'] = pgpass.lookup_pgpass(d, passfile)
	# Don't need the pgpassfile parameter anymore as the password
	# exists, or has been resolved.
	d.pop('pgpassfile', None)

def create(
	param_layers : "a sequence of mappings to apply to `params`",
	environ : "environment variables to apply" = os.environ,
	params : "The target mapping to fill the merged parameters into" = None,
	prompt_password : "whether to issue a password prompt" = False
):
	"""
	Create the normal parameter configuration.

	If `params` is `None`, a new dictionary will be returned. Otherwise,
	`params` is modified and returned.
	"""
	d = defaults(environ = environ, params = params)
	for x in param_layers:
		merge_params(d, x)

	if prompt_password is True:
		if sys.stdin.isatty():
			d['password'] = getpass("Password for user %s: " %(
				d['user'],
			))
		else:
			# getpass will throw an exception if it's not a tty,
			# so just take the next line; could be another command.
			pw = sys.stdin.readline()
			# try to clean it up..
			if pw.endswith(os.linesep):
				pw = pw[:len(pw)-len(os.linesep)]
			d['password'] = pw
	resolve_password(d)
	return d

if __name__ == '__main__':
	import pprint
	pprint.pprint(defaults(environ = os.environ))
