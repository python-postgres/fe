##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Collect client connection parameters from various sources.

This module provides functions for collecting client parameters from various
sources:

	- environment variables
	- pg_service.conf
	- .pgpass
"""
import sys
import os

from . import iri as pg_iri
from . import pgpassfile as pgpass
from . import pgservicefile as pgservice

try:
	from getpass import getuser, getpass
except ImportError:
	getpass = raw_input
	def getuser():
		return 'postgres'

default_host = 'localhost'
default_port = 5432

# posix
pg_home_passfile = '.pgpass'
pg_home_directory = '.postgresql'
# win32
pg_appdata_directory = 'postgresql'
pg_appdata_passfile = 'pgpass.conf'


# environment variables that will be in the parameters' "settings" dictionary.
envvar_settings_map = {
	'PGTZ' : 'timezone',
	'PGDATESTYLE' : 'datestyle',
	'PGCLIENTENCODING' : 'client_encoding',
	'PGGEQO' : 'geqo',
	'PGOPTIONS' : 'options',
}

# Environment variables that require no transformation.
envvar_map = {
	'PGUSER' : 'user',
	'PGDATABASE' : 'database',
	'PGHOST' : 'host',
	'PGPORT' : 'port',
	'PGPASSWORD' : 'password',
	'PGSSLMODE' : 'sslmode',
	'PGSSLKEY' : 'sslkey',
	'PGCONNECT_TIMEOUT' : 'connect_timeout',

	'PGREALM' : 'kerberos4_realm',
	'PGKRBSRVNAME' : 'kerberos5_service',

	# Extensions
	'PGROLE' : 'role', # SET ROLE $PGROLE

	# This keyword will never make it to a connect() function
	# as `resolve_password` should be called to fill in the
	# parameter accordingly.
	'PGPASSFILE' : 'pgpassfile',
}

#'PGGSSLIB' : 'gsslib',
#'PGLOCALEDIR' : 'localedir',

def convert_envvars(envvars):
	"""
	Create a clientparams dictionary from the given environment variables.

		PGUSER -> user
		PGDATABASE -> database
		PGHOST -> host
		PGHOSTADDR -> host (overrides PGHOST)
		PGPORT -> port

		PGPASSWORD -> password
		PGPASSFILE -> pgpassfile

		PGSSLMODE -> sslmode
		PGREQUIRESSL gets rewritten into "sslmode = 'require'".

		PGREALM -> kerberos4_realm
		PGKRBSVRNAME -> kerberos5_service
		PGSSLKEY -> sslkey

		PGROLE -> role (role to set when connected)

		PGTZ -> settings['timezone']
		PGDATESTYLE -> settings['datestyle']
		PGCLIENTENCODING -> settings['client_encoding']
		PGGEQO -> settings['geqo']

	PGSERVICE is *not* processed by this function.
	"""
	d = dict(((v, envvars[k]) for k, v in envvar_map.items() if k in envvars))
	if 'PGHOSTADDR' in envvars:
		d['host'] = envvars['PGHOSTADDR']

	settings = dict(((v, envvars[k]) for k, v in envvar_settings_map.items() if k in envvars))
	if settings:
		d['settings'] = settings

	if 'PGREQUIRESSL' in envvars and 'sslmode' not in d:
		rssl = envvars['PGREQUIRESSL']
		if rssl == '1':
			d['sslmode'] = 'require'
	return d

def defaults(getuser = getuser, environ = os.environ):
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
		pgdata = os.path.join(userdir, pg_home_directory)

	# if they exist, they appear in the returned parameters
	params = {
		'user' : user,
		'host' : default_host,
		'port' : default_port,
	}

	# only include in the params if the file exists.
	params.update((
		(k,v) for k,v in (
			('sslcrtfile', os.path.join(pgdata, 'postgresql.crt')),
			('sslkeyfile', os.path.join(pgdata, 'postgresql.key')),
			('sslrootcrtfile', os.path.join(pgdata, 'root.crt')),
			('sslrootcrlfile', os.path.join(pgdata, 'root.crl')),
			('pgpassfile', passfile),
		) if os.path.exists(v)
	))

	return params

def merge(base_dict, applied_dict):
	"""
	Create a new dictionary composed of `base_dict` and `applied_dict`.

	This takes special note of parameters like `settings` and `path`.
	"""
	rd = dict(base_dict)
	applied_dict = dict(applied_dict)
	rd.update(applied_dict)

	settings = dict()
	settings.update(base_dict.get('settings', ()))
	settings.update(applied_dict.get('settings', ()))
	rd['settings'] = settings
	if not rd['settings']:
		del rd['settings']

	# prefix the base path with the applied path.
	rd['path'] = list(applied_dict.get('path', ()))
	rd['path'].extend(base_dict.get('path', ()))
	if not rd['path']:
		del rd['path']

	# normalize 'dbname'
	dbname = rd.pop('dbname', None)
	if dbname is not None:
		rd.setdefault('database', dbname)

	return rd

def resolve_password(d):
	"""
	Given a parameters dictionary, resolve the 'password' key.

	If `prompt_password` is `True`.
	 If sys.stdin is a TTY, use `getpass` to prompt the user.
	 Otherwise, read a single line from sys.stdin.
	 delete 'prompt_password' from the dictionary.

	Otherwise.
	 If the 'password' key is `None`, attempt to resolve the password using the
	 'pgpassfile' key.

	Finally, remove the pgpassfile key as the password has been resolved for the
	given parameters.
	"""
	if 'prompt_password' in d:
		if d['prompt_password'] is True:
			if sys.stdin.isatty():
				prompt = d.pop('title', '')
				prompt += '(' + pg_iri.serialize(d) + ')'
				d['password'] = getpass("Password for " + prompt +": ")
			else:
				# getpass will throw an exception if it's not a tty,
				# so just take the next line.
				pw = sys.stdin.readline()
				# try to clean it up..
				if pw.endswith(os.linesep):
					pw = pw[:len(pw)-len(os.linesep)]
				d['password'] = pw
		del d['prompt_password']
	else:
		if d.get('password') is None:
			# No password? Look in the pgpassfile.
			passfile = d.get('pgpassfile')
			if passfile is not None:
				d['password'] = pgpass.lookup_pgpass(d, passfile)
	# Don't need the pgpassfile parameter anymore as the password
	# has been resolved.
	d.pop('pgpassfile', None)

if __name__ == '__main__':
	import pprint
	from . import clientoptparse as cop
	p = cop.DefaultParser(
		description = "print the clientparams dictionary for the environment"
	)
	(co, ca) = p.parse_args()
	dcp = defaults()
	dcp = merge(dcp, convert_envvars(os.environ))
	dcp = merge(dcp, cop.convert(co))
	resolve_password(dcp)
	pprint.pprint(dcp)
