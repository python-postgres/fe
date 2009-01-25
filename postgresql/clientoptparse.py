##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
'PostgreSQL client optparse options'
from optparse import make_option, OptionParser

# Callback support modules
import os
import configparser

import postgresql.iri as pg_iri
import postgresql.dsn as pg_dsn

from postgresql.pg_config import dictionary as pg_config

datadir = make_option('-D', '--datadir',
	dest = 'datadir',
	help = 'location of the database storage area',
	default = None,
)

database = make_option('-d', '--database',
	dest = 'database',
	help = "database's name",
	default = None,
)
port = make_option('-p', '--port',
	dest = 'port',
	help = 'database server port',
	type = 'int',
	default = None,
)

def set_socket_provider(option, opt_str, value, parser):
	"Set the socket provider along with it's type"
	parser.values.socket_provider = (
		option.socket_provider_type, value
	)
host = make_option('-h', '--host',
	help = 'database server host',
	type = 'str',
	action = 'callback',
	dest = 'socket_provider',
	callback = set_socket_provider
)
host.socket_provider_type = 'host'


def settings_callback(option, opt_str, value, parser):
	'split the string into a (key,value) pair tuple'
	l = parser.values.settings = parser.values.settings or []
	kv = value.split('=', 1)
	if len(kv) != 2:
		raise OptionValueError("invalid setting argument, %r" %(value,))
	l.append(kv)
settings = make_option('-s', '--setting',
	dest = 'settings',
	help = 'run-time parameters to set upon connecting',
	default = (),
	action = 'callback',
	callback = settings_callback,
	type = 'str',
)

user = make_option('-U', '--username',
	dest = 'user',
	help = 'user name to connect as',
	default = None,
)
password = make_option('-W', '--password',
	dest = 'prompt_password',
	help = 'prompt for password',
	action = 'store_true',
	default = False,
)

unix = make_option('--unix',
	help = 'path to filesystem socket',
	type = 'str',
	action = 'callback',
	dest = 'socket_provider',
	callback = set_socket_provider
)
unix.socket_provider_type = 'unix'

require_ssl = make_option('--require-ssl',
	dest = 'sslmode',
	help = 'require an SSL connection',
	action = 'store_const',
	const = 'require'
)
sslmode = make_option('--ssl-mode',
	dest = 'sslmode',
	help = 'SSL rules for connectivity',
	choices = ('require','prefer','allow','disable'),
	default = None,
)

in_xact = make_option('-1', '--with-transaction',
	dest = 'in_xact',
	help = 'run operation with a transaction block',
	action = 'store_true',
	default = False
)

role = make_option('--role',
	dest = 'role',
	help = 'run operation as the role',
	default = None
)

def prepend_path(option, opt_str, value, parser):
	"Set the socket provider along with it's type"
	parser.values.path = getattr(parser.values, 'path') or []
	l = value.split(':')
	l.extend(parser.values.path)
	parser.values.path = l

path = make_option('--path',
	dest = 'path',
	help = 'Path to prefix search_path with',
	default = (), # Callback will make it a list when needed.
	type = 'str',
	action = 'callback',
	callback = prepend_path
)

service_file = make_option('--pg-service-file',
	dest = 'pg_service_file',
	help = 'PostgreSQL service file use to for lookups',
	default = None,
)

# exact copies, host is treated specially for socket_provider.
service_likewise = [
	'user', 'port', 'database', 'options',
]

service_rewrite = {
	'dbname' : 'database',
}

def pg_service_callback(option, opt_str, value, parser):
	"Apply the service to the parser's values"
	if parser.values.pg_service_file is not None:
		f = parser.values.pg_service_file
	else:
		# Allow the user specify the environment dictionary within
		# the parser by setting the 'environ' attribute on the object
		env = getattr(parser, 'environ', getattr(os, 'environ', None))
		d = env is not None and env.get('PGSYSCONFDIR') or None

		if d is None:
			try:
				d = pg_config(env.get('PGCONFIG', 'pg_config'))['sysconfdir']
			except OSError:
				raise OptionValueError(
					"failed to extract service file location from pg_config"
				)
		f = os.path.join(d, 'pg_service.conf')
	cp = configparser.ConfigParser()
	fp = open(f)
	try:
		cp.readfp(fp, filename = f)
	finally:
		fp.close()

	try:
		# load the section(service)
		items = cp.items(value)
	except configparser.NoSectionError:
		# nothing to do; throw warning? libpq doesn't. - jwp 2008
		return

	parser.values.settings = parser.values.settings or []
	for k, v in items:
		if k == 'host':
			parser.values.socket_provider = ('host', v)
		elif k in service_rewrite:
			setattr(parser.values, dest_rewrite.get(k, k), v)
		elif k in service_likewise:
			setattr(parser.values, k, v)
		else:
			parser.values.settings.append((k, v))

service = make_option('--pg-service',
	help = 'Postgres service name to connect to',
	action = 'callback',
	callback = pg_service_callback,
	type = 'str'
)

def iri_callback(option, opt_str, value, parser):
	parser.values.iri = value
	d = pg_iri.parse(value)
	parser.values.settings = parser.values.settings or []
	parser.values.settings.extend(list(
		d.pop('settings', {}).items()
	))
	if 'unix' in d:
		parser.values.socket_provider = ('unix', d.pop('unix'))
	elif 'host' in d:
		parser.values.socket_provider = ('host', d.pop('host'))

	for k, v in d.items():
		if v is not None:
			setattr(parser.values, k, v)

iri = make_option('-I', '--iri',
	help = 'complete resource identifier, pq-IRI',
	action = 'callback',
	callback = iri_callback,
	type = 'str'
)

def dsn_callback(option, opt_str, value, parser):
	parser.values.dsn = value
	d = pg_dsn.parse(value)
	parser.values.settings = parser.values.settings or []
	parser.values.settings.extend(list(
		d.pop('settings', {}).items()
	))
	if 'unix' in d:
		parser.values.socket_provider = ('unix', d.pop('unix'))
	elif 'host' in d:
		parser.values.socket_provider = ('host', d.pop('host'))

	for k, v in d.items():
		if v is not None:
			setattr(parser.values, k, v)

dsn = make_option('--dsn',
	help = 'DSN for connection',
	action = 'callback',
	callback = dsn_callback,
	type = 'str'
)

# PostgreSQL Standard Options
standard = [database, host, port, user, password]

class StandardParser(OptionParser):
	"""
	Option parser limited to the basic -U, -h, -p, -W, and -D options.
	"""
	standard_option_list = standard
	def _add_help_option(self):
		# Only allow long --help so that it will not conflict with -h(host)
		self.add_option("--help",
			action = "help",
			help = "show this help message and exit",
		)

# Extended Options
default = standard + [
	unix,
	sslmode,
	require_ssl,
	role,
	settings,
	path,
	service_file,
	service,
	iri,
	dsn,
]

class DefaultParser(StandardParser):
	"""
	Parser that includes a variety of connectivity options.
	(IRI, DSN, sslmode, role(set role), settings)
	"""
	standard_option_list = default

optionattr = [
	'user',
	'port',
	'database',
	'settings',
	'sslmode',
	'role',
	'path',
	'prompt_password',
]

def convert(co, attrlist = optionattr):
	"""
	Convert an OptionParser instance into a `postgresql.clientparams` dictionary.
	"""
	for key in attrlist:
		v = getattr(co, key, None)
		if v is not None and v != () and v != [] and v != {}:
			yield (key, v)
	sp = getattr(co, 'socket_provider', None)
	if sp is not None:
		yield (sp[0], sp[1])
		if sp[0] == 'host' and co.port:
			yield ('port', co.port)

def parse_named_args(
	args, parser = DefaultParser,
) -> ('name', {}, ()):
	"""
	Given a sequence of command line arguments, parse the options of the
	"connection name"

	>>> parse_named_client_parameters(['src', '-h', 'localhost', 'dst', '-h', 'remote'])
	('src', {'host' : 'localhost'}, ('dst', '-h', 'remote'))

	This is useful for scripts that deal with multiple databases.
	"""
	args = list(args)
	name = args.pop(0)
	p = parser(description = "client connection parameters for " + repr(name))
	p.disable_interspersed_args()
	co, ca = p.parse_args(args)
	return (name, dict(convert(co)), ca)
