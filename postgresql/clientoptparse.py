##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
PostgreSQL command-line client optparse options.

This module is intended to be used in conjunction with
`postgresql.clientparameters`. The result of a parser should be applied to a
ClientParameter instance in order to simply comprehension of the output.

Usage of optparse in this module makes extensive use of the callback feature to
guarentee the order of the parameters. This is of notably importance in situations
where --unix is used with --host.
"""
import optparse
from functools import partial

datadir = optparse.make_option('-D', '--datadir',
	help = 'location of the database storage area',
	default = None,
	dest = 'datadir',
)

def append_db_client_parameters(option, opt_str, value, parser):
	# for options without arguments, None is passed in.
	value = True if value is None else value
	parser.values.db_client_parameters.append(
		((option.dest,), value)
	)

make_option = partial(optparse.make_option,
	action = 'callback',
	callback = append_db_client_parameters
)

user = make_option('-U', '--username',
	dest = 'user',
	type = 'str',
	help = 'user name to connect as',
)
database = make_option('-d', '--database',
	type = 'str',
	help = "database's name",
	dest = 'database',
)
password = make_option('-W', '--password',
	dest = 'prompt_password',
	help = 'prompt for password',
)
host = make_option('-h', '--host',
	help = 'database server host',
	type = 'str',
	dest = 'host',
)
port = make_option('-p', '--port',
	help = 'database server port',
	type = 'str',
	dest = 'port',
)

unix = make_option('--unix',
	help = 'path to filesystem socket',
	type = 'str',
	dest = 'unix',
)

def append_settings(option, opt_str, value, parser):
	'split the string into a (key,value) pair tuple'
	kv = value.split('=', 1)
	if len(kv) != 2:
		raise OptionValueError("invalid setting argument, %r" %(value,))
	parser.values.db_client_parameters.append(
		((option.dest, kv[0]), kv[1])
	)

settings = make_option('-s', '--setting',
	dest = 'settings',
	help = 'run-time parameters to set upon connecting',
	callback = append_settings,
	type = 'str',
)

sslmode = make_option('--ssl-mode',
	dest = 'sslmode',
	help = 'SSL rules for connectivity',
	choices = ('require','prefer','allow','disable'),
	type = 'choice',
)

in_xact = make_option('-1', '--with-transaction',
	dest = 'in_xact',
	help = 'run operation with a transaction block',
)

role = make_option('--role',
	dest = 'role',
	help = 'run operation as the role',
	type = 'str',
)

def append_db_client_x_parameters(option, opt_str, value, parser):
	# for options without arguments, None is passed in.
	parser.values.db_client_parameters.append((option.dest, value))
make_x_option = partial(make_option, callback = append_db_client_x_parameters)

iri = make_x_option('-I', '--iri',
	help = 'complete resource identifier, pq-IRI',
	type = 'str',
	dest = 'pq_iri',
)
dsn = make_x_option('--dsn',
	help = 'DSN for connection',
	type = 'str',
	dest = 'pq_dsn',
)

# PostgreSQL Standard Options
standard = [database, host, port, user, password]

class StandardParser(optparse.OptionParser):
	"""
	Option parser limited to the basic -U, -h, -p, -W, and -D options.
	This parser subclass is necessary for two reasons:

	 1. _add_help_option override to not conflict with -h
	 2. Initialize the db_client_parameters on the parser's values.

	See the DefaultParser for more fun.
	"""
	standard_option_list = standard
	def __init__(self, *args, **kw):
		super().__init__(*args, **kw)

	def get_default_values(self, *args, **kw):
		v = super().get_default_values(*args, **kw)
		v.db_client_parameters = []
		return v

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
	role,
	settings,
# Complex Options
	iri,
	dsn,
]

class DefaultParser(StandardParser):
	"""
	Parser that includes a variety of connectivity options.
	(IRI, DSN, sslmode, role(set role), settings)
	"""
	standard_option_list = default

if __name__ == '__main__':
	import pprint
	p = DefaultParser()
	p.add_option(in_xact)
	(co, ca) = p.parse_args()
	print("Parameters(co.db_client_parameters):")
	pprint.pprint(co.db_client_parameters)
	print("Remainder(ca):")
	pprint.pprint(ca)
