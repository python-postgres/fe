##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
py-postgresql is a Python package for using PostgreSQL. This includes low-level
protocol tools, a driver(PG-API and DB-API), and cluster management tools.

If it's not documented in the narratives, `postgresql.documentation.index`, then
the stability of the APIs should *not* be trusted.
"""
__all__ = [
	'__author__',
	'__date__',
	'__project__',
	'__project_id__',
	'__docformat__',
	'__version__',
	'version_info',
	'open',
]

__author__ = "James William Pye <x@jwp.name>"
__date__ = "2009-06-13 15:56:00-07"

__project__ = 'py-postgresql'
__project_id__ = 'http://python.projects.postgresql.org'

version_info = (0, 9, 0, 'final', 0)
__version__ = '.'.join(map(str, version_info[:3])) + (
	version_info[3] if version_info[3] != 'final' else ''
)

pg_iri = pg_driver = pg_param = None
def open(iri = None, prompt_title = None, **kw):
	"""
	Create a `postgresql.api.Connection` to the server referenced by the given
	`iri`::

		>>> import postgresql
		# General Format:
		>>> db = postgresql.open('pq://user:password@host:port/database')

		# Connect to 'postgres' at localhost.
		>>> db = postgresql.open('localhost/postgres')

	(Note: "pq" is the name of the protocol used to communicate with PostgreSQL)
	"""
	global pg_iri, pg_driver, pg_param
	if pg_iri is None:
		from . import iri as pg_iri
		from . import driver as pg_driver
		from . import clientparameters as pg_param

	return_connector = False
	if iri is not None:
		if iri.startswith('&'):
			return_connector = True
			iri = iri[1:]
		iri_params = pg_iri.parse(iri)
		iri_params.pop('path', None)
	else:
		iri_params = {}

	std_params = pg_param.collect(prompt_title = None)
	params = pg_param.normalize(
		list(pg_param.denormalize_parameters(std_params)) + \
		list(pg_param.denormalize_parameters(iri_params)) + \
		list(pg_param.denormalize_parameters(kw))
	)
	pg_param.resolve_password(params)

	C = pg_driver.default.fit(**params)
	if return_connector is True:
		return C
	else:
		c = C()
		c.connect()
		return c

__docformat__ = 'reStructuredText'
