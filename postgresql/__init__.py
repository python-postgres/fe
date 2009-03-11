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
	'__pkg_documentation__',
	'__docformat__',
	'__version__',
	'version_info',
	'open',
]

__author__ = "James William Pye <x@jwp.name>"
__date__ = "2009-01-19 21:15:00-07"

__project__ = 'py-postgresql'
__project_id__ = 'http://python.projects.postgresql.org'

__version__ = "0.8dev"
version_info = (0, 8, 0, 'dev', 0)

##
# IRI to base reference documentation.
__pkg_documentation__ = __project_id__ + '/v' + '.'.join([
	str(x) for x in version_info[:2]
]) + '/'

pg_iri = pg_driver = pg_cp = None
def open(
	iri : "The URL to connect to: pq://user:pass@host/dbname" = None,
):
	"""
	Create a `postgresql.api.Connection` to the server referenced by the given
	`iri` keyword.

	If the URL starts with an '&', return the connector.
	"""
	global pg_iri, pg_driver, pg_cp
	if None in (pg_iri, pg_driver, pg_cp):
		import postgresql.iri as pg_iri
		import postgresql.driver as pg_driver
		import postgresql.clientparameters as pg_cp

	return_connector = False
	if iri is not None:
		iri_params = pg_iri.parse(iri)
		iri_params.pop('path', None)
		s = iri_params.pop('scheme', None) or 'pq'
		if s.startswith('&'):
			return_connector = True
	else:
		iri_params = {}

	std_params = pg_cp.standard(prompt_title = None)
	params = pg_cp.normalize(
		list(pg_cp.denormalize_parameters(std_params)) + \
		list(pg_cp.denormalize_parameters(iri_params))
	)
	pg_cp.resolve_password(params)

	if return_connector is True:
		Ctype = pg_driver.default.select(
			host = params.get('host'),
			port = params.get('port')
		)
		return Ctype(**params)
	else:
		return pg_driver.default.connect(**params)

__docformat__ = 'reStructuredText'
