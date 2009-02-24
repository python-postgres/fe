##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
py-postgresql is a Python package for using PostgreSQL. This includes low-level
protocol tools, a driver(PG-API and DB-API), and cluster management tools.
"""
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

#'PGGSSLIB' : 'gsslib',
#'PGLOCALEDIR' : 'localedir',

__docformat__ = 'reStructuredText'
