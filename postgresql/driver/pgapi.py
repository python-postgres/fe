##
# copyright 2008, pg/python project.
# http://python.projects.postgresql.org
##
"""
PG-API module--only requires the connector and connection.
"""

from postgresql.driver.tracenull import \
	PGAPI_Connector as Connector, \
	PGAPI_Connection as Connection

def connect(**kw):
	return Connector(**kw)()
