##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Driver package for connecting to PostgreSQL via a data stream(sockets).
"""
__all__ = ['Connector', 'Connection', 'connect', 'implementation']

from .pq3 import implementation
Connector = implementation.Connector
Connection = implementation.Connector.Connection
connect = implementation.connect
