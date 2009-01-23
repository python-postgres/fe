##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Driver package for connecting to PostgreSQL via a data stream(sockets).
"""
__all__ = ['IP4', 'IP6', 'Host', 'Unix', 'connect', 'implementation']

from .pq3 import implementation

IP4 = implementation.IP4
IP6 = implementation.IP6
Host = implementation.Host
Unix = implementation.Unix

connect = implementation.connect
