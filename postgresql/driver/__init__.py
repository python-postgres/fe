##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Driver package for connecting to PostgreSQL via a data stream(sockets).
"""
__all__ = ['IP4', 'IP6', 'Host', 'Unix', 'connect', 'default']

from .pq3 import default

IP4 = default.IP4
IP6 = default.IP6
Host = default.Host
Unix = default.Unix

connect = default.connect
