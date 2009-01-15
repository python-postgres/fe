##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
This is an abstraction module that provides the working buffer implementation.
If a C compiler is not available on the system that built the package, the slower
`postgresql.protocol.pbuffer` module can be used in
`postgresql.protocol.cbuffer`'s absence.

This provides a convenient place to import the necessary module without
concerning the local code with the details.
"""
try:
	from postgresql.protocol.cbuffer import *
except ImportError:
	from postgresql.protocol.pbuffer import *
