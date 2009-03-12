##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
r'''
Console Scripts
***************

``pg_python``
=============

The ``pg_python`` command provides a simple way to write Python scripts against a
single target database. It acts like the regular Python console command, but
then takes standard PostgreSQL options as well to specify the client parameters
to make the connection with.

Usage
-----

Usage: pg_python [Connection Options] [script or script designator] ...

Connection Options:
  -d DATABASE, --database=DATABASE
                        database's name
  -h hostname, --host=hostname
                        database server host
  -p PORT, --port=PORT  database server port
  -U USER, --username=USER
                        user name to connect as
  -W, --password        prompt for password
  --unix=FILE_SYSTEM_PATH
                        path to filesystem socket
  --ssl-mode=SSLMODE    SSL rules for connectivity
   disable, allow, require, or prefer. prefer is the default.
  --role=ROLE           run operation as the role
  -s NAME=VALUE, --setting=NAME=VALUE
                        run-time parameters to set upon connecting
  -I IRI, --iri=IRI     complete resource identifier, pq-IRI
  -1, --with-transaction
                        run operation with a transaction block
  --pq-trace=PQ_TRACE   trace PQ protocol transmissions

  -C PYTHON_CONTEXT
                        Python context code to
                        run[file://,module:,<code>(__context__)]
  -m PYTHON_MAIN        Python module to run as script(__main__)
  -c PYTHON_MAIN        Python expression to run(__main__)
  --version             show program's version number and exit
  --help                show this help message and exit


Python Environment
------------------

``pg_python`` creates a Python environment with an already established
connection based on the given arguments. It provides the following additional
builtins:

 ``db``
  The PG-API connection object.

 ``xact``
  ``db.xact``

 ``settings``
  ``db.settings``

 ``prepare``
  ``db.prepare``

 ``proc``
  ``db.proc``


Interactive Console Backslash Commands
--------------------------------------

Inspired by ``psql``::

	>>> \?
	Backslash Commands:

	  \?      Show this help message.
	  \E      Edit a file or a temporary script.
	  \e      Edit and Execute the file directly in the context.
	  \i      Execute a Python script within the interpreter's context.
	  \set    Configure environment variables. \set without arguments to show all
	  \x      Execute the Python command within this process.
'''

__docformat__ = 'reStructuredText'
if __name__ == '__main__':
	import sys
	if (sys.argv + [None])[1] == 'dump':
		sys.stdout.write(__doc__)
	else:
		try:
			help(__package__ + '.bin')
		except NameError:
			help(__name__)
