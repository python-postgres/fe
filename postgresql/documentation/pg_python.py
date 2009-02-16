##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
``pg_python``
=============

The ``pg_python`` command provides a simple way to write Python scripts against a
single target database. It acts like the regular Python console command, but
then takes standard PostgreSQL options as well to specify the client parameters
to make the connection with.

Usage
-----

Usage: pg_python [connection options] [script] [-- script options] [args]

Options:
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
  --require-ssl         require an SSL connection (equivalent to --ssl-mode=require)
  --role=ROLE           run operation as the role
  -s NAME=VALUE, --setting=NAME=VALUE
                        run-time parameters to set upon connecting
  -I IRI, --iri=IRI     complete resource identifier, pq-IRI
  -1, --with-transaction
                        run operation with a transaction block
  -C PYTHON_CONTEXT
                        Python context code to
                        run[file://,module:,<code>(__context__)]
  -m PYTHON_MAIN        Python module to run as script(__main__)
  -c PYTHON_MAIN        Python expression to run(__main__)
  --pq-trace=PQ_TRACE   trace PQ protocol transmissions
  --version             show program's version number and exit
  --help                show this help message and exit

Python Environment
------------------

``pg_python`` creates a Python environment with an already established
connection based on the given arguments. It uses the `pkg:jwp_python_command`
package to aid in harnessing the basic Python command features and then
`pkg:pg_foundation` to fill in the connectivity options. In order to provide
global access to these additional object, it assigns them in ``__builtins__`` to
the following names:

 - ``db`` (the connection object)
 - ``xact`` (db.xact)
 - ``settings`` (db.settings)
 - ``query`` (db.query)
 - ``cquery`` (db.cquery)
 - ``proc`` (db.proc)
 - ``cursor`` (db.cursor)
 - ``statement`` (db.statement)

All of these are provided for convenience. With a single target being the
primary use-case, ambiguity is not an issue. Surely, saving four characters for
accessing each of these is not substantial, but it helps keep code concise and
tends to be very useful when using ``pg_python`` interactively.

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
"""
__docformat__ = 'reStructured Text'

if __name__ == '__main__':
	import sys
	if (sys.argv + [None])[1] == 'dump':
		sys.stdout.write(__doc__)
	else:
		try:
			help(__package__ + '.pg_python')
		except NameError:
			help(__name__)
