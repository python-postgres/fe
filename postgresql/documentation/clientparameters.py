##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Client Parameters
*****************

Connection creation interfaces in `postgresql.driver` are purposefully simple.
All parameters are keywords, and are taken literally. libpq-based drivers
tend differ as they inherit default client parameters from the environment.
Doing this by default is undesirable as it can cause trivial failures due to
unexpected parameter inheritance. However, using these parameters from the
environment and other sources are simply expected in *some* cases--
`postgresql.open`, `postgresql.bin.pg_python`, and other high-level utilities.
The `postgresql.clientparameters` module provides a means to collect them into
one dictionary-object for subsequent application to a connection creation
interface.

`postgresql.clientparameters` is primarily useful to script authors that want to
provide an interface consistent with PostgreSQL commands like ``psql``.

Collecting Parameters
=====================

The primary entry points in `postgresql.clientparameters` are
`postgresql.clientparameters.standard` and
`postgresql.clientparameters.resolve_password`.

``standard()`` provides a single function for collecting parameters all the
standard sources, and even prompting for a password when needed and ``stdin``
is a TTY.

 Build a client parameter dictionary from the environment and parsed command
 line options.

  ``co``
   Options parsed by `postgresql.clientparameters.StandardParser` or
   `postgresql.clientparameters.DefaultParser` instances.
  ``no_defaults``
   Don't include defaults like ``pgpassfile`` and ``user``. Defaults to `False`.
  ``environ``
   Environment variables to extract client parameter variables from.
   Defaults to `os.environ` and expects a `collections.Mapping` interface.
  ``environ_prefix``
   Environment variable prefix to use. Defaults to "PG". This allows the
   collection of non-standard environment variables whose keys are partially
   consistent with the standard variants. e.g. "PG_SRC_USER", "PG_SRC_HOST",
   etc.
  ``default_pg_sysconfdir``
   The location of the pg_service.conf file. The ``PGSYSCONFDIR`` environment
   variable will override this.
  ``pg_service_file``
   Explicit location of the service file. This will override the "sysconfdir"
   based path.
  ``prompt_title``
   Descriptive title to use if a password prompt is needed. `None` to disable
	password resolution--disables pgpassfile lookups.
  ``parameters``
   Base client parameters to use. These are set after the defaults are
   collected.
   (The defaults that can be disabled by ``no_defaults``).

 `postgresql.clientparameters.resolve_password`
 
 Resolve the password for the given client parameters dictionary returned by
 ``standard``. By default, this function need not be used as ``standard`` will
 resolve the password by default. However, password resolution 
 can be turned off by passing ``prompt_title`` keyword argument as `None`.
 ``resolve_password`` will use the configured ``pgpassfile`` keyword.

  ``parameters``
   First positional argument. Normalized client parameters dictionary to update
	in-place with the resolved password. If the 'prompt_password' key is in
	``parameters``, it will prompt regardless(normally comes from ``-W``).
  ``getpass``
   Function to call to prompt for the password. Defaults to `getpass.getpass`.
  ``prompt_title``
   Additional title to use if a prompt is requested. This can also be specified
	in the ``parameters`` as the ``prompt_title`` key.

Example usage:

	>>> import postgresql.clientparameters as pg_param
	>>> p = pg_param.DefaultParser()
	>>> co, ca = p.parse_args(...)
	>>> cp = pg_param.standard(co = co)
	>>> print(cp)

The `postgresql.clientparameters` module is executable, so you can see the
results of the above snippet by::

	$ python -m postgresql.clientparameters -h localhost -U a_db_user -ssearch_path=public
	{'host': 'localhost',
	 'password': None,
	 'port': 5432,
	 'settings': {'search_path': 'public'},
	 'user': 'a_db_user'}

Environment Variables
=====================

The following is a list of environment variables that will be collected by the
`postgresql.clientparameter.standard` function using the "PG" ``environ_prefix``:

 ===================== ======================================
 Environment Variable  Keyword
 ===================== ======================================
 ``PGUSER``            ``'user'``
 ``PGDATABASE``        ``'database'``
 ``PGHOST``            ``'host'``
 ``PGPORT``            ``'port'``
 ``PGPASSWORD``        ``'password'``
 ``PGSSLMODE``         ``'sslmode'``
 ``PGSSLKEY``          ``'sslkey'``
 ``PGCONNECT_TIMEOUT`` ``'connect_timeout'``
 ``PGREALM``           ``'kerberos4_realm'``
 ``PGKRBSRVNAME``      ``'kerberos5_service'``
 ``PGROLE``            ``'role'``
 ``PGPASSFILE``        ``'pgpassfile'``
 ``PGTZ``              ``'settings' = {'timezone': }``
 ``PGDATESTYLE``       ``'settings' = {'datestyle': }``
 ``PGCLIENTENCODING``  ``'settings' = {'client_encoding': }``
 ``PGGEQO``            ``'settings' = {'geqo': }``
 ===================== ======================================

The "PG" prefix is adjustable using the ``environ_prefix`` keyword.
This is useful in cases where multiple connections are being established by a
single script.
"""

__docformat__ = 'reStructuredText'
if __name__ == '__main__':
	import sys
	if (sys.argv + [None])[1] == 'dump':
		sys.stdout.write(__doc__)
	else:
		try:
			help(__package__ + '.clientparameters')
		except NameError:
			help(__name__)
