##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
r"""
===================
`postgresql.driver`
===================

The `postgresql.driver` implements PG-API, `postgresql.api`, using PQ version
3.0 to communicate with PostgreSQL servers. It makes use of the protocol's
extended features to provide binary datatype transmission and protocol level
prepared statements.

-------------
Compatibility
-------------

`postgresql.driver` currently supports PostgreSQL servers as far back as 8.0.
Prior versions are not tested. While any version of PostgreSQL supporting
version 3.0 of the PQ protocol *should* work, some of the higher level features
will not work due to the lack of supporting stored procedures.

-----------
Conventions
-----------

The following identifiers are regularly used as shorthands for instances of
significant interface elements:

 ``db``
  `postgresql.api.Connection`, a database connection.

 ``ps``
  `postgresql.api.PreparedStatement`, a prepared statement.

 ``c``
  `postgresql.api.Cursor``, a cursor; the results of a prepared statement.

------
Design
------

`postresql.driver` implements a prepared statement driven interface.
This means that most interactions that occur, start with a statement being
defined and subsequently used. This design provides a familiar means for
managing the set of requests used to extract information from the resource.
Generally, prepared statements are treated like functions.

-----------
Connections
-----------

The simple connection creation interface is `postgresql.driver.connect`::

	>>> import postgresql.driver as pg_driver
	>>> db = pg_driver.connect(user = 'usename', password = 'secret', host = 'localhost', port = 5432)

This will create a connection to the server listening on port ``5432``
on the host ``localhost`` as the user ``usename`` with the password ``secret``.

.. note::
 `connect` will *not* inherit parameters from the environment as libpq-based drivers do.

Connection Keywords
===================

The following is a list of keywords accepted by connection creation
interfaces:

 ``user``
  The user to connect as.
 ``password``
  The user's password.
 ``database``
  The name of the database to connect to. (PostgreSQL defaults it to `user`)
 ``host``
  The hostname or IP address to connect to.
 ``port``
  The port on the host to connect to.
 ``settings``
  A dictionary or key-value pair sequence stating the parameters to give to the
  database. These settings are included in the startup packet, and should be
  used carefully as when an invalid setting is given, it will cause the
  connection to fail.

 ``connect_timeout``
  Amount of time to wait for a connection to be made. (in seconds)
 ``server_encoding``
  Hint given to the driver to properly encode password data and some information
  in the startup packet.
  This should only be used in cases where connections cannot be made due to
  authentication failures that occur while using known-correct credentials.

 ``sslmode``
  ``'disable'``
   Don't allow SSL connections.
  ``'allow'``
   Try without SSL, but if that doesn't work, try with.
  ``'prefer'``
   Try SSL first, then without.
  ``'require'``
   Require an SSL connection.

 ``sslcrtfile``
  Certificate file path given to `ssl.wrap_socket`.
 ``sslkeyfile``
  Key file path given to `ssl.wrap_socket`.
 ``sslrootcrtfile``
  Root certificate file path given to `ssl.wrap_socket`
 ``sslrootcrlfile``
  Revocation list file path. [Currently not checked.]

Connection Metadata
===================

When a connection is established, certain pieces of metadata are collected from
the backend. The following are the attributes set on the connection object after
the connection is made:

 ``version``
  The results of ``SELECT version()``
 ``version_info``
  A ``sys.version_info`` form of the ``server_version`` setting. eg. ``(8, 1, 2,
  'final', 0)``.
 ``security``
  `None` if no security. ``'ssl'`` if SSL is enabled.
 ``backend_id``
  The process-id of the backend process.
 ``backend_start``
  When backend was started. ``datetime.datetime`` instance.
 ``client_address``
  The client address that the backend is communicating with.
 ``client_port``
  The port of the client that the backend is communicating with.

The latter three are collected from pg_stat_activity. If this information is
unavailable, the attributes will be `None`.

Database Interface Entry Points
===============================

After a connection is established, the interface entry points are ready for use.
These entry points are the primary interfaces used to create prepared statements
or stored procedure references. These entry points exist as attributes on the
connection object:

 ``prepare(sql_statement_string)``
  Create a `postgresql.api.PreparedStatement` object for querying the database.
  See `Prepared Statement` for more information.

 ``proc(procedure_id)``
  Create a `postgresql.api.StoredProcedure` object referring to a stored
  procedure on the database. The returned object will provide a
  `collections.Callable` interface to the stored procedure on the server.

 ``statement_from_id(statement_id)``
  Create a `postgresql.api.PreparedStatement` object from an existing statement
  identifier. This is used in cases where the statement was prepared on the
  server.

 ``cursor_from_id(cursor_id)``
  Create a `postgresql.api.Cursor` object from an existing cursor identifier.
  This is used in cases where the cursor was declared on the server.

 ``execute(sql_statements_string)``
  Run a block of SQL on the server. This method returns `None` unless an error
  occurs. If errors occur, the processing of the statements will stop and the
  the error will be raised.

 ``xact``
  The `postgresql.api.TransactionManager` instance for managing the connection's
  transactions. See `Transaction Management` for more information.

 ``settings``
  A `collections.MutableMapping` interface to the database's SQL settings. See
  `Settings Management` for more information.

Client Parameters
=================

Connection creation interfaces in `postgresql.driver` are purposefully simple.
All parameters are keywords, and are taken literally. libpq-based drivers
tend differ as they inherit default client parameters from the environment.
Doing this by default is undesirable as it can cause trivial failures due to
unexpected parameter inheritance. However, using these parameters from the
environment and other sources are simply expected in *some* cases. The
`postgresql.clientparameters` module provides a means to collect them into one
dictionary-object for subsequent application to a connection creation interface.

`postgresql.clientparameters` is primarily useful to script authors that want to
provide an interface consistent with PostgreSQL commands like ``psql``.

The primary entry points are `postgresql.clientparameters` is

 `postgresql.clientparameters.standard(...)`
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

Example usage::

	import postgresql.clientparameters as pg_param
	p = pg_param.DefaultParser()
	co, ca = p.parse_args(...)
	cp = pg_param.standard(co = co)
	print(cp)

The `postgresql.clientparameters` module is executable, so you can see the
results of the above snippet by::

	$ python -m postgresql.clientparameters -h localhost -U a_db_user -ssearch_path=public
	{'host': 'localhost',
	 'password': None,
	 'port': 5432,
	 'settings': {'search_path': 'public'},
	 'user': 'a_db_user'}

Environment Variables
---------------------

The following is a list of environment variables that will be collected by the
`postgresql.clientparameter.standard` function using the "PG" ``environ_prefix``:

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


Connectors
==========

Connectors are the supporting objects used to instantiate a connection. They
exist for the purpose of providing connections with the necessary abstractions
for facilitating the client's communication with the server, and to act as a
container for the client parameters. The latter purpose is of primary interest
to this section.

Each connection object is associated with its connector by the ``connector``
attribute on the connection. This provides the user with access to the
parameters used to establish the connection in the first place. The attributes
on the connector should *not* be altered. If parameter changes are needed, a
new connector should be created.

The attributes available on a connector are consistent with the names of the
connection parameters described in `Connection Keywords`, so that list can be
used as a reference to identify the information available on the connector.

Connectors fit into the category of "connection creation interfaces", so
connector creation takes the same parameters that the
`postgresql.driver.connect` callable takes.

-------------------
Prepared Statements
-------------------

Prepared statements are the primary entry point for initiating an operation on
the database. Prepared statement objects represent a request that will, likely,
be sent to the database at some point in the future. A statement is a single
SQL command.

The ``prepare`` entry point on the connection provides the standard method for
creating a `postgersql.api.PreparedStatement` instance bound to the
connection(``db``):

	>>> ps = db.prepare("SELECT 'hello, world!'")

Prepared statements are normally executed just like functions:

	>>> my_results = ps()

``my_results``, the object returned by ``ps.__call__``, is a cursor with a
`postgresql.api.Cursor` interface.

.. note::
 Don't confuse PG-API cursors with DB-API cursors.
 PG-API cursors are SQL cursors and don't contain methods
 for executing more queries "within the cursor". Rather, they
 only provide interfaces to retrieving the results.

Prepared statement objects have a few ways to submit the request to the database:

 ``__call__(...)``
  As shown before, statement objects can be simply invoked like a function to get a
  cursor to the statement's results.

 ``__iter__()``
  Convenience interface that executes the ``__call__`` method without arguments.
  This enables the following syntax:

  >>> for table_name, in db.prepare("SELECT table_name FROM information_schema.tables"):
  ...  print(table_name)

 ``first(...)``
  For simple statements, a cursor object can be a bit tiresome to get data from.
  Consider the data contained in ``my_results``, 'hello world!'. To get at this
  data directly from the ``__call__(...)`` method, it looks something like::

	>>> ps().read()[0][0]

  While it's certainly easy to understand, it can be quite cumbersome and
  perhaps even error prone for more complicated queries returing single values.

  To simplify access to simple data, the ``first`` method will simply return
  the "first" of the result set.

  The first value.
   When there is a single row with a single column, ``first()`` will return
   the contents of that cell.

  The first row.
   When there is a single row with multiple columns, ``first()`` will return
   that row.

  The first, and only, row count.
   When DML--for instance, an INSERT-statement--is executed, ``first()`` will
   return the row count returned by the statement as an integer.

  The result set created by the statement determines what is actually returned.
  Naturally, a statement used with ``first()`` should be crafted with these
  rules in mind.


Parameterized Statements
========================

Statement objects can take parameters. To do this, the statement must be defined using
PostgreSQL's positional parameter notation. ``$1``, ``$2``, ``$3``, etc. If the
statement object ``ps`` were to be re-written to take a parameter::

	>>> ps = db.prepare("SELECT $1")

And, re-create the ``my_cursor``::

	>>> my_cursor = ps('hello, world!')

And using ``first()``::

	>>> 'hello, world!' == ps.first('hello, world!')
	True

PostgreSQL determines the type of the parameter based on the context of the
parameter's identifier.

	>>> ps = db.prepare(
	...  "SELECT * FROM information_schema.tables WHERE table_name = $1 LIMIT $2"
	... )
	>>> c = ps("tables", 1)
	>>> c.read()

Parameter ``$1`` in the above statement will take on the type of the
``table_name`` column and ``#2`` will take on the type required by the LIMIT
clause:

However, types can be forced to a specific type using explicit casts:

	>>> ps = db.prepare("SELECT $1::integer")
	>>> ps.first(-400)
	-400


Statement Metadata
==================

In order to provide the appropriate type transformations, the driver must
acquire metadata about the statement's parameters and results. This data is
published via the following attributes on the statement object:

 ``sql_parameter_types``
  A sequence of SQL type names specifying the types of the parameters used in
  the statement. The indexes of the sequence correspond to the
  parameter identifier, N+1.

 ``pg_parameter_types``
  A sequence of PostgreSQL type Oids specifying the types of the parameters
  used in the statement. The indexes of the sequence correspond to the
  parameter identifier, N+1.

 ``sql_column_types``
  A sequence of SQL type names specifying the types of the columns produced by
  the statement. `None` if the statement does not return row-data.

 ``pg_column_types``
  A sequence of PostgreSQL type Oids specifying the types of the columns produced by
  the statement. `None` if the statement does not return row-data.

 ``column_names``
  A sequence of `str` objects specifying the names of the columns produced by
  the statement. `None` if the statement does not return row-data.

Inserting and DML
=================

Loading data into the database is facilitated by prepared statements. In these
examples, a table definition is necessary for a complete illustration::

	>>> db.execute(
	... 	'''
	... CREATE TABLE employee (
	... 	employee_name text,
	... 	employee_salary numeric,
	... 	employee_dob date,
	... 	employee_hire_date data
	... );
	... 	'''
	... )

Create the INSERT statement:

	>>> mk_employee = db.prepare("INSERT INTO employee VALUES ($1, $2, $3, $4)")

And add "Mr. Johnson" to the table:

	>>> import datetime
	>>> dmlr = mk_employee(
	... 	"John Johnson",
	... 	"92,000",
	... 	datetime.date(1950, 12, 10),
	... 	datetime.date(1998, 4, 23)
	... )
	>>> print(dmlr.command())
	INSERT
	>>> print(dmlr.count())
	1

The execution of DML will return a utility cursor. Utility cursors are fully
completed prior to returning control to the caller and any database error that
occurs will be immediately raised regardless of the context.


Cursors
=======

When a prepared statement is called, a `postgresql.api.Cursor` is created and
returned. The type of statement ultimately decides the kind of cursor used to
manage the results.

For cursors that return rows, these interfaces are provided for accessing those
results:

 ``__next__()``
  This fetches the next row in the cursor object. Cursors support the iterator
  protocol. While equivalent to ``cursor.read(1)[0]``, `StopIteration` is raised
  if the returned sequence is empty.

 ``read(nrows)``
  This method name is borrowed from `file` objects, and are semantically
  similar. However, this being a cursor, rows are returned instead of bytes or
  characters. When the number of rows returned is less then the absolute value
  of the number requested, it means that cursor has been exhausted, and there
  are no more rows to be read.

  In cases where the cursor is scrollable, backward fetches are available via
  negative read quantities.

 ``chunks``
  This access point is designed for situations where rows are being read
  quickly. It is a property that provides an ``collections.Iterator`` that returns
  *sequences* of rows. The size of the "chunks" produced is *normally* consistent
  with the ``chunksize`` attribute on the cursor object itself. This is
  normally the fastest way to get rows out of the cursor.

Scrollable Cursors
------------------

By default, cursors are not scrollable. It is assumed, for performance reasons,
that the user just wants the results in a linear fashion. However, scrollable
cursors are supported. To create a scrollable cursor, call the statement with
the ``with_scroll`` keyword argument set to `True`.

.. note::
 Scrollable cursors never pre-fetch to provide guaranteed positioning.

The cursor interface supports scrolling using the ``seek`` method. Like
``read``, it is semantically similar to a file's ``seek()``. ``seek`` takes two
arguments: ``position`` and ``whence``:

 ``position``
  The position to scroll to. The meaning of this is determined by ``whence``.

 ``whence``
  How to use the position: absolute, relative, or absolute from end:

   absolute: ``'ABSOLUTE'`` or ``0`` (default)
    seek to the absolute position in the cursor relative to the beginning of the
    cursor.

   relative: ``'RELATIVE'`` or ``1``
    seek to the relative position. 

   from end: ``'FROM_END'`` or ``2``
    seek to the absolute position relative from the end of the cursor.

Scrolling through employees:

	>>> emps_by_age = db.prepare('''
	... SELECT
	... 	employee_name, employee_salary, employee_dob, employee_hire_date,
	... 	EXTRACT(years FROM AGE(employee_dob)) AS age
	... ORDER BY age ASC
	... ''')
	>>> snapshot = emps_by_age(with_scroll = True)
	>>> # seek to the end
	>>> snapshot.seek(0, 'FROM_END')
	>>> # scroll back one
	>>> snapshot.seek(-1, 'RELATIVE')
	>>> # and back to the beginning again
	>>> snapshot.seek(0)

Additionally, scrollable cursors support backward fetches using negative read
counts:

	>>> snapshot.seek(0, -2)
	>>> snapshot.read(-1)

COPY
----

`postgresql.driver` transparently supports PostgreSQL's COPY command. To the
user, it will act exactly like a cursor that produces tuples with the only
recommendation being that the cursor *should* be completed before other
actions take place on the connection.

In situations where other actions are invoked during a ``COPY TO STDOUT``, the
entire result set of the COPY will be read. However, no error will be raised so
long as there is enough memory available, so it is *very* desirable to avoid
doing other actions on the connection while a COPY is active.

In situations where other actions are invoked during a ``COPY FROM STDIN``, a
COPY failure error will be thrown by the database. The driver manages the
connection state in such a way that will purposefully cause the error as the
COPY was inappropriately interrupted. This not usually a problem as the
``load(...)`` method must complete the COPY before returning.

Copy data is always transferred using ``bytes`` objects. Even in cases where the
COPY is not in ``BINARY`` mode. Any needed encoding transformations must be made
the caller. This is done to avoid any unnecessary overhead by default.

``COPY FROM STDIN`` commands are supported via
`postgresql.api.PreparedStatement.load`. Each invocation to ``load``
is a single invocation of COPY. ``load`` takes an iterable of COPY lines
to send to the server::

	>>> db.execute('''
	... CREATE TABLE sample_copy (
	...	sc_number int,
	...	sc_text text
	... );
	... ''')
	>>> copyin = db.prepare('COPY sample_copy FROM STDIN')
	>>> copyin.load([
	... 	b'123\tone twenty three\n',
	... 	b'350\ttree fitty\n',
	... ])

Copy cursors and the load interface was designed to be used together so that
direct transfers from a source database to a destination database could be made:

	>>> copyout = src.prepare('COPY atable TO STDOUT')
	>>> copyin = dst.prepare('COPY atable FROM STDIN')
	>>> copyin.load(copyout())

----------------------
Transaction Management
----------------------

To simplify transaction management, an interface extension is provided on the
``xact`` attribute of connection objects. This extension provides interfaces for
managing the transaction state of the database. It provides the necessary
interfaces for starting, committing, and aborting transactions blocks and,
transparently, savepoints.

The attributes available on ``xact``:

 ``start(...)``
  Start a transaction block, or set a savepoint if already in a transaction
  block. The savepoint identifier is automatically generated based on the
  transaction "depth".

 ``depth``
  The "depth" of the transaction state. This is incremented every time the
  ``start()`` method is called. If the depth is greater than `1`, it means that
  the database is in a savepoint.

 ``commit()``
  Commit the transaction block or release the savepoint.

 ``abort()``
  Abort the transaction block or rollback to the savepoint associated with the
  current depth.

 ``failed``
  A property indicating one of three states:

   `False`
    Not failed, and in an active transaction-block.

   `True`
    In a failed transaction-block. Savepoints can make this `False` again
	 without reaching zero-depth.

   `None`
    Not in a transaction block. (``start()`` hasn't been called)

With the introduction of with-statements, Python introduced a mechanism
that allows for the provision of a convenient syntax for transaction
management. It's a very obvious application of with-statements:

	>>> with db.xact:
	... 	...

And savepoints are completely abstracted as well::

	>>> with db.xact:
	... 	with db.xact:
	... 		...
	... 	with db.xact:
	... 		...

**Using the with-statement syntax for managing transactions is strongly
recommended.** By using the transaction manager's CM, it allows for Python
exceptions to be properly treated as fatal to the transaction as if an error of
any kind occurs within a transaction block, it is unlikely that the state of the
transaction can be trusted. Additionally, the ``__exit__`` method provides a
safe-guard against invalid commits. This can occur if a database error is
inappropriately caught within a block without being raised.

.. note::
 "Zero-depth" means no active transaction block. It's the default
 "transaction depth" when a connection is establish. It's normally referred to
 when the transaction state has ascended to the ceiling.

Handling database errors inside transaction blocks is generally discouraged as
any database operation that occurs within a failed transaction is an error
itself. This means that it's important to traps any recoverable operation
outside of the transaction block:

	>>> try:
	...  with db.xact:
	...   ...
	... except postgresql.exceptions.UniqueError:
	...  pass

In cases where the transaction is in a failure state, but the context exits
without an exception, a `postgresql.exceptions.InFailedTransactionError` is
raised:

	>>> with db.xact:
	...  try:
	...   ...
	...  except postgresql.exceptions.UniqueError:
	...   pass
	...
	Traceback (most recent call last):
	 ...
	postgresql.exceptions.InFailedTransactionError: invalid block exit detected
	CODE: 25P02
	SEVERITY: ERROR
	SOURCE: DRIVER

Normally, if a ``COMMIT`` is issued on a failed transaction, the command implies a
``ROLLBACK`` without error. This is a very undesirable result for the CM's exit
as it may allow for code to be ran that presumes the transaction was committed.
The driver intervenes here and instantiates the
`postgresql.exceptions.InFailedTransactionError` to provide safe-guards against
the implied ``ROLLBACK``. This effect is consistent with savepoint releases that
occur in a failed transactions.


Transaction Configuration
=========================

In order to specify the isolation level, some configuration is necessary.
The ``xact`` property on a connection is callable; this interface
uses the keywords arguments for establishing the configuration:

	>>> with db.xact(isolation = 'serializeable'):
	... 	...

The actual string given as the isolation level is given to the database.

Read-only is also supported:

	>>> with db.xact(readonly = True):
	... 	...

Savepoint transactions have no configuration--the identifiers are
automatically generated based on the transaction depth.

Prepared Transactions - Two Phase Commit
========================================

PostgreSQL's two-phase commit is supported by the `gid` keyword given to the
transaction's configuration:

	>>> with db.xact(gid = 'global identifier'):
	... 	...

If the ``commit()`` bringing the transaction to zero-depth sees a configured
`gid`, it will cause the transaction manager to issue a ``PREPARE TRANSACTION``
statement instead of a commit. Of course, provided that ``failed`` is `False`,
in which case the usual ``ABORT`` will be issued.

The global identifier can be configured at any time during the transaction, but
it is *strongly* recommended to specify it within the first transaction context
to enjoy clarity.

-------------------
Settings Management
-------------------

SQL's SHOW and SET provides a means to configure runtime parameters on the
database("GUC"s). In order to save the user some grief, a
`collections.MutableMapping` interface is provided to simplify configuration.
This is especially useful for things settings like "search_path".

The ``settings`` attribute on the connection provides the interface extension. 

The standard dictionary interface is supported:

	>>> db.settings['search_path'] = "$user,public"

And ``update(...)`` is better performing for multiple:

	>>> db.settings.update({
	... 	'search_path' : "$user,public",
	... 	'default_statistics_target' : "1000"
	... })

Settings Context Manager
========================

`postgresql.api.Settings` objects are context managers as well. This provides
the user with the ability to specify sections of code that are to be ran with
certain settings. The settings' context manager makes heavy use of keyword
arguments:

	>>> with db.settings(search_path = 'local,public', timezone = 'mst'):
	... 	...

When the block exits, the settings will be restored to the values that they had
when the block entered.

------------
Type Support
------------

The driver supports a large number of PostgreSQL types at the binary level.
Most types are converted to standard Python types. The remaining types are
usually PostgreSQL specific types that are converted in objects whose class is
defined in `postgresql.types`.

When a conversion function is not available for a particular type, the driver
will use the string format of the type and instantiate a `str` object
for the data. It will also expect `str` data when parameter of a type without a
conversion function is bound.

Mulit-dimensional arrays and composite types are completely supported.

.. note::
   Generally, these standard types are provided for convenience. If conversions into
   these datatypes are not desired, it is recommended that explicit casts into
   ``varchar`` are made in statement string.

.. table:: Python types used to represent PostgreSQL types.

 ================================= ================================== ===========
 PostgreSQL Types                  Python Types                       SQL Types
 ================================= ================================== ===========
 `postgresql.types.INT2OID`        `int`                              smallint
 `postgresql.types.INT4OID`        `int`                              integer
 `postgresql.types.INT8OID`        `int`                              bigint
 `postgresql.types.FLOAT4OID`      `float`                            float
 `postgresql.types.FLOAT8OID`      `float`                            double
 `postgresql.types.VARCHAROID`     `str`                              varchar
 `postgresql.types.BPCHAROID`      `str`                              char
 `postgresql.types.XMLOID`         `xml.etree` (cElementTree)         xml

 `postgresql.types.DATEOID`        `datetime.date`                    date
 `postgresql.types.TIMESTAMPOID`   `datetime.datetime`                timestamp
 `postgresql.types.TIMESTAMPTZOID` `datetime.datetime` (UTC timezone) timestamptz
 `postgresql.types.TIMEOID`        `datetime.time`                    time
 `postgresql.types.TIMETZOID`      `datetime.time`                    timetz
 `postgresql.types.INTERVALOID`    `datetime.timedelta`               interval

 `postgresql.types.NUMERICOID`     `decimal.Decimal`
 `postgresql.types.BYTEAOID`       `bytes`
 `postgresql.types.TEXTOID`        `str`
 ================================= ================================== ===========

The mapping in the above table *normally* goes both ways. So when a parameter
is passed to a statement, the type *should* be consistent with the corresponding
Python type. However, many times, for convenience, the object will be passed
through the type's constructor, so it is not always necessary.
"""

__docformat__ = 'reStructuredText'
if __name__ == '__main__':
	import sys
	if (sys.argv + [None])[1] == 'dump':
		sys.stdout.write(__doc__)
	else:
		try:
			help(__package__ + '.driver')
		except NameError:
			help(__name__)
