##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
r'''
******
Driver
******

`postgresql.driver` implements PG-API, `postgresql.api`, using PQ version
3.0 to communicate with PostgreSQL servers. It makes use of the protocol's
extended features to provide binary datatype transmission and protocol level
prepared statements for strongly typed parameters.

`postgresql.driver` currently supports PostgreSQL servers as far back as 8.0.
Prior versions are not tested. While any version of PostgreSQL supporting
version 3.0 of the PQ protocol *should* work.

The following identifiers are regularly used as shorthands for significant
interface elements:

 ``db``
  `postgresql.api.Connection`, a database connection. `Connections`_

 ``ps``
  `postgresql.api.PreparedStatement`, a prepared statement. `Prepared Statements`_

 ``c``
  `postgresql.api.Cursor`, a cursor; the results of a prepared statement.
  `Cursors`_

 ``C``
  `postgresql.api.Connector`, a connector. `Connectors`_


Establishing a Connection
=========================

There are many ways to establish a `postgresql.api.Connection` to a PostgreSQL
server. This section discusses those interfaces.

`postgresql.open`
-----------------

In the root package module, the ``open()`` function is provided for accessing
databases using a single locator string. The string taken by `postgresql.open` is
a URL:

	>>> import postgresql
	>>> db = postgresql.open("pq://localhost/postgres")

This will connect to the host, ``localhost`` and to the database named
``postgres`` via the ``pq`` protocol. open will inherit client parameters from
the environment, so the user name given to the server will come from ``$PGUSER``
or if that is unset, the result of ``getpass.getuser()``. The user's
"pgpassfile" will also be referenced if no password is given:

	>>> db = postgresql.open("pq://username:password@localhost/postgres")

In this case, the password is given, so ``~/.pgpass`` would never be referenced.
The ``user`` client parameter is also given, ``username``, so ``$PGUSER`` or
`getpass.getuser` will not be given to the server.

Settings can also be provided by the query portion of the URL:

	>>> db = postgresql.open("pq://user@localhost/postgres?search_path=public&timezone=mst")

However, the above syntax passes those as GUC settings(see the description of
the ``settings`` keyword in `Connection Keywords`). Driver parameters require a
distinction. This distinction is made when the setting's name is wrapped in
square-brackets, '[' and ']':

	>>> db = postgresql.open("pq://user@localhost/postgres?[sslmode]=require&[connect_timeout]=5")


`postgresql.driver.connect`
---------------------------

`postgresql.open` is a high-level interface to connection creation. It provides
password resolution services and client parameter inheritance. For some
applications, this is undesirable as such implicit inheritance may lead to
failures due to unanticipated parameters being used. For those applications,
use of `postgresql.open` is not recommended. Rather, `postgresql.driver.connect`
should be used when explicit parameterization is desired by an application:

	>>> import postgresql.driver as pg_driver
	>>> db = pg_driver.connect(user = 'usename', password = 'secret', host = 'localhost', port = 5432)

This will create a connection to the server listening on port ``5432``
on the host ``localhost`` as the user ``usename`` with the password ``secret``.

.. note::
 `connect` will *not* inherit parameters from the environment as libpq-based drivers do.

See `Connection Keywords`_ for a full list of acceptable keyword parameters and
their meaning.


Connectors
----------

Connectors are the supporting objects used to instantiate a connection. They
exist for the purpose of providing connections with the necessary abstractions
for facilitating the client's communication with the server, *and to act as a
container for the client parameters*. The latter purpose is of primary interest
to this section.

Each connection object is associated with its connector by the ``connector``
attribute on the connection. This provides the user with access to the
parameters used to establish the connection in the first place, and the means to
create another connection to the same server. The attributes on the connector
should *not* be altered. If parameter changes are needed, a new connector should
be created.

The attributes available on a connector are consistent with the names of the
connection parameters described in `Connection Keywords`_, so that list can be
used as a reference to identify the information available on the connector.

Connectors fit into the category of "connection creation interfaces", so
connector creation takes the same parameters that the
`postgresql.driver.connect` function takes.

The driver, `postgresql.driver.default` provides a set connectors for making a
connection:

 ``Host``
  Provides a ``getaddrinfo()`` abstraction for establishing a connection.

 ``IP4``
  Connect to a single, IPv4 addressed host.

 ``IP6``
  Connect to a single, IPv6 addressed host.

 ``Unix``
  Connect to a unix domain socket.

``Host`` is usual connector used to establish a connection:

	>>> C = postgresql.driver.default.Host(
	...  user = 'auser',
	...  host = 'foo.com',
	...  port = 5432)
	>>> # now establish a connection
	>>> db = C()

Connectors can also create connections that have not been established.
Sometimes, it is useful to have a reference to the connection prior to
establishing it:

	>>> db = C.create()
	>>> db.connect()

Additionally, ``db.connect()`` on ``db.__enter__()`` for with-statement support:

	>>> db = C.create()
	>>> with db:
	...  ...


Connection Keywords
-------------------

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


Connections
===========

`postgresql.open` and `postgresql.driver.connect` provide the means to
establish a connection. Connections provide a `postgresql.api.Database`
interface to a remote PostgreSQL server; specifically, a
`postgresql.api.Connection`.

Connections are one-time objects. Once, it is closed or lost, it can longer be
used to interact with the database provided by the server. If further use of the
server is desired, a new connection *must* be established.


Database Interface Points
-------------------------

After a connection is established, the primary interface points are ready for
use. These entry points exist as properties and methods on the connection
object:

 ``prepare(sql_statement_string)``
  Create a `postgresql.api.PreparedStatement` object for querying the database.
  This provides an "SQL statement template" that can be executed multiple times.
  See `Prepared Statements`_ for more information.

 ``proc(procedure_id)``
  Create a `postgresql.api.StoredProcedure` object referring to a stored
  procedure on the database. The returned object will provide a
  `collections.Callable` interface to the stored procedure on the server. See
  `Stored Procedures`_ for more information.

 ``statement_from_id(statement_id)``
  Create a `postgresql.api.PreparedStatement` object from an existing statement
  identifier. This is used in cases where the statement was prepared on the
  server. See `Prepared Statements`_ for more information.

 ``cursor_from_id(cursor_id)``
  Create a `postgresql.api.Cursor` object from an existing cursor identifier.
  This is used in cases where the cursor was declared on the server. See
  `Cursors`_ for more information.

 ``execute(sql_statements_string)``
  Run a block of SQL on the server. This method returns `None` unless an error
  occurs. If errors occur, the processing of the statements will stop and the
  the error will be raised.

 ``xact(gid = None, isolation = None, mode = None)``
  The `postgresql.api.Transaction` constructor for creating transactions.
  This method creates a transaction reference. The transaction will not be
  started until it's instructed to do so. See `Transactions`_ for more
  information.

 ``settings``
  A property providing a `collections.MutableMapping` interface to the
  database's SQL settings. See `Settings`_ for more information.


Connection Metadata
-------------------

When a connection is established, certain pieces of metadata are collected from
the backend. The following are the attributes set on the connection object after
the connection is made:

 ``version``
  The results of ``SELECT version()``.
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


Prepared Statements
===================

Prepared statements are the primary entry point for initiating an operation on
the database. Prepared statement objects represent a request that will, likely,
be sent to the database at some point in the future. A statement is a single
SQL command.

The ``prepare`` entry point on the connection provides the standard method for
creating a `postgersql.api.PreparedStatement` instance bound to the
connection(``db``) from an SQL statement.

Statement objects may also be created from a statement identifier using the
``statement_from_id`` method on the connection. When this method is used, the
statement must have already been prepared or an error will be raised.


Prepared Statement Interface Points
-----------------------------------

Prepared statements are normally executed just like functions:

	>>> ps = db.prepare("SELECT 'hello, world!'")
	>>> c = ps()
	>>> c.read()
	[('hello, world!',)]

``c``, the object returned by the invocation of ``ps.__call__``, is a cursor
object providing a `postgresql.api.Cursor` interface.

.. note::
 Don't confuse PG-API cursors with DB-API 2.0 cursors.
 PG-API cursors are single result-set SQL cursors and don't contain
 methods for executing more queries "within the cursor". They
 only provide interfaces to retrieving the results to that specific
 invocation of the statement.

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
  Consider the data contained in ``c`` from above, 'hello world!'. To get at this
  data directly from the ``__call__(...)`` method, it looks something like::

	>>> ps().read()[0][0]

  While it's certainly easy to understand, it can be quite cumbersome and
  perhaps even error prone depending on the statement.

  To simplify access to simple data, the ``first`` method will simply return
  the "first" of the result set.

  The first value.
   When the result set consists of a single column, ``first()`` will return
   that column in the first row.

  The first row.
   When the result set consists of multiple columns, ``first()`` will return
   that first row.

  The first, and only, row count.
   When DML--for instance, an INSERT-statement--is executed, ``first()`` will
   return the row count returned by the statement as an integer.

  The result set created by the statement determines what is actually returned.
  Naturally, a statement used with ``first()`` should be crafted with these
  rules in mind.


Statement Metadata
------------------

In order to provide the appropriate type transformations, the driver must
acquire metadata about the statement's parameters and results. This data is
published via the following properties on the statement object:

 ``sql_parameter_types``
  A sequence of SQL type names specifying the types of the parameters used in
  the statement.

 ``sql_column_types``
  A sequence of SQL type names specifying the types of the columns produced by
  the statement. `None` if the statement does not return row-data.

 ``pg_parameter_types``
  A sequence of PostgreSQL type Oid's specifying the types of the parameters
  used in the statement.

 ``pg_column_types``
  A sequence of PostgreSQL type Oid's specifying the types of the columns produced by
  the statement. `None` if the statement does not return row-data.

 ``parameter_types``
  A sequence of Python types that the statement expects.

 ``column_types``
  A sequence of Python types that the statement will produce.

 ``column_names``
  A sequence of `str` objects specifying the names of the columns produced by
  the statement. `None` if the statement does not return row-data.

The indexes of the sequence correspond to the parameter's identifier, N+1.

In order for this information to be available, the statement must be fully
prepared, so if the statement is closed, it will be re-prepared when this
information is accessed.

	>>> ps = db.prepare("SELECT $1::integer AS intname, $2::varchar AS chardata")
	>>> ps.sql_parameter_types
	('integer','varchar')
	>>> ps.sql_column_types
	('integer','varchar')
	>>> ps.column_names
	('intname','chardata')
	>>> ps.column_types
	(<class 'int'>, <class 'str'>)


Parameterized Statements
------------------------

Statements can take parameters. Using statement parameters is the recommended
way to interrogate the database when variable information is needed to formulate
a complete request. In order to do this, the statement must be defined using
PostgreSQL's positional parameter notation. ``$1``, ``$2``, ``$3``, etc:

	>>> ps = db.prepare("SELECT $1")
	>>> c = ps('hello, world!')
	>>> c.read()[0][0]
	'hello, world!'

PostgreSQL determines the type of the parameter based on the context of the
parameter's identifier.

	>>> ps = db.prepare(
	...  "SELECT * FROM information_schema.tables WHERE table_name = $1 LIMIT $2"
	... )
	>>> c = ps("tables", 1)
	>>> c.read()
	[('postgres', 'information_schema', 'tables', 'VIEW', None, None, None, None, None, 'NO', 'NO', None)]

Parameter ``$1`` in the above statement will take on the type of the
``table_name`` column and ``$2`` will take on the type required by the LIMIT
clause(text and int8).

However, types can be forced to a specific type using explicit casts:

	>>> ps = db.prepare("SELECT $1::integer")
	>>> ps.first(-400)
	-400

Parameters are typed. PostgreSQL servers provide the driver with the
type information about a positional parameter, and the driver will require that
a given parameter is in the appropriate type as required by the serialization
routines. The Python types expected by the driver for a given SQL and PostgreSQL
type are listed in `Type Support`_.

This usage of Python types that are included in the standard library is not always
convenient. Notably, the `datetime` module does not provide a friendly way for a
user to express intervals, dates, or times. There is a likely inclination to
forego these parameter type requirements.

In such cases, explicit casts can be made to work-around the type requirements:

	>>> ps = db.prepare("SELECT $1::text::date")
	>>> ps.first('yesterday')
	datetime.date(2009, 3, 11)

The parameter, ``$1``, is given to the database as a string, which is then
promptly cast into a date. Of course, without the explicit cast as text, the
outcome would be different:

	>>> ps = db.prepare("SELECT $1::text::date")
	>>> ps.first('yesterday')
	Traceback:
	 ...
	AttributeError: 'str' object has no attribute 'toordinal'

The function that processes the parameter expects a `datetime.date` object, and
the given `str` object does not provide the necessary interfaces for the
conversion.


Inserting and DML
-----------------

Loading data into the database is facilitated by prepared statements. In these
examples, a table definition is necessary for a complete illustration::

	>>> db.execute(
	... 	"""
	... CREATE TABLE employee (
	... 	employee_name text,
	... 	employee_salary numeric,
	... 	employee_dob date,
	... 	employee_hire_date data
	... );
	... 	"""
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

Cursors can also be created directly from ``cursor_id``'s using the
``cursor_from_id`` method on connection objects.


Cursor Interface Points
-----------------------

For cursors that return row data, these interfaces are provided for accessing
those results:

 ``__next__()``
  This fetches the next row in the cursor object. Cursors support the iterator
  protocol. While equivalent to ``cursor.read(1)[0]``, `StopIteration` is raised
  if the returned sequence is empty.

 ``read(nrows)``
  This method name is borrowed from `file` objects, and are semantically
  similar. However, this being a cursor, rows are returned instead of bytes or
  characters. When the number of rows returned is less then the absolute value
  of the number requested, it means that cursor has been exhausted,
  and there are no more rows to be read in that direction.

  In cases where the cursor is scrollable, backward fetches are available via
  negative read quantities.

 ``chunks``
  This access point is designed for situations where rows are being streamed out
  quickly. It is a property that provides an ``collections.Iterator`` that returns
  *sequences* of rows. The size of the "chunks" produced is *normally* consistent
  with the ``chunksize`` attribute on the cursor object itself. This is
  normally the most efficient way to get rows out of the cursor.

 ``seek(position[, whence = 0])``
  When the cursor is scrollable, this seek interface can be used to move the
  position of the cursor. See `Scrollable Cursors`_ for more information.


Cursor Metadata
---------------

Cursors normally share metadata with the statements that create them, so it is
usually unnecessary for referencing the cursor's column descriptions directly.
However, when a cursor is opened from an identifier, the cursor interface must
collect the metadata itself. These attributes provide the metadata in absence of
a statement object:

 ``sql_column_types``
  A sequence of SQL type names specifying the types of the columns produced by
  the cursor. `None` if the cursor does not return row-data.

 ``pg_column_types``
  A sequence of PostgreSQL type Oid's specifying the types of the columns produced by
  the cursor. `None` if the cursor does not return row-data.

 ``column_types``
  A sequence of Python types that the cursor will produce.

 ``column_names``
  A sequence of `str` objects specifying the names of the columns produced by
  the cursor. `None` if the cursor does not return row-data.


Scrollable Cursors
------------------

By default, cursors are not scrollable. It is assumed, for performance reasons,
that the user just wants the results in a linear fashion. However, scrollable
cursors are supported for applications that need to implement paging. To create
a scrollable cursor, call the statement with the ``with_scroll`` keyword
argument set to `True`.

.. note::
 Scrollable cursors never pre-fetch in order to provide guaranteed positioning.

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
    seek to the relative position. Negative ``position``'s will cause a MOVE
    backwards, while positive ``position``'s will MOVE forwards.

   from end: ``'FROM_END'`` or ``2``
    seek to the end of the cursor and then MOVE backwards by the given
    ``position``.

Scrolling through employees:

	>>> emps_by_age = db.prepare("""
	... SELECT
	... 	employee_name, employee_salary, employee_dob, employee_hire_date,
	... 	EXTRACT(years FROM AGE(employee_dob)) AS age
	... ORDER BY age ASC
	... """)
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


COPY Cursors
------------

`postgresql.driver` transparently supports PostgreSQL's COPY command. To the
user, it will act exactly like a cursor that produces tuples; COPY tuples,
however, are `bytes` objects. The only distinction in usability is that the
cursor *should* be completed before other actions take place on the connection.

In situations where other actions are invoked during a ``COPY TO STDOUT``, the
entire result set of the COPY will be read. However, no error will be raised so
long as there is enough memory available, so it is *very* desirable to avoid
doing other actions on the connection while a COPY is active.

In situations where other actions are invoked during a ``COPY FROM STDIN``, a
COPY failure error will occur. The driver manages the connection state in such
a way that will purposefully cause the error as the COPY was inappropriately
interrupted. This not usually a problem as the ``load(...)`` method must
complete the COPY command before returning.

Copy data is always transferred using ``bytes`` objects. Even in cases where the
COPY is not in ``BINARY`` mode. Any needed encoding transformations *must* be
made the caller. This is done to avoid any unnecessary overhead by default.

``COPY FROM STDIN`` commands are supported via
`postgresql.api.PreparedStatement.load`. Each invocation to ``load``
is a single invocation of COPY. ``load`` takes an iterable of COPY lines
to send to the server::

	>>> db.execute("""
	... CREATE TABLE sample_copy (
	...	sc_number int,
	...	sc_text text
	... );
	... """)
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


Stored Procedures
=================

The ``proc`` method on `postgresql.api.Database` objects provides a means to
create a reference to a stored procedure on the remote database.
`postgresql.api.StoredProcedure` objects are used to represent the referenced
SQL routine.

This provides a direct interface to the function stored on the database. It
leverages knowledge of the parameters and results of the function in order
to provide the user with a natural interface to the procedure:

	>>> get_version = db.proc('version()')
	>>> get_version()
	'PostgreSQL 8.3.6 on ...'

Set-returning functions, SRFs, on the other hand, return a sequence:

	>>> generate_series = db.proc('generate_series(int,int)')
	>>> gs = generate_series(1, 20)
	>>> gs
	<generator object <genexpr>>
	>>> next(gs)
	1
	>>> list(gs)
	[2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]

For functions like ``generate_series()``, the driver is able to identify that
the return is a sequence of *solitary* integer objects, so the result of the
function is just that.

Functions returning composite types are recognized, and return row objects:

	>>> db.execute("""
	... CREATE FUNCTION composite(OUT i int, OUT t text)
	... LANGUAGE SQL AS
	... $body$
	...  SELECT 900::int AS i, 'sample text'::text AS t;
	... $body$;
	... """)
	>>> composite = db.proc('composite()')
	>>> r = composite()
	>>> r
	(900, 'sample text')
	>>> r['i']
	900
	>>> r['t']
	'sample text'

Functions returning a set of composites are recognized, and the result is a
`postgresql.api.Cursor` object whose column names are consistent with the names
of the OUT parameters:

	>>> db.execute("""
	... CREATE FUNCTION srfcomposite(out i int, out t text)
	... RETURNS SETOF RECORD
	... LANGUAGE SQL AS
	... $body$
	...  SELECT 900::int AS i, 'sample text'::text AS t
	...  UNION ALL
	...  SELECT 450::int AS i, 'more sample text'::text AS t
	... $body$;
	... """)
	>>> srfcomposite = db.proc('srfcomposite()')
	>>> c = srfcomposite()
	>>> c
	<postgresql.driver.pq3.DeclaredCursor object>
	>>> c.read(1)
	[(900, 'sample text')]
	>>> r = c.read(1)[0]
	>>> r['i'], r['t']
	(450, 'more sample text')


Transactions
============

Transactions are managed by creating an object corresponds to a
transaction started on the server. A transaction is a transaction block,
a savepoint, or a prepared transaction. The ``xact(...)`` method on the
connection object provides the standard method for creating a
`postgresql.api.Transaction` object to manage a transaction on the connection.

The creation of a transaction object does not open the transaction. Rather, the
transaction must be explicitly started. Usually, transactions should be managed
with context managers:

	>>> with db.xact():
	...  ...

The transaction in the above example is opened, started, by the ``__enter__``
method invoked by the with-statement's usage. It will be subsequently
committed or rolled-back depending on the exception state and the error state
of the connection when ``__exit__`` is called.

**Using the with-statement syntax for managing transactions is strongly
recommended.** By using the transaction's context manager, it allows for Python
exceptions to be properly treated as fatal to the transaction as when an
exception of any kind occurs within a transaction block, it is unlikely that
the state of the transaction can be trusted. Additionally, the ``__exit__``
method provides a safe-guard against invalid commits. This can occur if a
database error is inappropriately caught within a block without being raised.

The context manager interfaces are higher level interfaces to the explicit
instruction methods provided by `postgresql.api.Transaction` objects.


Transaction Interface Points
----------------------------

The methods available on transaction objects manage the state of the transaction
and relay any necessary instructions to the remote server in order to reflect
that change of state.

	>>> x = db.xact(...)

 ``x.start()``
  Start the transaction.

 ``x.commit()``
  Commit the transaction.

 ``x.rollback()``
  Abort the transaction. For prepared transactions, this can be called at any
  phase.

 ``x.recover()``
  Identify the existence of the prepared transaction.

 ``x.prepare()``
  Prepare the transaction for the final commit. Once prepared, the second commit
  may be ran to finalize the transaction, ``x.commit()``. 

These methods are primarily provided for applications that manage transactions
in a way that cannot be formed around single, sequential blocks of code.
Generally, using these methods require additional work to be performed by the
code that is managing the transaction.
If usage of these direct, instructional methods is necessary, there are some
important factors to keep in mind:

 * If the transaction is configured with a `gid`, the ``prepare()`` method
   *must* be invoked prior to the ``commit()``.
 * If the database is in an error state when the commit() is issued, an implicit
   rollback will occur. Usually, when the database is in an error state,
   a database exception will have been thrown, so it is likely that it will be
   understood that a rollback should occur.


Error Control
-------------

Handling *database* errors inside transaction CMs is generally discouraged as
any database operation that occurs within a failed transaction is an error
itself. It is important to trap any recoverable database error outside of the
scope of the transaction's context manager:

	>>> try:
	...  with db.xact():
	...   ...
	... except postgresql.exceptions.UniqueError:
	...  pass

In cases where the database is in an error state, but the context exits
without an exception, a `postgresql.exceptions.InFailedTransactionError` is
raised by the driver:

	>>> with db.xact():
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
The driver intervenes here and raises the
`postgresql.exceptions.InFailedTransactionError` to safe-guard against such
cases. This effect is consistent with savepoint releases that occur during an
error state. The distinction between the two cases is made using the ``source``
property on the raised exception.


Transaction Configuration
-------------------------

Keyword arguments given to ``xact()`` provide the means for configuring the
properties of the transaction. Only three points of configuration are available:

 ``gid``
  The global identifier to use. Identifies the transaction as using two-phase
  commit. The ``prepare()`` method must be called first.

 ``isolation``
  The isolation level of the transaction. This must be a string. It will be
  interpolated directly into the START TRANSACTION statement. Normally,
  'SERIALIZABLE' or 'READ COMMITTED'.

 ``mode``
  A string, 'READ ONLY' or 'READ WRITE'. States the mutability of stored
  information in the database.

The specification of any of these transaction properties imply that the transaction
is a block. Savepoints do not take configuration, so if a transaction identified
as a block is started while another block is running, an exception will be
raised.


Prepared Transactions
---------------------

Transactions configured with the ``gid`` keyword will require two steps to fully
commit the transaction. First, ``prepare()``, then ``commit()``. The prepare
method will issue the necessary PREPARE TRANSACTION statement, and commit will
finalize the operation with the issuance of the COMMIT PREPARED statement. At
any state of the transaction, ``rollback()`` may be used to abort the
transaction. If the transaction has yet to be prepared, it will issue the ABORT
statement, and if it has, it will issue ROLLBACK PREPARED instead.

Prepared transactions can be used with with-statements:

	>>> with db.xact(gid='global-id') as gxact:
	...  with gxact:
	...   ...

The ``__enter__`` method on the transaction is invoked twice. The transaction
keeps its state, so only one START TRANSACTION statement will be executed.
The ``__exit__`` method is also invoked twice. Exit will analyze the transaction
and make the appropriate action for the state of the transaction object. If the
transaction is open, it will prepare the transaction using ``prepare()``, and if
the transaction is prepared, it will commit the transaction using ``commit()``.

When a prepared transaction is partially committed, the transaction should be
rolled-back or committed at some point in time. It is possible
for a transaction to be prepared, but the connection lost before the final
commit or rollback. Cases where this occurs require a recovery operation to
determine the fate of the transaction.

In order to recover a prepared transaction, the ``recover()`` method can be
used:

	>>> gxact = db.xact(gid='a-lost-transaction')
	>>> gxact.recover()

Once recovered, it may then be committed or rolled-back using the appropriate
methods:

	>>> gxact.commit()

When a transaction is recovered and the configured ``gid`` does not exist, the
driver will throw a `postgresql.exceptions.UndefinedObjectError`. This is
consistent with the error that is caused by ROLLBACK PREPARED and COMMIT
PREPARED when the global identifier does not exist.

	>>> gxact = db.xact(gid='a-non-existing-gid')
	>>> gxact.recover()
	postgresql.exceptions.UndefinedObjectError

This allows recovery operations to identify the existence of the prepared
transaction.


Settings
========

SQL's SHOW and SET provides a means to configure runtime parameters on the
database("GUC"s). In order to save the user some grief, a
`collections.MutableMapping` interface is provided to simplify configuration.

The ``settings`` attribute on the connection provides the interface extension. 

The standard dictionary interface is supported:

	>>> db.settings['search_path'] = "$user,public"

And ``update(...)`` is better performing for multiple sets:

	>>> db.settings.update({
	...  'search_path' : "$user,public",
	...  'default_statistics_target' : "1000"
	... })


Settings Contexts
-----------------

`postgresql.api.Settings` objects are context managers as well. This provides
the user with the ability to specify sections of code that are to be ran with
certain settings. The settings' context manager takes full advantage of keyword
arguments:

	>>> with db.settings(search_path = 'local,public', timezone = 'mst'):
	...  ...

When the block exits, the settings will be restored to the values that they had
when the block entered.


Type Support
============

The driver supports a large number of PostgreSQL types at the binary level.
Most types are converted to standard Python types. The remaining types are
usually PostgreSQL specific types that are converted in objects whose class is
defined in `postgresql.types`.

When a conversion function is not available for a particular type, the driver
will use the string format of the type and instantiate a `str` object
for the data. It will also expect `str` data when parameter of a type without a
conversion function is bound.


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
 `postgresql.types.TIMESTAMPTZOID` `datetime.datetime` (tzinfo)       timestamptz
 `postgresql.types.TIMEOID`        `datetime.time`                    time
 `postgresql.types.TIMETZOID`      `datetime.time`                    timetz
 `postgresql.types.INTERVALOID`    `datetime.timedelta`               interval

 `postgresql.types.NUMERICOID`     `decimal.Decimal`                  numeric
 `postgresql.types.BYTEAOID`       `bytes`                            bytea
 `postgresql.types.TEXTOID`        `str`                              text
 ================================= ================================== ===========

The mapping in the above table *normally* goes both ways. So when a parameter
is passed to a statement, the type *should* be consistent with the corresponding
Python type. However, many times, for convenience, the object will be passed
through the type's constructor, so it is not always necessary.


Arrays
------

Arrays of PostgreSQL types are supported with near transparency. For simple
arrays, arbitrary iterables can just be given as a statement's parameter and the
array's constructor will consume the objects produced by the iterator into a
`postgresql.types.Array` instance. However, in situations where the array has
multiple dimensions, list objects are used to delimit the boundaries of the
array.

	>>> ps = db.prepare("select $1::int[]")
	>>> ps.first([(1,2), (2,3)])
	TypeError

In the above case, it is apparent that this array is supposed to have two
dimensions. However, this is not the case for other types:

	>>> ps = db.prepare("select $1::point[]")
	>>> ps.first([(1,2), (2,3)])
	postgresql.types.Array([postgresql.types.point((1.0, 2.0)), postgresql.types.point((2.0, 3.0))])

Lists are used to provide the necessary boundary information:

	>>> ps = db.prepare("select $1::int[]")
	>>> ps.first([[1,2],[2,3]])
	postgresql.types.Array([[1,2],[2,3]])

The above is the appropriate way to define the array from the original example.

.. hint::
 The root-iterable object given as an array parameter does not need to be a
 list-type as it's assumed to be made up of elements.
'''

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
