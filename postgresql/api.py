##
# copyright 2008, pg/python project.
# http://python.projects.postgresql.org
##
"""
PG-API
======

postgresql.api is a Python API to the PostgreSQL RDBMS. It is designed to take
full advantage of the database elements provided by PostgreSQL to provide the
Python programmer with substantial convenience.

This module is used to define the PG-API. It creates a set of classes
that makes up the basic interfaces used to work with a PostgreSQL database.

Connection objects aren't required to be a part in any special class hierarchy.
Merely, the Python protocol described here *must* be supported. For instance, a
module object could be a connection to the database. However, it is recommended
that implementations inherit from these objects in order to benefit from the
provided doc-strings.

The examples herein will regularly refer to a ``pg`` object; this object is the
`connection` instance--a PG-API Connection.


Exceptions
----------

For the most part, PG-API tries to stay out of the databases' business. When
a database error occurs, it should be mapped to the corresponding exception in
``postgresql.exceptions`` and raised. In the cases of fatal errors, panics, or
unexpected closures, the same exception must be raised anytime an operation is
enacted on the connection until the connection is explicitly closed.

When a connection is closed and an operation is attempted on the connection--
other than ``connect``, the `postgresql.exceptions.ConnectionDoesNotExistError`
error must be raised.

When a connection's link is somehow lost on an operation, the
`postgresql.exceptions.ConnectionFailureError` exception should be raised. If
exception chains are supported by the Python implementation, it should chain the
literal exception onto the ``ConnectionFailureError`` instance. If no explicit
exception caused the loss, then the ``ConnectionFailureError`` error message
should describe the symptom indicating the loss.
"""
from abc import ABCMeta, abstractproperty, abstractmethod
import collections

class PreparedStatement(
	collections.Callable,
	collections.Iterable,
	metaclass = ABCMeta
):
	"""
	Bound to `Connections` as `query`.

	A PreparedStatement is an Iterable as well as Callable. This feature is
	supported for queries that have the default arguments filled in or take no
	arguments at all. It allows for things like:

		>>> for x in connection.query('select * FROM table'):
		...  pass
	"""

	@abstractmethod
	def __call__(self, *args):
		"""
		Execute the prepared statement with the given arguments as parameters. If
		the query returns rows, a cursor object should be returned, otherwise a
		resulthandle object.

		Usage:

		>>> q=pg.query("SELECT table_name FROM information_schema.tables WHERE
		... table_schema = $1")
		>>> q('public')
		<cursor object>
		"""

	@abstractmethod
	def first(self, *args):
		"""
		Execute the prepared statement with the given arguments as parameters. If
		the query returns rows with multiple columns, return the first row. If the
		query returns rows with a single column, return the first column in the
		first row. If the query does not return rows at all, return the count or
		`None` if no count exists in the completion message. Usage:

		>>> pg.query("SELECT * FROM ttable WHERE key = $1").first("somekey")
		('somekey', 'somevalue')
		>>> pg.query("SELECT 'foo'").first()
		'foo'
		>>> pg.query("INSERT INTO atable (col) VALUES (1)").first()
		1
		"""

	@abstractmethod
	def load(self, iterable):
		"""
		Given an iterable, `iterable`, feed the produced parameters to the query.
		This is a bulk-loading interface for parameterized queries.

		Effectively, it is equivalent to:
		
		>>> q = pg.query(sql)
		>>> for i in iterable:
		...  q(*i)

		Its purpose is to allow the implementation to take advantage of the
		knowledge that a series of parameters are to be loaded.
		"""

	@abstractmethod
	def __invert__(self):
		"""
		Shorthand for a call to the `first` method without any arguments.
		Useful for resolving static queries. Example usage:

		>>> ~pg.query("INSERT INTO ttable VALUES ('value')")
		1
		>>> ~pg.query("SELECT 'somestring'")
		'somestring'
		"""

	@abstractmethod
	def close(self):
		"""
		Close the prepraed statement releasing resources associated with it.
		"""

	@abstractmethod
	def prepare(self):
		"""
		Prepare the already instantiated query for use. This method would only be
		used if the query were closed at some point.
		"""

	@abstractmethod
	def reprepare(self):
		"""
		Shorthand for ``close`` then ``prepare``.
		"""

class Cursor(
	collections.Iterable,
	collections.Iterator,
	metaclass = ABCMeta
):
	"""
	A `cursor` object is an interface to a sequence of tuples(rows). A result set.
	Cursors publish a file-like interface for reading tuples from the database.

	`cursor` objects are created by invoking `query` objects or by calling
	`connection.cursor` with the declared cursor's name.
	"""

	@abstractmethod
	def read(self, quantity = -1):
		"""
		Read the specified number of tuples and return them in a list.
		This advances the cursor's position.
		"""

	@abstractmethod
	def close(self):
		"""
		Close the cursor to release its resources.
		"""

	@abstractmethod
	def __next__(self):
		"""
		Get the next tuple in the cursor. Advances the position by one.
		"""

	@abstractmethod
	def seek(self, offset, whence = 0):
		"""
		Set the cursor's position to the given offset with respect to the
		whence parameter.

		Whence values:

		 ``0``
		  Absolute.
		 ``1``
		  Relative.
		 ``2``
		  Absolute from end.
		"""

	@abstractmethod
	def scroll(self, rows):
		"""
		Set the cursor's position relative to the current position.
		Negative numbers can be used to scroll backwards.

		This is a convenient interface to `seek` with a relative whence(``1``).
		"""

	@abstractmethod
	def __getitem__(self, idx):
		"""
		Get the rows at the given absolute position.

		This may only be available on scrollable cursors.
		"""

class StoredProcedure(
	collections.Callable,
	metaclass = ABCMeta
):
	"""
	A `proc` object is an interface to a stored procedure. A `proc` object is
	created by `connection.proc`.
	"""

	@abstractmethod
	def __call__(self, *args, **kw):
		"""
		Execute the procedure with the given arguments. If keyword arguments are
		passed they must be mapped to the argument whose name matches the key. If
		any positional arguments are given, they must fill in any gaps created by
		the filled keyword arguments. If too few or too many arguments are given,
		a TypeError must be raised. If a keyword argument is passed where the
		procedure does not have a corresponding argument name, then, likewise, a
		TypeError must be raised.
		"""

class Transaction(metaclass = ABCMeta):
	"""
	A xact object is the connection's transaction manager. It is already
	instantiated for every connection. It keeps the state of the transaction and
	provides methods for managing the state thereof.

	Normal usage would merely entail the use of the with-statement::

		with pg.xact:
		...
	
	Or, in cases where two-phase commit is desired::

		with pg.xact('gid'):
		...
	"""

	@abstractproperty
	def failed(self) -> "True|False":
		"""
		bool stating if the current transaction has failed due to an error.
		`None` if not in a transaction block.
		"""
	
	@abstractproperty
	def closed(self) -> "True|False":
		"""
		`bool` stating if there is an open transaction block.
		"""

	@abstractmethod
	def start(self):
		"""
		Start a transaction block. If a transaction block has already been
		started, set a savepoint.
		``start``, ``begin``, and ``__enter__`` are synonyms.
		"""
	__enter__ = begin = start

	@abstractmethod
	def commit(self):
		"""
		Commit the transaction block, release a savepoint, or prepare the
		transaction for commit. If the number of running transactions is greater
		than one, then the corresponding savepoint is released. If no savepoints
		are set and the transaction is configured with a 'gid', then the
		transaction is prepared instead of committed, otherwise the transaction is
		simply committed.
		"""

	@abstractmethod
	def rollback(self):
		"""
		Abort the current transaction or rollback to the last started savepoint.
		`rollback` and `abort` are synonyms.
		"""
	abort = rollback

	@abstractmethod
	def restart(self):
		"""
		Abort and start the transaction or savepoint.
		"""

	@abstractmethod
	def checkpoint(self):
		"""
		Commit and start a transaction block or savepoint. Not to be confused with
		the effect of the CHECKPOINT command.
		"""

	@abstractmethod
	def __call__(self, gid = None, isolation = None, readonly = None):
		"""
		Initialize the transaction using parameters and return self to support a
		convenient with-statement syntax.

		The configuration only applies to transaction blocks as savepoints have no 
		parameters to be configured.

		If the `gid`, the first keyword parameter, is configured, the transaction
		manager will issue a ``PREPARE TRANSACTION`` with the specified identifier
		instead of a ``COMMIT``.

		If `isolation` is specified, the ``START TRANSACTION`` will include it as
		the ``ISOLATION LEVEL``. This must be a character string.

		If the `readonly` parameter is specified, the transaction block will be
		started in the ``READ ONLY`` mode if True, and ``READ WRITE`` mode if False.
		If `None`, neither ``READ ONLY`` or ``READ WRITE`` will be specified.

		Read-only transaction::

			>>> with pg.xact(readonly = True):
			...

		Read committed isolation::

			>>> with pg.xact(isolation = 'READ COMMITTED'):
			...

		Database configured defaults apply to all `xact` operations.
		"""

	@abstractmethod
	def __context__(self):
		'Return self'

	@abstractmethod
	def __exit__(self, typ, obj, tb):
		"""
		Commit the transaction, or abort if the given exception is not `None`. If
		the transaction level is greater than one, then the savepoint
		corresponding to the current level will be released or rolled back in
		cases of an exception.

		If an exception was raised, then the return value must indicate the need
		to further raise the exception, unless the exception is an
		`postgresql.exceptions.AbortTransaction`. In which case, the transaction
		will be rolled back accordingly, but the no exception will be raised.
		"""

	@abstractmethod
	def commit_prepared(self, gid):
		"""
		Commit the prepared transaction with the given `gid`.
		"""

	@abstractmethod
	def rollback_prepared(self, *gids):
		"""
		Rollback the prepared transaction with the given `gid`.
		"""

	@abstractproperty
	def prepared(self) -> "sequence of prepared transaction identifiers":
		"""
		A sequence of available prepared transactions for the current user on the
		current database. This is intended to be more relavent for the current
		context than selecting the contents of ``pg_prepared_xacts``. So, the view
		*must* be limited to those of the current database, and those which the
		user can commit.
		"""

class Settings(
	collections.MutableMapping,
	metaclass = ABCMeta
):
	"""
	A mapping interface to the session's settings. This dictionary-like object
	provides a direct interface to ``SHOW`` or ``SET`` commands. Identifiers and
	values need not be quoted specially as the implementation must do that work
	for the user.
	"""

	def getpath(self) -> "Sequence of schema names that make up the search_path":
		"""
		Returns a sequence of the schemas that make up the current search_path.
		"""
	def setpath(self, seq : "Sequence of schema names"):
		"""
		Set the "search_path" setting to the given a sequence of schema names, 
		[Implementations must properly escape and join the strings]
		"""
	path = abstractproperty(getpath, setpath,
		doc = """
		An interface to a structured ``search_path`` setting:

		>>> pg.settings.path
		['public', '$user']

		It may also be used to set the path:

		>>> pg.settings.path = ('public', 'tools')
		"""
	)
	del getpath, setpath

	@abstractmethod
	def __getitem__(self, key):
		"""
		Return the setting corresponding to the given key. The result should be
		consistent with what the ``SHOW`` command returns. If the key does not
		exist, raise a KeyError.
		"""

	@abstractmethod
	def __setitem__(self, key, value):
		"""
		Set the setting with the given key to the given value. The action should
		be consistent with the effect of the ``SET`` command.
		"""

	@abstractmethod
	def __call__(self, **kw):
		"""
		Configure settings for the next established context and return the
		settings object. This is normally used in conjunction with a
		with-statement:

		>>> with pg.settings(search_path = 'local,public'):
		...

		When called, the settings' object will configure itself to use the given
		settings for the duration of the block, when the block exits, the previous
		settings will be restored.

		If a configuration has already been stored when invoked, the old
		configuration will be overwritten. Users are expected to set the
		configuration immediately.
		"""

	@abstractmethod
	def __enter__(self):
		"""
		Set the settings configured using the __call__ method.

		If nothing has been configured, do nothing.
		"""

	@abstractmethod
	def __exit__(self, exc, val, tb):
		"""
		Immediately restore the settings if the connection is not in an error
		state. Otherwise, make the restoration pending until the state is
		corrected.
		"""

	@abstractmethod
	def get(self, key, default = None):
		"""
		Get the setting with the corresponding key. If the setting does not exist,
		return the `default`.
		"""

	@abstractmethod
	def getset(self, keys):
		"""
		Return a dictionary containing the key-value pairs of the requested
		settings. If *any* of the keys do not exist, a `KeyError` must be raised
		with the set of keys that did not exist.
		"""

	@abstractmethod
	def update(self, mapping):
		"""
		For each key-value pair, incur the effect of the `__setitem__` method.
		"""

	@abstractmethod
	def keys(self):
		"""
		Return an iterator to all of the settings' keys.
		"""

	@abstractmethod
	def values(self):
		"""
		Return an iterator to all of the settings' values.
		"""

	@abstractmethod
	def items(self):
		"""
		Return an iterator to all of the setting value pairs.
		"""

	@abstractmethod
	def subscribe(self, key, callback):
		"""
		Subscribe to changes of the setting using the callback. When the setting
		is changed, the callback will be invoked with the connection, the key,
		and the new value. If the old value is locally cached, its value will
		still be available for inspection, but there is no guarantee.
		If `None` is passed as the key, the callback will be called whenever any
		setting is remotely changed.

		>>> def watch(connection, key, newval):
		...
		>>> pg.settings.subscribe('TimeZone', watch)
		"""

	@abstractmethod
	def unsubscribe(self, key, callback):
		"""
		Stop listening for changes to a setting. The setting name(`key`), and the
		callback used to subscribe must be given again for successful termination
		of the subscription.

		>>> pg.settings.unsubscribe('TimeZone', watch)
		"""

class TypeIO(metaclass = ABCMeta):
	"""
	A TypeIO object is a container for type I/O management facilities. This means
	it's the single place to go to retrieve functions for serializing objects for
	transport over the wire. This class is only pertinent to connections that
	communicate with the server via PQ.
	"""


class Connection(metaclass = ABCMeta):
	"""
	The connection interface.
	"""

	@abstractmethod
	def query(self, sql, *default_args, **kw):
		"""
		Create a new `.query` instance that provides an interface to the prepared statement.

		Given a single SQL statement, and optional default query arguments, create the
		prepared statement. The object returned is the interface to the
		prepared statement.

		The default arguments fill in the query's positional parameters.

		The ``title`` keyword argument is only used to help identify queries.
		The given value will be set to the query object's 'title' attribute.
		It is analogous to a function name.

		The ``prepare`` keyword argument tells the driver whether or not to actually
		prepare the query when it is instantiated. When `False`, defer preparation
		until execution or until it is explicitly ordered to prepare.

		>>> q = pg.query("SELECT 1")
		>>> p = q()
		>>> p.next()
		(1,)

		It allows default arguments to be configured:

		>>> q = pg.query("SELECT $1::int", 1)
		>>> q().next()
		(1,)

		And they are overrideable:

		>>> q(2).next()
		(2,)
		"""

	@abstractmethod
	def cquery(self, sql, *default_args, **kw):
		"""
		Exactly like `query`, but cache the created `.query` object using the
		given `sql` as the key. If the same `sql` is given again, look it up and
		return the existing `.query` object instead of creating a new one.
		"""

	@abstractmethod
	def statement(self, statement_id, *default_args, **kw):
		"""
		Create a `.query` object that was already prepared on the server. The distinction
		between this and a regular query is that it must be explicitly closed if it is no
		longer desired, and it is instantiated using the statement identifier as
		opposed to the SQL statement itself.

		If no ``title`` keyword is given, it will default to the statement_id.
		"""
	
	@abstractmethod
	def cursor(self, cursor_id):
		"""
		Create a `.cursor` object from the given `cursor_id` that was already declared
		on the server.
		
		`.cursor` object created this way must *not* be closed when the object is garbage
		collected. Rather, the user must explicitly close it for the server
		resources to be released. This is in contrast to `.cursor` object that
		are created by invoking a `.query` object.
		"""

	@abstractmethod
	def proc(self, proc_id):
		"""
		Create a reference to a stored procedure on the database. The given
		identifier can be either an Oid or a valid ``regprocedure`` string pointing at
		the desired function.

		The `proc_id` given can be either an ``Oid``, or a ``regprocedure`` identifier.

		>>> p = pg.proc('version()')
		>>> p()
		'PostgreSQL 8.3.0'

		>>> ~pg.query("select oid from pg_proc where proname = 'generate_series'")
		1069
		>>> p = pg.proc(1069)
		>>> list(p(1,5))
		[1, 2, 3, 4, 5]
		"""

	@abstractmethod
	def connect(self):
		"""
		Establish the connection to the server. Does nothing if the connection is
		already established.
		"""

	@abstractmethod
	def close(self):
		"""
		Close the connection. Does nothing if the connection is already closed.
		"""

	@abstractmethod
	def reconnect(self):
		"""
		Method drawing the effect of ``close`` then ``connect``.
		"""

	@abstractmethod
	def reset(self):
		"""
		Reset as much connection configuration as possible.

		Issues a ``RESET ALL`` to the database. If the database supports removing
		temporary tables created in the session, then remove them. Reapply
		initial configuration settings such as path. If inside a transaction
		block when called, reset the transaction state using the `reset`
		method on the connection's transaction manager, `xact`.

		The purpose behind this method is to provide a soft-reconnect method that
		reinitializes the connection into its original state. One obvious use of this
		would be in a connection pool where the connection is done being used.
		"""

	@abstractmethod
	def __nonzero__(self):
		"""
		Returns `True` if there are no known error conditions that would impede an
		action, otherwise `False`.

		If the connection is in a failed transaction block, this must be `False`.
		If the connection is closed, this must be `False`.

		>>> bool(con) in (True, False)
		True
		"""

	@abstractmethod
	def __enter__(self):
		"""
		Synonym to `connect` for with-statement support.
		"""

	@abstractmethod
	def __exit__(self, typ, obj, tb):
		"""
		Closes the connection and returns `True` when an exception is passed in,
		`False` when `None`.

		If the connection has any operations queued or running, abort them.
		"""

	@abstractmethod
	def __context__(self):
		"""
		Returns the connection object.
		"""

	@abstractmethod
	def execute(sql):
		"""
		Execute an arbitrary block of SQL. Always returns `None` and raises an
		exception on error.
		"""

	type = property(
		doc = """
		`type` is a property providing the name of the database type. 'PostgreSQL'
		would be the usual case. However, other "kinds" of Postgres exist in the
		wild. Greenplum for example.
		"""
	)

	version_info = property(
		doc = """
		A version tuple of the database software similar Python's `sys.version_info`.

		>>> pg.version_info
		(8, 1, 3, '', 0)
		"""
	)

	closed = property(
		doc = """
		A property that indicates whether the connection is open. If the connection
		is not open, then accessing closed will return True. If the connection is
		open, closed with return False.

		Additionally, setting it to `True` or `False` can open and close the
		connection. If the value set is not `True` or `False`, a `ValueError` must be
		raised.

		>>> pg.closed
		True
		"""
	)

	user = property(
		doc = """
		A property that provides an interface to "SELECT current_user", ``SET
		ROLE``, and ``RESET ROLE``. When the attribute is resolved, the current user will
		be given as a character string(unicode). When the attribute is set, it will
		issue a ``SET ROLE`` command to the server, changing the session's user. When
		the attribute is deleted, a ``RESET ROLE`` command will be issued to the
		server.
		"""
	)

	@abstractproperty
	def xact(self):
		"""
		A `xact` instance applicable to the connection that the attribute is bound
		to.
		"""

	@abstractproperty
	def settings(self):
		"""
		A `settings` instance applicable to the conneciton that the attribute is
		bound to.
		"""

class Cluster(metaclass = ABCMeta):
	"""
	Interface to a PostgreSQL cluster--a data directory. An implementation of
	this provides a means to control a server.
	"""
	@classmethod
	def create(cls,
		path : "where to create the cluster",
		initdb : "path to the initdb to use",
	):
		"""
		Create a cluster using the specified initdb at the given path and return a
		`Cluster` instance to that cluster.
		"""
		raise NotImplementedError("classmethod 'Cluster.create' not implemented")

	@abstractmethod
	def start(self):
		"""
		Start the cluster.
		"""

	@abstractmethod
	def stop(self):
		"""
		Signal the server to shutdown.
		"""

	@abstractmethod
	def restart(self):
		"""
		Restart the cluster; effectively stop() and start().
		"""

	@abstractmethod
	def kill(self):
		"""
		Kill the server.
		"""

	@abstractmethod
	def drop(self):
		"""
		Kill the server and completely remove the data directory.
		"""

	@abstractproperty
	def pid(self):
		"""
		The process id of the running daemon.

		This must be extracted from the run-info from the cluster directory.
		"""

	@abstractproperty
	def settings(self):
		"""
		A `Settings` interface to the postgresql.conf file associated with the
		cluster.
		"""

	def __enter__(self):
		if not self.running():
			self.start()

	def __context__(self):
		return self

	def __exit__(self, exc, val, tb):
		self.stop()
		return exc is None

if __name__ == '__main__':
	help('postgresql.api')

__docformat__ = 'reStructuredText'
##
# vim: ts=3:sw=3:noet:
