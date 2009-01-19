##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Application Programmer Interface specifications for PostgreSQL (ABCs).

PG-API
======

``postgresql.api`` is a Python API to the PostgreSQL RDBMS. It is designed to take
full advantage of PostgreSQL's features to provide the Python programmer with
substantial convenience.

This module is used to define the PG-API. It creates a set of ABCs
that makes up the basic interfaces used to work with a PostgreSQL.
"""
from abc import ABCMeta, abstractproperty, abstractmethod
import collections
from operator import attrgetter, methodcaller

class InterfaceElement(metaclass = ABCMeta):
	"""
	IFE - InterFace Element
	=======================

	The purpose of the IFE ABC is to provide a general mechanism for specifying
	the ancestry of a given object. Ancestry in this case is referring to the
	instances that ultimately lead to the creation of another instance; or more
	appropriately, the elements that ultimately lead to the creation another
	element. Elements tend to the high-level programmer interfaces to database
	elements. For instance, prepared statements, cursors, transactions,
	connections, etc.

	This ancestry is important for PG-API as it provides the foundation for
	collecting the information on the causes leading to an effect. Most notably,
	a database error. When raised, it provides you with an error message; but,
	this information gives you little clue as to what connection the exception
	came from. While, it is possible for a given user to find out using a
	debugger, it not possible to do so efficiently if fair amounts of
	information about exception's lineage is required--consider a query's
	execution where parameters ultimately caused the failure.

	To save the user time, IFEs ancestry allows `postgresql.exceptions` to
	include substantial information about an error. A printed exception has
	the general structure::
	
		<Python Traceback>
		postgresql.exceptions.Error: <message>

		[ Element Traceback ]

		DRIVER: postgresql.driver.pq3
		CONNECTOR: pq://user@localhost:5432/database
		CONNECTION: <connection_title> <backend_id> <socket information>
			<settings, transaction state, connection state>
		QUERY: <query_title> <statement_id> <parameter info>
			<query body>
		CURSOR: <cursor_id>
			<parameters>
		ERROR: <message>
	"""
	@abstractproperty
	def ife_ancestor(self):
		"""
		The ancestor element of this element.

		This can be strictly defined by the element as a read-only property, or
		the element can allow it to be set after instantiation.
		"""

	@abstractproperty
	def ife_label(self):
		"""
		ife_label is a string that identifies the kind of element.
		It should be used in messages to provide a more concise name for a
		particular piece of context.

		For instance, `PreparedStatement`'s ife_label is 'QUERY'.

		Usually, this is set directly on the ABC itself.
		"""

	@abstractmethod
	def __str__(self):
		"""
		Return a string describing the element.

		For instance, a `PreparedStatement` would likely return the query string,
		information about its parameters, and the statement identifier.

		The returned string should *not* be prefixed with `ife_label`.
		"""

	def ife_ancestry(self) -> "Sequence of IFE ancestors":
		"""
		Collect all the ancestor elements that led up to the existence of this
		element in a list and return it.

		Useful in cases where the lineage of a given element needs to be
		presented. (exceptions, warnings, etc)
		"""
		ancestors = []
		ife = self.ife_ancestor
		while ife is not None:
			if ife in ancestors or ife is self:
				raise TypeError("recursive element ancestry detected")
			if isinstance(ife, InterfaceElement):
				stack.append(ife)
			else:
				break
			ife = getattr(ife, 'ife_ancestor', None)
		return ancestors

	def ife_generations(self : "ancestor", ife : "descendent") -> (int, None):
		"""
		The number of ancestors between `self` and `ife` (the descendent).

		`None` if `ife` is not a descendent of `self`.
		"""
		ancestors = []
		while ife is not None and ife is not self:
			if ife in ancestors:
				raise TypeError("recursive element ancestry detected")
			if isinstance(ife, InterfaceElement):
				stack.append(ife)
			else:
				break
			ife = getattr(ife, 'ife_ancestor', None)
		return None if ife is None else len(ancestors)

	def ife_descend(self,
		*args : "`InterfaceElement`'s descending from `self`"
	) -> None:
		"""
		Set the `ife_ancestor` attribute on the arguments to `self`.

		That is, specify the `InterfaceElement`s in `args` directly descend
		from `self`.
		"""
		for x in args:
			x.ife_ancestor = self

class Message(InterfaceElement):
	"A message emitted by PostgreSQL"
	ife_label = 'MESSAGE'
	ife_ancestor = None
	code = "00000"
	message = None
	details = None

	def __init__(self,
		message : "The primary information of the message",
		code : "Message code to attach (SQL state)" = None,
		details : "additional information associated with the message" = {},
	):
		self.message = message
		self.details = details
		if code is not None and self.code != code:
			self.code = code

	def __repr__(self):
		return "{mod}.{typname}({message!r}{code}{details}{source})".format(
			mod = self.__module__,
			typname = self.__class__.__name__,
			message = self.message,
			code = (
				"" if self.code == type(self).code
				else ", code = " + repr(self.code)
			),
			details = (
				"" if not self.details
				else ", details = " + repr(self.details)
			),
			source = (
				"" if self.source is None
				else ", source = " + repr(self.source)
			)
		)

	def __str__(self):
		details = self.details
		loc = [
			details.get(k, '?') for k in ('file', 'line', 'function')
		]
		locstr = (
			"" if tuple(loc) == ('?', '?', '?')
			else "LOCATION: File {0!r}, line {1!s}, in {2!s}".format(*loc) + os.linesep
		)

		sev = details.get('severity')
		if sev:
			sevmsg = "SEVERITY: " + sev.upper() + os.linesep
		return self.message + os.linesep + sevmsg + \
			os.linesep.join((
				': '.join((k.upper(), v)) for k, v in sorted(details.items(), key = itemgetter(0))
				if k not in ('message', 'severity', 'file', 'function', 'line')
			)) + locstr

class Cursor(
	InterfaceElement,
	collections.Iterator,
	collections.Iterable,
):
	"""
	A `Cursor` object is an interface to a sequence of tuples(rows). A result set.
	Cursors publish a file-like interface for reading tuples from the database.

	`Cursor` objects are created by invoking `PreparedStatement` objects or by
	direct name-based instantation(`cursor` method on `Connection` objects).
	"""
	ife_label = 'CURSOR'

	@abstractmethod
	def read(self,
		quantity : "Number of rows to read" = None
	) -> "List of Rows":
		"""
		Read the specified number of rows and return them in a list.
		This advances the cursor's position.
		"""

	@abstractmethod
	def close(self) -> None:
		"""
		Close the cursor.
		"""

	@abstractmethod
	def __next__(self) -> "Row":
		"""
		Get the next tuple in the cursor. Advances the cursor position by one.
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
	def scroll(self,
		nrows : "Number of rows to scroll forward or backward; negative numbers dicate backward scolls"
	):
		"""
		Set the cursor's position relative to the current position.
		Negative numbers can be used to scroll backwards.

		This is a convenient interface to `seek` with a relative whence(``1``).
		"""

	@abstractmethod
	def __getitem__(self, idx):
		"""
		Get the rows at the given absolute position.

		This is only available on scrollable cursors, where seeking is possible.
		"""

class PreparedStatement(
	InterfaceElement,
	collections.Callable,
	collections.Iterable,
):
	"""
	Instances of `PreparedStatement` are returned by the `query` method of
	`Connection` instances.

	A PreparedStatement is an Iterable as well as Callable. This feature is
	supported for queries that have the default arguments filled in or take no
	arguments at all. It allows for things like:

		>>> for x in connection.query('select * FROM table'):
		...  pass
	"""
	ife_label = 'QUERY'

	@abstractmethod
	def __call__(self, *args) -> Cursor:
		"""
		Execute the prepared statement with the given arguments as parameters. If
		the query returns rows, a `Cursor` object should be returned, otherwise a
		`ResultHandle` object.

		Usage:

		>>> q=pg_con.query("SELECT column FROM ttable WHERE key = $1")
		>>> q('identifier')
		<cursor object>
		"""

	@abstractmethod
	def first(self, *args) -> "'First' object that is yield by the query":
		"""
		Execute the prepared statement with the given arguments as parameters. If
		the query returns rows with multiple columns, return the first row. If the
		query returns rows with a single column, return the first column in the
		first row. If the query does not return rows at all, return the count or
		`None` if no count exists in the completion message. Usage:

		>>> pg_con.query("SELECT * FROM ttable WHERE key = $1").first("somekey")
		('somekey', 'somevalue')
		>>> pg_con.query("SELECT 'foo'").first()
		'foo'
		>>> pg_con.query("INSERT INTO atable (col) VALUES (1)").first()
		1
		"""

	@abstractmethod
	def load(self,
		iterable : "A iterable of sequences to execute the statement with"
	):
		"""
		Given an iterable, `iterable`, feed the produced parameters to the query.
		This is a bulk-loading interface for parameterized queries.

		Effectively, it is equivalent to:
		
			>>> q = pg_con.query(sql)
			>>> for i in iterable:
			...  q(*i)

		Its purpose is to allow the implementation to take advantage of the
		knowledge that a series of parameters are to be loaded and subsequently
		optimize the operation.
		"""

	@abstractmethod
	def declare(self,
		*args : "The arguments--positional parameters--to bind to the cursor.",
		hold : "Whether or not to state 'WITH HOLD' in the DECLARE statement" = True,
		scroll : "Whether or not to state 'WITH SCROLL' in the DECLARE statement" = False,
		cursor_id : "If none, generate the cursor_id, otherwise use the given string" = None
	) -> Cursor:
		"""
		Declare a cursor for the prepared statement and return the cursor object.
		This differs from `__call__` as it allows the 
		"""

	@abstractmethod
	def close(self) -> None:
		"""
		Close the prepraed statement releasing resources associated with it.
		"""

	@abstractmethod
	def prepare(self) -> None:
		"""
		Prepare the already instantiated query for use. This method would only be
		used if the query were closed at some point.
		"""

class StoredProcedure(
	InterfaceElement,
	collections.Callable,
):
	"""
	A function stored on the database.
	"""
	ife_label = 'FUNCTION'

	@abstractmethod
	def __call__(self, *args, **kw) -> (object, Cursor, collections.Iterable):
		"""
		Execute the procedure with the given arguments. If keyword arguments are
		passed they must be mapped to the argument whose name matches the key. If
		any positional arguments are given, they must fill in gaps created by
		the stated keyword arguments. If too few or too many arguments are given,
		a TypeError must be raised. If a keyword argument is passed where the
		procedure does not have a corresponding argument name, then, likewise, a
		TypeError must be raised.

		In the case where the `StoredProcedure` references a set returning
		function(SRF), the result *must* be an iterable. SRFs that return single
		columns *must* return an iterable of that column; not row data. If the SRF
		returns a composite(OUT parameters), it *should* return a `Cursor`.
		"""

class TransactionManager(
	InterfaceElement
):
	"""
	A `TranactionManager` is the `Connection`'s transaction manager. It provides
	methods for managing the transaction state.

	Normal usage would entail the use of the with-statement::

		with pg_con.xact:
		...
	
	Or, in cases where two-phase commit is desired::

		with pg_con.xact('gid'):
		...
	"""
	ife_label = 'XACT'
	ife_ancestor = property(attrgetter('connection'))

	@abstractproperty
	def connection(self):
		"""
		The connection whose transaction state is managed by the
		`TransactionManager` instance.
		"""

	@abstractproperty
	def failed(self) -> bool:
		"""
		bool stating if the current transaction has failed due to an error.
		`None` if not in a transaction block.
		"""

	@abstractproperty
	def level(self) -> int:
		"""
		`int` stating the current transaction depth.

		The level starts at zero, indicating no transactions have been started.
		For each call to `start`, this is incremented by one.
		For each call to `abort` or `commit`, this is decremented by one.

		Implementation must protect against negative levels.
		"""

	@abstractmethod
	def start(self) -> None:
		"""
		Start a transaction block. If a transaction block has already been
		started, make a savepoint.
		``start``, ``begin``, and ``__enter__`` are synonyms.
		"""
	__enter__ = begin = start

	def __context__(self):
		return self

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
	def commit(self) -> None:
		"""
		Commit the transaction block, release a savepoint, or prepare the
		transaction for commit. If the number of running transactions is greater
		than one, then the corresponding savepoint is released. If no savepoints
		are set and the transaction is configured with a 'gid', then the
		transaction is prepared instead of committed, otherwise the transaction is
		simply committed.
		"""

	@abstractmethod
	def rollback(self) -> None:
		"""
		Abort the current transaction or rollback to the last started savepoint.
		`rollback` and `abort` are synonyms.
		"""
	abort = rollback

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

			>>> with pg_con.xact(readonly = True):
			...

		Read committed isolation::

			>>> with pg_con.xact(isolation = 'READ COMMITTED'):
			...

		Database configured defaults apply to all `TransactionManager` operations.
		"""

	@abstractmethod
	def commit_prepared(self, gid : str):
		"""
		Commit the prepared transaction with the given `gid`.
		"""

	@abstractmethod
	def rollback_prepared(self, *gids : str):
		"""
		Rollback the prepared transactions with the given `gid`.
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
	InterfaceElement,
	collections.MutableMapping
):
	"""
	A mapping interface to the session's settings. This provides a direct interface
	to ``SHOW`` or ``SET`` commands. Identifiers and values need not be quoted
	specially as the implementation must do that work for the user.
	"""
	ife_label = 'SETTINGS'

	def getpath(self) -> "Sequence of schema names that make up the search_path":
		"""
		Returns a sequence of the schemas that make up the current search_path.
		"""
	def setpath(self, seq : "Sequence of schema names"):
		"""
		Set the "search_path" setting to the given a sequence of schema names, 
		[Implementations must properly escape and join the strings]
		"""
	path = abstractproperty(
		getpath, setpath,
		doc = """
		An interface to a structured ``search_path`` setting:

		>>> pg_con.settings.path
		['public', '$user']

		It may also be used to set the path:

		>>> pg_con.settings.path = ('public', 'tools')
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

		>>> with pg_con.settings(search_path = 'local,public'):
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
		>>> pg_con.settings.subscribe('TimeZone', watch)
		"""

	@abstractmethod
	def unsubscribe(self, key, callback):
		"""
		Stop listening for changes to a setting. The setting name(`key`), and the
		callback used to subscribe must be given again for successful termination
		of the subscription.

		>>> pg_con.settings.unsubscribe('TimeZone', watch)
		"""

class Connection(
	InterfaceElement
):
	"""
	The connection interface.
	"""
	ife_label = 'CONNECTION'
	ife_ancestor = property(attrgetter('connector'))

	@abstractproperty
	def backend_id(self):
		"""
		The backend's process identifier.
		"""

	@abstractmethod
	def query(self,
		sql : "The query text.",
		title : "The query's name, used in tracebacks when available" = None,
	) -> PreparedStatement:
		"""
		Create a new `PreparedStatement` instance bound to the connection with the
		given SQL.

		The ``title`` keyword argument is only used to help identify queries.
		The given value *must* be set to the PreparedStatement's 'title' attribute.
		It is analogous to a function name.

		>>> q = pg_con.query("SELECT 1")
		>>> p = q()
		>>> p.next()
		(1,)

		It allows default arguments to be configured:

		>>> q = pg_con.query("SELECT $1::int", 1)
		>>> q().next()
		(1,)

		And they are overrideable:

		>>> q(2).next()
		(2,)
		"""

	@abstractmethod
	def cquery(self,
		sql : "The query text.",
		title : "The query's name, used in tracebacks when available" = None,
	) -> PreparedStatement:
		"""
		Exactly like `query`, but cache the created `PreparedStatement` using the
		given `sql` as the key. If the same `sql` is given again, look it up and
		return the existing `PreparedStatement` instead of creating a new one.

		This method is provided to the user for convenience.
		"""

	@abstractmethod
	def statement(self,
		statement_id : "The statement's identification string.",
		*default_args : "The default positional parameters to pass to the statement.",
		title : "The query's name, used in tracebacks when available" = None
	) -> PreparedStatement:
		"""
		Create a `PreparedStatement` object that was already prepared on the server.
		The distinction between this and a regular query is that it must be
		explicitly closed if it is no longer desired, and it is instantiated using
		the statement identifier as opposed to the SQL statement itself.

		If no ``title`` keyword is given, it will default to the statement_id.
		"""

	@abstractmethod
	def cursor(self,
		cursor_id : "The cursor's identification string."
	) -> Cursor:
		"""
		Create a `Cursor` object from the given `cursor_id` that was already declared
		on the server.
		
		`Cursor` objects created this way must *not* be closed when the object is garbage
		collected. Rather, the user must explicitly close it for the server
		resources to be released. This is in contrast to `Cursor` objects that
		are created by invoking a `PreparedStatement` or a SRF `StoredProcedure`.
		"""

	@abstractmethod
	def proc(self,
		proc_id : "The procedure identifier; a valid ``regprocedure`` or Oid."
	) -> StoredProcedure:
		"""
		Create a `StoredProcedure` instance using the given identifier.

		The `proc_id` given can be either an ``Oid``, or a ``regprocedure`` that
		identifies the stored procedure to create the interface for.

		>>> p = pg_con.proc('version()')
		>>> p()
		'PostgreSQL 8.3.0'

		>>> pg_con.query("select oid from pg_proc where proname = 'generate_series'").first()
		1069
		>>> generate_series = pg_con.proc(1069)
		>>> list(generate_series(1,5))
		[1, 2, 3, 4, 5]
		"""

	@abstractmethod
	def connect(self) -> None:
		"""
		Establish the connection to the server.

		Does nothing if the connection is already established.
		"""

	@abstractmethod
	def close(self) -> None:
		"""
		Close the connection.

		Does nothing if the connection is already closed.
		"""

	@abstractmethod
	def reconnect(self) -> None:
		"""
		Method drawing the effect of ``close`` then ``connect``.
		"""

	@abstractmethod
	def reset(self) -> None:
		"""
		Reset the connection into it's original state.

		Issues a ``RESET ALL`` to the database. If the database supports removing
		temporary tables created in the session, then remove them. Reapply
		initial configuration settings such as path. If inside a transaction
		block when called, reset the transaction state using the `reset`
		method on the connection's transaction manager, `xact`.

		The purpose behind this method is to provide a soft-reconnect method that
		re-initializes the connection into its original state. One obvious use of this
		would be in a connection pool where the connection is done being used.
		"""

	__enter__ = methodcaller('connect')
	def __exit__(self, typ, obj, tb):
		"""
		Closes the connection and returns `True` when an exception is passed in,
		`False` when `None`.

		If the connection has any operations queued or running, abort them.
		"""
		self.close()
		return typ is not None

	def __context__(self):
		"""
		Returns the connection object.
		"""
		return self

	@abstractmethod
	def execute(sql) -> None:
		"""
		Execute an arbitrary block of SQL. Always returns `None` and raises an
		exception on error.
		"""

	@abstractproperty
	def version_info(self):
		"""
		A version tuple of the database software similar Python's `sys.version_info`.

		>>> pg_con.version_info
		(8, 1, 3, '', 0)
		"""

	@abstractproperty
	def closed(self) -> bool:
		"""
		`True` if the `Connection` is closed, `False` if the `Connection` is open.

		>>> pg_con.closed
		True
		"""

	user = property(
		doc = """
		A property that provides an interface to "SELECT current_user", ``SET
		ROLE``, and ``RESET ROLE``. When the attribute is resolved, the current user will
		be given as a character string. When the attribute is set, it will issue a
		``SET ROLE`` command to the server, changing the session's user. When
		the attribute is deleted, a ``RESET ROLE`` command will be issued to the
		server.
		"""
	)

	@abstractproperty
	def xact(self) -> TransactionManager:
		"""
		A `TransactionManager` instance bound to the `Connection`.
		"""

	@abstractproperty
	def settings(self) -> Settings:
		"""
		A `Settings` instance bound to the `Connection`.
		"""

class Connector(
	InterfaceElement
):
	ife_label = 'CONNECTOR'

	@abstractproperty
	def driver(self):
		"""
		The driver implementation that created the `Connector` class.
		"""

	@abstractproperty
	def Connection(self):
		"""
		The `Connection` class to instantiate when creating a connection.
		"""

	def create(self, *args, **kw):
		"""
		Instantiate and return the connection class.
		"""
		return self.Connection(self, *args, **kw)

	def __call__(self, *args, **kw):
		"""
		Create and connect. Arguments will be given to the `Connection` instance's
		``connect`` method.
		"""
		c = self.Connection(self)
		c.connect(*args, **kw)
		return c

class Driver(InterfaceElement):
	"""
	The `Driver` element provides the `Connector` and other information
	pertaining to the implementation of the driver. Information about what the
	driver supports is available in instances.
	"""
	ife_label = "DRIVER"
	ife_ancestor = None

	@abstractproperty
	def Connector(self):
		"""
		The `Connector` implementation for the driver.
		"""

	def connect(**kw):
		"""
		Create a connection using the given parameters for the Connector.

		This caches the `Connector` instance for re-use when the same parameters
		are given again.
		"""
		id = set(kw.items())
		cr = self._connectors.get(id)
		if cr is None:
			cr = self.Connector(**kw)
			c = cr()
			self._connectors[id] = cr
		return c

	def __init__(self):
		self._connectors = {}

class Cluster(InterfaceElement):
	"""
	Interface to a PostgreSQL cluster--a data directory. An implementation of
	this provides a means to control a server.
	"""
	ife_label = 'CLUSTER'
	ife_ancestor = None

	@abstractmethod
	def init(self,
		initdb : "path to the initdb to use" = None,
		superusername : "name of the cluster's superuser" = None,
		superuserpass : "superuser's password" = None,
		encoding : "the encoding to use for the cluster" = None,
		locale : "the locale to use for the cluster" = None,
		collate : "the collation to use for the cluster" = None,
		ctype : "the ctype to use for the cluster" = None,
		monetary : "the monetary to use for the cluster" = None,
		numeric : "the numeric to use for the cluster" = None,
		time : "the time to use for the cluster" = None,
		text_search_config : "default text search configuration" = None,
		xlogdir : "location for the transaction log directory" = None,
	):
		"""
		Create the cluster at the `data_directory` associated with the Cluster
		instance.
		"""

	@abstractmethod
	def drop(self):
		"""
		Kill the server and completely remove the data directory.
		"""

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
	def kill(self):
		"""
		Kill the server.
		"""

	@abstractmethod
	def restart(self):
		"""
		Restart the cluster.
		"""

	@abstractproperty
	def settings(self):
		"""
		A `Settings` interface to the ``postgresql.conf`` file associated with the
		cluster.
		"""

	@abstractmethod
	def wait_until_started(self,
		timeout : "maximum time to wait" = 10
	):
		"""
		After the start() method is ran, the database may not be ready for use.
		This method provides a mechanism to block until the cluster is ready for
		use.

		If the `timeout` is reached, the method *must* throw a
		`postgresql.exceptions.ClusterTimeoutError`.
		"""

	@abstractmethod
	def wait_until_stopped(self,
		timeout : "maximum time to wait" = 10
	):
		"""
		After the stop() method is ran, the database may still be running.
		This method provides a mechanism to block until the cluster is completely
		shutdown.

		If the `timeout` is reached, the method *must* throw a
		`postgresql.exceptions.ClusterTimeoutError`.
		"""

	def __enter__(self):
		if not self.running():
			self.start()
		self.wait

	def __context__(self):
		return self

	def __exit__(self, exc, val, tb):
		self.stop()
		return exc is None

__docformat__ = 'reStructuredText'
if __name__ == '__main__':
	help('postgresql.api')
##
# vim: ts=3:sw=3:noet:
