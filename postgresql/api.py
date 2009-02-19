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

The `InterfaceElement` is the common ABC, the methods and attributes defined
within that class can, should, be mostly ignored while extracting information.
"""
import os
import warnings
import collections
from abc import ABCMeta, abstractproperty, abstractmethod
from operator import methodcaller, itemgetter

from .python.doc import Doc
from .python.decorlib import propertydoc

class Receptor(collections.Callable):
	"""
	A receptor is a type of callable used by `InterfaceElement`'s `ife_emit`
	method.
	"""

	@abstractmethod
	def __call__(self,
		source_ife : "The element whose `ife_emit` method was called.",
		receiving_ife : "The element that included the `Receptor` was placed on.",
		obj : "The object that was given to `ife_emit`"
	) -> bool:
		"""
		This is the type signature of receptor capable functions.

		If the receptor returns `True`, further propagation will be halted if the
		`allow_consumption` parameter given to `ife_emit` is `True`(default).
		"""

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
		DRIVER: postgresql.driver.pq3
		CONNECTOR: pq://user@localhost:5432/database
		CONNECTION: <connection_title> <backend_id> <socket information>
			<settings, transaction state, connection state>
		QUERY: <query_title> <statement_id> <parameter info>
			<query body>
		CURSOR: <cursor_id>
			<parameters>
	

	Receptors
	---------

	Reception is a faculty created to support PostgreSQL message and warning
	propagation in a context specific way. For instance, the NOTICE emitted by
	PostgreSQL when creating a table with a PRIMARY KEY might unnecessary in an
	program automating the creation of tables as it's expected. So, providing a
	filter for these messages is useful to reducing noise.


	WARNING
	-------

	Many of these APIs are used to support features that users are *not*
	expected to use. Almost everything on `InterfaceElement` is subject to
	deprecation.
	"""
	ife_object_title = "<untitled>"

	@propertydoc
	@abstractproperty
	def ife_ancestor(self):
		"""
		The element that created this element.

		Uses:

			. Propagate emitted messages(objects) to source objects.
			. State acquisition on error for lineage reporting.
		"""

	@propertydoc
	@abstractproperty
	def ife_label(self) -> str:
		"""
		ife_label is a string that identifies the kind of element.
		It should be used in messages to provide a more concise name for a
		particular piece of context.

		For instance, `PreparedStatement`'s ife_label is 'QUERY'.

		Usually, this is set directly on the ABC itself.
		"""

	@abstractmethod
	def ife_snapshot_text(self) -> str:
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
				ancestors.append(ife)
			else:
				break
			ife = getattr(ife, 'ife_ancestor', None)
		return ancestors

	def ife_ancestry_snapshot_text(self) -> [(object, str, str)]:
		"""
		Return a snapshot of this `InterfaceElement`'s ancestry.

		Returns a sequence of tuples consisting of the `InterfaceElement`s in this
		element's ancestry their associated `ife_label`, and the result of their
		`ife_snapshot_text` method::
			[
				(`InterfaceElement`,
				 `InterfaceElement`.`ife_label`,
				 `InterfaceElement`.`ife_snapshot_text`()),
				...
			]

		This gives a full snapshot while allowing for later filtering.
		"""
		a = self.ife_ancestry()
		l = [
			(
				x, getattr(x, 'ife_label', type(x).__name__),
				(x.ife_snapshot_text() if hasattr(x, 'ife_snapshot_text') else str(x))
			) for x in a
			if getattr(x, '_ife_exclude_snapshot', False) is not True
		]
		return l

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

	def ife_emit(self,
		obj : "object to emit",
		allow_consumption : "whether or not receptors are allowed to stop further propagation" = True,
	) -> (False, (collections.Callable)):
		"""
		Send an arbitrary object through the ancestry.

		This is used in situations where the effects of an element result in an
		object that is not returned by the element's interaction(method call,
		property get, etc).

		To handle these additional results, the object is passed up through the
		ancestry. Any ancestor that has receptors will see the object.

		If `obj` was consumed by a receptor, the receptor that consumed it will be
		returned.
		"""
		# Don't include ancestors without receptors.
		a = [
			x for x in self.ife_ancestry()
			if getattr(x, '_ife_receptors', None) is not None
		]
		if getattr(self, '_ife_receptors', None) is not None:
			a.insert(0, self)

		for ife in a:
			for recep in ife._ife_receptors:
				# (emit source element, reception element, object)
				r = recep(self, ife, obj)
				if r is True and allow_consumption:
					# receptor indicated halt
					return (recep, ife)
		# if went unstopped
		return False

	def ife_connect(self,
		*args : (Receptor,)
	) -> None:
		"""
		Add the `Receptor`s to the element. "Connecting" a receptor allows it to
		receive objects "emitted" by descendent elements.

		Whenever an object is given to `ife_emit`, the given `Receptor`s will be
		called with the `obj`.

		NOTE: The given objects do *not* have to instances of `Receptor`, rather,
		they must merely support the call's type signature.
		"""
		if not hasattr(self, '_ife_receptors'):
			self._ife_receptors = list(args)
			return
		# Prepend the list. Newer receptors are given priority.
		new = list(args)
		new.extend(self._ife_receptors)
		self._ife_receptors = new

	def ife_sever(self,
		*args : (Receptor,)
	) -> None:
		"""
		Remove the `Receptor`s from the element.
		"""
		if hasattr(self, '_ife_receptors'):
			for x in args:
				if x in self._ife_receptors:
					self._ife_receptors.remove(x)
			if not self._ife_receptors:
				del self._ife_receptors

class Message(InterfaceElement):
	"A message emitted by PostgreSQL"
	ife_label = 'MESSAGE'
	ife_ancestor = None

	severities = (
		'DEBUG',
		'INFO',
		'NOTICE',
		'WARNING',
		'ERROR',
		'FATAL',
		'PANIC',
	)
	sources = (
		'SERVER',
		'DRIVER',
	)

	# What generated the message?
	source = 'SERVER'

	code = "00000"
	message = None
	details = None

	def __init__(self,
		message : "The primary information of the message",
		code : "Message code to attach (SQL state)" = None,
		details : "additional information associated with the message" = {},
		source : "What generated the message(SERVER, DRIVER)" = None,
	):
		self.message = message
		self.details = details
		if code is not None and self.code != code:
			self.code = code
		if source is not None and self.source != source:
			self.source = source

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
		ss = getattr(self, 'snapshot', None)
		if ss is None:
			ss = obj.ife_ancestry_snapshot_text()
		sev = self.details.get('severity', self.ife_label).upper()
		detailstr = self.details_string
		if detailstr:
			detailstr = os.linesep + detailstr
		locstr = self.location_string
		if locstr:
			locstr = os.linesep + locstr

		code = "" if not self.code or self.code == "00000" else '(' + self.code + ')'
		return sev + code + ': ' + self.message + locstr + detailstr + \
			os.linesep + \
			os.linesep.join([': '.join(x[1:]) for x in ss]) + \
			os.linesep

	@property
	def location_string(self):
		details = self.details
		loc = [
			details.get(k, '?') for k in ('file', 'line', 'function')
		]
		return (
			"" if loc == ['?', '?', '?']
			else "LOCATION: File {0!r}, "\
			"line {1!s}, in {2!s}".format(*loc)
		)

	@property
	def details_string(self):
		return os.linesep.join((
			': '.join((k.upper(), str(v)))
			for k, v in sorted(self.details.items(), key = itemgetter(0))
			if k not in ('message', 'severity', 'file', 'function', 'line')
		))

	def ife_snapshot_text(self):
		details = self.details
		code = (os.linesep + "CODE: " + self.code) if self.code else ""
		sev = details.get('severity')
		sevmsg = ""
		if sev:
			sevmsg = os.linesep + "SEVERITY: " + sev.upper()
		detailstr = self.details_string
		if detailstr:
			detailstr = os.linesep + detailstr
		locstr = self.location_string
		if locstr:
			locstr = os.linesep + locstr
		return self.message + code + sevmsg + detailstr + locstr

	def emit(self):
		self.snapshot = self.ife_ancestry_snapshot_text()
		self.ife_emit(self)

class Cursor(
	InterfaceElement,
	collections.Iterator,
	collections.Iterable,
):
	"""
	A `Cursor` object is an interface to a sequence of tuples(rows). A result
	set. Cursors publish a file-like interface for reading tuples from the
	database.

	`Cursor` objects are created by invoking `PreparedStatement` objects or by
	direct name-based instantation(`cursor` method on `Connection` objects).
	"""
	ife_label = 'CURSOR'
	ife_ancestor = None
	_seek_whence_map = {
		0 : 'ABSOLUTE',
		1 : 'RELATIVE',
		2 : 'LAST',
	}

	@propertydoc
	@abstractproperty
	def cursor_id(self) -> str:
		"""
		The cursor's identifier.
		"""

	@propertydoc
	@abstractproperty
	def parameters(self) -> (tuple, None):
		"""
		The parameters bound to the cursor. `None`, if unknown.
		"""

	@propertydoc
	@abstractproperty
	def statement(self) -> ("PreparedStatement", None):
		"""
		The query object used to create the cursor. `None`, if unknown.
		"""

	@propertydoc
	@abstractproperty
	def insensitive(self) -> bool:
		"""
		Whether or not the cursor is insensitive. Extant versions of PostgreSQL
		only support insensitive cursors.
		"""

	@propertydoc
	@abstractproperty
	def with_scroll(self) -> bool:
		"""
		Whether or not the cursor is scrollable.
		"""

	@propertydoc
	@abstractproperty
	def with_hold(self) -> bool:
		"""
		Whether or not the cursor will persist across transactions.
		"""

	@abstractmethod
	def read(self,
		quantity : "Number of rows to read" = None
	) -> [()]:
		"""
		Read the specified number of rows and return them in a list.
		This alters the cursor's position.

		If the quantity is a negative value, read backwards.
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
	def seek(self, offset, whence = 'ABSOLUTE'):
		"""
		Set the cursor's position to the given offset with respect to the
		whence parameter.

		Whence values:

		 ``0`` or ``"ABSOLUTE"``
		  Absolute.
		 ``1`` or ``"RELATIVE"``
		  Relative.
		 ``2`` or ``"FROM_END"``
		  Absolute from end.
		"""

class PreparedStatement(
	InterfaceElement,
	collections.Callable,
	collections.Iterable,
):
	"""
	Instances of `PreparedStatement` are returned by the `prepare` method of
	`Database` instances.

	A PreparedStatement is an Iterable as well as Callable. This feature is
	supported for queries that have the default arguments filled in or take no
	arguments at all. It allows for things like:

		>>> for x in db.prepare('select * FROM table'):
		...  pass
	"""
	ife_label = 'STATEMENT'

	@propertydoc
	@abstractproperty
	def statement_id(self) -> str:
		"""
		The statment's identifier.
		"""

	@propertydoc
	@abstractproperty
	def string(self) -> str:
		"""
		The SQL string of the prepared statement.

		`None` if not available. This can happen in cases where a statement is
		prepared on the server and a reference to the statement is sent to the
		client which subsequently uses the statement via the `Database`'s
		`statement` constructor.
		"""

	@abstractmethod
	def __call__(self, *args,
		with_hold : \
			"Whether or not to request 'WITH HOLD'" = True,
		with_scroll : \
			"Whether or not to request 'SCROLL'" = False,
		cursor_id : \
			"If `None`, generate the cursor_id, " \
			"otherwise use the given string" = None
	) -> Cursor:
		"""
		Execute the prepared statement with the given arguments as parameters.

		Usage:

		>>> p=db.prepare("SELECT column FROM ttable WHERE key = $1")
		>>> p('identifier')
		<`Cursor` instance>
		"""

	@abstractmethod
	def first(self, *args) -> "'First' object that is yield by the query":
		"""
		Execute the prepared statement with the given arguments as parameters.
		If the statement returns rows with multiple columns, return the first
		row. If the statement returns rows with a single column, return the
		first column in the first row. If the query does not return rows at all,
		return the count or `None` if no count exists in the completion message.

		Usage:

		>>> db.prepare("SELECT * FROM ttable WHERE key = $1").first("somekey")
		('somekey', 'somevalue')
		>>> db.prepare("SELECT 'foo'").first()
		'foo'
		>>> db.prepare("INSERT INTO atable (col) VALUES (1)").first()
		1
		"""

	@abstractmethod
	def load(self,
		iterable : "A iterable of tuples to execute the statement with"
	):
		"""
		Given an iterable, `iterable`, feed the produced parameters to the
		query. This is a bulk-loading interface for parameterized queries.

		Effectively, it is equivalent to:
		
			>>> q = db.prepare(sql)
			>>> for i in iterable:
			...  q(*i)

		Its purpose is to allow the implementation to take advantage of the
		knowledge that a series of parameters are to be loaded and subsequently
		optimize the operation.
		"""

	@abstractmethod
	def close(self) -> None:
		"""
		Close the prepraed statement releasing resources associated with it.
		"""

	@abstractmethod
	def prepare(self) -> None:
		"""
		Prepare the statement for use.

		If the query has already been prepared, not self.closed,
		the implementation *must* prepare it again.

		This can be useful for forcing the update of a plan.
		"""

class StoredProcedure(
	InterfaceElement,
	collections.Callable,
):
	"""
	A function stored on the database.
	"""
	ife_label = 'FUNCTION'
	ife_ancestor = None

	@abstractmethod
	def __call__(self, *args, **kw) -> (object, Cursor, collections.Iterable):
		"""
		Execute the procedure with the given arguments. If keyword arguments are
		passed they must be mapped to the argument whose name matches the key.
		If any positional arguments are given, they must fill in gaps created by
		the stated keyword arguments. If too few or too many arguments are
		given, a TypeError must be raised. If a keyword argument is passed where
		the procedure does not have a corresponding argument name, then,
		likewise, a TypeError must be raised.

		In the case where the `StoredProcedure` references a set returning
		function(SRF), the result *must* be an iterable. SRFs that return single
		columns *must* return an iterable of that column; not row data. If the
		SRF returns a composite(OUT parameters), it *should* return a `Cursor`.
		"""

class TransactionManager(
	InterfaceElement
):
	"""
	A `TranactionManager` is the `Connection`'s transaction manager.
	`TransactionManager` compliant instances *must* exist on a `Connection`
	instance's `xact` attribute.

	Normal usage would entail the use of the with-statement::

		with db.xact:
		...
	
	Or, in cases where two-phase commit is desired::

		with db.xact('gid'):
		...
	"""
	ife_label = 'XACT'

	@propertydoc
	@abstractproperty
	def failed(self) -> (bool, None):
		"""
		bool stating if the current transaction has failed due to an error.
		`None` if not in a transaction block.
		"""

	@propertydoc
	@abstractproperty
	def depth(self) -> int:
		"""
		`int` stating the current transaction depth.

		The depth starts at zero, indicating no transactions have been started.
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
		Commit the transaction, or abort if the given exception is not `None`.
		If the transaction level is greater than one, then the savepoint
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
		transaction is prepared instead of committed, otherwise the transaction 
		is simply committed.
		"""

	@abstractmethod
	def rollback(self) -> None:
		"""
		Abort the current transaction or rollback to the last started savepoint.
		`rollback` and `abort` are synonyms.
		"""
	abort = rollback

	@abstractmethod
	def __call__(self, gid = None, isolation = None, read_only = None):
		"""
		Initialize the transaction using parameters and return self to support a
		convenient with-statement syntax.

		The configuration only applies to transaction blocks as savepoints have 
		no parameters to be configured.

		If the `gid`, the first keyword parameter, is configured, the
		transaction manager will issue a ``PREPARE TRANSACTION`` with the
		specified identifier instead of a ``COMMIT``.

		If `isolation` is specified, the ``START TRANSACTION`` will include it
		as the ``ISOLATION LEVEL``. This must be a character string.

		If the `read_only` parameter is specified, the transaction block will be
		started in the ``READ ONLY`` mode if True, and ``READ WRITE`` mode if
		False.  If `None`, neither ``READ ONLY`` or ``READ WRITE`` will be
		specified.

		Read-only transaction::

			>>> with db.xact(read_only = True):
			...

		Read committed isolation::

			>>> with pg_con.xact(isolation = 'READ COMMITTED'):
			...

		Database configured defaults apply to all `TransactionManager`
		operations.
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

	@propertydoc
	@abstractproperty
	def prepared(self) -> "sequence of prepared transaction identifiers":
		"""
		A sequence of available prepared transactions for the current user on
		the current database. This is intended to be more relavent for the
		current context than selecting the contents of ``pg_prepared_xacts``.
		So, the view *must* be limited to those of the current database, and
		those which the user can commit.
		"""

class Settings(
	InterfaceElement,
	collections.MutableMapping
):
	"""
	A mapping interface to the session's settings. This provides a direct
	interface to ``SHOW`` or ``SET`` commands. Identifiers and values need
	not be quoted specially as the implementation must do that work for the
	user.
	"""
	ife_label = 'SETTINGS'

	def getpath(self) -> [str]:
		"""
		Returns a sequence of the schemas that make up the current search_path.
		"""
	def setpath(self, seq : [str]):
		"""
		Set the "search_path" setting to the given a sequence of schema names, 
		[Implementations must properly escape and join the strings]
		"""
	path = propertydoc(abstractproperty(
		getpath, setpath,
		doc = """
		An interface to a structured ``search_path`` setting:

		>>> db.settings.path
		['public', '$user']

		It may also be used to set the path:

		>>> db.settings.path = ('public', 'tools')
		"""
	))
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

		>>> with db.settings(search_path = 'local,public'):
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
		Get the setting with the corresponding key. If the setting does not
		exist, return the `default`.
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
		>>> db.settings.subscribe('TimeZone', watch)
		"""

	@abstractmethod
	def unsubscribe(self, key, callback):
		"""
		Stop listening for changes to a setting. The setting name(`key`), and
		the callback used to subscribe must be given again for successful
		termination of the subscription.

		>>> db.settings.unsubscribe('TimeZone', watch)
		"""

class Database(InterfaceElement):
	"""
	The interface to an individual database. `Connection` objects inherit from
	this
	"""
	ife_label = 'DATABASE'

	@propertydoc
	@abstractproperty
	def backend_id(self) -> (int, None):
		"""
		The backend's process identifier.
		"""

	@propertydoc
	@abstractproperty
	def version_info(self) -> tuple:
		"""
		A version tuple of the database software similar Python's `sys.version_info`.

		>>> db.version_info
		(8, 1, 3, '', 0)
		"""

	@propertydoc
	@abstractproperty
	def client_address(self) -> (str, None):
		"""
		The client address that the server sees. This is obtainable by querying
		the ``pg_catalog.pg_stat_activity`` relation.

		`None` if unavailable.
		"""

	@propertydoc
	@abstractproperty
	def client_port(self) -> (int, None):
		"""
		The client port that the server sees. This is obtainable by querying
		the ``pg_catalog.pg_stat_activity`` relation.

		`None` if unavailable.
		"""

	user = propertydoc(abstractproperty(
		fget = Doc(
			"Give the string returned by ``SELECT current_user``",
			**{'return':str}
		),
		fset = Doc("Pass the given string as a parameter to ``SET ROLE``"),
		fdel = Doc("Issue ``RESET ROLE`` to the server"),
	))

	@propertydoc
	@abstractproperty
	def xact(self) -> TransactionManager:
		"""
		A `TransactionManager` instance bound to the `Database`.
		"""

	@propertydoc
	@abstractproperty
	def settings(self) -> Settings:
		"""
		A `Settings` instance bound to the `Database`.
		"""

	@abstractmethod
	def execute(sql) -> None:
		"""
		Execute an arbitrary block of SQL. Always returns `None` and raise
		a `postgresql.exceptions.Error` subclass on error.
		"""

	@abstractmethod
	def prepare(self,
		sql : str, title : str = None, statement_id : str = None,
	) -> PreparedStatement:
		"""
		Create a new `PreparedStatement` instance bound to the connection
		using the given SQL.

		The ``title`` keyword argument is only used to help identify queries.
		The given value *must* be set to the PreparedStatement's
		'ife_object_title' attribute.
		It is analogous to a function name.

		>>> s = db.prepare("SELECT 1")
		>>> c = s()
		>>> c.next()
		(1,)
		"""

	@abstractmethod
	def statement_from_id(self,
		statement_id : "The statement's identification string.",
		title : "The query's name, used in tracebacks when available" = None
	) -> PreparedStatement:
		"""
		Create a `PreparedStatement` object that was already prepared on the
		server. The distinction between this and a regular query is that it
		must be explicitly closed if it is no longer desired, and it is
		instantiated using the statement identifier as opposed to the SQL
		statement itself.

		If no ``title`` keyword is given, it will default to the statement_id.
		"""

	@abstractmethod
	def cursor_from_id(self,
		cursor_id : "The cursor's identification string."
	) -> Cursor:
		"""
		Create a `Cursor` object from the given `cursor_id` that was already
		declared on the server.
		
		`Cursor` objects created this way must *not* be closed when the object
		is garbage collected. Rather, the user must explicitly close it for
		the server resources to be released. This is in contrast to `Cursor`
		objects that are created by invoking a `PreparedStatement` or a SRF
		`StoredProcedure`.
		"""

	@abstractmethod
	def proc(self,
		procedure_id : \
			"The procedure identifier; a valid ``regprocedure`` or Oid."
	) -> StoredProcedure:
		"""
		Create a `StoredProcedure` instance using the given identifier.

		The `proc_id` given can be either an ``Oid``, or a ``regprocedure``
		that identifies the stored procedure to create the interface for.

		>>> p = db.proc('version()')
		>>> p()
		'PostgreSQL 8.3.0'
		>>> qstr = "select oid from pg_proc where proname = 'generate_series'"
		>>> db.prepare(qstr).first()
		1069
		>>> generate_series = db.proc(1069)
		>>> list(generate_series(1,5))
		[1, 2, 3, 4, 5]
		"""

	@abstractmethod
	def reset(self) -> None:
		"""
		Reset the connection into it's original state.

		Issues a ``RESET ALL`` to the database. If the database supports
		removing temporary tables created in the session, then remove them.
		Reapply initial configuration settings such as path. If inside a
		transaction block when called, reset the transaction state using the
		`reset` method on the connection's transaction manager, `xact`.

		The purpose behind this method is to provide a soft-reconnect method
		that re-initializes the connection into its original state. One
		obvious use of this would be in a connection pool where the connection
		is done being used.
		"""

class Connector(InterfaceElement):
	"""
	A connector is a "bookmark" object and an abstraction layer for the
	employed communication mechanism. `Connector` types should exist for each
	mode of addressing. This allows for easier type checking and cleaner
	implementation.

	`Connector` implementations supply the tools to make a connected socket.
	Sockets produced by the `Connector` are used by the `Connection` to
	facilitate negotiation; once negotiation is complete, the connection is
	made.
	"""
	ife_label = 'CONNECTOR'

	@propertydoc
	@abstractproperty
	def Connection(self) -> "`Connection`":
		"""
		The default `Connection` class that is used.
		This *should* be available on the type object.
		"""

	@propertydoc
	@abstractproperty
	def fatal_exception(self) -> Exception:
		"""
		The exception that is raised by sockets that indicate a fatal error.

		The exception can be a base exception as the `fatal_error_message` will
		indicate if that particular exception is actually fatal.
		"""

	@propertydoc
	@abstractproperty
	def timeout_exception(self) -> Exception:
		"""
		The exception raised by the socket when an operation could not be
		completed due to a configured time constraint.
		"""

	@propertydoc
	@abstractproperty
	def tryagain_exception(self) -> Exception:
		"""
		The exception raised by the socket when an operation was interrupted, but
		should be tried again.
		"""

	@propertydoc
	@abstractproperty
	def tryagain(self, err : Exception) -> bool:
		"""
		Whether or not `err` suggests the operation should be tried again.
		"""

	@abstractmethod
	def fatal_exception_message(self, err : Exception) -> (str, None):
		"""
		A function returning a string describing the failure, this string will be
		given to the `postgresql.exceptions.ConnectionFailure` instance that will
		subsequently be raised by the `Connection` object.

		Returns `None` when `err` is not actually fatal.
		"""

	@abstractmethod
	def socket_secure(self, socket : "socket object") -> "secured socket":
		"""
		Return a reference to the secured socket using the given parameters.

		If securing the socket for the connector is impossible, the user should
		never be able to instantiate the connector with parameters requesting
		security.
		"""

	@abstractmethod
	def socket_factory_sequence(self) -> [collections.Callable]:
		"""
		Return a sequence of `SocketCreator`s that `Connection` objects will use to
		create the socket object. 
		"""

	def create(self, *args, **kw):
		"""
		Instantiate and return the connection class.

		Must *not* execute the `connect` method on the created `Connection`
		instance.
		"""
		return self.Connection(self, *args, **kw)

	def __call__(self, *args, **kw):
		"""
		Create and connect. Arguments will be given to the `Connection` instance's
		`connect` method.
		"""
		c = self.Connection(self)
		c.connect(*args, **kw)
		return c

	def __init__(self,
		user : "required keyword specifying the user name(str)" = None,
		password : str = None,
		database : str = None,
		settings : (dict, [(str,str)]) = None,
		path : "sequence of schema names to set the search_path to" = None
	):
		if user is None:
			# sure, it's a "required" keyword, makes for better documentation
			raise TypeError("`user` is a required keyword")
		self.user = user
		self.password = password
		self.database = database
		self.settings = settings
		self.path = path

class Connection(Database):
	"""
	The interface to a connection to a PostgreSQL database. This is a
	`Database` interface with the additional connection management tools that
	are particular to using a remote database.
	"""
	ife_label = 'CONNECTION'

	@propertydoc
	@abstractproperty
	def connector(self) -> Connector:
		"""
		The `Connector` instance facilitating the `Connection` object's
		communication and initialization.
		"""

	@propertydoc
	@abstractproperty
	def closed(self) -> bool:
		"""
		`True` if the `Connection` is closed, `False` if the `Connection` is
		open.

		>>> db.closed
		True
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

	def reconnect(self, *args, **kw) -> None:
		"""
		Method drawing the effect of `close` then `connect`.
		
		Any arguments given will be applied to `connect`.
		"""
		self.close()
		self.connect(*args, **kw)

	__enter__ = methodcaller('connect')
	def __exit__(self, typ, obj, tb):
		"""
		Closes the connection and returns `False` when an exception is passed in,
		`True` when `None`.
		"""
		self.close()
		return typ is None

	def __context__(self):
		"""
		Returns the connection object.
		"""
		return self

class Driver(InterfaceElement):
	"""
	The `Driver` element provides the `Connector` and other information
	pertaining to the implementation of the driver. Information about what the
	driver supports is available in instances.
	"""
	ife_label = "DRIVER"
	ife_ancestor = None

	@abstractmethod
	def connect(**kw):
		"""
		Create a connection using the given parameters for the Connector.

		This should cache the `Connector` instance for re-use when the same
		parameters are given again.
		"""

	def print_message(self, msg, file = None):
		"""
		Standard message printer.
		"""
		file = sys.stderr if not file else file
		if file and not file.closed:
			file.write(str(msg))
		else:
			warnings.warn("sys.stderr unavailable for printing messages")

	def handle_warnings_and_messages(self, source, this, obj):
		"""
		Send warnings to `warnings.warn` and print `Message`s to standard error.
		"""
		if isinstance(obj, Message):
			self.print_message(obj)
		elif isinstance(obj, warnings.Warning):
			warnings.warn(obj)

	def __init__(self):
		"""
		The driver, by default will emit warnings and messages.
		"""
		self.ife_connect(self.handle_warnings_and_messages)

class Installation(InterfaceElement):
	"""
	Interface to a PostgreSQL installation. Instances would provide various
	information about an installation of PostgreSQL accessible by the Python 
	"""
	ife_label = "INSTALLATION"

	@propertydoc
	@abstractproperty
	def version(self):
		"""
		A version string consistent with what `SELECT version()` would output.
		"""

	@propertydoc
	@abstractproperty
	def version_info(self):
		"""
		A tuple specifying the version in a form similar to Python's
		sys.version_info. (8, 3, 3, 'final', 0)

		See `postgresql.versionstring`.
		"""

	@propertydoc
	@abstractproperty
	def type(self):
		"""
		The "type" of PostgreSQL. Normally, the first component of the string
		returned by pg_config.
		"""

class Cluster(InterfaceElement):
	"""
	Interface to a PostgreSQL cluster--a data directory. An implementation of
	this provides a means to control a server.
	"""
	ife_label = 'CLUSTER'
	ife_ancestor = None

	@propertydoc
	@abstractproperty
	def installation(self) -> Installation:
		"""
		The installation used by the cluster.
		"""

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

	@propertydoc
	@abstractproperty
	def settings(self):
		"""
		A `Settings` interface to the ``postgresql.conf`` file associated with the
		cluster.
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
