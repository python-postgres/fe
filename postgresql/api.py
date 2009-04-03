##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Application Programmer Interface specifications for PostgreSQL (ABCs).

PG-API
======

``postgresql.api`` is a Python API for the PostgreSQL DBMS. It is designed to take
full advantage of PostgreSQL's features to provide the Python programmer with
substantial convenience.

This module is used to define the PG-API. It creates a set of ABCs
that makes up the basic interfaces used to work with a PostgreSQL.
"""
import os
import sys
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

	This class is used to describe the signature of callables connected to an
	InterfaceElement via `InterfaceElement.ife_connect`.
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
	a database error. When raised, it provides the user with an error message;
	but, this information gives you little clue as to what connection the
	exception came from. While, it is possible for a given user to find out
	using a debugger, it not possible to do so efficiently if fair amounts of
	information about exception's lineage is required--consider a query's
	execution where parameters ultimately caused the failure.

	To save the user time, IFEs ancestry allows `postgresql.exceptions` to
	include substantial information about an error::

		<Python Traceback>
		postgresql.exceptions.Error: <message>
		CURSOR: <cursor_id>
			<parameters>
		STATEMENT: <statement_id> <parameter info>
			<query body>
		CONNECTION: <connection_title> <backend_id> <socket information>
			<settings, transaction state, connection state>
		CONNECTOR: pq://user@localhost:5432/database
		DRIVER: postgresql.driver.pq3


	Receptors
	---------

	Reception is a faculty created to support PostgreSQL message and warning
	propagation in a context specific way. For instance, the NOTICE emitted by
	PostgreSQL when creating a table with a PRIMARY KEY might be unnecessary in
	a program automating the creation of tables as it's expected. Providing a
	filter for these messages is useful to reducing noise.


	WARNING
	-------

	Many of these APIs are used to support features that users are *not*
	expected to use directly. Almost everything on `InterfaceElement` is subject
	to deprecation.
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
		'CLIENT',
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
		source : "What generated the message(SERVER, CLIENT)" = None,
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
			locstr = os.linesep + '  LOCATION: ' + locstr

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
			else "File {0!r}, "\
			"line {1!s}, in {2!s}".format(*loc)
		)

	@property
	def details_string(self):
		return os.linesep.join((
			': '.join(('  ' + k.upper(), str(v)))
			for k, v in sorted(self.details.items(), key = itemgetter(0))
			if k not in ('message', 'severity', 'file', 'function', 'line')
		))

	def ife_snapshot_text(self):
		details = self.details
		code = (os.linesep + "  CODE: " + self.code) if self.code else ""
		sev = details.get('severity')
		sevmsg = ""
		if sev:
			sevmsg = os.linesep + "  SEVERITY: " + sev.upper()
		detailstr = self.details_string
		if detailstr:
			detailstr = os.linesep + detailstr
		locstr = self.location_string
		if locstr:
			locstr = os.linesep + "  LOCATION: " + locstr
		return self.message + code + sevmsg + detailstr + locstr

	def emit(self):
		'Emit the message'
		self.snapshot = self.ife_ancestry_snapshot_text()
		self.ife_emit(self)

class Chunks(
	collections.Iterator,
	collections.Iterable,
):
	"""
	A `CursorChunks` object is an interface to an iterator of row-sets produced
	by a cursor. Normally, a chunks object is created by the user accessing the
	`chunks` property on a given cursor.
	"""

	@propertydoc
	@abstractproperty
	def cursor(self) -> "Cursor":
		"""
		The cursor the iterator is bound to.

		This is the object where the chunks iterator gets its rows from.
		"""

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
		2 : 'FROM_END',
	}
	_direction_map = {
		True : 'FORWARD',
		False : 'BACKWARD',
	}

	@propertydoc
	@abstractproperty
	def cursor_id(self) -> str:
		"""
		The cursor's identifier.
		"""

	@propertydoc
	@abstractproperty
	def sql_column_types(self) -> [str]:
		"""
		The type of the columns produced by the cursor.

		A sequence of `str` objects stating the SQL type name::

			['INTEGER', 'CHARACTER VARYING', 'INTERVAL']
		"""

	@propertydoc
	@abstractproperty
	def pg_column_types(self) -> [int]:
		"""
		The type Oids of the columns produced by the cursor.

		A sequence of `int` objects stating the SQL type name::

			[27, 28]
		"""

	@propertydoc
	@abstractproperty
	def column_names(self) -> [str]:
		"""
		The attribute names of the columns produced by the cursor.

		A sequence of `str` objects stating the column name::

			['column1', 'column2', 'emp_name']
		"""

	@propertydoc
	@abstractproperty
	def column_types(self) -> [str]:
		"""
		The Python types of the columns produced by the cursor.

		A sequence of type objects::

			[<class 'int'>, <class 'str'>]
		"""

	@propertydoc
	@abstractproperty
	def parameters(self) -> (tuple, None):
		"""
		The parameters bound to the cursor. `None`, if unknown.

		These *should* be the original parameters given to the invoked statement.
		"""

	@propertydoc
	@abstractproperty
	def statement(self) -> ("PreparedStatement", None):
		"""
		The query object used to create the cursor. `None`, if unknown.
		"""

	@propertydoc
	@abstractproperty
	def direction(self) -> bool:
		"""
		The default `direction` argument for read().

		When `True` reads are FORWARD.
		When `False` reads are BACKWARD.

		Cursor operation option.
		"""

	@propertydoc
	@abstractproperty
	def chunksize(self) -> int:
		"""
		Cursor configuration for determining how many rows to fetch with each
		request.

		Cursor operation option.
		"""

	@abstractmethod
	def read(self,
		quantity : "Number of rows to read" = None,
		direction : "Direction to fetch in, defaults to `self.direction`" = None,
	) -> ["Row"]:
		"""
		Read, fetch, the specified number of rows and return them in a list.
		If quantity is `None`, all records will be fetched.

		`direction` can be used to override the default configured direction.

		This alters the cursor's position.
		"""

	@abstractmethod
	def close(self) -> None:
		"""
		Close the cursor.
		"""

	@abstractmethod
	def __next__(self) -> "Row":
		"""
		Get the next tuple in the cursor.
		Advances the cursor position by one.
		"""

	@abstractmethod
	def seek(self, offset, whence = 'ABSOLUTE'):
		"""
		Set the cursor's position to the given offset with respect to the
		whence parameter and the configured direction.

		Whence values:

		 ``0`` or ``"ABSOLUTE"``
		  Absolute.
		 ``1`` or ``"RELATIVE"``
		  Relative.
		 ``2`` or ``"FROM_END"``
		  Absolute from end.

		Direction effects whence. If direction is BACKWARD, ABSOLUTE positioning
		will effectively be FROM_END, RELATIVE's position will be negated, and
		FROM_END will effectively be ABSOLUTE.
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
	def string(self) -> object:
		"""
		The SQL string of the prepared statement.

		`None` if not available. This can happen in cases where a statement is
		prepared on the server and a reference to the statement is sent to the
		client which subsequently uses the statement via the `Database`'s
		`statement` constructor.
		"""

	@propertydoc
	@abstractproperty
	def sql_parameter_types(self) -> [str]:
		"""
		The type of the parameters required by the statement.

		A sequence of `str` objects stating the SQL type name::

			['INTEGER', 'VARCHAR', 'INTERVAL']
		"""

	@propertydoc
	@abstractproperty
	def sql_column_types(self) -> [str]:
		"""
		The type of the columns produced by the statement.

		A sequence of `str` objects stating the SQL type name::

			['INTEGER', 'VARCHAR', 'INTERVAL']
		"""

	@propertydoc
	@abstractproperty
	def pg_parameter_types(self) -> [int]:
		"""
		The type Oids of the parameters required by the statement.

		A sequence of `int` objects stating the PostgreSQL type Oid::

			[27, 28]
		"""

	@propertydoc
	@abstractproperty
	def pg_column_types(self) -> [int]:
		"""
		The type Oids of the columns produced by the statement.

		A sequence of `int` objects stating the SQL type name::

			[27, 28]
		"""

	@propertydoc
	@abstractproperty
	def column_names(self) -> [str]:
		"""
		The attribute names of the columns produced by the statement.

		A sequence of `str` objects stating the column name::

			['column1', 'column2', 'emp_name']
		"""

	@propertydoc
	@abstractproperty
	def column_types(self) -> [type]:
		"""
		The Python types of the columns produced by the statement.

		A sequence of type objects::

			[<class 'int'>, <class 'str'>]
		"""

	@propertydoc
	@abstractproperty
	def parameter_types(self) -> [type]:
		"""
		The Python types expected of parameters given to the statement.

		A sequence of type objects::

			[<class 'int'>, <class 'str'>]
		"""

	@abstractmethod
	def __call__(self, *parameters : "Positional Parameters") -> ["Row"]:
		"""
		Execute the prepared statement with the given arguments as parameters.

		Usage:

		>>> p=db.prepare("SELECT column FROM ttable WHERE key = $1")
		>>> p('identifier')
		[...]
		"""

	@abstractmethod
	def rows(self, *parameters, chunksize = None) -> "Iterator(Row)":
		"""
		Return an iterator producing rows produced by the cursor
		created from the statement bound with the given parameters.

		Row iterators are never scrollable.

		Supporting cursors will be WITH HOLD when outside of a transaction.

		`rows` is designed for the situations involving large data sets.

		Each iteration returns a single row. Arguably, best implemented:

			return itertools.chain.from_iterable(self.chunks(*parameters))
		"""

	@abstractmethod
	def chunks(self, *parameters, chunksize = None) -> Chunks:
		"""
		Return an iterator producing sequences of rows produced by the cursor
		created from the statement bound with the given parameters.

		Chunking iterators are *never* scrollable.

		Supporting cursors will be WITH HOLD when outside of a transaction.

		`chunks` is designed for the situations involving large data sets.

		Each iteration returns sequences of rows *normally* of length(seq) ==
		chunksize. If chunksize is unspecified, a default, positive integer will
		be filled in.
		"""

	@abstractmethod
	def declare(self, *parameters) -> Cursor:
		"""
		Return a scrollable cursor with hold using the statement bound with the
		given parameters.
		"""

	@abstractmethod
	def first(self, *parameters) -> "'First' object that is returned by the query":
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
		Close the prepared statement releasing resources associated with it.
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

##
# Arguably, it would be wiser to isolate blocks, prepared transactions, and
# savepoints, but the utility of the separation is not significant. It's really
# more interesting as a formality that the user may explicitly state the
# type of the transaction. However, this capability is not completely absent
# from the current interface as the configuration parameters, or lack thereof,
# help imply the expectations.
class Transaction(InterfaceElement):
	"""
	A `Tranaction` is an element that represents a transaction in the session.
	Once created, it's ready to be started, and subsequently committed or
	rolled back.

	Read-only transaction:

		>>> with db.xact(mode = 'read only'):
		...  ...

	Read committed isolation:

		>>> with db.xact(isolation = 'READ COMMITTED'):
		...  ...

	Savepoints are created if inside a transaction block:

		>>> with db.xact():
		...  with db.xact():
		...   ...

	Or, in cases where two-phase commit is desired:

		>>> with db.xact(gid = 'gid') as gxact:
		...  with gxact:
		...   # phase 1 block
		...   ...
		>>> # fully committed at this point

	Considering that transactions decide what's saved and what's not saved, it is
	important that they are used properly. In most situations, when an action is
	performed where state of the transaction is unexpected, an exception should
	occur.
	"""
	ife_label = 'XACT'

	@propertydoc
	@abstractproperty
	def mode(self) -> (None, str):
		"""
		The mode of the transaction block:

			START TRANSACTION [ISOLATION] <mode>;

		The `mode` property is a string and will be directly interpolated into the
		START TRANSACTION statement.
		"""

	@propertydoc
	@abstractproperty
	def isolation(self) -> (None, str):
		"""
		The isolation level of the transaction block:

			START TRANSACTION <isolation> [MODE];

		The `isolation` property is a string and will be directly interpolated into
		the START TRANSACTION statement.
		"""

	@propertydoc
	@abstractproperty
	def gid(self) -> (None, str):
		"""
		The global identifier of the transaction block:

			PREPARE TRANSACTION <gid>;

		The `gid` property is a string that indicates that the block is a prepared
		transaction.
		"""

	@abstractmethod
	def start(self) -> None:
		"""
		Start the transaction.

		If the database is in a transaction block, the transaction should be
		configured as a savepoint. If any transaction block configuration was
		applied to the transaction, raise a `postgresql.exceptions.OperationError`.

		If the database is not in a transaction block, start one using the
		configuration where:

		`self.isolation` specifies the ``ISOLATION LEVEL``. Normally, ``READ
		COMMITTED``, ``SERIALIZABLE``, or ``READ UNCOMMITTED``.

		`self.mode` specifies the mode of the transaction. Normally, ``READ
		ONLY`` or ``READ WRITE``.

		If the transaction is open--started or prepared, do nothing.

		If the transaction has been committed or aborted, raise an
		`postgresql.exceptions.OperationError`.
		"""
	begin = start

	@abstractmethod
	def commit(self) -> None:
		"""
		Commit the transaction.

		If the transaction is configured with a `gid` issue a COMMIT PREPARED
		statement with the configured `gid`.

		If the transaction is a block, issue a COMMIT statement.

		If the transaction was started inside a transaction block, it should be
		identified as a savepoint, and the savepoint should be released.

		If the transaction has already been committed, do nothing.
		"""

	@abstractmethod
	def rollback(self) -> None:
		"""
		Abort the transaction.

		If the transaction is configured with a `gid` *and* has been prepared, issue
		a ROLLBACK PREPARE statement with the configured `gid`.

		If the transaction is a savepoint, ROLLBACK TO the savepoint identifier.

		If the transaction is a transaction block, issue an ABORT.

		If the transaction has already been aborted, do nothing.
		"""
	abort = rollback

	@abstractmethod
	def recover(self) -> None:
		"""
		If the transaction is assigned a `gid`, recover may be used to identify
		the transaction as prepared and ready for committing or aborting.

		This method is used in recovery procedures where a prepared transaction
		needs to be committed or rolled back.

		If no prepared transaction with the configured `gid` exists, a
		`postgresql.exceptions.UndefinedObjectError` must be raised.
		[This is consistent with the error raised by ROLLBACK/COMMIT PREPARED]

		Once this method has been ran, it should identify the transaction as being
		prepared so that subsequent invocations to `commit` or `rollback` should
		cause the appropriate ROLLBACK PREPARED or COMMIT PREPARED statements to
		be executed.
		"""

	@abstractmethod
	def prepare(self) -> None:
		"""
		Explicitly prepare the transaction with the configured `gid` by issuing a
		PREPARE TRANSACTION statement with the configured `gid`.
		This *must* be called for the first phase of the commit.

		If the transaction is already prepared, do nothing.
		"""

	@abstractmethod
	def __enter__(self):
		"""
		Synonym for `start` returning self.
		"""

	def __context__(self):
		'Return self'
		return self

	@abstractmethod
	def __exit__(self, typ, obj, tb):
		"""
		If an exception is indicated by the parameters, run the transaction's
		`rollback` method iff the database is still available(not closed), and
		return a `False` value.

		If an exception is not indicated, but the database's transaction state is
		in error, run the transaction's `rollback` method and raise a
		`postgresql.exceptions.InFailedTransactionError`. If the database is
		unavailable, the `rollback` method should cause a
		`postgresql.exceptions.ConnectionDoesNotExistError` exception to occur.

		Otherwise, run the transaction's `commit` method. If the commit fails,
		a `gid` is configured, and the connection is still available, run the
		transaction's `rollback` method.

		When the `commit` is ultimately unsuccessful or not ran at all, the purpose
		of __exit__ is to resolve the error state of the database iff the
		database is available(not closed) so that more commands can be after the
		block's exit.
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
		Create a context manager applying the given settings on __enter__ and
		restoring the old values on __exit__.

		>>> with db.settings(search_path = 'local,public'):
		...  ...
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

	@propertydoc
	@abstractproperty
	def xact(self,
		gid : "global identifier to configure" = None,
		isolation : "ISOLATION LEVEL to use with the transaction" = None,
		mode : "Mode of the transaction, READ ONLY or READ WRITE" = None,
	) -> Transaction:
		"""
		Create a `Transaction` object using the given keyword arguments as its
		configuration.
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
		Reapply initial configuration settings such as path.

		The purpose behind this method is to provide a soft-reconnect method
		that re-initializes the connection into its original state. One
		obvious use of this would be in a connection pool where the connection
		is being recycled.
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

	def __call__(self, *args, **kw):
		"""
		Create and connect. Arguments will be given to the `Connection` instance's
		`connect` method.
		"""
		return self.Connection(self)

	def __init__(self,
		user : "required keyword specifying the user name(str)" = None,
		password : str = None,
		database : str = None,
		settings : (dict, [(str,str)]) = None,
	):
		if user is None:
			# sure, it's a "required" keyword, makes for better documentation
			raise TypeError("'user' is a required keyword")
		self.user = user
		self.password = password
		self.database = database
		self.settings = settings

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

	__enter__ = methodcaller('connect')
	def __exit__(self, typ, obj, tb):
		"""
		Closes the connection and returns `False` when an exception is passed in,
		`True` when `None`.
		"""
		self.close()

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
			try:
				file.write(str(msg))
			except Exception:
				try:
					sys.excepthook(*sys.exc_info())
				except Exception:
					# What more can be done?
					pass

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

	@propertydoc
	@abstractproperty
	def ssl(self) -> bool:
		"""
		Whether the installation supports SSL.
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
		user : "name of the cluster's superuser" = None,
		password : "superuser's password" = None,
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

	@abstractmethod
	def __enter__(self):
		"""
		Start the cluster if it's not already running, and wait for it to be
		readied.
		"""

	def __context__(self):
		return self

	@abstractmethod
	def __exit__(self, exc, val, tb):
		"""
		Stop the cluster and wait for it to shutdown *iff* it was started by the
		corresponding enter.
		"""

__docformat__ = 'reStructuredText'
if __name__ == '__main__':
	help('postgresql.api')
##
# vim: ts=3:sw=3:noet:
