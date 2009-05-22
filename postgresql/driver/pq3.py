##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
PG-API interface for PostgreSQL that support the PQ version 3.0 protocol.
"""
import sys
import os
import warnings
import weakref

import errno
import socket
import ssl

from operator import attrgetter, itemgetter, is_, is_not
get0 = itemgetter(0)
get1 = itemgetter(1)
from itertools import repeat, islice, chain
from functools import partial

from abc import abstractmethod, abstractproperty
import collections

from .. import versionstring as pg_version
from .. import iri as pg_iri
from .. import exceptions as pg_exc
from .. import string as pg_str
from ..encodings import aliases as pg_enc_aliases
from .. import api as pg_api
from ..python.itertools import interlace

from ..protocol.buffer import pq_message_stream
from ..protocol import xact3 as pq
from ..protocol import typio as pg_typio

from .. import types as pg_types

TypeLookup = """
SELECT
 ns.nspname as namespace,
 bt.typname,
 bt.typtype,
 bt.typlen,
 bt.typelem,
 bt.typrelid,
 ae.oid AS ae_typid,
 ae.typreceive::oid != 0 AS ae_hasbin_input,
 ae.typsend::oid != 0 AS ae_hasbin_output
FROM pg_type bt
 LEFT JOIN pg_type ae
  ON (
	bt.typlen = -1 AND
   bt.typelem != 0 AND
   bt.typelem = ae.oid
  )
 LEFT JOIN pg_namespace ns
  ON (ns.oid = bt.typnamespace)
WHERE bt.oid = $1
"""

CompositeLookup = """
SELECT
 CAST(atttypid AS oid) AS atttypid,
 CAST(attname AS VARCHAR) AS attname
FROM
 pg_type t LEFT JOIN pg_attribute a
  ON (t.typrelid = a.attrelid)
WHERE
 attrelid = $1 AND NOT attisdropped AND attnum > 0
ORDER BY attnum ASC
"""

ProcedureLookup = """
SELECT
 pg_proc.oid,
 pg_proc.*,
 pg_proc.oid::regproc AS _proid,
 pg_proc.oid::regprocedure as procedure_id,
  -- mm, the pain. the sweet, sweet pain. oh it's portable.
  -- it's so portable that it runs on BDB on win32.
  COALESCE(
   string_to_array(
    replace(trim(textin(oidvectorout(proargtypes)), '{}'), ',', ' '), ' '
   )::oid[],
   '{}'::oid[]
  ) AS proargtypes,
 (pg_type.oid = 'record'::regtype or pg_type.typtype = 'c') AS composite
FROM
 pg_proc LEFT JOIN pg_type ON (
  pg_proc.prorettype = pg_type.oid
 )
"""

PreparedLookup = """
SELECT
	COALESCE(ARRAY(
		SELECT
			gid::text
		FROM
			pg_catalog.pg_prepared_xacts
		WHERE
			database = current_database()
			AND (
				owner = $1::text
				OR (
					(SELECT rolsuper FROM pg_roles WHERE rolname = $1::text)
				)
			)
		ORDER BY prepared ASC
	), ('{}'::text[]))
"""

TransactionIsPrepared = """
SELECT TRUE FROM pg_catalog.pg_prepared_xacts
WHERE gid::text = $1
"""

GetPreparedStatement = """
SELECT
	statement
FROM
	pg_catalog.pg_prepared_statements
WHERE
	statement_id = $1
"""

IDNS = '%s(py:%s)'
def ID(s, title = None):
	'generate an id for a client statement or cursor'
	return IDNS %(title or 'untitled', hex(id(s)))

def direction_str_to_bool(str):
	s = str.upper()
	if s == 'FORWARD':
		return True
	elif s == 'BACKWARD':
		return False
	else:
		raise ValueError("invalid direction " + repr(str))

def direction_to_bool(v):
	if isinstance(v, str):
		return direction_str_to_bool(v)
	elif v is not True and v is not False:
		raise TypeError("invalid direction " + repr(v))
	else:
		return v

class ClosedConnection(pg_api.InterfaceElement):
	ife_label = 'CLOSED'
	ife_ancestor = attrgetter('connection')
	_asyncs = ()
	state = pq.Complete
	fatal = True

	def ife_snapshot_text(self):
		return '(connection has been killed)'

	def asyncs(self):
		return self._asyncs

	error_message = pq.element.Error(
		severity = b'FATAL',
		code = pg_exc.ConnectionDoesNotExistError.code.encode('ascii'),
		message = b"operation on closed connection",
		hint = b"Call the 'connect' method on the connection object.",
	)
	def __init__(self, connection):
		self.connection = connection

class TypeIO(pg_typio.TypeIO):
	def __init__(self, database):
		self.database = database
		super().__init__()

	def lookup_type_info(self, typid):
		return self.database.prepare(TypeLookup).first(typid)

	def lookup_composite_type_info(self, typid):
		return self.database.prepare(CompositeLookup)(typid)

class Chunks(pg_api.Chunks):
	cursor = None

	def __init__(self, cursor):
		self.cursor = cursor

	def __iter__(self):
		return self

	def __next__(self):
		if self.cursor._last_reqsize == 0xFFFFFFFF:
			raise StopIteration
		self.cursor._expand()

		# Grab the whole thing.
		if self.cursor._offset == 0:
			chunk = self.cursor._buffer
		else:
			chunk = self.cursor._buffer[self.cursor._offset:]

		self.cursor.__dict__.update({
			'_offset': 0,
			'_buffer': [],
		})

		if not chunk:
			self.cursor._buffer_more(self.cursor.chunksize or 128, self.cursor.direction)
			if not self.cursor._buffer \
			and self.cursor._last_increase != self.cursor._last_reqsize:
				raise StopIteration
			# offset is expected to be zero.
			chunk = self.cursor._buffer
			self.cursor.__dict__.update({
				'_offset': 0,
				'_buffer': [],
			})
		else:
			self.cursor._dispatch_for_more(self.cursor.direction)

		return chunk

##
# Base Cursor class and cursor creation entry points.
class Cursor(pg_api.Cursor):
	ife_ancestor = None

	closed = None
	cursor_id = None
	statement = None
	parameters = None

	with_hold = None
	scroll = None
	insensitive = None

	chunksize = 64

	_output = None
	_output_io = None
	_output_formats = None
	_output_attmap = None

	_complete_message = None

	@classmethod
	def from_statement(
		typ,
		parameters,
		statement,
		scroll = False,
		with_hold = None,
		insensitive = True,
	):
		if statement._input is not None:
			if len(parameters) != len(statement._input):
				raise TypeError("statement requires %d parameters, given %d" %(
					len(statement._input), len(parameters)
				))
		c = super().__new__(typ)
		c.parameters = parameters
		c.statement = statement
		c.with_hold = with_hold
		c.scroll = scroll
		c.insensitive = insensitive
		c.__init__(ID(c), statement.database)
		return c

	def __init__(self, cursor_id, database):
		self.closed = True
		self.__dict__['direction'] = True
		if not cursor_id:
			# Driver uses the empty id(b'').
			##
			raise ValueError("invalid cursor identifier, " + repr(cursor_id))
		self.cursor_id = str(cursor_id)
		self._quoted_cursor_id = '"' + self.cursor_id.replace('"', '""') + '"'
		self.database = database
		self._pq_cursor_id = database.typio.encode(self.cursor_id)
		if ID(self) == self.cursor_id:
			addgarbage = self.database._closeportals.append
			typio = self.database.typio
			curid = self.cursor_id
			self._del = weakref.ref(
				self, lambda _: addgarbage(typio.encode(curid))
			)

	def __iter__(self):
		return self

	def get_direction(self):
		return self.__dict__['direction']
	def set_direction(self, value):
		self.__dict__['direction'] = direction_to_bool(value)
	direction = property(
		fget = get_direction,
		fset = set_direction,
	)
	del get_direction, set_direction

	def _which_way(self, direction):
		if direction is not None:
			direction = direction_to_bool(direction)
			# -1 * -1 = 1, -1 * 1 = -1, 1 * 1 = 1
			return not ((not self.direction) ^ (not direction))
		else:
			return self.direction

	def _statement_string(self):
		if self.statement:
			return self.statement.string
		return None

	@property
	def state(self):
		if self.closed:
			return 'closed'
		else:
			return 'open'

	@property
	def column_names(self):
		if self._output is not None:
			return list(self.database.typio.decodes(self._output.keys()))

	@property
	def column_types(self):
		if self._output is not None:
			return [self.database.typio.type_from_oid(x[3]) for x in self._output]

	@property
	def pg_column_types(self):
		if self._output is not None:
			return [x[3] for x in self._output]

	@property
	def sql_column_types(self):
		return [
			pg_types.oid_to_sql_name.get(x) or \
			self.database.typio.sql_type_from_oid(x)
			for x in self.pg_column_types
		]

	def close(self):
		if self.closed is False:
			self.database._closeportals.append(
				self.database.typio.encode(self.cursor_id)
			)
		self.closed = True
		if hasattr(self, '_del'):
			del self._del

	def _raise_parameter_tuple_error(self, procs, tup, itemnum):
		# The element traceback will include the full list of parameters.
		data = repr(tup[itemnum])
		if len(data) > 80:
			# Be sure not to fill screen with noise.
			data = data[:75] + ' ...'
		te = pg_exc.ParameterError(
			"failed to pack parameter %s::%s for transfer" %(
				('$' + str(itemnum + 1)),
				self.database.typio.sql_type_from_oid(
					self.statement.pg_parameter_types[itemnum]
				) or '<unknown>',
			),
			details = {
				'data': data,
				'hint' : "Try casting parameter to 'text', then to the target type."
			},
		)
		te.index = itemnum
		self.ife_descend(te)
		te.raise_exception()

	def _raise_column_tuple_error(self, procs, tup, itemnum):
		'for column processing'
		# The element traceback will include the full list of parameters.
		data = repr(tup[itemnum])
		if len(data) > 80:
			# Be sure not to fill screen with noise.
			data = data[:75] + ' ...'
		te = pg_exc.ColumnError(
			"failed to unpack column %r, %s::%s, from wire data" %(
				itemnum,
				self.column_names[itemnum],
				self.database.typio.sql_type_from_oid(
					self.statement.pg_column_types[itemnum]
				) or '<unknown>',
			),
			details = {
				'data': data,
				'hint' : "Try casting the column to 'text'."
			},
		)
		te.index = itemnum
		self.ife_descend(te)
		te.raise_exception()

	def _pq_parameters(self):
		return pg_typio.process_tuple(
			self.statement._input_io, self.parameters,
			self._raise_parameter_tuple_error
		)

	def _init(self):
		"""
		Based on the cursor parameters and the current transaction state,
		select a cursor strategy for managing the response from the server.
		"""
		if self.statement is not None:
			# If the cursor comes from a statement object, always
			# get the output information from it.
			self._output = self.statement._output
			self._output_formats = self.statement._output_formats
			self._output_io = self.statement._output_io
			self._output_attmap = self.statement._output_attmap

			if self._output is None:
				self.__class__ = UtilityCursor
				return self._init()

			##
			# Finish any outstanding transactions to identify
			# the current transaction state.
			if self.database._pq_xact is not None:
				self.database._pq_complete()
			##
			# In auto-commit mode or with_hold is on?
			if ((self.database._pq_state == b'I' and self.with_hold is None)\
			or self.with_hold is True or self.scroll is True) \
			and self.statement.string is not None:
				##
				# with_hold or scroll require a DeclaredCursor.
				self.__class__ = DeclaredCursor
			else:
				self.__class__ = ProtocolCursor
		else:
			# If there's no statement, it's probably a server declared cursor.
			# Treat it as an SQL declared cursor with special initialization.
			self.__class__ = ServerDeclaredCursor
		return self._init()

	def _fini(self):
		##
		# Mark the Cursor as open.
		self.closed = False

	def ife_snapshot_text(self):
		return self.cursor_id + ('' if self.parameters is None else (
			os.linesep + ' PARAMETERS: ' + repr(self.parameters)
		))

	def _operation_error_(self, *args, **kw):
		e = pg_exc.OperationError(
			"cursor type does not support that operation"
		)
		self.ife_descend(e)
		e.raise_exception()
	__next__ = read = seek = _operation_error_

	def command(self):
		"The completion message's command identifier"
		if self._complete_message is not None:
			return self._complete_message.extract_command().decode('ascii')

	def count(self):
		"The completion message's count number"
		if self._complete_message is not None:
			return self._complete_message.extract_count()

class CursorStrategy(Cursor):
	"""
	It's a stretch to call this a strategy as the strategy selection occurs
	after instantiation and by the Cursor class itself, not the caller.

	It's more like a union-class where the ob_type is the current "selection".
	"""
	def __init__(self, *args, **kw):
		raise TypeError("cannot instantiate CursorStrategy-types directly")

class ReadableCursor(CursorStrategy):
	def _init(self):
		self._offset = 0
		self._buffer = []
		self._this_reqsize = -1
		self._this_direction = None
		self._last_reqsize = 0
		self._last_increase = 0
		self._last_direction = self.direction
		self._xact = None

	@abstractmethod
	def _expansion(self):
		"""
		Return a sequence of the new data provided by the transaction
		_xact. "the expansion of data for the buffer"
		"""

	@abstractmethod
	def _expand(self):
		"""
		For a given state, extract the data from the transaction and
		append it to the buffer.
		"""

	def _contract(self):
		"reduce the number of items in the buffer"
		# when chunksize is zero, it will trim the entire buffer.
		maxtrim = self._offset - self.chunksize
		trim = self.chunksize or maxtrim
		if trim >= self.chunksize:
			self.__dict__.update({
				'_offset' : self._offset - trim,
				'_buffer' : self._buffer[trim:]
			})

	def _maintain(self, direction):
		if self._last_direction is not None and direction is not self._last_direction:
			# change in direction, reset buffer.
			if self._xact is not None:
				if self._xact.state is not pq.Complete \
				and self._xact is self.database._pq_xact:
					self.database._pq_complete()
			self.__dict__.update({
				'_offset' : 0,
				'_buffer' : [],
				'_last_increase' : 0,
				'_last_reqsize' : 0,
				# Don't identify a direction change again.
				'_last_direction' : direction,
				'_this_direction' : None,
				'_xact' : None,
				'_this_reqsize' : 0,
			})
			return
		# Reduce the buffer, if need be.
		if self._offset >= self.chunksize and len(self._buffer) >= (3 * self.chunksize):
			self._contract()
		# Expand it if reading-ahead,
		# and the offset is nearing the end of the chunksize.
		if self._xact is not None and \
		(len(self._buffer) - self._offset) > (self.chunksize // 8):
			# Transaction ready, but only attempt expanding if it
			# will be needed soon.
			##
			self._expand()

	def __next__(self):
		self._maintain(self.direction)

		if self._offset >= len(self._buffer):
			if self._last_increase != self._last_reqsize \
			or not (self._buffer_more(1, self.direction) > 0):
				raise StopIteration

		t = self._buffer[self._offset]
		self._offset = self._offset + 1
		return t

	def read(self, quantity = None, direction = None):
		dir = self._which_way(direction)
		self._maintain(dir)

		if quantity is None:
			# Read all in the direction.
			##
			while self._last_increase == self._last_reqsize:
				self._buffer_more(None, dir)
			quantity = len(self._buffer) - self._offset
		else:
			# Read some.
			##
			left_to_read = (quantity - (len(self._buffer) - self._offset))
			expanded = 0
			while left_to_read > 0 and self._last_increase == self._last_reqsize:
				# In scroll situations, there's no concern
				# about reading already requested data as
				# there is no pre-fetching going on.
				expanded = self._buffer_more(left_to_read, dir)
				left_to_read -= expanded
			quantity = min(len(self._buffer) - self._offset, quantity)

		end_of_block = self._offset + quantity
		t = self._buffer[self._offset:end_of_block]
		self._offset = end_of_block
		return t

class TupleCursor(ReadableCursor):
	def _init(self, setup):
		super()._init()
		##
		# chunksize determines whether or not to pre-fetch.
		# If the cursor is not scrollable, use the default.
		if self.scroll:
			# This restriction on scroll was set to insure
			# any needed consistency with the cursor position.
			# If this was not done, than compensation would need
			# to be made when direction changes occur.
			##
			self.chunksize = 0
			more = ()
		else:
			more = self._pq_xp_fetchmore(self.chunksize, self.direction)

		x = pq.Instruction(
			setup + more + (pq.element.SynchronizeMessage,)
		)
		self.__dict__.update({
			'_xact' : x,
			'_this_reqsize' : self.chunksize
		})
		self.database._pq_push(self._xact)
		self._fini()

	def _dispatch_for_more(self, direction):
		if self._xact:
			# didn't expand
			raise RuntimeError("invalid state for dispatch")
		more = self._pq_xp_fetchmore(self.chunksize, direction)
		x = more + (pq.element.SynchronizeMessage,)
		x = pq.Instruction(x)
		self.ife_descend(x)
		self.__dict__.update({
			'_xact' : x,
			'_this_reqsize' : self.chunksize,
			'_this_direction' : direction,
		})
		self.database._pq_push(self._xact)

	def _expand(self):
		"""
		[internal] Expand the _buffer using the data in _xact
		"""
		if self._xact is not None:
			# complete the _xact
			if self._xact.state is not pq.Complete:
				self.database._pq_push(self._xact)
				if self._xact.state is not pq.Complete:
					self.database._pq_complete()
			expansion = self._expansion()
			self.__dict__.update({
				'_buffer' : self._buffer + expansion,
				'_xact' : None,
				'_last_increase' : len(expansion),
				'_last_reqsize' : self._this_reqsize,
				'_last_direction' : self._this_direction,
				'_this_reqsize' : None,
				'_this_direction' : None,
			})

	def _buffer_more(self, quantity, direction):
		"""
		Expand the buffer with more tuples. Does *not* alter offset.
		"""
		##
		# The final fallback of 64 is to handle scrollable cursors
		# where read(None) is invoked.
		rquantity = self.chunksize or quantity or 64
		if rquantity < 0:
			raise RuntimeError("cannot buffer negative quantities")

		if self._xact is None:
			# No previous transaction started, so make one.
			##
			##
			# Use chunksize if it's non-zero. This will allow the cursor to
			# complete the transaction and process the rows while more are
			# coming in.
			more = self._pq_xp_fetchmore(rquantity, direction)
			x = pq.Instruction(more + (pq.element.SynchronizeMessage,))
			self.ife_descend(x)
			self.__dict__.update({
				'_xact' : x,
				'_this_reqsize' : rquantity,
				'_this_direction' : direction,
			})
			self.database._pq_push(x)

		self._expand()

		if self.scroll is False and (
			self._last_increase == self._last_reqsize \
			and (
				(len(self._buffer) - self._offset) >= (self.chunksize // 4) \
				or quantity > rquantity
			)
		):
			# If not scrolling and
			# The last buffer increase was the same as the request and
			# A quarter of the chunksize remains, dispatch for another.
			#  or, if the quantity is greater than the rquantity.
			##
			self._dispatch_for_more(direction)

		return self._last_increase

	def _expansion(self):
		return [
			pg_types.Row.from_sequence(
				self._output_attmap,
				pg_typio.process_tuple(
					self._output_io, y, self._raise_column_tuple_error
				),
			)
			for y in self._xact.messages_received()
			if y.type is pq.element.Tuple.type
		]

	def _pq_xp_move(self, position, whence):
		'make a command sequence for a MOVE single command'
		return (
			pq.element.Parse(b'',
				b'MOVE ' + whence + b' ' + position + b' IN ' + \
				self.database.typio.encode(self._quoted_cursor_id),
				()
			),
			pq.element.Bind(b'', b'', (), (), ()),
			pq.element.Execute(b'', 1),
		)

	def seek(self, offset, whence = 'ABSOLUTE'):
		rwhence = self._seek_whence_map.get(whence, whence)
		if rwhence is None or rwhence.upper() not in \
		self._seek_whence_map.values():
			raise TypeError(
				"unknown whence parameter, %r" %(whence,)
			)
		rwhence = rwhence.upper()
		if self.direction is False:
			if rwhence == 'RELATIVE':
				offset = -offset
			elif rwhence == 'ABSOLUTE':
				rwhence = 'FROM_END'
			else:
				rwhence = 'ABSOLUTE'

		if rwhence == 'RELATIVE':
			if offset < 0:
				cmd = self._pq_xp_move(
					str(-offset).encode('ascii'), b'BACKWARD'
				)
			else:
				cmd = self._pq_xp_move(
					str(offset).encode('ascii'), b'RELATIVE'
				)
		elif rwhence == 'ABSOLUTE':
			cmd = self._pq_xp_move(str(offset).encode('ascii'), b'ABSOLUTE')
		else:
			# move to last record, then consume it to put the position at
			# the very end of the cursor.
			cmd = self._pq_xp_move(b'', b'LAST') + \
				self._pq_xp_move(b'', b'NEXT') + \
				self._pq_xp_move(str(offset).encode('ascii'), b'BACKWARD')

		x = pq.Instruction(cmd + (pq.element.SynchronizeMessage,))
		self.ife_descend(x)
		# moves are a full reset
		self.__dict__.update({
			'_offset' : 0,
			'_buffer' : [],
			'_xact' : x,
			'_this_reqsize' : 0,
			'_this_direction' : None,
			'_last_reqsize' : 0,
			'_last_increase' : 0,
			'_last_direction' : None,
		})
		self.database._pq_push(x)

class ProtocolCursor(TupleCursor):
	cursor_type = 'protocol'

	def _init(self):
		# Protocol-bound cursor.
		##
		if self.scroll:
			# That doesn't work.
			##
			e = pg_exc.OperationError("cannot bind cursor scroll = True")
			self.ife_descend(e)
			e.raise_exception()
		if self.with_hold:
			# That either.
			##
			e = pg_exc.OperationError("cannot bind cursor with_hold = True")
			self.ife_descend(e)
			e.raise_exception()

		if self.database._pq_state == b'I':
			# have to fetch them all. as soon as the next sync occurs, the
			# cursor will be dropped.
			self.chunksize = 0xFFFFFFFF

		return super()._init((
			pq.element.Bind(
				self._pq_cursor_id,
				self.statement._pq_statement_id,
				self.statement._input_formats,
				self._pq_parameters(),
				self._output_formats,
			),
		))

	def _pq_xp_fetchmore(self, quantity, direction):
		if direction is not True:
			err = pg_exc.OperationError(
				"cannot read backwards with protocol cursors"
			)
			self.ife_descend(err)
			err.raise_exception()
		return (
			pq.element.Execute(self._pq_cursor_id, quantity),
		)

class DeclaredCursor(TupleCursor):
	cursor_type = 'declared'

	def _statement_string(self):
		qstr = super()._statement_string()
		return 'DECLARE {name}{insensitive} {scroll} '\
			'CURSOR {hold} FOR {source}'.format(
				name = self._quoted_cursor_id,
				insensitive = ' INSENSITIVE' if self.insensitive else '',
				scroll = 'SCROLL' if (self.scroll is True) else 'NO SCROLL',
				hold = 'WITH HOLD' if (self.with_hold is True) else 'WITHOUT HOLD',
				source = qstr
			)

	def _init(self):
		##
		# Force with_hold as there is no transaction block.
		# If this were not forced, the cursor would disappear
		# before the user had a chance to read rows from it.
		# Of course, the alternative is to read all the rows like ProtocolCursor
		# does. :(
		if self.database._pq_state == b'I':
			self.with_hold = True

		return super()._init((
			pq.element.Parse(b'', self.database.typio.encode(self._statement_string()), ()),
			pq.element.Bind(
				b'', b'', self.statement._input_formats, self._pq_parameters(), ()
			),
			pq.element.Execute(b'', 1),
		))

	def _pq_xp_fetchmore(self, quantity, direction):
		##
		# It's an SQL declared cursor, manually construct the fetch commands.
		qstr = "FETCH " + ("FORWARD " if direction else "BACKWARD ")
		if quantity is None:
			qstr = qstr + "ALL IN " + self._quoted_cursor_id
		else:
			qstr = qstr \
				+ str(quantity) + " IN " + self._quoted_cursor_id
		return (
			pq.element.Parse(b'', self.database.typio.encode(qstr), ()),
			pq.element.Bind(b'', b'', (), (), self._output_formats),
			# The "limit" is defined in the fetch query.
			pq.element.Execute(b'', 0xFFFFFFFF),
		)

class ServerDeclaredCursor(DeclaredCursor):
	cursor_type = 'server'

	def _init(self):
		# scroll and hold are unknown, so assume them to be true.
		# This means that fetch-ahead is disabled.
		self.scroll = True
		self.with_hold = True
		self.chunksize = 0
		##
		# The portal description is needed, so get it.
		return TupleCursor._init(
			self, (pq.element.DescribePortal(self._pq_cursor_id),)
		)

	def _fini(self):
		if self._xact.state is not pq.Complete:
			if self.database._pq_xact is not self._xact:
				self.database._pq_push(self._xact)
			self.database._pq_complete()
		for m in self._xact.messages_received():
			if m.type is pq.element.TupleDescriptor.type:
				self._output = m
				self._output_attmap = \
					self.database.typio.attribute_map(self._output)
				# tuple output
				self._output_io = self.database.typio.resolve_descriptor(
					self._output, 1 # (input, output)[1]
				)
				self._output_formats = [
					pq.element.StringFormat
					if x is None
					else pq.element.BinaryFormat
					for x in self._output_io
				]
				self._output_io = tuple([
					x or self.database.typio.decode for x in self._output_io
				])
				super()._fini()
		# Done with the first transaction.
		self._xact = None
		if self.closed:
			e = pg_exc.OperationError("failed to discover cursor output")
			self.ife_descend(e)
			e.raise_exception()

class UtilityCursor(CursorStrategy):
	cursor_type = 'utility'

	def _init(self):
		self._xact = pq.Instruction((
			pq.element.Bind(
				b'',
				self.statement._pq_statement_id,
				self.statement._input_formats,
				self._pq_parameters(),
				(),
			),
			pq.element.Execute(b'', 1),
			pq.element.SynchronizeMessage,
		))
		self.ife_descend(self._xact)

		self.database._pq_push(self._xact)
		while self._xact.state != pq.Complete:
			# in case it's a copy
			self.database._pq_step()
			for x in self._xact.messages_received():
				if x.type is pq.element.CopyToBegin.type:
					self.__class__ = CopyCursor
					return self._init()
					# The COPY TO STDOUT transaction terminates the loop
					# *without* finishing the transaction.
					# Buffering all of the COPY data would be a bad idea(tm).
					##
				elif x.type in pq.element.Null.type:
					break
				elif x.type is pq.element.Complete.type:
					self._complete_message = x
		self._fini()

class CopyCursor(ReadableCursor):
	cursor_type = 'copy'

	def _init(self):
		x = self._xact
		super()._init()
		self._xact = x
		self._last_extension = None
		self._last_direction = True
		self._this_direction = True
		self._fini()

	def _expansion(self):
		ms = self._xact.completed[0][1]
		if ms:
			if type(ms[0]) is bytes and type(ms[-1]) is bytes:
				return ms
		return [
			y for y in ms if type(y) is bytes
		]

	def _expand(self):
		if self._xact.completed:
			if self._last_extension is self._xact.completed[0]:
				del self._xact.completed[0]
				if not self._xact.completed:
					return
			expansions = self._expansion()
			if self._buffer:
				buffer = self._buffer + expansions
			else:
				buffer = expansions
			l = len(expansions)
			# There is no reqsize, so reveal the end of the
			# copy when the last_increase is zero *and*
			# the transaction is over.
			self.__dict__.update({
				'_buffer' : buffer,
				'_last_extension' : self._xact.completed[0],
				'_last_increase' : l,
				'_last_reqsize' : \
					-1 if l == 0 and self._xact.state is pq.Complete else l
			})

	def _dispatch_for_more(self, direction):
		# nothing to do for copies
		pass

	def _buffer_more(self, quantity, direction):
		"""
		[internal] helper function to put more copy data onto the buffer for
		reading. This function will only append to the buffer and never
		set the offset.

		Used to support ``COPY ... TO STDOUT ...;``
		"""
		if direction is not True:
			e = pg_exc.OperationError(
				"cannot read COPY backwards"
			)
			self.ife_descend(e)
			e.raise_exception()

		while self.database._pq_xact is self._xact and not self._xact.completed:
			self.database._pq_step()

		# Find the Complete message when _pq_xact is done
		if self._xact.state is pq.Complete and self._complete_message is None:
			i = self._xact.reverse()
			ready = next(i)
			complete = next(i)
			self._complete_message = complete

		if not self._xact.completed:
			# completed list depleted, can't buffer more
			self._last_increase = 0
			return 0

		bufsize = len(self._buffer)
		self._expand()
		newbufsize = len(self._buffer)
		return newbufsize - bufsize

class PreparedStatement(pg_api.PreparedStatement):
	ife_ancestor = None
	string = None
	database = None
	statement_id = None
	_input = None
	_output = None
	_output_io = None
	_output_formats = None
	_output_attmap = None

	@classmethod
	def from_string(
		typ,
		string : "SQL statement to prepare",
		database : "database reference to bind the statement to",
		statement_id : "statement_id to use instead of generating one" = None
	) -> "PreparedStatement instance":
		"""
		Create a PreparedStatement from a query string.
		"""
		r = super().__new__(typ)
		r.string = string
		r.__init__(statement_id or ID(r), database)
		return r

	def __init__(self, statement_id, database):
		if not statement_id:
			raise ValueError("invalid statement identifier, " + repr(cursor_id))
		self.statement_id = statement_id
		self.database = database
		self._pq_xact = None
		self._pq_statement_id = None
		self.closed = None
		if ID(self) == self.statement_id:
			addgarbage = self.database._closestatements.append
			typio = self.database.typio
			sid = self.statement_id
			self._del = weakref.ref(
				self, lambda _: addgarbage(typio.encode(sid))
			)

	def __repr__(self):
		return '<{mod}.{name}[{ci}] {state}>'.format(
			mod = type(self).__module__,
			name = type(self).__name__,
			ci = self.database.connector._pq_iri,
			state = self.state,
		)

	def _raise_parameter_tuple_error(self, procs, tup, itemnum):
		typ = self.database.typio.sql_type_from_oid(
			self.pg_parameter_types[itemnum]
		) or '<unknown>'

		data = repr(tup[itemnum])
		if len(data) > 80:
			# Be sure not to fill screen with noise.
			data = data[:75] + ' ...'
		te = pg_exc.ParameterError(
			"failed to pack parameter %s::%s for transfer" %(
				('$' + str(itemnum + 1)), typ,
			),
			details = {
				'data': data,
				'hint' : "Try casting the parameter to 'text', then to the target type."
			},
		)
		te.index = itemnum
		self.ife_descend(te)
		te.raise_exception()

	def _raise_column_tuple_error(self, procs, tup, itemnum):
		typ = self.database.typio.sql_type_from_oid(
			self.pg_column_types(itemnum)
		) or '<unknown>'

		data = repr(tup[itemnum])
		if len(data) > 80:
			# Be sure not to fill screen with noise.
			data = data[:75] + ' ...'
		te = pg_exc.ColumnError(
			"failed to unpack column %r, %s::%s, from wire data" %(
				itemnum, self.column_names[itemnum], typ
			),
			details = {
				'data': data,
				'hint' : "Try casting the column to 'text'."
			},
		)
		te.index = itemnum
		self.ife_descend(te)
		te.raise_exception()

	@property
	def state(self) -> str:
		if self.closed:
			if self._pq_xact is not None:
				if self.string is not None:
					return 'parsing'
				else:
					return 'describing'
			return 'closed'
		return 'prepared'

	@property
	def column_names(self):
		if self.closed is None:
			self._fini()
		if self._output is not None:
			return list(self.database.typio.decodes(self._output.keys()))

	@property
	def parameter_types(self):
		if self.closed is None:
			self._fini()
		if self._input is not None:
			return [self.database.typio.type_from_oid(x) for x in self._input]

	@property
	def column_types(self):
		if self.closed is None:
			self._fini()
		if self._output is not None:
			return [
				self.database.typio.type_from_oid(x[3]) for x in self._output
			]

	@property
	def pg_parameter_types(self):
		if self.closed is None:
			self._fini()
		return self._input

	@property
	def pg_column_types(self):
		if self.closed is None:
			self._fini()
		if self._output is not None:
			return [x[3] for x in self._output]

	@property
	def sql_column_types(self):
		if self.closed is None:
			self._fini()
		if self._output is not None:
			return [
				pg_types.oid_to_sql_name.get(x) or \
				self.database.typio.sql_type_from_oid(x)
				for x in self.pg_column_types
			]

	@property
	def sql_parameter_types(self):
		if self.closed is None:
			self._fini()
		if self._input is not None:
			return [
				pg_types.oid_to_sql_name.get(x) or \
				self.database.typio.sql_type_from_oid(x)
				for x in self.pg_parameter_types
			]

	def close(self):
		if not (self.closed is True):
			self.database._closestatements.append(self._pq_statement_id)
		self.closed = True
		if hasattr(self, '_del'):
			del self._del

	def ife_snapshot_text(self):
		s = ""
		s += "[" + self.state + "] "
		if self.ife_object_title != pg_api.InterfaceElement.ife_object_title:
			s += self.ife_object_title + ", "
		s += "statement_id(" + repr(self.statement_id) + ")"
		s += os.linesep*2 + ' '*2 + (os.linesep + ' ' * 2).join(
			str(self.string).split(os.linesep)
		) + os.linesep
		return s

	def _init(self):
		"""
		Push initialization messages to the server, but don't wait for
		the return as there may be things that can be done while waiting
		for the return. Use the _fini() to complete.
		"""
		self._pq_statement_id = self.database.typio._encode(
			self.statement_id
		)[0]
		if self.string is not None:
			q = self.database.typio._encode(self.string)[0]
			cmd = [
				pq.element.CloseStatement(self._pq_statement_id),
				pq.element.Parse(self._pq_statement_id, q, ()),
			]
		else:
			cmd = []
		cmd.extend(
			(
				pq.element.DescribeStatement(self._pq_statement_id),
				pq.element.SynchronizeMessage,
			)
		)
		self._pq_xact = pq.Instruction(cmd)
		self.ife_descend(self._pq_xact)
		self.database._pq_push(self._pq_xact)

	def _fini(self):
		"""
		Complete initialization that the _init() method started.
		"""
		# assume that the transaction has been primed.
		if self._pq_xact is None:
			raise RuntimeError("_fini called prior to _init; invalid state")
		if self._pq_xact is self.database._pq_xact:
			self.database._pq_complete()

		(*head, argtypes, tupdesc, last) = self._pq_xact.messages_received()

		if tupdesc is None or tupdesc is pq.element.NoDataMessage:
			# Not typed output.
			self._output = None
			self._output_attmap = None
			self._output_io = None
			self._output_formats = None
		else:
			self._output = tupdesc
			self._output_attmap = dict(
				self.database.typio.attribute_map(tupdesc)
			)
			# tuple output
			self._output_io = \
				self.database.typio.resolve_descriptor(tupdesc, 1)
			self._output_formats = [
				pq.element.StringFormat
				if x is None
				else pq.element.BinaryFormat
				for x in self._output_io
			]
			self._output_io = tuple([
				x or self.database.typio.decode for x in self._output_io
			])

		self._input = argtypes
		packs = []
		formats = []
		for x in argtypes:
			pack = (self.database.typio.resolve(x) or (None,None))[0]
			packs.append(pack or self.database.typio.encode)
			formats.append(
				pq.element.StringFormat
				if x is None
				else pq.element.BinaryFormat
			)
		self._input_io = tuple(packs)
		self._input_formats = formats
		self.closed = False
		self._pq_xact = None

	def _cursor(self, *parameters, **kw):
		if self.closed is None:
			self._fini()
		# Tuple output per the results of DescribeStatement.
		##
		cursor = Cursor.from_statement(parameters, self, **kw)
		self.ife_descend(cursor)
		cursor._init()
		return cursor

	def __call__(self, *parameters):
		# get em' all!
		c = self._cursor(*parameters, with_hold = False, scroll = False)
		if isinstance(c, UtilityCursor):
			return (c.command(), c.count())
		else:
			return c.read()

	def declare(self, *parameters):
		return self._cursor(*parameters, scroll = True, with_hold = True)

	def chunks(self, *parameters, chunksize = 256):
		if chunksize < 1:
			raise ValueError("cannot create chunk iterator with chunksize < 1")
		c = self._cursor(*parameters, scroll = False)
		c.chunksize = chunksize
		return Chunks(c)

	def rows(self, *parameters):
		return chain.from_iterable(self.chunks(*parameters))
	__iter__ = rows

	def first(self, *parameters):
		if self.closed is None:
			self._fini()
		# Parameters? Build em'.
		c = self.database

		if self._input_io:
			params = pg_typio.process_tuple(
				self._input_io, parameters,
				self._raise_parameter_tuple_error
			)
		else:
			params = ()

		# Run the statement
		x = pq.Instruction((
			pq.element.Bind(
				b'',
				self._pq_statement_id,
				self._input_formats,
				params,
				self._output_formats,
			),
			pq.element.Execute(b'', 0xFFFFFFFF),
			pq.element.SynchronizeMessage
		))
		self.ife_descend(x)
		c._pq_push(x)
		c._pq_complete()

		if self._output_io:
			##
			# Look for the first tuple.
			for xt in x.messages_received():
				if xt.type is pq.element.Tuple.type:
					break
			else:
				return None

			if len(self._output_io) > 1:
				return pg_types.Row.from_sequence(
					self._output_attmap,
					pg_typio.process_tuple(
						self._output_io, xt,
						self._raise_column_tuple_error
					),
				)
			else:
				if xt[0] is None:
					return None
				io = self._output_io[0] or self.database.typio.decode
				return io(xt[0])
		else:
			##
			# It doesn't return rows, so return a count.
			for cm in x.messages_received():
				if getattr(cm, 'type', None) == pq.element.Complete.type:
					break
			else:
				# Probably a Null command.
				return None
			return cm.extract_count() or cm.extract_command()

	def _copy_data_in(self,
		iterable,
		tps : "tuples per *set*" = None,
	):
		"""
		Given an iterable, execute the COPY ... FROM STDIN statement and
		send the copy lines produced by the iterable to the remote end.

		`tps` is the number of tuples to buffer prior to giving the data
		to the socket's send.
		"""
		tps = tps or 500
		x = pq.Instruction((
			pq.element.Bind(
				b'',
				self._pq_statement_id,
				(), (), (),
			),
			pq.element.Execute(b'', 1),
			pq.element.SynchronizeMessage,
		))
		self.ife_descend(x)
		self.database._pq_push(x)

		# Get the COPY started.
		while x.state is not pq.Complete:
			self.database._pq_step()
			if hasattr(x, 'CopyFailSequence') and x.messages is x.CopyFailSequence:
				break
		else:
			# Oh, it's not a COPY at all.
			e = pg_exc.OperationError(
				"_copy_data_in() used on a non-COPY FROM STDIN query",
			)
			x.ife_descend(e)
			e.raise_exception()

		# Make a Chunks object.
		if isinstance(iterable, CopyCursor):
			iterable = Chunks(iterable)

		if isinstance(iterable, Chunks):
			# optimized, each iteration == row sequence
			while x.messages:
				while x.messages is not x.CopyFailSequence:
					self.database._pq_step()
				x.messages = []
				for rows in iterable:
					x.messages.extend([
						pq.element.CopyData(l) for l in rows
					])
					if len(x.messages) > tps:
						break
		else:
			# each iteration == one row
			iterable = iter(iterable)
			while x.messages:
				# Process any messages setup for sending.
				while x.messages is not x.CopyFailSequence:
					self.database._pq_step()
				x.messages = [
					pq.element.CopyData(l) for l in islice(iterable, tps)
				]
		x.messages = x.CopyDoneSequence
		self.database._pq_complete()

	def _load_bulk_tuples(self, tupleseq, tps = None):
		tps = tps or 64
		if isinstance(tupleseq, Chunks):
			tupleseqiter = chain.from_iterable(tupleseq)
		else:
			tupleseqiter = iter(tupleseq)
		pte = self._raise_parameter_tuple_error
		last = pq.element.FlushMessage
		try:
			while last is pq.element.FlushMessage:
				c = 0
				xm = []
				for t in tupleseqiter:
					params = pg_typio.process_tuple(
						self._input_io, tuple(t), pte
					)
					xm.extend((
						pq.element.Bind(
							b'',
							self._pq_statement_id,
							self._input_formats,
							params,
							(),
						),
						pq.element.Execute(b'', 1),
					))
					if c == tps:
						break
					c += 1
				else:
					last = pq.element.SynchronizeMessage
				xm.append(last)
				self.database._pq_push(pq.Instruction(xm))
			self.database._pq_complete()
		except:
			##
			# In cases where row packing errors or occur,
			# synchronize, finishing any pending transaction,
			# and raise the error.
			##
			# If the data sent to the remote end is invalid,
			# _complete will raise the exception and the current
			# exception being marked as the cause, so there should
			# be no [exception] information loss.
			##
			self.database.synchronize()
			raise

	def load(self, iterable, tps = None):
		"""
		Execute the query for each parameter set in `iterable`.

		In cases of ``COPY ... FROM STDIN``, iterable must be an iterable `bytes`.
		"""
		if self.closed is None:
			self._fini()
		if not self._input:
			return self._copy_data_in(iterable, tps = tps)
		else:
			return self._load_bulk_tuples(iterable, tps = tps)

class StoredProcedure(pg_api.StoredProcedure):
	ife_ancestor = None
	procedure_id = None

	def ife_snapshot_text(self):
		return self.procedure_id

	def __repr__(self):
		return '<%s:%s>' %(
			self.procedure_id, self.statement.string
		)

	def __call__(self, *args, **kw):
		if kw:
			input = []
			argiter = iter(args)
			try:
				word_idx = [(kw[k], self._input_attmap[k]) for k in kw]
			except KeyError as k:
				raise TypeError("%s got unexpected keyword argument %r" %(
						self.name, k.message
					)
				)
			word_idx.sort(key = get1)
			current_word = word_idx.pop(0)
			for x in range(argc):
				if x == current_word[1]:
					input.append(current_word[0])
					current_word = word_idx.pop(0)
				else:
					input.append(argiter.next())
		else:
			input = args

		if self.srf is True:
			if self.composite is True:
				return self.statement.rows(*input)
			else:
				# A generator expression is very appropriate here
				# as SRFs returning large number of rows would require
				# substantial amounts of memory.
				return map(get0, self.statement.rows(*input))
		else:
			if self.composite is True:
				return self.statement(*input)[0]
			else:
				return self.statement(*input)[0][0]

	def __init__(self, ident, database, description = ()):
		# Lookup pg_proc on database.
		if isinstance(ident, int):
			proctup = database.prepare(
				ProcedureLookup + ' WHERE pg_proc.oid = $1',
			).first(int(ident))
		else:
			proctup = database.prepare(
				ProcedureLookup + ' WHERE pg_proc.oid = regprocedurein($1)',
			).first(ident)
		if proctup is None:
			raise LookupError("no function with identifier %s" %(str(ident),))

		self.procedure_id = proctup["procedure_id"]
		self.oid = proctup[0]
		self.name = proctup["proname"]

		self._input_attmap = {}
		argnames = proctup.get('proargnames') or ()
		for x in range(len(argnames)):
			an = argnames[x]
			if an is not None:
				self._input_attmap[an] = x

		tio = database.typio
		proargs = proctup['proargtypes']
		for x in proargs:
			tio.resolve(x)

		self.statement = database.prepare(
			"SELECT * FROM %s(%s) AS func%s" %(
				proctup['_proid'],
				# ($1::type, $2::type, ... $n::type)
				', '.join([
					 '$%d::%s' %(x + 1, tio.sql_type_from_oid(proargs[x]))
					 for x in range(len(proargs))
				]),
				# Description for anonymous record returns
				(description and \
					'(' + ','.join(description) + ')' or '')
			)
		)
		self.ife_descend(self.statement)
		self.srf = bool(proctup.get("proretset"))
		self.composite = proctup["composite"]

class SettingsCM(object):
	def __init__(self, database, settings_to_set):
		self.database = database
		self.settings_to_set = settings_to_set

	def __context__(self):
		return self

	def __enter__(self):
		if hasattr(self, 'stored_settings'):
			raise RuntimeError("cannot re-use setting CMs")
		self.stored_settings = self.database.settings.getset(
			self.settings_to_set.keys()
		)
		self.database.settings.update(self.settings_to_set)

	def __exit__(self, typ, val, tb):
		self.database.settings.update(self.stored_settings)

class Settings(pg_api.Settings):
	ife_ancestor = property(attrgetter('database'))

	def __init__(self, database):
		self.database = database
		self.cache = {}

	def _clear_cache(self):
		self.cache.clear()

	def __getitem__(self, i):
		v = self.cache.get(i)
		if v is None:
			db = self.database
			r = db.prepare(
				"SELECT setting FROM pg_settings WHERE name = $1",
			)(i)

			if r:
				v = r[0][0]
			else:
				raise KeyError(i)
		return v

	def __setitem__(self, i, v):
		cv = self.cache.get(i)
		if cv == v:
			return

		setas = self.database.prepare(
			"SELECT set_config($1, $2, false)",
		).first(i, v)
		self.cache[i] = setas

	def __delitem__(self, k):
		self.database.execute(
			'RESET "' + k.replace('"', '""') + '"'
		)
		self.cache.pop(k, None)

	def __len__(self):
		return self.database.prepare("SELECT count(*) FROM pg_settings").first()

	def ife_snapshot_text(self):
		return "Settings"

	def __call__(self, **settings):
		return SettingsCM(self.database, settings)

	def path():
		def fget(self):
			return pg_str.split_ident(self["search_path"])
		def fset(self, value):
			self.settings['search_path'] = ','.join([
				'"%s"' %(x.replace('"', '""'),) for x in value
			])
		def fdel(self):
			if self.database.connector.path is not None:
				self.path = self.database.connector.path
			else:
				self.database.execute("RESET search_path")
		doc = 'structured search_path interface'
		return locals()
	path = property(**path())

	def get(self, k, alt = None):
		if k in self.cache:
			return self.cache[k]

		db = self.database
		r = db.prepare(
			"SELECT setting FROM pg_settings WHERE name = $1",
		)(k)
		if r:
			v = r[0][0]
			self.cache[k] = v
		else:
			v = alt
		return v

	def getset(self, keys):
		setmap = {}
		rkeys = []
		for k in keys:
			v = self.cache.get(k)
			if v is not None:
				setmap[k] = v
			else:
				rkeys.append(k)

		if rkeys:
			r = self.database.prepare(
				"SELECT name, setting FROM pg_settings WHERE name = ANY ($1)",
			)(rkeys)
			self.cache.update(r)
			setmap.update(r)
			rem = set(rkeys) - set([x['name'] for x in r])
			if rem:
				raise KeyError(rem)
		return setmap

	def keys(self):
		return map(
			get0, self.database.prepare(
				"SELECT name FROM pg_settings ORDER BY name",
			).rows()
		)
	__iter__ = keys

	def values(self):
		return map(
			get0, self.database.prepare(
				"SELECT setting FROM pg_settings ORDER BY name",
			).rows()
		)

	def items(self):
		return self.database.prepare(
			"SELECT name, setting FROM pg_settings ORDER BY name",
		).rows()

	def update(self, d):
		kvl = [list(x) for x in d.items()]
		self.cache.update(
			self.database.prepare(
				"SELECT ($1::text[][])[i][1] AS key, " \
				"set_config(($1::text[][])[i][1], $1[i][2], false) AS value " \
				"FROM generate_series(1, array_upper(($1::text[][]), 1)) g(i)",
			)(kvl)
		)

	def _notify(self, msg):
		subs = getattr(self, '_subscriptions', {})
		d = self.database.typio._decode
		key = d(msg.name)[0]
		val = d(msg.value)[0]
		for x in subs.get(key, ()):
			x(self.database, key, val)
		if None in subs:
			for x in subs[None]:
				x(self.database, key, val)
		self.cache[key] = val

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
		subs = self._subscriptions = getattr(self, '_subscriptions', {})
		callbacks = subs.setdefault(key, [])
		if callback not in callbacks:
			callbacks.append(callback)

	def unsubscribe(self, key, callback):
		"""
		Stop listening for changes to a setting. The setting name(`key`), and
		the callback used to subscribe must be given again for successful
		termination of the subscription.

		>>> db.settings.unsubscribe('TimeZone', watch)
		"""
		subs = getattr(self, '_subscriptions', {})
		callbacks = subs.get(key, ())
		if callback in callbacks:
			callbacks.remove(callback)

class Transaction(pg_api.Transaction):
	ife_ancestor = property(attrgetter('database'))
	database = None

	mode = None
	isolation = None
	gid = None

	def __init__(self, database, gid = None, isolation = None, mode = None):
		self.database = database
		self.gid = gid
		self.isolation = isolation
		self.mode = mode
		self.state = 'initialized'
		self.type = None

	def __context__(self):
		return self

	def __enter__(self):
		self.start()
		return self

	def __exit__(self, typ, value, tb):
		if typ is None:
			# No exception, but in a failed transaction?
			if self.database._pq_state == b'E':
				err = pg_exc.InFailedTransactionError(
					"invalid transaction block exit detected",
					source = 'CLIENT',
					details = {
						'cause': \
							'Database was in an error-state, ' \
							'but no exception was raised.'
					},
				)
				self.ife_descend(err)
				if not self.database.closed:
					self.rollback()
				err.raise_exception()
			else:
				# No exception, and no error state. Everything is good.
				try:
					self.commit()
					# If an error occurs, clean up the transaction state
					# and raise as needed.
				except pg_exc.ActiveTransactionError as err:
					##
					# Failed COMMIT PREPARED <gid>?
					# Likely cases:
					#  - User exited block without preparing the transaction.
					##
					if not self.database.closed and self.gid is not None:
						# adjust the state so rollback will do the right thing and abort.
						self.state = 'open'
						self.rollback()
					##
					# The other exception that *can* occur is
					# UndefinedObjectError in which:
					#  - User issued C/RB P <gid> before exit, but not via xact methods.
					#  - User adjusted gid after prepare().
					#
					# But the occurrence of this exception means it's not in an active
					# transaction, which means no cleanup other than raise is necessary.
					err.details['cause'] = \
						"The prepared transaction was not " \
						"prepared prior to the block's exit."
					raise
		else:
			# There's an exception, so only rollback if the connection
			# exists. If the rollback() was called here, it would just
			# contribute noise to the error.
			if not self.database.closed:
				self.rollback()

	def ife_snapshot_text(self):
		content = filter(
			partial(is_not, None), [
				None if self.isolation is None else (
					' ISOLATION: ' + repr(self.isolation)
				),
				None if self.gid is None else (
					' GID: ' + repr(self.gid)
				),
				None if self.mode is None else (
					' MODE: ' + repr(self.mode)
				),
			]
		)
		return (
			(self.type + ' ' if self.type else '') + self.state + \
			(os.linesep if content else "") + os.linesep.join(content)
		)

	@staticmethod
	def _start_xact_string(isolation = None, mode = None):
		q = 'START TRANSACTION'
		if isolation is not None:
			if ';' in isolation:
				raise ValueError("invalid transaction isolation " + repr(mode))
			q += ' ISOLATION LEVEL ' + isolation
		if mode is not None:
			if ';' in mode:
				raise ValueError("invalid transaction mode " + repr(isolation))
			q += ' ' + mode
		return q + ';'

	@staticmethod
	def _savepoint_xact_string(id):
		return 'SAVEPOINT "' + id.replace('"', '""') + '";'

	def start(self):
		if self.state == 'open':
			return
		if self.state != 'initialized':
			err = pg_exc.OperationError(
				"transactions cannot be restarted",
				details = {
					'hint': \
					'Create a new transaction object instead of re-using an old one.'
				}
			)
			self.ife_descend(err)
			err.raise_exception()

		if self.database._pq_state == b'I':
			self.type = 'block'
			q = self._start_xact_string(
				isolation = self.isolation,
				mode = self.mode,
			)
		else:
			self.type = 'savepoint'
			if (self.gid, self.isolation, self.mode) != (None,None,None):
				err = pg_exc.OperationError(
					"configured transaction used inside a transaction block",
					details = {
						'cause': 'A transaction block was already started.'
					}
				)
				self.ife_descend(err)
				err.raise_exception()
			q = self._savepoint_xact_string(hex(id(self)))
		self.database.execute(q)
		self.state = 'open'
	begin = start

	@staticmethod
	def _prepare_string(id):
		"2pc prepared transaction 'gid'"
		return "PREPARE TRANSACTION '" + id.replace("'", "''") + "';"

	@staticmethod
	def _release_string(id):
		'release "";'
		return 'RELEASE "xact(' + id.replace('"', '""') + ')";'

	def prepare(self):
		if self.state == 'prepared':
			return
		if self.state != 'open':
			err = pg_exc.OperationError(
				"transaction state must be 'open' in order to prepare",
			)
			self.ife_descend(err)
			err.raise_exception()
		if self.type != 'block':
			err = pg_exc.OperationError(
				"improper transaction type to prepare",
			)
			self.ife_descend(err)
			err.raise_exception()
		q = self._prepare_string(self.gid)
		self.database.execute(q)
		self.state = 'prepared'

	def recover(self):
		if self.state != 'initialized':
			err = pg_exc.OperationError(
				"improper state for prepared transaction recovery",
			)
			self.ife_descend(err)
			err.raise_exception()
		if self.database.prepare(TransactionIsPrepared).first(self.gid):
			self.state = 'prepared'
			self.type = 'block'
		else:
			err = pg_exc.UndefinedObjectError(
				"prepared transaction does not exist",
				source = 'CLIENT',
			)
			self.ife_descend(err)
			err.raise_exception()

	def commit(self):
		if self.state == 'committed':
			return
		if self.state not in ('prepared', 'open'):
			err = pg_exc.OperationError(
				"commit attempted on transaction with unexpected state, " + repr(self.state),
			)
			self.ife_descend(err)
			err.raise_exception()

		if self.type == 'block':
			if self.gid is not None:
				# User better have prepared it.
				q = "COMMIT PREPARED '" + self.gid.replace("'", "''") + "';"
			else:
				q = 'COMMIT'
		else:
			if self.gid is not None:
				err = pg_exc.OperationError(
					"savepoint configured with global identifier",
					details = {
						'cause': "Prepared transaction started inside transaction block?"
					}
				)
				self.ife_descend(err)
				err.raise_exception()
			q = self._release_string(hex(id(self)))
		self.database.execute(q)
		self.state = 'committed'

	@staticmethod
	def _rollback_to_string(id):
		return 'ROLLBACK TO "' + id.replace('"', '""') + '";'

	def rollback(self):
		if self.state == 'aborted':
			return
		if self.state not in ('prepared', 'open'):
			err = pg_exc.OperationError(
				"aborted attempted on transaction with unexpected state, " \
				+ repr(self.state),
			)
			self.ife_descend(err)
			err.raise_exception()

		if self.type == 'block':
			if self.state == 'prepared':
				q = "ROLLBACK PREPARED '" + self.gid.replace("'", "''") + "'"
			else:
				q = 'ABORT;'
		elif self.type == 'savepoint':
			q = self._rollback_to_string(hex(id(self)))
		else:
			raise RuntimeError("unknown transaction type " + repr(self.type))
		self.database.execute(q)
		self.state = 'aborted'
	abort = rollback

class Connection(pg_api.Connection):
	ife_ancestor = property(attrgetter('connector'))
	connector = None

	type = None
	version_info = None
	version = None

	security = None
	backend_id = None
	client_address = None
	client_port = None

	# Replaced with instances on connection instantiation.
	settings = Settings

	_socketfactory = None

	_tracer = None
	def tracer():
		def fget(self):
			return self._tracer
		def fset(self, value):
			self._tracer = value
			self._write_messages = self._traced_write_messages
			self._read_messages = self._traced_read_messages
		def fdel(self):
			del self._tracer
			self._write_messages = self._standard_write_messages
			self._read_messages = self._standard_read_messages
		doc = 'Callable object to pass protocol trace strings to. '\
			'(Normally a write method.)'
		return locals()
	tracer = property(**tracer())

	def _decode_pq_message(self, msg):
		'[internal] decode the values in a message(E,N)'
		dmsg = {}
		for k, v in msg.items():
			try:
				# code should always be ascii..
				if k == "code":
					v = v.decode('ascii')
				else:
					v = self.typio._decode(v)[0]
			except UnicodeDecodeError:
				# Fallback to the bytes representation.
				# This should be sufficiently informative in most cases,
				# and in the cases where it isn't, an element traceback should
				# yield the pertinent information
				v = repr(v)[2:-1]
			dmsg[k] = v
		return dmsg

	def _N(self, msg, xact):
		'[internal] emit a `Message` via'
		dmsg = self._decode_pq_message(msg)
		m = dmsg.pop('message')
		c = dmsg.pop('code')
		if dmsg['severity'].upper() == 'WARNING':
			mo = pg_exc.WarningLookup(c)(m, code = c, details = dmsg)
		else:
			mo = pg_api.Message(m, code = c, details = dmsg)
		mo.database = self
		xact.ife_descend(mo)
		mo.emit()

	def _A(self, msg, xact):
		'[internal] Send notification to any listeners; NOTIFY messages'
		subs = getattr(self, '_subscriptions', {})
		for x in subs.get(msg.relation, ()):
			x(self, msg)
		if None in subs:
			subs[None](self, msg)

	def _S(self, msg, xact):
		'[internal] Receive ShowOption message'
		self.settings._notify(msg)

	def _update_encoding(database, key, value):
		'[internal] subscription method to client_encoding on settings'
		database.typio.set_encoding(value)
	_update_encoding = staticmethod(_update_encoding)

	def __repr__(self):
		return '<%s.%s[%s] %s>' %(
			type(self).__module__,
			type(self).__name__,
			self.connector._pq_iri,
			self.closed and 'closed' or '%s' %(self._pq_state,)
		)

	def ife_snapshot_text(self):
		lines = [
			('version', repr(self.version) if self.version else "<unknown>"),
			('backend_id',
				repr(self.backend_id) if self.backend_id else "<unknown>"),
			('client_address', str(self.client_address)),
			('client_port', str(self.client_port)),
		]
		# settings state
		return "[" + self.state + "] " + (
			str(self._socketfactory)
		) + (
			"" + os.linesep + '  ' + (
				(os.linesep + '  ').join([x[0] + ': ' + x[1] for x in lines])
			)
		)

	def __context__(self):
		return self

	def __exit__(self, type, value, tb):
		'Close the connection on exit.'
		self.close()

	def synchronize(self):
		"""
		Explicitly send a Synchronize message to the backend.
		Useful for forcing the completion of lazily processed transactions.
		[Avoids garbage collection not pushing; _complete, then set _xact.]
		"""
		if self._pq_xact is not None:
			self._pq_complete()
		x = pq.Instruction((pq.element.SynchronizeMessage,))
		self._pq_xact = x
		self._pq_complete()

	def interrupt(self, timeout = None):
		cq = pq.element.CancelRequest(
			self.backend_id, self._pq_killinfo.key
		).bytes()
		# XXX: doesn't use SSL :(
		s = self._socketfactory(timeout = timeout)
		try:
			s.sendall(cq)
		finally:
			s.close()

	def execute(self, query : str) -> None:
		q = pq.Instruction((
			pq.element.Query(self.typio._encode(query)[0]),
		))
		self.ife_descend(q)
		self._pq_push(q)
		self._pq_complete()

	def xact(self, gid = None, isolation = None, mode = None):
		x = Transaction(self, gid = gid, isolation = isolation, mode = mode)
		return x

	def prepare(self,
		sql_statement_string : str,
		statement_id = None,
	) -> PreparedStatement:
		ps = PreparedStatement.from_string(sql_statement_string, self)
		self.ife_descend(ps)
		ps._init()
		if self._pq_state != b'I':
			ps._fini()
		return ps

	def statement_from_id(self, statement_id : str) -> PreparedStatement:
		ps = PreparedStatement(statement_id, self)
		self.ife_descend(ps)
		ps._init()
		if self._pq_state != b'I':
			ps._fini()
		return ps

	def proc(self, proc_id : (str, int)) -> StoredProcedure:
		sp =  StoredProcedure(proc_id, self)
		self.ife_descend(sp)
		return sp

	def cursor_from_id(self, cursor_id : str) -> Cursor:
		c = Cursor(cursor_id, self)
		self.ife_descend(c)
		c._init()
		if self._pq_state != b'I':
			c._fini()
		return c

	def close(self):
		if self.closed:
			return
		# Write out the disconnect message if the socket is around.
		# If the connection is known to be lost, don't bother. It will
		# generate an extra exception.
		self._pq_xact = self._pq_closed_xact
		try:
			if self.socket is not None and self._pq_state != 'LOST':
				self._write_messages((pq.element.DisconnectMessage,))
		finally:
			if self.socket is not None:
				self.socket.close()
			# the data in the closed connection transaction is in utf-8.
			self.typio.set_encoding('utf-8')

	@property
	def closed(self) -> bool:
		return isinstance(self._pq_xact, ClosedConnection) \
		or self._pq_state in ('LOST', None)

	@property
	def state(self) -> str:
		if isinstance(self._pq_xact, pq.Negotiation):
			return 'negotiating'
		if self.version is None:
			if self.socket is not None:
				return 'ready'
			else:
				return 'disconnected'
		if self.closed:
			return 'closed'
		return 'connected'

	def reset(self):
		"""
		restore original settings, reset the transaction, drop temporary
		objects.
		"""
		self.execute("ABORT; RESET ALL;")

	def connect(self, timeout = None):
		'Establish the connection to the server'
		# already connected?
		if self.closed is False:
			return
		else:
			if self._pq_xact is not None:
				err = pg_exc.OperationError(
					"dead connection",
					details = {
						'hint' : 'Try creating a new connection instead.'
					}
				)
				self.ife_descend(err)
				err.raise_exception()
		self.typio.set_encoding('ascii')

		timeout = timeout or self.connector.connect_timeout
		sslmode = self.connector.sslmode or 'prefer'

		connection_failures = []
		socket_makers = ()

		try:
			# get the list of sockets to try
			socket_makers = self.connector.socket_factory_sequence()
		except Exception as exc:
			err = pg_exc.ClientCannotConnectError(
				"failed to resolve socket makers",
				details = {
					"severity" : "FATAL",
				},
				source = 'CLIENT'
			)
			self.ife_descend(err)
			err.database = self
			err.set_connection_failures(connection_failures)
			err.raise_exception(raise_from = exc)

		# resolve when to do SSL.
		with_ssl = zip(repeat(True, len(socket_makers)), socket_makers)
		without_ssl = zip(repeat(False, len(socket_makers)), socket_makers)
		if sslmode == 'allow':
			# first, without ssl, then with. :)
			socket_makers = list(interlace(
				without_ssl, with_ssl
			))
		elif sslmode == 'prefer':
			# first, with ssl, then without. :)
			socket_makers = list(interlace(
				with_ssl, without_ssl
			))
			# prefer is special, because it *may* be possible to
			# skip the subsequent "without" in situations SSL is off.
		elif sslmode == 'require':
			# the above insanity is not required here
			# as if the ssl handshake fails on prefer
			# it's not a pqv3 server.
			socket_makers = list(with_ssl)
		elif sslmode == 'disable':
			socket_makers = list(without_ssl)
		else:
			raise ValueError("invalid sslmode {0!r}".format(sslmode))

		# can_skip is used when 'prefer' is the sslmode.
		# if the ssl negotiation returns 'N' (nossl), then
		# ssl "failed", but the socket is still usable for nossl.
		# in these cases, can_skip is set to True so that the
		# subsequent non-ssl attempt is skipped.
		can_skip = False

		# for each potential socket connection
		for (dossl, socket_maker) in socket_makers:
			supported = None
			if can_skip is True:
				# the last attempt tried without
				# SSL because the SSL handshake "failed"(N).
				can_skip = False
				continue
			try:
				self.socket = socket_maker(timeout = timeout)
				if dossl is True:
					supported = self._negotiate_ssl()
					if supported is None:
						# probably not PQv3..
						raise pg_exc.ProtocolError(
							"server did not support SSL negotiation",
							source = 'CLIENT',
							details = {
								'hint' : \
								'The server is probably not PostgreSQL.'
							}
						)
					if not supported and sslmode == 'require':
						# ssl is required..
						raise pg_exc.InsecurityError(
							"sslmode required a secure connection, " \
							"but was unsupported by server",
							source = 'CLIENT'
						)
					if supported:
						self.socket = self.connector.socket_secure(self.socket)
					else:
						dossl = None
				# time to negotiate
				negxact = self._negotiation()
				self._pq_xact = negxact
				self._pq_complete()
				self._pq_killinfo = negxact.killinfo
				# Use this for `interrupt` and state snapshots.
				self._socketfactory = socket_maker
				self.security = 'ssl' if supported is True else None
				# success!
				break
			except (self.connector.fatal_exception, pg_exc.Error) as e:
				# Just treat *any* PostgreSQL error as FATAL.
				##
				if sslmode == 'prefer' and dossl is None:
					# In this case, the server doesn't support SSL or it's
					# turned off. Therefore, the "without_ssl" attempt need
					# *not* be ran because it has already been noted to be
					# a failure.
					can_skip = True
				# If the exception is a socket error, chances are that it's
				# going to fail again.
				if isinstance(e, self.connector.fatal_exception):
					if sslmode == 'prefer' and dossl is True:
						# when 'prefer', the first attempt
						# is marked with dossl is True
						can_skip = True
					elif sslmode == 'allow' and dossl is False:
						# when 'allow', the first attempt
						# is marked with dossl is False
						can_skip = True
				if self.socket is not None:
					self.socket.close()
					self.socket = None
				self._reset()
				self._pq_in_buffer.truncate()

				connection_failures.append(
					(dossl, socket_maker, e)
				)
		else:
			# No servers available. (see the break-statement after establishing)
			err = pg_exc.ClientCannotConnectError(
				"failed to connect to server",
				details = {
					"severity" : "FATAL",
				},
				# It's really a sequence of exceptions.
				source = 'CLIENT'
			)
			self.ife_descend(err)
			err.database = self
			err.set_connection_failures(connection_failures)
			# it's over.
			self._pq_xact = self._pq_closed_xact
			err.raise_exception()
		self._init()

	def __enter__(self):
		self.connect()
		return self

	def _negotiate_ssl(self) -> (bool, None):
		"""
		[internal] Negotiate SSL

		If SSL is available--received b'S'--return True.
		If SSL is unavailable--received b'N'--return False.
		Otherwise, return None. Indicates non-PQv3 endpoint.
		"""
		r = pq.element.NegotiateSSLMessage.bytes()
		while r:
			r = r[self.socket.send(r):]
		status = self.socket.recv(1)
		if status == b'S':
			return True
		elif status == b'N':
			return False
		return None

	def _negotiation(self):
		'[internal] create the negotiation transaction'
		sp = self.connector._startup_parameters
		se = self.connector.server_encoding or 'utf-8'
		##
		# Attempt to accommodate for literal treatment of startup data.
		##
		smd = {
			# All keys go in utf-8. However, ascii would probably be good enough.
			k.encode('utf-8') : \
			# If it's a str(), encode in the hinted server_encoding.
			# Otherwise, convert the object(int, float, bool, etc) into a string
			# and treat it as utf-8.
			v.encode(se) if type(v) is str else str(v).encode('utf-8')
			for k, v in sp.items()
		}
		sm = pq.element.Startup(**smd)
		# encode the password in the hinted server_encoding as well
		return pq.Negotiation(sm, (self.connector.password or '').encode(se))

	def _init(self):
		'[internal] configure the database after negotiation'
		# Use the version_info and integer_datetimes setting to identify
		# the necessary binary type i/o functions to use.
		self.backend_id = self._pq_killinfo.pid

		sv = self.settings.cache.get("server_version", "0.0")
		self.version_info = pg_version.normalize(pg_version.split(sv))
		self.typio.select_time_io(
			self.version_info,
			self.settings.cache.get("integer_datetimes", "off").lower() in (
				't', 'true', 'on', 'yes',
			),
		)
		# Get the *full* version string.
		self.version = self.prepare("SELECT pg_catalog.version()").first()
		# First word from the version string.
		self.type = self.version.split()[0]

		try:
			r = self.prepare(
				"SELECT * FROM " + (
					'"pg_catalog".' if self.version_info[:2] > (7,2) else ""
				) + '"pg_stat_activity" ' \
				"WHERE procpid = " + str(self.backend_id)
			).first()
			if r is not None:
				self.client_address = r.get('client_addr')
				self.client_port = r.get('client_port')
				self.backend_start = r.get('backend_start')
		except pg_exc.Error as e:
			w = pg_exc.Warning("failed to get pg_stat_activity data")
			e.ife_descend(w)
			w.emit()
			# Toss a warning instead of the error.
			# This is not vital information, but is useful in exceptions.

		try:
			scstr = self.settings.cache.get('standard_conforming_strings')
			if scstr is None or scstr.lower() not in ('on','true','yes'):
				self.settings['standard_conforming_strings'] = str(True)
		except (
			pg_exc.UndefinedObjectError,
			pg_exc.ImmutableRuntimeParameterError
		):
			# warn about non-standard strings.
			w = pg_exc.DriverWarning(
				'standard conforming strings are unavailable',
			)
			self.ife_descend(w)
			w.emit()

	def _read_into(self):
		'[internal] protocol message reader. internal use only'
		while not self._pq_in_buffer.has_message():
			if self._read_data is not None:
				self._pq_in_buffer.write(self._read_data)
				self._read_data = None
				continue

			try:
				self._read_data = self.socket.recv(self._readbytes)
			except self.connector.fatal_exception as e:
				msg = self.connector.fatal_exception_message(e)
				if msg is not None:
					lost_connection_error = pg_exc.ConnectionFailureError(
						msg,
						details = {
							'severity' : 'FATAL',
							'detail' : 
								'Connector identified the exception as fatal.',
						},
						source = 'DRIVER'
					)
					lost_connection_error.connection = self
					# descend LCE from whatever we have :)
					(self._pq_xact or self).ife_descend(
						lost_connection_error
					)
					self._pq_state = b'LOST'
					lost_connection_error.raise_exception(raise_from = e)
				# It's probably a non-fatal error.
				raise

			##
			# nothing read from a blocking socket? it's over.
			# Asynchronous frameworks need to make sure the socket object is
			# raising the appropriate error.
			if not self._read_data:
				lost_connection_error = pg_exc.ConnectionFailureError(
					"unexpected EOF from server",
					details = {
						'severity' : 'FATAL',
						'detail' : \
							"Zero-length read " \
							"from the connection's socket.",
					},
					source = 'DRIVER'
				)
				lost_connection_error.connection = self
				self._pq_state = b'LOST'
				(self._pq_xact or self).ife_descend(lost_connection_error)
				lost_connection_error.raise_exception()

			# Got data. Put it in the buffer and clear _read_data.
			self._read_data = self._pq_in_buffer.write(self._read_data)

	def _standard_read_messages(self):
		'[internal] read more messages into self._read when self._read is empty'
		if not self._read:
			self._read_into()
			self._read = self._pq_in_buffer.read()
	_read_messages = _standard_read_messages

	def _send_message_data(self):
		'[internal] send unsent data initialized for delivery'
		try:
			while self._message_data:
				# Send data while there is data to send.
				self._message_data = self._message_data[
					self.socket.send(self._message_data):
				]
		except self.connector.fatal_exception as e:
			msg = self.connector.fatal_exception_message(e)
			if msg is not None:
				lost_connection_error = pg_exc.ConnectionFailureError(
					msg,
					details = {
						'severity' : 'FATAL',
					},
					source = 'DRIVER',
				)
				lost_connection_error.connection = self
				self._pq_state = b'LOST'
				lost_connection_error.raise_exception(e)
			# It wasn't fatal, so just raise:
			raise

	def _standard_write_messages(self, messages):
		'[internal] protocol message writer'
		if self._writing is not self._written:
			self._message_data += b''.join([x.bytes() for x in self._writing])
			self._written = self._writing

		if messages is not self._writing:
			self._writing = messages
			self._message_data += b''.join([x.bytes() for x in self._writing])
			self._written = self._writing
		self._send_message_data()
	_write_messages = _standard_write_messages

	def _traced_write_messages(self, messages):
		'[internal] _message_writer used when tracing'
		for msg in messages:
			t = getattr(msg, 'type', None)
			if t is not None:
				data_out = msg.bytes()
				self._tracer('↑ {type}({lend}): {data}{nl}'.format(
					type = repr(t)[2:-1],
					lend = len(data_out),
					data = repr(data_out),
					nl = os.linesep
				))
			else:
				# It's not a message instance, so assume raw data.
				self._tracer('↑__(%d): %r%s' %(
					len(msg), msg, os.linesep
				))
		self._standard_write_messages(messages)

	def _traced_read_messages(self):
		'[internal] _message_reader used when tracing'
		self._standard_read_messages()
		for msg in self._read:
			self._tracer('↓ %r(%d): %r%s' %(
				msg[0], len(msg[1]), msg[1], os.linesep)
			)

	def _backend_gc(self):
		"""
		[internal] close portals and statements slated for closure.

		WARNING: This will trample any existing transaction.
		"""
		xm = []
		portals = 0
		for x in self._closeportals:
			xm.append(pq.element.ClosePortal(x))
			portals += 1
		statements = 0
		for x in self._closestatements:
			xm.append(pq.element.CloseStatement(x))
			statements += 1
		xm.append(pq.element.SynchronizeMessage)
		x = pq.Instruction(xm)
		self._pq_xact = x
		del self._closeportals[:portals], self._closestatements[:statements]
		self._pq_complete()

	def _pq_push(self, xact : pq.Transaction):
		'[internal] setup the given transaction to be processed'
		# Push any queued closures onto the transaction or a new transaction.
		if xact.state is pq.Complete:
			return
		if self._pq_xact is not None:
			self._pq_complete()
		if self._closestatements or self._closeportals:
			self._backend_gc()
		# set it as the current transaction and begin
		self._pq_xact = xact
		self._pq_step()

	def _postgres_error(self, em : pq.element.Error) -> pg_exc.Error:
		'[internal] lookup a PostgreSQL error and instantiate it'
		m = self._decode_pq_message(em)
		c = m.pop('code')
		ms = m.pop('message')
		err = pg_exc.ErrorLookup(c)

		err = err(ms, code = c, details = m)
		err.database = self
		##
		# Some expectation of the caller over-riding it.
		self.ife_descend(err)
		return err

	def _procasyncs(self):
		'[internal] process the async messages in self._asyncs'
		if self._n_procasyncs:
			# recursion protection.
			return
		try:
			self._n_procasyncs = True
			while self._asyncs:
				for x in self._asyncs[0][1]:
					getattr(self, '_' + x.type.decode('ascii'))(x, self._asyncs[0][0])
				del self._asyncs[0]
		finally:
			self._n_procasyncs = False

	def _pq_step(self, xact = None):
		'[internal] make a single transition on the transaction'
		xact = xact or self._pq_xact
		try:
			dir, op = xact.state
			if dir is pq.Sending:
				self._write_messages(xact.messages)
				# The "op" callable will either switch the state, or
				# set the 'messages' attribute with a new sequence
				# of message objects for more writing.
				op()
			elif dir is pq.Receiving:
				assert self._pq_xact is xact
				self._read_messages()
				self._read = self._read[op(self._read):]
				self._pq_state = getattr(xact, 'last_ready', self._pq_state)
		except (socket.error, IOError) as e:
			# Unlike _complete, this catches at the outermost level
			# as there is no loop here for more transitioning.
			if e.errno == errno.EINTR:
				# Can't read or write, ATM? Consider it a transition. :(
				return errno.EINTR
			else:
				raise
		if xact.state is pq.Complete:
			self._pq_pop(xact)

	def _pq_complete(self):
		'[internal] complete the current transaction'
		# Continue to transition until all transactions have been
		# completed, or an exception occurs that does not signal retry.
		while self._pq_xact.state is not pq.Complete:
			try:
				while self._pq_xact.state[0] is pq.Receiving:
					self._read_messages()
					self._read = self._read[self._pq_xact.state[1](self._read):]
				# _pq_push() always takes one step, so it is likely that
				# the transaction is done sending out data by the time
				# _pq_complete() is called.
				while self._pq_xact.state[0] is pq.Sending:
					self._write_messages(self._pq_xact.messages)
					# Multiple calls to get() without signaling
					# completion *should* yield the same set over
					# and over again.
					self._pq_xact.state[1]()
			except self.connector.tryagain_exception as e:
				if not self.connector.tryagain(e):
					raise
		self._pq_state = getattr(self._pq_xact, 'last_ready', self._pq_state)
		self._pq_pop()

	def _pq_pop(self, xact = None):
		'[internal] remove the transaction and raise the exception if any'
		# collect any asynchronous messages from the xact
		if self._pq_xact not in (x[0] for x in self._asyncs):
			self._asyncs.append(
				(self._pq_xact, list(self._pq_xact.asyncs()))
			)
			self._procasyncs()

		em = getattr(self._pq_xact, 'error_message', None)
		if em is not None:
			xact_error = self._postgres_error(em)
			self._pq_xact.ife_descend(xact_error)
			if self._pq_xact.fatal is not True:
				# only remove the transaction if it's *not* fatal
				self._pq_xact = None
			xact_error.raise_exception()
		# state is Complete, so remove it as the working transaction
		self._pq_xact = None

	def _reset(self):
		"""
		[internal] Reset state and connection information attributes.
		"""
		self.socket = None
		self._read = ()
		self._read_data = None
		self._message_data = b''
		self._writing = None
		self._written = None
		self._asyncs = []
		self._n_procasyncs = None

		self._pq_xact = None
		self._pq_killinfo = None
		self._pq_state = None
		self.backend_id = None
		self.backend_start = None
		self.client_address = None
		self.client_port = None
		self.security = None

	def __init__(self, connector, *args, **kw):
		"""
		Create a connection based on the given connector.
		"""
		self._pq_closed_xact = ClosedConnection(self)
		self.connector = connector
		self._closestatements = []
		self._closeportals = []
		self._pq_in_buffer = pq_message_stream()
		self._readbytes = 2048

		self.typio = TypeIO(self)
		self.settings = Settings(self)
		# Update the _encode and _decode attributes on the connection
		# when a client_encoding ShowOption message comes in.
		self.settings.subscribe('client_encoding', self._update_encoding)
		self._reset()
# class Connection

class Connector(pg_api.Connector):
	"""
	All arguments to Connector are keywords. At the very least, user,
	and socket, may be provided. If socket, unix, or process is not
	provided, host and port must be.
	"""
	ife_ancestor = None
	Connection = Connection

	@property
	def _pq_iri(self):
		return pg_iri.serialize(
			{
				k : v for k,v in self.__dict__.items()
				if v is not None \
				and k not in ('_startup_parameters', '_address_family', '_pq_iri', 'ife_ancestor')
			},
			obscure_password = True
		)

	def __repr__(self):
		keywords = (',' + os.linesep + ' ').join([
			'%s = %r' %(k, getattr(self, k, None)) for k in self.__dict__
			if k not in ('_startup_parameters', '_address_family', '_pq_iri', 'ife_ancestor') \
			and getattr(self, k, None) is not None
		])
		return '{mod}.{name}({keywords})'.format(
			mod = type(self).__module__,
			name = type(self).__name__,
			keywords = os.linesep + ' ' + keywords if keywords else ''
		)

	@abstractmethod
	def socket_factory_sequence(self) -> [collections.Callable]:
		"""
		Generate a list of callables that will be used to attempt to make the
		connection to the server. It is assumed that each factory will produce
		an object with a socket interface that is ready for reading and writing 
		data.

		The callables in the sequence must take a timeout parameter.
		"""

	@abstractmethod
	def socket_secure(self, socket):
		"""
		Given a socket produced using one of the callables created in the
		`socket_factory_sequence`, secure it using SSL with the SSL parameters:
		
		 - sslcrtfile
		 - sslkeyfile
		 - sslrootcrtfile
		 - sslrootcrlfile
		"""

	def __init__(self,
		connect_timeout : int = None,
		server_encoding : "server encoding hint for driver" = None,
		sslmode : ('allow', 'prefer', 'require', 'disable') = None,
		sslcrtfile : "filepath" = None,
		sslkeyfile : "filepath" = None,
		sslrootcrtfile : "filepath" = None,
		sslrootcrlfile : "filepath" = None,
		**kw
	):
		super().__init__(**kw)

		self.server_encoding = server_encoding
		self.connect_timeout = connect_timeout
		self.sslmode = sslmode
		self.sslkeyfile = sslkeyfile
		self.sslcrtfile = sslcrtfile
		self.sslrootcrtfile = sslrootcrtfile
		self.sslrootcrlfile = sslrootcrlfile

		if self.sslrootcrlfile is not None:
			w = pg_exc.IgnoredClientParameterWarning(
				"Certificate Revocation Lists are *not* checked."
			)
			self.ife_descend(w)
			w.emit()

		# Startup message parameters.
		tnkw = {}
		if self.settings:
			s = dict(self.settings)
			if 'search_path' in self.settings:
				sp = s.get('search_path')
				if sp is None:
					self.settings.pop('search_path')
				elif not isinstance(sp, str):
					s['search_path'] = ','.join(
						pg_str.quote_ident(x) for x in sp
					)
			tnkw.update(s)

		tnkw['user'] = self.user
		if self.database is not None:
			tnkw['database'] = self.database

		self._startup_parameters = tnkw
# class Connector

class SocketCreator(object):
	def __call__(self, timeout = None):
		s = socket.socket(*self.socket_create)
		s.settimeout(float(timeout) if timeout is not None else None)
		s.connect(self.socket_connect)
		s.settimeout(None)
		return s

	def __init__(self,
		socket_create : "positional parameters given to socket.socket()",
		socket_connect : "parameter given to socket.connect()",
	):
		self.socket_create = socket_create
		self.socket_connect = socket_connect

	def __str__(self):
		return 'socket' + repr(self.socket_connect)

class SocketConnector(Connector):
	'abstract connector for using `socket` and `ssl`'
	def ife_snapshot_text(self):
		return '[' + type(self).__name__ + '] ' + self._pq_iri

	fatal_exception_messages = {
		errno.ECONNRESET : 'server explicitly closed the connection',
		errno.EPIPE : 'broken connection detected on send',
		errno.ECONNREFUSED : 'server refused connection',
	}
	timeout_exception = socket.timeout
	fatal_exception = socket.error
	tryagain_exception = socket.error

	def tryagain(self, err) -> bool:
		# pretty easy; should the user try the operation again?
		return getattr(err, 'errno', 0) == errno.EINTR

	def connection_refused(self, err) -> bool:
		return getattr(err, 'errno', 0) == errno.ECONNREFUSED

	def fatal_exception_message(self, err) -> (str, None):
		# None indicates that it was not fatal.
		self.fatal_exception_messages.get(err.errno)

	@abstractmethod
	def socket_factory_sequence(self) -> [SocketCreator]:
		"""
		Return a sequence of `SocketCreator`s for a connection to use to connect
		to the target host.
		"""

	def socket_secure(self, socket : socket.socket) -> ssl.SSLSocket:
		"take a would be connection socket and secure it with SSL"
		return ssl.wrap_socket(
			socket,
			keyfile = self.sslkeyfile,
			certfile = self.sslcrtfile,
			ca_certs = self.sslrootcrtfile,
		)

class IP4(SocketConnector):
	'Connector for establishing IPv4 connections'
	ipv = 4

	def socket_factory_sequence(self):
		return self._socketcreators

	def __init__(self,
		host : "IPv4 Address (str)" = None,
		port : int = None,
		ipv = 4,
		**kw
	):
		if ipv != self.ipv:
			raise TypeError("'ipv' keyword must be '4'")
		if host is None:
			raise TypeError("'host' is a required keyword and cannot be 'None'")
		if port is None:
			raise TypeError("'port' is a required keyword and cannot be 'None'")
		self.host = host
		self.port = int(port)
		# constant socket connector
		self._socketcreator = SocketCreator(
			(socket.AF_INET, socket.SOCK_STREAM), (self.host, self.port)
		)
		self._socketcreators = (
			self._socketcreator,
		)
		super().__init__(**kw)

class IP6(SocketConnector):
	'Connector for establishing IPv6 connections'
	ipv = 6
	def socket_factory_sequence(self):
		return self._socketcreators

	def __init__(self,
		host : "IPv6 Address (str)" = None,
		port : int = None,
		ipv = 6,
		**kw
	):
		if ipv != self.ipv:
			raise TypeError("'ipv' keyword must be '6'")
		if host is None:
			raise TypeError("'host' is a required keyword and cannot be 'None'")
		if port is None:
			raise TypeError("'port' is a required keyword and cannot be 'None'")
		self.host = host
		self.port = int(port)
		# constant socket connector
		self._socketcreator = SocketCreator(
			(socket.AF_INET6, socket.SOCK_STREAM), (self.host, self.port)
		)
		self._socketcreators = (
			self._socketcreator,
		)
		super().__init__(**kw)

class Unix(SocketConnector):
	'Connector for establishing unix domain socket connections'
	def socket_factory_sequence(self):
		return self._socketcreators

	def __init__(self, unix = None, **kw):
		if unix is None:
			raise TypeError("'unix' is a required keyword and cannot be 'None'")
		self.unix = unix
		# constant socket connector
		self._socketcreator = SocketCreator(
			(socket.AF_UNIX, socket.SOCK_STREAM), self.unix
		)
		self._socketcreators = (self._socketcreator,)
		super().__init__(**kw)

class Host(SocketConnector):
	"""
	Connector for establishing hostname based connections.

	This connector exercises socket.getaddrinfo.
	"""

	def socket_factory_sequence(self):
		"""
		Return a list of `SocketCreator`s based on the results of
		`socket.getaddrinfo`.
		"""
		return [
			# (AF, socktype, proto), (IP, Port)
			SocketCreator(x[0:3], x[4][:2])
			for x in socket.getaddrinfo(
				self.host, self.port, self._address_family, socket.SOCK_STREAM
			)
		]

	def __init__(self,
		host : str = None,
		port : (str, int) = None,
		ipv : int = None,
		address_family : "address family to use(AF_INET,AF_INET6)" = None,
		**kw
	):
		if host is None:
			raise TypeError("'host' is a required keyword")
		if port is None:
			raise TypeError("'port' is a required keyword")

		if address_family is not None and ipv is not None:
			raise TypeError("'ipv' and 'address_family' on mutually exclusive")

		if ipv is None:
			self._address_family = address_family or socket.AF_UNSPEC
		elif ipv == 4:
			self._address_family = socket.AF_INET
		elif ipv == 6:
			self._address_family = socket.AF_INET6
		else:
			raise TypeError("unknown IP version selected: 'ipv' = " + repr(ipv))
		self.host = host
		self.port = port
		super().__init__(**kw)

class Driver(pg_api.Driver):
	ife_ancestor = None

	def ip4(self, **kw):
		C = IP4(**kw)
		self.ife_descend(C)
		return C

	def ip6(self, **kw):
		C = IP6(**kw)
		self.ife_descend(C)
		return C

	def host(self, **kw):
		C = Host(**kw)
		self.ife_descend(C)
		return C

	def unix(self, **kw):
		C = Unix(**kw)
		self.ife_descend(C)
		return C

	def fit(self,
		unix = None,
		host = None,
		port = None,
		**kw
	) -> Connector:
		"""
		Create the appropriate `postgresql.api.Connector` based on the parameters.

		This also protects against mutually exclusive parameters.
		"""
		if unix is not None:
			if host is not None:
				raise TypeError("'unix' and 'host' keywords are exclusive")
			if port is not None:
				raise TypeError("'unix' and 'port' keywords are exclusive")
			return self.unix(unix = unix, **kw)
		else:
			if host is None or port is None:
				raise TypeError("'host' and 'port', or 'unix' must be supplied")
			# We have a host and a port.
			# If it's an IP address, IP4 or IP6 should be selected.
			if ':' in host:
				# There's a ':' in host, good chance that it's IPv6.
				try:
					socket.inet_pton(socket.AF_INET6, host)
					return self.ip6(host = host, port = port, **kw)
				except (socket.error, NameError):
					pass

			# Not IPv6, maybe IPv4...
			try:
				socket.inet_aton(host)
				# It's IP4
				return self.ip4(host = host, port = port, **kw)
			except socket.error:
				pass

			# neither host, nor port are None, probably a hostname.
			return self.host(host = host, port = port, **kw)

	def connect(self, **kw) -> Connection:
		"""
		For more information on acceptable keywords, see help on:

			`postgresql.driver.pq3.Connector`
			 Keywords that apply to PostgreSQL, user
			`postgresql.driver.pq3.IP4`
			 Keywords that apply to IPv4 only connections.
			`postgresql.driver.pq3.IP6`
			 Keywords that apply to IPv6 only connections.
			`postgresql.driver.pq3.Unix`
			 Keywords that apply to Unix Domain Socket connections.
			`postgresql.driver.pq3.Host`
			 Keywords that apply to host-based connections(resolving connector).
		"""
		c = self.fit(**kw)()
		c.connect()
		return c

	def ife_snapshot_text(self):
		return 'postgresql.driver.pq3'

	def __init__(self, typio = TypeIO):
		self.typio = typio
		super().__init__()
