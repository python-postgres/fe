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

import errno
import socket
from traceback import format_exception

from operator import itemgetter
get0 = itemgetter(0)
get1 = itemgetter(1)
from itertools import repeat, islice, chain
from functools import partial

from abc import abstractmethod, abstractproperty

from .. import versionstring as pg_version
from .. import iri as pg_iri
from .. import exceptions as pg_exc
from .. import string as pg_str
from .. import api as pg_api
from ..encodings import aliases as pg_enc_aliases

from ..python.itertools import interlace
from ..python.socket import SocketFactory

from ..protocol import xact3 as xact
from ..protocol import element3 as element
from ..protocol import client3 as client
from ..protocol import typio as pg_typio

from .. import types as pg_types

is_showoption = lambda x: getattr(x, 'type', None) is element.ShowOption.type

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
  COALESCE(string_to_array(trim(textin(array_out(string_to_array(
   replace(
    trim(textin(oidvectorout(proargtypes)), '{}'),
    ',', ' '
   ), ' ')::oid[]::regtype[])), '{}'), ',')::text[], '{}'::text[])
	 AS _proargs,
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

IDNS = 'py:%s'
def ID(s, title = None):
	'generate an id for a client statement or cursor'
	return IDNS %(hex(id(s)),)

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

class TypeIO(pg_typio.TypeIO):
	def __init__(self, database):
		self.database = database
		super().__init__()

	def lookup_type_info(self, typid):
		return self.database.prepare(TypeLookup).first(typid)

	def lookup_composite_type_info(self, typid):
		return self.database.prepare(CompositeLookup)(typid)

class Chunks(pg_api.Chunks):
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
				self.cursor.close()
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

	def _e_metas(self):
		yield ('direction', 'FORWARD' if self.direction else 'BACKWORD')

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
		self._ah = partial(self.database._receive_async, controller = self)
		self._pq_cursor_id = database.typio.encode(self.cursor_id)

	def __del__(self):
		if not self.closed and ID(self) == self.cursor_id:
			self.database.pq.garbage_cursors.append(
				self.database.typio.encode(self.cursor_id)
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
			self.database.pq.garbage_cursors.append(
				self.database.typio.encode(self.cursor_id)
			)
			self.closed = True

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
			creator = self
		)
		te.index = itemnum
		raise te

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
			creator = self
		)
		te.index = itemnum
		raise te

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
			if self.database.pq.xact is not None:
				self.database._pq_complete()
			##
			# In auto-commit mode or with_hold is on?
			if ((self.database.pq.state == b'I' and self.with_hold is None)\
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

	def _operation_error_(self, *args, **kw):
		e = pg_exc.OperationError(
			"cursor type does not support that operation",
			creator = self
		)
		raise e
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
				if self._xact.state is not xact.Complete \
				and self._xact is self.database.pq.xact:
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

		x = xact.Instruction(
			setup + more + (element.SynchronizeMessage,),
			asynchook = self._ah
		)
		self.__dict__.update({
			'_xact' : x,
			'_this_reqsize' : self.chunksize
		})
		self.database._pq_push(self._xact, self)
		self._fini()

	def _dispatch_for_more(self, direction):
		if self._xact:
			# didn't expand
			raise RuntimeError("invalid state for dispatch")
		more = self._pq_xp_fetchmore(self.chunksize, direction)
		x = more + (element.SynchronizeMessage,)
		x = xact.Instruction(x, asynchook = self._ah)
		self.__dict__.update({
			'_xact' : x,
			'_this_reqsize' : self.chunksize,
			'_this_direction' : direction,
		})
		self.database._pq_push(self._xact, self)

	def _expand(self):
		"""
		[internal] Expand the _buffer using the data in _xact
		"""
		if self._xact is not None:
			# complete the _xact
			if self._xact.state is not xact.Complete:
				self.database._pq_push(self._xact, self)
				if self._xact.state is not xact.Complete:
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
			x = xact.Instruction(
				more + (element.SynchronizeMessage,),
				asynchook = self._ah
			)
			self.__dict__.update({
				'_xact' : x,
				'_this_reqsize' : rquantity,
				'_this_direction' : direction,
			})
			self.database._pq_push(x, self)

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
			if y.type is element.Tuple.type
		]

	def _pq_xp_move(self, position, whence):
		'make a command sequence for a MOVE single command'
		return (
			element.Parse(b'',
				b'MOVE ' + whence + b' ' + position + b' IN ' + \
				self.database.typio.encode(self._quoted_cursor_id),
				()
			),
			element.Bind(b'', b'', (), (), ()),
			element.Execute(b'', 1),
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

		x = xact.Instruction(
			cmd + (element.SynchronizeMessage,),
			asynchook = self._ah
		)
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
		self.database._pq_push(x, self)

class ProtocolCursor(TupleCursor):
	cursor_type = 'protocol'

	def _init(self):
		# Protocol-bound cursor.
		##
		if self.scroll:
			# That doesn't work.
			##
			e = pg_exc.OperationError(
				"cannot bind cursor scroll = True",
				creator = self,
			)
			raise e
		if self.with_hold:
			# That either.
			##
			e = pg_exc.OperationError(
				"cannot bind cursor with_hold = True",
				creator = self
			)
			raise e

		if self.database.pq.state == b'I':
			# have to fetch them all. as soon as the next sync occurs, the
			# cursor will be dropped.
			self.chunksize = 0xFFFFFFFF

		return super()._init((
			element.Bind(
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
				"cannot read backwards with protocol cursors",
				creator = self
			)
			raise err
		return (
			element.Execute(self._pq_cursor_id, quantity),
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
		if self.database.pq.state == b'I':
			self.with_hold = True

		return super()._init((
			element.Parse(b'', self.database.typio.encode(self._statement_string()), ()),
			element.Bind(
				b'', b'', self.statement._input_formats, self._pq_parameters(), ()
			),
			element.Execute(b'', 1),
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
			element.Parse(b'', self.database.typio.encode(qstr), ()),
			element.Bind(b'', b'', (), (), self._output_formats),
			# The "limit" is defined in the fetch query.
			element.Execute(b'', 0xFFFFFFFF),
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
			self, (element.DescribePortal(self._pq_cursor_id),)
		)

	def _fini(self):
		if self._xact.state is not xact.Complete:
			if self.database.pq.xact is not self._xact:
				self.database._pq_push(self._xact, self)
			self.database._pq_complete()
		for m in self._xact.messages_received():
			if m.type is element.TupleDescriptor.type:
				self._output = m
				self._output_attmap = \
					self.database.typio.attribute_map(self._output)
				# tuple output
				self._output_io = self.database.typio.resolve_descriptor(
					self._output, 1 # (input, output)[1]
				)
				self._output_formats = [
					element.StringFormat
					if x is None
					else element.BinaryFormat
					for x in self._output_io
				]
				self._output_io = tuple([
					x or self.database.typio.decode for x in self._output_io
				])
				super()._fini()
		# Done with the first transaction.
		self._xact = None
		if self.closed:
			e = pg_exc.OperationError(
				"failed to discover cursor output",
				creator = self
			)
			raise e

class UtilityCursor(CursorStrategy):
	cursor_type = 'utility'

	def __del__(self):
		# utility cursors must be finished immediately,
		# so the cursor_id goes unused.
		pass

	def _init(self):
		self._xact = xact.Instruction((
				element.Bind(
					b'',
					self.statement._pq_statement_id,
					self.statement._input_formats,
					self._pq_parameters(),
					(),
				),
				element.Execute(b'', 1),
				element.SynchronizeMessage,
			),
			asynchook = self._ah
		)

		self.database._pq_push(self._xact, self)
		while self._xact.state != xact.Complete:
			# in case it's a copy
			self.database._pq_step()
			for x in self._xact.messages_received():
				if x.type is element.CopyToBegin.type:
					self.__class__ = CopyCursor
					return self._init()
					# The COPY TO STDOUT transaction terminates the loop
					# *without* finishing the transaction.
					# Buffering all of the COPY data would be a bad idea(tm).
					##
				elif x.type in element.Null.type:
					break
				elif x.type is element.Complete.type:
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
					-1 if l == 0 and self._xact.state is xact.Complete else l
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
				"cannot read COPY backwards",
				creator = self
			)
			raise e

		while self.database.pq.xact is self._xact and not self._xact.completed:
			self.database._pq_step()

		# Find the Complete message when pq.xact is done
		if self._xact.state is xact.Complete and self._complete_message is None:
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
	string = None
	database = None
	statement_id = None
	_input = None
	_output = None
	_output_io = None
	_output_formats = None
	_output_attmap = None

	def _e_metas(self):
		spt = self.sql_parameter_types
		if spt is not None:
			yield ('sql_parameter_types', spt)
		cn = self.column_names
		ct = self.sql_column_types
		if cn is not None:
			if ct is not None:
				yield (
					'results',
					'(' + ', '.join([
						n + ' ' + t for n,t in zip(cn,ct)
					]) + ')'
				)
			else:
				yield ('sql_column_names', cn)
		elif ct is not None:
			yield ('sql_column_types', ct)

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
		self._ah = partial(self.database._receive_async, controller = self)
		self._pq_xact = None
		self._pq_statement_id = None
		self.closed = None

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
			creator = self
		)
		te.index = itemnum
		raise te

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
			creator = self
		)
		te.index = itemnum
		raise te

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
			self.database.pq.garbage_statements.append(self._pq_statement_id)
		self.closed = True

	def __del__(self):
		# Only close statements that have generated IDs as the ones
		# with explicitly created
		if not self.closed and ID(self) == self.statement_id:
			# Always close CPSs as the way the statement_id is generated
			# might cause a conflict if Python were to reuse the previously
			# used id()[it can and has happened]. - jwp 2007
			self.close()

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
			q = self.database.typio._encode(str(self.string))[0]
			cmd = [
				element.CloseStatement(self._pq_statement_id),
				element.Parse(self._pq_statement_id, q, ()),
			]
		else:
			cmd = []
		cmd.extend(
			(
				element.DescribeStatement(self._pq_statement_id),
				element.SynchronizeMessage,
			)
		)
		self._xact = xact.Instruction(
			cmd,
			asynchook = self._ah
		)
		self.database._pq_push(self._xact, self)

	def _fini(self):
		"""
		Complete initialization that the _init() method started.
		"""
		# assume that the transaction has been primed.
		if self._xact is None:
			raise RuntimeError("_fini called prior to _init; invalid state")
		if self._xact is self.database.pq.xact:
			try:
				self.database._pq_complete()
			except:
				self.closed = True
				raise

		(*head, argtypes, tupdesc, last) = self._xact.messages_received()

		if tupdesc is None or tupdesc is element.NoDataMessage:
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
				element.StringFormat
				if x is None
				else element.BinaryFormat
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
				element.StringFormat
				if x is None
				else element.BinaryFormat
			)
		self._input_io = tuple(packs)
		self._input_formats = formats
		self.closed = False
		self._xact = None

	def _cursor(self, *parameters, **kw):
		if self.closed is None:
			self._fini()
		# Tuple output per the results of DescribeStatement.
		##
		cursor = Cursor.from_statement(parameters, self, **kw)
		cursor._init()
		return cursor

	def __call__(self, *parameters):
		# get em' all!
		c = self._cursor(*parameters, with_hold = False, scroll = False)
		if isinstance(c, UtilityCursor):
			return (c.command(), c.count())
		else:
			r = c.read()
			c.close()
			return r

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
		x = xact.Instruction((
				element.Bind(
					b'',
					self._pq_statement_id,
					self._input_formats,
					params,
					self._output_formats,
				),
				element.Execute(b'', 0xFFFFFFFF),
				element.SynchronizeMessage
			),
			asynchook = self._ah
		)
		c._pq_push(x, self)
		c._pq_complete()

		if self._output_io:
			##
			# Look for the first tuple.
			for xt in x.messages_received():
				if xt.type is element.Tuple.type:
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
				if getattr(cm, 'type', None) == element.Complete.type:
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
		x = xact.Instruction((
				element.Bind(
					b'',
					self._pq_statement_id,
					(), (), (),
				),
				element.Execute(b'', 1),
				element.SynchronizeMessage,
			),
			asynchook = self._ah
		)
		self.database._pq_push(x, self)

		# Get the COPY started.
		while x.state is not xact.Complete:
			self.database._pq_step()
			if hasattr(x, 'CopyFailSequence') and x.messages is x.CopyFailSequence:
				break
		else:
			# Oh, it's not a COPY at all.
			e = pg_exc.OperationError(
				"_copy_data_in() used on a non-COPY FROM STDIN query",
				creator = self
			)
			raise e

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
						element.CopyData(l) for l in rows
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
					element.CopyData(l) for l in islice(iterable, tps)
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
		last = element.FlushMessage
		try:
			while last is element.FlushMessage:
				c = 0
				xm = []
				for t in tupleseqiter:
					params = pg_typio.process_tuple(
						self._input_io, tuple(t), pte
					)
					xm.extend((
						element.Bind(
							b'',
							self._pq_statement_id,
							self._input_formats,
							params,
							(),
						),
						element.Execute(b'', 1),
					))
					if c == tps:
						break
					c += 1
				else:
					last = element.SynchronizeMessage
				xm.append(last)
				self.database._pq_push(
					xact.Instruction(xm, asynchook = self._ah),
					self
				)
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
	_e_factors = ('database', 'procedure_id')
	procedure_id = None

	def _e_metas(self):
		yield ('oid', self.oid)

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

		proargs = proctup['_proargs']
		self.statement = database.prepare(
			"SELECT * FROM %s(%s) AS func%s" %(
				proctup['_proid'],
				# ($1::type, $2::type, ... $n::type)
				', '.join([
					 '$%d::%s' %(x + 1, proargs[x])
					 for x in range(len(proargs))
				]),
				# Description for anonymous record returns
				(description and \
					'(' + ','.join(description) + ')' or '')
			)
		)
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
	_e_factors = ('database',)

	def __init__(self, database):
		self.database = database
		self.cache = {}

	def _e_metas(self):
		yield (None, str(len(self.cache)))

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
	database = None

	mode = None
	isolation = None
	gid = None

	_e_factors = ('database', 'gid', 'isolation', 'mode')

	def _e_metas(self):
		yield (None, self.state)

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
			if self.database.pq.state == b'E':
				err = pg_exc.InFailedTransactionError(
					"invalid transaction block exit detected",
					source = 'CLIENT',
					details = {
						'cause': \
							'Database was in an error-state, ' \
							'but no exception was raised.'
					},
					creator = self
				)
				if not self.database.closed:
					self.rollback()
				raise err
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
				},
				creator = self
			)
			raise err

		if self.database.pq.state == b'I':
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
					},
					creator = self
				)
				raise err
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
				creator = self
			)
			raise err
		if self.type != 'block':
			err = pg_exc.OperationError(
				"improper transaction type to prepare",
				creator = self
			)
			raise err
		q = self._prepare_string(self.gid)
		self.database.execute(q)
		self.state = 'prepared'

	def recover(self):
		if self.state != 'initialized':
			err = pg_exc.OperationError(
				"improper state for prepared transaction recovery",
				creator = self
			)
			raise err
		if self.database.prepare(TransactionIsPrepared).first(self.gid):
			self.state = 'prepared'
			self.type = 'block'
		else:
			err = pg_exc.UndefinedObjectError(
				"prepared transaction does not exist",
				source = 'CLIENT',
				creator = self
			)
			raise err

	def commit(self):
		if self.state == 'committed':
			return
		if self.state not in ('prepared', 'open'):
			err = pg_exc.OperationError(
				"commit attempted on transaction with unexpected state, " + repr(self.state),
				creator = self
			)
			raise err

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
					},
					creator = self
				)
				raise err
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
				creator = self
			)
			raise err

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

	def _e_metas(self):
		yield (None, '[' + self.state + ']')
		if self.client_address is not None:
			yield ('client_address', self.client_address)
		if self.client_port is not None:
			yield ('client_port', self.client_port)
		if self.version is not None:
			yield ('version', self.version)
		att = getattr(self, 'failures', None)
		if att:
			count = 0
			for x in att:
				errstr = ''.join(format_exception(type(x.error), x.error, None))
				factinfo = str(x.socket_factory)
				if hasattr(x, 'ssl_negotiation'):
					if x.ssl_negotiation is True:
						factinfo = 'SSL ' + factinfo
					else:
						factinfo = 'NOSSL ' + factinfo
				yield (
					'failures[' + str(count) + ']',
					factinfo + os.linesep + errstr
				)
				count += 1

	def __repr__(self):
		return '<%s.%s[%s] %s>' %(
			type(self).__module__,
			type(self).__name__,
			self.connector._pq_iri,
			self.closed and 'closed' or '%s' %(self.pq.state,)
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
		[Avoids garbage collection]
		"""
		self.pq.synchronize()

	def interrupt(self, timeout = None):
		self.pq.interrupt(timeout = timeout)

	def execute(self, query : str) -> None:
		q = xact.Instruction((
				element.Query(self.typio._encode(query)[0]),
			),
			asynchook = self._receive_async
		)
		self._pq_push(q, self)
		self._pq_complete()

	def xact(self, gid = None, isolation = None, mode = None):
		x = Transaction(self, gid = gid, isolation = isolation, mode = mode)
		return x

	def prepare(self,
		sql_statement_string : str,
		statement_id = None,
	) -> PreparedStatement:
		ps = PreparedStatement.from_string(sql_statement_string, self)
		ps._init()
		ps._fini()
		return ps

	def statement_from_id(self, statement_id : str) -> PreparedStatement:
		ps = PreparedStatement(statement_id, self)
		ps._init()
		ps._fini()
		return ps

	def proc(self, proc_id : (str, int)) -> StoredProcedure:
		sp = StoredProcedure(proc_id, self)
		return sp

	def cursor_from_id(self, cursor_id : str) -> Cursor:
		c = Cursor(cursor_id, self)
		c._init()
		return c

	@property
	def closed(self) -> bool:
		if not hasattr(self, 'pq'):
			return True
		if hasattr(self.pq, 'socket') and self.pq.xact is not None:
			return self.pq.xact.fatal is True
		return False

	def close(self):
		if self.closed:
			return
		# Write out the disconnect message if the socket is around.
		# If the connection is known to be lost, don't bother. It will
		# generate an extra exception.
		x = self.pq.xact
		if x:
			# don't raise?
			self.pq.complete()
			if self.closed:
				return
		self.pq.push(xact.Closing())
		self.pq.complete()

	@property
	def state(self) -> str:
		if not hasattr(self, 'pq'):
			return 'initialized'
		if hasattr(self, 'failures'):
			return 'failed'
		if self.closed:
			return 'closed'
		if isinstance(self.pq.xact, xact.Negotiation):
			return 'negotiating'
		if self.pq.xact is None:
			return 'idle'
		else:
			return 'busy'

	def reset(self):
		"""
		restore original settings, reset the transaction, drop temporary
		objects.
		"""
		self.execute("ABORT; RESET ALL;")

	def __enter__(self):
		self.connect()
		return self

	def connect(self):
		'Establish the connection to the server'
		if self.closed is False:
			# already connected? just return.
			return
		# It's closed.

		if hasattr(self, 'pq'):
			# It's closed, *but* there's a PQ connection..
			self._raise_pq_error()
			# gah, the fatality of the connection does not
			# appear to exist...
			raise RuntimeError(
				"closed connection has no fatal error"
			)

		self.pq = None
		# if any exception occurs past this point, the connection
		# will not be usable.
		timeout = self.connector.connect_timeout
		sslmode = self.connector.sslmode or 'prefer'
		failures = []
		exc = None
		try:
			# get the list of sockets to try
			socket_factories = self.connector.socket_factory_sequence()
		except Exception as e:
			socket_factories = ()
			exc = e

		# When ssl is None: SSL negotiation will not occur.
		# When ssl is True: SSL negotiation will occur *and* it must succeed.
		# When ssl is False: SSL negotiation will occur but NOSSL is okay.
		if sslmode == 'allow':
			# first, without ssl, then with. :)
			socket_factories = interlace(
				zip(repeat(None, len(socket_factories)), socket_factories),
				zip(repeat(True, len(socket_factories)), socket_factories)
			)
		elif sslmode == 'prefer':
			# first, with ssl, then without. [maybe] :)
			socket_factories = interlace(
				zip(repeat(False, len(socket_factories)), socket_factories),
				zip(repeat(None, len(socket_factories)), socket_factories)
			)
			# prefer is special, because it *may* be possible to
			# skip the subsequent "without" in situations where SSL is off.
		elif sslmode == 'require':
			socket_factories = zip(repeat(True, len(socket_factories)), socket_factories)
		elif sslmode == 'disable':
			# None = Do Not Attempt SSL negotiation.
			socket_factories = zip(repeat(None, len(socket_factories)), socket_factories)
		else:
			raise ValueError("invalid sslmode: " + repr(sslmode))

		# can_skip is used when 'prefer' or 'allow' is the sslmode.
		# if the ssl negotiation returns 'N' (nossl), then
		# ssl "failed", but the socket is still usable for nossl.
		# in these cases, can_skip is set to True so that the
		# subsequent non-ssl attempt is skipped if it failed with the 'N' response.
		can_skip = False
		for (ssl, sf) in socket_factories:
			if can_skip is True:
				# the last attempt failed and knows this attempt will fail too.
				can_skip = False
				continue
			pq = client.Connection(
				sf, self.connector._startup_parameters,
				password = self.connector._password,
			)
			neg = pq.xact
			pq.connect(ssl = ssl, timeout = timeout)

			didssl = getattr(pq, 'ssl_negotiation', -1)
			if pq.xact is None:
				self.pq = pq
				for x in filter(is_showoption, neg.asyncs):
					self._receive_async(x)
				self.security = 'ssl' if didssl is True else None
				# success!
				break

			if sslmode == 'prefer' and ssl is False and didssl is False:
				# In this case, the server doesn't support SSL or it's
				# turned off. Therefore, the "without_ssl" attempt need
				# *not* be ran because it has already been noted to be
				# a failure.
				can_skip = True
			elif hasattr(pq.xact, 'exception'):
				# If a Python exception occurred, chances are that it is
				# going to fail again iff it is going to hit the same host.
				if sslmode == 'prefer' and ssl is False:
					# when 'prefer', the first attempt
					# is marked with ssl is "False"
					can_skip = True
				elif sslmode == 'allow' and ssl is None:
					# when 'allow', the first attempt
					# is marked with dossl is "None"
					can_skip = True
			err = self._error_lookup(pq.xact.error_message)
			pq.error = err
			if getattr(pq.xact, 'exception', None) is not None:
				err.__cause__ = pq.xact.exception
			failures.append(pq)
		else:
			# No servers available. (see the break-statement in the for-loop)
			msg = "failed to establish connection to server"
			ce = element.ClientError(
				message = msg,
				severity = "FATAL",
				code = "08001",
			)
			err = self._error_lookup(ce)
			err.creator = self
			self.failures = failures or ()
			if exc is not None:
				err.__cause__ = exc
			# it's over.
			raise err
		##
		# connected, now initialize metadata
		# Use the version_info and integer_datetimes setting to identify
		# the necessary binary type i/o functions to use.
		self.backend_id = self.pq.backend_id

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
		try:
			scstr = self.settings.cache.get('standard_conforming_strings')
			if scstr is None or scstr.lower() not in ('on','true','yes'):
				self.settings['standard_conforming_strings'] = str(True)
		except (
			pg_exc.UndefinedObjectError,
			pg_exc.ImmutableRuntimeParameterError
		):
			# warn about non-standard strings
			cm = element.ClientNotice(
				message = 'standard conforming strings are not available',
				severity = 'WARNING',
				code = '01-00',
			)
			cm = self._convert_pq_message(cm)
			cm.creator = self
			cm.raise_message()

	def _pq_push(self, xact, controller = None):
		x = self.pq.xact
		if x is not None:
			self.pq.complete()
			self._raise_pq_error(x)
		xact.controller = controller or self
		self.pq.push(xact)

	def _pq_complete(self):
		x = self.pq.xact
		if self.pq.xact is not None:
			self.pq.complete()
			self._raise_pq_error(x)

	def _pq_step(self):
		x = self.pq.xact
		if x is not None:
			self.pq.step()
			if x.state is xact.Complete:
				self._raise_pq_error(x)

	def _raise_pq_error(self, xact = None):
		if xact is not None:
			x = xact
		else:
			x = self.pq.xact
		if x.fatal is None:
			# No error occurred..
			return
		err = self._error_lookup(x.error_message)
		fromexc = getattr(x, 'exception', None)
		fromcontroller = getattr(x, 'controller', self)
		err.creator = fromcontroller
		if fromexc is not None:
			err.__cause__ = fromexc
		raise err

	def _decode_pq_message(self, msg):
		'decode the values in a message(E,N)'
		if type(msg) in (element.ClientError, element.ClientNotice):
			return dict(msg)
		decode = self.typio._decode
		dmsg = {}
		for k, v in msg.items():
			try:
				# code should always be ascii..
				if k == "code":
					v = v.decode('ascii')
				else:
					v = decode(v)[0]
			except UnicodeDecodeError:
				# Fallback to the bytes representation.
				# This should be sufficiently informative in most cases,
				# and in the cases where it isn't, an element traceback should
				# ultimately yield the pertinent information
				v = repr(v)[2:-1]
			dmsg[k] = v
		return dmsg

	def _convert_pq_message(self, msg, source = 'SERVER'):
		'create a message, warning or error'
		dmsg = self._decode_pq_message(msg)
		m = dmsg.pop('message')
		c = dmsg.pop('code')
		sev = dmsg['severity'].upper()
		if sev in ('ERROR', 'PANIC', 'FATAL'):
			mo = pg_exc.ErrorLookup(c)(
				m, code = c, details = dmsg, source = source
			)
		if sev == 'WARNING':
			mo = pg_exc.WarningLookup(c)(
				m, code = c, details = dmsg, source = source
			)
		else:
			mo = pg_api.Message(
				m, code = c, details = dmsg, source = source
			)
		mo.database = self
		return mo

	def _error_lookup(self, om : element.Error,) -> pg_exc.Error:
		'lookup a PostgreSQL error and instantiate it'
		m = self._decode_pq_message(om)
		src = 'SERVER'
		if type(om) is element.ClientError:
			src = 'CLIENT'
		c = m.pop('code')
		ms = m.pop('message')
		err = pg_exc.ErrorLookup(c)
		err = err(ms, code = c, details = m, source = src)
		err.database = self
		return err

	def _receive_async(self, msg, controller = None):
		c = controller or self
		if msg.type is element.ShowOption.type:
			if msg.name == b'client_encoding':
				self.typio.set_encoding(msg.value.decode('ascii'))
			self.settings._notify(msg)
		elif msg.type is element.Notice.type:
			src = 'SERVER'
			if type(msg) is element.ClientNotice:
				src = 'CLIENT'
			m = self._convert_pq_message(msg, source = src)
			m.creator = c
			m.raise_message()
		elif msg.type is element.Notify.type:
			subs = getattr(self, '_subscriptions', {})
			for x in subs.get(msg.relation, ()):
				x(self, msg)
			if None in subs:
				subs[None](self, msg)
		else:
			w = self._warning_lookup("-1000")
			w(
				"unknown asynchronous message: " + repr(msg),
				creator = c
			).raise_message()

	def __init__(self, connector, *args, **kw):
		"""
		Create a connection based on the given connector.
		"""
		self.connector = connector
		self.typio = TypeIO(self)
		self.typio.set_encoding('ascii')
		self.settings = Settings(self)
# class Connection

class Connector(pg_api.Connector):
	"""
	All arguments to Connector are keywords. At the very least, user,
	and socket, may be provided. If socket, unix, or process is not
	provided, host and port must be.
	"""
	@property
	def _pq_iri(self):
		return pg_iri.serialize(
			{
				k : v for k,v in self.__dict__.items()
				if v is not None and not k.startswith('_') and k != 'driver'
			},
			obscure_password = True
		)

	def _e_metas(self):
		yield (None, '[' + self.__class__.__name__ + '] ' + self._pq_iri)

	def __repr__(self):
		keywords = (',' + os.linesep + ' ').join([
			'%s = %r' %(k, getattr(self, k, None)) for k in self.__dict__
			if not k.startswith('_') and getattr(self, k, None) is not None
		])
		return '{mod}.{name}({keywords})'.format(
			mod = type(self).__module__,
			name = type(self).__name__,
			keywords = os.linesep + ' ' + keywords if keywords else ''
		)

	@abstractmethod
	def socket_factory_sequence(self):
		"""
		Generate a list of callables that will be used to attempt to make the
		connection to the server. It is assumed that each factory will produce
		an object with a socket interface that is ready for reading and writing 
		data.

		The callables in the sequence must take a timeout parameter.
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
			pg_exc.IgnoredClientParameterWarning(
				"certificate revocation lists are *not* checked",
				creator = self,
			).raise_message()

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

		se = self.server_encoding or 'utf-8'
		##
		# Attempt to accommodate for literal treatment of startup data.
		##
		self._startup_parameters = {
			# All keys go in utf-8. However, ascii would probably be good enough.
			k.encode('utf-8') : \
			# If it's a str(), encode in the hinted server_encoding.
			# Otherwise, convert the object(int, float, bool, etc) into a string
			# and treat it as utf-8.
			v.encode(se) if type(v) is str else str(v).encode('utf-8')
			for k, v in tnkw.items()
		}
		self._password = (self.password or '').encode(se)
		self._socket_secure = {
			'keyfile' : self.sslkeyfile,
			'certfile' : self.sslcrtfile,
			'ca_certs' : self.sslrootcrtfile,
		}
# class Connector

class SocketConnector(Connector):
	'abstract connector for using `socket` and `ssl`'
	@abstractmethod
	def socket_factory_sequence(self):
		"""
		Return a sequence of `SocketFactory`s for a connection to use to connect
		to the target host.
		"""

class IP4(SocketConnector):
	'Connector for establishing IPv4 connections'
	ipv = 4
	def socket_factory_sequence(self):
		return self._socketcreators

	def __init__(self,
		driver,
		host : "IPv4 Address (str)" = None,
		port : int = None,
		ipv = 4,
		**kw
	):
		self.driver = driver
		if ipv != self.ipv:
			raise TypeError("'ipv' keyword must be '4'")
		if host is None:
			raise TypeError("'host' is a required keyword and cannot be 'None'")
		if port is None:
			raise TypeError("'port' is a required keyword and cannot be 'None'")
		self.host = host
		self.port = int(port)
		# constant socket connector
		self._socketcreator = SocketFactory(
			(socket.AF_INET, socket.SOCK_STREAM),
			(self.host, self.port)
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
		driver,
		host : "IPv6 Address (str)" = None,
		port : int = None,
		ipv = 6,
		**kw
	):
		self.driver = driver
		if ipv != self.ipv:
			raise TypeError("'ipv' keyword must be '6'")
		if host is None:
			raise TypeError("'host' is a required keyword and cannot be 'None'")
		if port is None:
			raise TypeError("'port' is a required keyword and cannot be 'None'")
		self.host = host
		self.port = int(port)
		# constant socket connector
		self._socketcreator = SocketFactory(
			(socket.AF_INET6, socket.SOCK_STREAM),
			(self.host, self.port)
		)
		self._socketcreators = (
			self._socketcreator,
		)
		super().__init__(**kw)

class Unix(SocketConnector):
	'Connector for establishing unix domain socket connections'
	def socket_factory_sequence(self):
		return self._socketcreators

	def __init__(self, driver, unix = None, **kw):
		self.driver = driver
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
			SocketFactory(x[0:3], x[4][:2], self._socket_secure)
			for x in socket.getaddrinfo(
				self.host, self.port, self._address_family, socket.SOCK_STREAM
			)
		]

	def __init__(self,
		driver,
		host : str = None,
		port : (str, int) = None,
		ipv : int = None,
		address_family : "address family to use(AF_INET,AF_INET6)" = None,
		**kw
	):
		self.driver = driver
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
	def _e_metas(self):
		yield (None, type(self).__module__ + '.' + type(self).__name__)

	def ip4(self, **kw):
		return IP4(self, **kw)

	def ip6(self, **kw):
		return IP6(self, **kw)

	def host(self, **kw):
		return Host(self, **kw)

	def unix(self, **kw):
		return Unix(self, **kw)

	def fit(self,
		unix = None,
		host = None,
		port = None,
		**kw
	) -> Connector:
		"""
		Create the appropriate `postgresql.api.Connector` based on the
		parameters.

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
		For information on acceptable keywords, see:

			`postgresql.documentation.driver`:Connection Keywords
		"""
		c = self.fit(**kw)()
		c.connect()
		return c

	def __init__(self, connection = Connection, typio = TypeIO):
		self.connection = connection
		self.typio = typio
