##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
PG-API interface for PostgreSQL that support the PQ version 3.0 protocol.
"""
import sys
import os

import errno
import socket
import ssl

from operator import attrgetter, itemgetter
from itertools import repeat, chain

from abc import abstractmethod, abstractproperty
import collections

from .. import versionstring as pg_version
from .. import iri as pg_iri
from .. import exceptions as pg_exc
from .. import string as pg_str
from ..encodings import aliases as pg_enc_aliases
from .. import api as pg_api

from ..protocol.buffer import pq_message_stream
from ..protocol import client3 as pq
from ..protocol import typio as pg_typio

TypeLookup = """
SELECT
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
 array_in(array_out(proargtypes::regtype[]), 'text'::regtype, 0) AS _proargs,
 (pg_type.oid = 'record'::regtype or pg_type.typtype = 'c') AS composite
FROM
 pg_proc LEFT OUTER JOIN pg_type ON (
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

class ClosedConnection(object):
	asyncs = ()
	state = pq.Complete
	fatal = True

	error_message = pq.element.Error(
		severity = 'FATAL',
		code = pg_exc.ConnectionDoesNotExistError.code,
		message = "operation on closed connection",
		hint = "Call the 'connect' method on the connection object.",
	)
	def __new__(subtype):
		return ClosedConnectionTransaction
ClosedConnectionTransaction = object.__new__(ClosedConnection)

class TypeIO(pg_typio.TypeIO):
	def __init__(self, connection):
		self.connection = connection
		super().__init__()

	def lookup_type_info(self, typid):
		return self.connection.cquery(TypeLookup).first(typid)

	def lookup_composite_type_info(self, typid):
		return self.connection.cquery(CompositeLookup)(typid)

class ResultHandle(pg_api.InterfaceElement):
	"""
	Object used to provide interfaces for COPY and simple commands.
	(Utility statements, DDL, and DML)
	"""
	ife_label = 'ResultHandle'
	ife_ancestor = property(attrgetter('query'))
	_result_types = (
		pq.element.CopyFromBegin.type,
		pq.element.CopyToBegin.type,
		pq.element.Complete.type,
		pq.element.Null.type,
	)
	_expect_types = _result_types + (pq.element.Tuple.type,)

	def __init__(self, parameters, query):
		if query._output_formats:
			# this is for ddl, copy, and utility statements.
			raise TypeError(
				"ResultHandle cannot be created " \
				"against a query returning tuple data"
			)
		self.query = query
		self.connection = query.connection
		self.parameters = parameters

	@property
	def closed(self):
		"it's closed if it's not the current transaction of the conneciton"
		return self._pq_xact is not self.connection._pq_xact
	def close(self):
		self._pq_xact.messages = self._pq_xact.CopyDoneSequence
		self.connection._pq_complete()

	def _init(self):
		self._type = None
		self._complete = None
		self._pq_xact = None
		if self.parameters:
			_pq_parameters = list(pg_typio.row_pack(
				self.parameters, self.query._input_io,
				self.connection.typio.encode
			))
		else:
			_pq_parameters = ()
		self._pq_xact = pq.Transaction((
			pq.element.Bind(
				b'',
				self.query._pq_statement_id,
				self.query._input_formats,
				_pq_parameters,
				(), # result formats; not expecting tuple data.
			),
			pq.element.Execute(b'', 1),
			pq.element.SynchronizeMessage,
		))
		self.ife_descend(self._pq_xact)
		self.connection._pq_push(self._pq_xact)

	def _fini(self):
		typ = self._discover_type(self._pq_xact)
		if typ.type == pq.element.Complete.type:
			self._complete = typ
			if self._pq_xact.state is not pq.Complete:
				self.connection._pq_complete()
		elif typ.type == pq.element.CopyToBegin.type:
			self._state = (0, 0, [])
			self._last_readsize = 0
			self.buffersize = 100
		elif typ.type == pq.element.CopyFromBegin.type:
			self._lastcopy = None
		else:
			err = pg_exc.ProtocolError(
				"%r expected COPY or utility statement" %(type(self).__name__,),
			)
			self.ife_descend(err)
			err.raise_exception()
		self._type = typ.type

	def _discover_type(self, xact):
		"""
		helper method to step until the result handle's type message
		is received. This is a copy begin message or a complete message.
		"""
		typ = self._type
		while typ is None:
			for x in xact.messages_received():
				if x.type in self._result_types:
					typ = x
					break
			else:
				# If the handle's transaction is not the current,
				# then it's already complete.
				if xact is self.connection._pq_xact:
					self.connection._pq_step()
				else:
					raise TypeError("unable to identify ResultHandle type")
				continue
			break
		else:
			# XXX: protocol error is inappropriate
			err = pg_exc.ProtocolError(
				"protocol transaction for %r finished before result " \
				"type could be identified" %(type(self).__name__,),
				source = "DRIVER",
			)
			self.ife_descend(err)
			err.connection = self.connection
			err.raise_exception()
		return typ

	def command(self):
		"The completion message's command identifier"
		if self._complete is not None:
			return self._complete.extract_command()
		if self._pq_xact is self.connection._pq_xact:
			self._fini()
			return self._complete.extract_command()

	def count(self):
		"The completion message's count number"
		if self._complete is not None:
			return self._complete.extract_count()
		if self._pq_xact is self.connection._pq_xact:
			self._fini()
			return self._complete.extract_count()

	def ife_snapshot_text(self):
		return "ResultHandle"

	def __iter__(self):
		return self

	def __next__(self):
		'get the next copy line'
		if self._state[0] > self.buffersize:
			self._contract()
		# nothing more to give?
		while self._state[0] == self._state[1]:
			# _expand() until it does
 			if self._expand() is not True: break
		offset, bufsize, buffer = self._state
		if offset == bufsize:
			raise StopIteration
		line = buffer[offset]
		self._last_readsize = 1
		self._state = (offset + 1, bufsize, buffer)
		# XXX: interrupt loss
		return line

	def _expand(self):
		"""
		[internal] helper function to put more copy data onto the buffer for
		reading. This function will only append to the buffer and never
		set the offset.

		Used to support ``COPY ... TO STDOUT ...;``
		"""
		offset, oldbufsize, buffer = self._state
		x = self._pq_xact

		# get more data while incomplete and within buffer needs/limits
		while self.connection._pq_xact is x and len(x.completed) < 2:
			self.connection._pq_step()

		if not x.completed:
			# completed list depleted, can't buffer more
			return False

		# Find the Complete message when _pq_xact is done
		if x.state is pq.Complete and self._complete is None:
			for t in x.reverse():
				##
				# NOTE: getattr is used here to avoid AttributeErrors
				# if a COPY line is found. COPY lines are not formalized
				# to reduce CPU and memory consumption.
				typ = getattr(t, 'type', None)
				##
				# Ignore the ready type
				if typ == pq.element.Ready.type:
					# The Complete message is being looked
					# for and it must come directly before the Ready.
					continue
				break
			if typ == pq.element.Complete.type:
				self._complete = t
			else:
				err = pg_exc.ProtocolError(
					"complete message not in expected location",
					source = 'DRIVER'
				)
				t.ife_descend(err)
				err.raise_exception()

		# get copy data
		extension = [
			y for y in x.completed[0][1] if type(y) is bytes
		]

		# set the next state
		self._state = (
			offset,
			oldbufsize + len(extension),
			buffer + extension,
		)
		# XXX: interrupt gain
		del x.completed[0]
		return True

	def _contract(self):
		"[internal] reduce the buffer's size"
		offset, bufsize, buffer = self._state
		# remove up to the offset minus the last read
		trim = offset - self._last_readsize

		newoffset = offset - trim
		if trim > 0 and newoffset >= 0:
			self._state = (
				newoffset,
				bufsize - trim,
				buffer[trim:]
			)

	def read(self, count = None):
		"""
		Read the specified number of COPY lines from the connection.
		"""
		if self._state[0] > self.buffersize:
			self._contract()
		offset, bufsize, buffer = self._state
		# nothing more to give, get more?
		if offset == bufsize:
			if self._pq_xact.state is pq.Complete \
			and not self._pq_xact.completed:
				# Nothing more to read...
				return []
			else:
				# not done, so get more data
				while self._expand() is True and bufsize <= offset:
					offset, bufsize, buffer = self._state
				offset, bufsize, buffer = self._state
		remain = bufsize - offset
		if count is None or count > remain:
			count = remain

		lines = buffer[offset:offset + count]
		self._last_readsize = count
		self._state = (
			offset + count,
			bufsize,
			buffer
		)
		return lines

	def __call__(self, copyseq, buffersize = 100):
		i = iter(copyseq)
		if isinstance(i, ResultHandle) and \
		i._type.type == pq.element.CopyToBegin.type:
			# connection-to-connection copy
			if getattr(self, '_lastcopyseq', None) is None:
				self._lastcopyseq = i.read()
			while self._lastcopyseq:
				self.write(self._lastcopyseq)
				# XXX: interrupt gain
				self._lastcopyseq = None
				self._lastcopyseq = i.read()
		else:
			# apparently, a random iterator
			self._lastcopyseq = getattr(self, '_lastcopyseq', [])
			self._lastcopyseq_x = getattr(self, '_lastcopyseq_x', None)
			if self._lastcopyseq_x is not None:
				self._lastcopyseq.append(self._lastcopyseq_x)
				# XXX: interrupt gain
				self._lastcopyseq_x = None

			while self._lastcopyseq is not None:
				c = len(self._lastcopyseq)
				while self._pq_xact.messages is not self._pq_xact.CopyFailSequence:
					self.connection._pq_step()
				for self._lastcopyseq_x in i:
					c += 1
					self._lastcopyseq.append(
						pq.element.CopyData(self._lastcopyseq_x)
					)
					# XXX: interrupt gain
					self._lastcopyseq_x = None
					if c > buffersize:
						self._pq_xact.messages = self._lastcopyseq
						self._lastcopyseq = []
						c = 0
						break
				else:
					# iterator depleted.
					self._pq_xact.messages = self._lastcopyseq
					self._lastcopyseq = None
					while self._pq_xact.messages is not self._pq_xact.CopyFailSequence:
						self.connection._pq_step()

	def write(self, copylines : [b'copy_line']):
		"""
		Write a sequence of COPY lines to the connection.
		"""
		if self._lastcopy is not None:
			self._pq_xact.messages.append(pq.element.CopyData(self._lastcopy))
			self._lastcopy = None
		# Step through the transaction until it's ready for more.
		while self._pq_xact.messages is not self._pq_xact.CopyFailSequence:
			self.connection._pq_step()
		m = self._pq_xact.messages = []

		for self._lastcopy in copylines:
			m.append(pq.element.CopyData(self._lastcopy))
			# XXX: interrupt gain
			self._lastcopy = None

		# send the new copy data
		self.connection._pq_step()

##
# The cursor implementation covers many cases of a cursor which contribute to
# the unfortunate complexity of this class:
#
# 1. Cursor declared by the server.
#    . It's already open(declared).
#    . The output data is *not* known. A DescribePortal message is necessary.
#    . It *must* use SQL FETCH for typed data to be received.
#    . WITH HOLD and WITH SCROLL is unknown; it is assumed to have both.
#    .. This may be resolvable if pg_cursors is available.
#    ... It will not be resolved automatically by the class.
#    . Extra metadata may be discovered using ``pg_catalog.pg_cursors``
# 2. Cursor declared by SQL from a query object.
#    . It's not open yet, and the query string must be interpolated into a
#      DECLARE statement.
#    . The output data is already known as the query has been prepared.
# 3. Cursor bound via the protocol from a query object.
#    . Only usable if WITHOUT HOLD and NO SCROLL.
#    . It uses postgresql.protocol.element3.Bind()
#    . It always pre-fetches(server-side state is not a concern).
#    . The output data is already known as the query must have been prepared.
##
# The `fetchcount` attribute dictates whether or not to read-ahead. This
# can be applied to cursors of any style, and is disabled by default on
# scrollable cursors and server declared cursors. (0 is disabled)
##
# Prior, it was suspected that these different cases would indicate the need
# for sub-classing, but the amount of overlapping functionality caused the
# integration. For instance, in the case of '1', if query discovery is
# possible, the restart() method is available and may change how the cursor
# is treated after the restart.
# 
class Cursor(pg_api.Cursor):
	"""
	Cursor interface handling all cursor objects. This class is exclusively
	used to provide an interface with an arbitrary SQL cursor.

	How, or whether, the cursor is declared depends on the configuration. If
	the cursor is initialized without reference to a query a DescribeMessage
	must be employed.
	"""
	ife_ancestor = None

	default_fetchcount = 40

	cursor_id = None
	query = None
	parameters = None

	with_hold = None
	with_scroll = None
	insensitive = None

	_output_io = None
	_output = None
	_output_formats = None
	_output_attmap = None
	_cid = -1
	_state = None
	_cursor_type = None

	@classmethod
	def from_query(type,
		parameters, query,
		with_hold = False,
		with_scroll = False,
		insensitive = True,
		fetchcount = None,
	):
		c = super().__new__(type)
		c.parameters = parameters
		c.query = query
		c.with_hold = with_hold
		c.with_scroll = with_scroll
		c.insensitive = insensitive
		##
		# fetchcount determines whether or not to pre-fetch.
		# If the cursor is not scrollable, and fetchcount
		# was not supplied, set it as the default fetchcount.
		if not with_scroll and fetchcount is None:
			fetchcount = type.default_fetchcount
		c.__init__(ID(c), query.connection, fetchcount = fetchcount)
		return c

	def __init__(self, cursor_id, connection, fetchcount = None):
		if not cursor_id:
			raise ValueError("invalid cursor identifier, " + repr(cursor_id))
		self.cursor_id = str(cursor_id)
		self._quoted_cursor_id = '"' + self.cursor_id.replace('"', '""') + '"'
		self.connection = connection
		self.fetchcount = fetchcount or 0

	def __del__(self):
		if not self.closed and ID(self) == self.cursor_id:
			self.close()

	def __iter__(self):
		return self

	@property
	def closed(self):
		return self._cid != self.connection._cid

	def close(self):
		if self.closed is False:
			self.connection._closeportals.append(self.cursor_id)
			self._cid = -2
			del self._state

	def _init(self):
		if self.query is not None:
			# If the cursor comes from a query, always
			# get the output information from it if it's closed.
			if self.query.closed:
				self.query.prepare()
			self._output = self.query._output
			self._output_formats = self.query._output_formats
			self._output_io = self.query._output_io
			self._output_attmap = self.query._output_attmap
			if self._output is None:
				raise RuntimeError(
					"cursor created against query that does not return rows"
				)

		if self._output is None and self.query is None:
			# If there's no _output, it must have been declared on the server.
			# treat it as an SQL declared cursor.
			self._cursor_type = 'declared'
			##
			# There *may* not be a way to determine the configuration.
			self.with_scroll = None
			self.with_hold = None
			##
			# The result description is needed, so get it, but *only* the
			# description. At this point it's not known whether or not
			# pre-fetching should occur.
			setup = (
				pq.element.DescribePortal(self._pq_cursor_id),
				pq.element.SynchronizeMessage,
			)
		else:
			# There's a query that's giving rows.
			##
			self._pq_cursor_id = self.connection.typio._encode(self.cursor_id)[0]
			if self.query._input_io:
				if self.parameters is None:
					raise RuntimeError(
						"cannot bind cursor when parameters are unknown"
					)
				_pq_parameters = list(pg_typio.row_pack(
					self.parameters, self.query._input_io,
					self.connection.typio.encode
				))
			else:
				# Query takes no parameters.
				self.parameters = ()
				_pq_parameters = ()
			##
			# Finish any outstanding transactions to identify
			# the current transaction state.
			if self.connection._pq_xact is not None:
				self.connection._pq_complete()
			##
			# In auto-commit mode or with_hold is on?
			if (self.connection._pq_state == b'I' or self.with_hold is True) \
			and self.query.string is not None:
				# The existence of the query string is also necessary here
				##
				##
				# Force with_hold as there is no transaction block.
				# If this were not forced, the cursor would disappear
				# before the user had a chance to read rows from it.
				# Of course, the alternative is to read all the rows. :(
				self.with_hold = True
				self._cursor_type = 'declared'
				qstr = self.query.string
				qstr = \
				'DECLARE {name}{insensitive} {scroll} '\
				'CURSOR {hold} FOR {query}'.format(
					name = self._quoted_cursor_id,
					insensitive = ' INSENSITIVE' if self.insensitive else '',
					scroll = 'SCROLL' if self.with_scroll else 'NO SCROLL',
					hold = 'WITH HOLD' if self.with_hold else 'WITHOUT HOLD',
					query = qstr
				)
				setup = (
					pq.element.Parse(b'', self.connection.typio.encode(qstr), ()),
					pq.element.Bind(
						b'', b'', self.query._input_formats, _pq_parameters, ()
					),
					pq.element.Execute(b'', 1),
				)
			else:
				# Protocol-bound cursor.
				##
				if self.with_scroll:
					# That doesn't work.
					##
					raise RuntimeError("cannot bind cursor WITH SCROLL")
				self._cursor_type = 'protocol'
				setup = (
					pq.element.Bind(
						self._pq_cursor_id,
						self.query._pq_statement_id,
						self.query._input_formats,
						_pq_parameters,
						self._output_formats,
					),
				)

		if self.fetchcount:
			# a configured fetchcount means a pre-fetching cursor.
			##
			initial_fetch = self._pq_xp_fetchmore(self.fetchcount)
		else:
			initial_fetch = ()

		x = pq.Transaction(
			setup + initial_fetch + (pq.element.SynchronizeMessage,)
		)
		self._state = (
			# position in buffer, buffer, nextdata (pq_xact)
			0, (), x
		)
		self.connection._pq_push(x)

	def _fini(self):
		if self._state[-1] is self.connection._pq_xact:
			self.connection._pq_complete()

		# if _output is unknown, _init better have described.
		if self._output is None:
			tupdesc = None
			for x in self._state[-1].messages_received():
				if getattr(x, 'type', None) == pq.element.TupleDescriptor.type:
					tupdesc = x
					break
			else:
				raise RuntimeError("cursor does not return tuples")
			# Find the TupleDescriptor message in the results, if there is None,
			# then the cursor does not return data, which means this is the wrong
			# place to process the data.
			##
			self._output = tupdesc
			self._output_attmap = self.connection.typio.attribute_map(tupdesc)
			# tuple output
			self._output_io = self.connection.typio.resolve_descriptor(
				tupdesc, 1 # (input, output)[1]
			)
			self._output_formats = [
				pq.element.StringFormat
				if x is None
				else pq.element.BinaryFormat
				for x in self._output_io
			]
		##
		# Mark the Cursor as open.
		self._cid = connection._cid

		# Cursor is ready for fetching rows.
		##

	def ife_snapshot_text(self):
		return self.cursor_id + '' if self.parameters is None else (
			os.linesep + ' PARAMETERS: ' + repr(self.parameters)
		)

	def _pq_xact_get_rows(self):
		return [
			pg_typio.Row(
				pg_typio.row_unpack(
					y,
					self._output_io,
					self.connection.typio.decode
				),
				attmap = self._output_attmap
			)
			for y in self._state[-1].messages_received()
			if y.type == pq.element.Tuple.type
		]

	def _pq_xp_fetchmore(self, count):
		'[internal] make and return a transaction to get more rows'
		if self.fetchcount:
			# ignore the count if fetchcount is non-zero
			count = self.fetchcount
		if count == 0:
			return ()

		if self._cursor_type == 'protocol':
			# If it's not a _declared_cursor, it's a protocol bound cursor.
			# Meaning, Execute will give the typed data that is longed for.
			##
			return (
				pq.element.Execute(self._pq_cursor_id, count),
			)
		elif self._cursor_type == 'declared':
			# It's an SQL declared cursor, manually construct the fetch commands.
			##
			if count is None:
				qstr = "FETCH ALL IN " + self._quoted_cursor_id
			else:
				qstr = "FETCH FORWARD " + str(count) + " IN " + \
					self._quoted_cursor_id
			return (
				pq.element.Parse(b'', self.connection.typio.encode(qstr), ()),
				pq.element.Bind(b'', b'', (), (), self._output_formats),
				# The "limit" is defined in the fetch query.
				pq.element.Execute(b'', 0xFFFFFFFF),
			)
		else:
			raise RuntimeError("invalid cursor type, " + repr(self._cursor_type))

	def _pq_xp_move(self, whence, position):
		'[internal] make a command sequence for a MOVE single command'
		return (
			pq.element.Parse(b'',
				self.connection.typio.encode(
					'MOVE %s %s IN ' %(whence, position) + self._quoted_cursor_id
				),
				()
			),
			pq.element.Bind(b'', b'', (), (), ()),
			pq.element.Execute(b'', 1),
		)

	def _contract(self):
		"reduce the number of tuples in the buffer by removing past tuples"
		offset, buffer, x = self._state
		trim = offset - self.fetchcount
		if trim > self.fetchcount:
			self._state = (
				offset - trim,
				buffer[trim:],
				x
			)

	def _expand(self, count):
		"""
		[internal] Expand the buffer with more tuples. Does not alter position.
		"""
		if self._output is None:
			# Make sure the cursor is initialized.
			if self._state is None:
				self._init()
			self._fini()
		##
		# get the current state information
		offset, buffer, x = self._state
		if x is None:
			# No previous transaction started, so make one.
			x = self._pq_xp_fetchmore(count) + (pq.element.SynchronizeMessage,)
			x = pq.Transaction(x)
			self.ife_descend(x)
			self._state = (offset, buffer, x)

		if x.state is not pq.Complete:
			# Push and complete.
			self.connection._pq_push(x)
			if self.connection._pq_xact is x:
				self.connection._pq_complete()
		elif hasattr(x, 'error_message'):
			# Retry.
			# Chances are that it will fail again,
			# but a consistent error is important.
			x.reset()
			self.connection._pq_push(x)
			if self.connection._pq_xact is x:
				self.connection._pq_complete()

		# At this point, it is expected that the transaction has more tuples
		# It's the cursor's current transaction and that won't change until
		# all the tuples have been extracted.

		##
		# Extension to the buffer.
		extension = self._pq_xact_get_rows()
		newbuffer = tuple(chain(buffer, extension))

		##
		# Final message before the Ready message('Z')
		mi = x.reverse()
		ready_msg = next(mi)
		status_msg = next(mi)

		if status_msg.type == pq.element.Complete.type:
			new_x = None
			if self.fetchcount and self._cursor_type == 'declared' \
			and len(extension) == self.fetchcount or count:
				# If the cursor is pre-fetching(not-scrollable), and
				# the extension length was fulfilled indicating
				# the possibility of more rows, setup a new transaction.
				##
				new_x = self._pq_xp_fetchmore(count) + \
					(pq.element.SynchronizeMessage,)
				new_x = pq.Transaction(new_x)
				self.ife_descend(new_x)
		elif status_msg.type == pq.element.Suspension.type:
			# Infer a protocol bound cursor.
			##
			new_x = self._pq_xp_fetchmore(count) + \
				(pq.element.SynchronizeMessage,)
			new_x = pq.Transaction(new_x)
			self.ife_descend(new_x)

		self._state = (
			offset,
			newbuffer,
			new_x
		)
		# Push must occur *after* the state is set, otherwise it would
		# be possible for data loss from the portal.
		if new_x is not None:
			self.connection._pq_push(new_x)
		return len(extension)

	def __next__(self):
		# Reduce set size if need be.
		if len(self._state[1]) > (2 * self.fetchcount):
			self._contract()

		offset, buffer, x = self._state
		while offset >= len(buffer):
			if self._expand(1) == 0:
				# End of cursor.
				##
				raise StopIteration
			offset, buffer, x = self._state
		t = buffer[offset]
		self._state = (offset + 1, buffer, x)
		return t

	def read(self, quantity = None):
		# reduce set size if need be
		if len(self._state[1]) > (2 * self.fetchcount):
			self._contract()

		offset = self._state[0]
		if quantity is None:
			# Read all.
			##
			while self._expand(None) > 0:
				pass
			##
			# Reading all, so the quantity becomes the difference
			# in the buffer length(len(1)) and the offset(0).
			quantity = len(self._state[1]) - offset
		else:
			# Read some.
			##
			left_to_read = (quantity - (len(self._state[1]) - offset))
			while left_to_read > 0:
				left_to_read -= self._expand(left_to_read)

		end_of_block = offset + quantity
		t = self._state[1][offset:end_of_block]
		self._state = (end_of_block, self._state[1], self._state[2])
		return t

	def scroll(self, quantity):
		offset, buffer, x = self._state

		newpos = offset + quantity
		if newpos < 0:
			raise RuntimeError("cannot scroll before the buffer window")
		else:
			# in range of the existing buffer
			offset = newpos
		self._state = (offset, buffer, x)

	def move(self, position : "absolute position to move to"):
		if position < 0:
			cmd = self._pq_xp_move(b'LAST', b'') + \
				self._pq_xp_move(b'BACKWARD', str(-position).decode('ascii'))
		else:
			cmd = self._pq_xp_move(b'ABSOLUTE', str(position).decode('ascii'))
		x = pq.Transaction(
			chain(
				cmd, self._pq_xp_fetchmore(0),
				(pq.element.SynchronizeMessage,)
			)
		)
		self.ife_descend(x)
		##
		# Clear all the state, the position has changed.
		self._state = (0, (), x)
		self.connection._pq_push(x)

class PreparedStatement(pg_api.PreparedStatement):
	ife_ancestor = None
	_cid = -1
	string = None
	connection = None
	statement_id = None

	@classmethod
	def from_query_string(type,
		string : "SQL statement to prepare",
		connection : "connection to bind the query to",
		statement_id : "statement_id to use instead of generating one" = None
	) -> "PreparedStatement instance":
		"""
		Create a PreparedStatement from a query string.
		"""
		r = super().__new__(type)
		r.string = string
		r.__init__(statement_id or ID(r), connection)
		return r

	def __init__(self, statement_id, connection):
		# Assume that the statement is open; it's normally a
		# statement prepared on the server in this context.
		self.statement_id = statement_id
		self.connection = connection
		self._pq_xact = None
		self._pq_statement_id = None

	def __repr__(self):
		return '<%s.%s[%s]%s>' %(
			type(self).__module__,
			type(self).__name__,
			repr(self.connection.connector),
			self.closed and ' closed' or ''
		)

	@property
	def command(self) -> str:
		"get the detected command (first uncommented word in string)"
		if self.string:
			# FIXME: needs a comment filter for proper functionality
			s = self.string.strip().split()
			if s:
				return s[0]
		return None

	@property
	def closed(self) -> bool:
		return self._cid != self.connection._cid

	def close(self):
		if self._cid == self.connection._cid:
			self.connection._closestatements.append(self._pq_statement_id)
		# Make a distinction between explicitly closed and uninitialized.
		# (could be handy)
		self._cid = -2
		self._pq_statement_id = None

	def prepare(self):
		# If there's no _init_xact or the _init_xact is not the current
		# transaction
		if self._pq_xact is None \
		or self._pq_xact is not self.connection._pq_xact:
			self._init()
		# Finalize the statement for use. It should be ready.
		self._fini()

	def ife_snapshot_text(self):
		s = ""
		if self.closed:
			s += "[closed] "
		if self.title:
			s += self.title + ", "
		s += "statement_id(" + repr(self._pq_statement_id or self.statement_id) + ")"
		s += os.linesep + ' ' * 2 + (os.linesep + ' ' * 2).join(
			str(self.string).split(os.linesep)
		)
		return s

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
		self._pq_statement_id = self.connection.typio._encode(self.statement_id)[0]
		if self.string is not None:
			q = self.connection.typio._encode(self.string)[0]
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
		self._pq_xact = pq.Transaction(cmd)
		self.connection._pq_push(self._pq_xact)

	def _fini(self):
		"""
		Complete initialization that the _init() method started.
		"""
		# assume that the transaction has been primed.
		if self._pq_xact is None:
			raise RuntimeError("_fini called prior to _init; invalid state")
		if self._pq_xact is self.connection._pq_xact:
			self.connection._pq_complete()

		(*head, argtypes, tupdesc, last) = self._pq_xact.messages_received()

		if tupdesc is None or tupdesc is pq.element.NoDataMessage:
			# Not typed output.
			self._output = None
			self._output_attmap = None
			self._output_io = None
			self._output_formats = None
		else:
			self._output = tupdesc
			self._output_attmap = self.connection.typio.attribute_map(tupdesc)
			# tuple output
			self._output_io = self.connection.typio.resolve_descriptor(tupdesc, 1)
			self._output_formats = [
				pq.element.StringFormat
				if x is None
				else pq.element.BinaryFormat
				for x in self._output_io
			]

		self._input = argtypes
		self._input_io = [
			(self.connection.typio.resolve(x) or (None,None))[0]
			for x in argtypes
	 	]
		self._input_formats = [
			pq.element.StringFormat
			if x is None
			else pq.element.BinaryFormat
			for x in self._input_io
		]
		self._cid = self.connection._cid
		self._pq_xact = None

	def __call__(self, *parameters):
		if self.closed:
			self.prepare()

		if self._output:
			# Tuple output per the results of DescribeStatement.
			##
			portal = Cursor.from_query(
				parameters, self,
				with_hold = False,
				with_scroll = False,
			)
		else:
			# non-tuple output(copy, ddl, non-returning dml)
			portal = ResultHandle(parameters, self)
		portal._init()
		return portal
	__iter__ = __call__

	def first(self, *parameters):
		if self.closed:
			self.prepare()
		# Parameters? Build em'.
		c = self.connection

		if self._input_io:
			params = list(
				pg_typio.row_pack(
					parameters,
					self._input_io,
					c.typio.encode
				)
			)
		else:
			params = ()

		# Run the statement
		x = pq.Transaction((
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
				return pg_typio.Row(
					pg_typio.row_unpack(xt, self._output_io, c.typio.decode),
					attmap = self._output_attmap
				)
			else:
				if xt[0] is None:
					return None
				io = self._output_io[0] or self.connection.typio.decode
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

	def declare(self,
		*args,
		with_hold = True,
		with_scroll = True
	):
		"""
		In contrast to the __call__ interface which defaults with_hold and
		with_scroll to False; identifying it as simple fetch all rows usage.
		"""
		dc = Cursor.from_query(args, self, with_hold = with_hold, with_scroll = with_scroll)
		dc._init()
		return dc

	def load(self, tupleseq, tps = 40):
		if self.closed:
			self.prepare()
		last = pq.element.FlushMessage
		tupleseqiter = iter(tupleseq)
		try:
			while last is pq.element.FlushMessage:
				c = 0
				xm = []
				for t in tupleseqiter:
					params = list(
						pg_typio.row_pack(
							t, self._input_io,
							self.connection.typio.encode
						)
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
				self.connection._pq_push(pq.Transaction(xm))
			self.connection._pq_complete()
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
			self.connection.synchronize()
			raise

class StoredProcedure(pg_api.StoredProcedure):
	ife_ancestor = None
	procedure_id = None

	def ife_snapshot_text(self):
		return self.procedure_id

	def __repr__(self):
		return '<%s:%s>' %(
			self.procedure_id, self.query.string
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
			word_idx.sort(key = itemgetter(1))
			current_word = word_idx.pop(0)
			for x in range(argc):
				if x == current_word[1]:
					input.append(current_word[0])
					current_word = word_idx.pop(0)
				else:
					input.append(argiter.next())
		else:
			input = args

		r = self.query(*input)
		if self.srf is True:
			if self.composite is True:
				return r
			else:
				# A generator expression is very appropriate here
				# as SRFs returning large number of rows would require
				# substantial amounts of memory.
				return (x[0] for x in r)
		else:
			if self.composite is True:
				return r.read(1)[0]
			else:
				return r.read(1)[0][0]

	def __init__(self, ident, connection, description = ()):
		# Lookup pg_proc on connection.
		if ident.isdigit():
			proctup = connection.cquery(
				ProcedureLookup + ' WHERE pg_proc.oid = $1',
				title = 'func_lookup_by_oid'
			)(int(ident))
		else:
			proctup = connection.cquery(
				ProcedureLookup + ' WHERE pg_proc.oid = $1::regprocedure',
				title = 'func_lookup_by_name'
			)(ident)
		proctup = proctup.read(1)
		if not proctup:
			raise LookupError("no function with an oid %d" %(oid,))
		proctup = proctup[0]

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
		self.query = connection.query(
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
		self.ife_descend(self.query)
		self.srf = bool(proctup.get("proretset"))
		self.composite = proctup["composite"]

class Settings(pg_api.Settings):
	ife_ancestor = property(attrgetter('connection'))

	def __init__(self, connection):
		self.connection = connection
		self.cache = {}
		self._store = []
		self._restore = []
		self._restored = {}

	def _clear_cache(self):
		self.cache.clear()

	def __getitem__(self, i):
		v = self.cache.get(i)
		if v is None:
			c = self.connection
			r = c.cquery(
				"SELECT setting FROM pg_settings WHERE name = $1",
				title = 'lookup_setting_by_name'
			)(i).read()

			if r:
				v = r[0][0]
			else:
				raise KeyError(i)
		return v

	def __setitem__(self, i, v):
		cv = self.cache.get(i)
		if cv == v:
			return

		c = self.connection
		setas = c.cquery(
			"SELECT set_config($1, $2, false)",
			title = 'set_setting',
		).first(i, v)
		self.cache[i] = setas

	def __delitem__(self, k):
		self.connection.execute(
			'RESET "{1}"'.format(k.replace('"', '""'))
		)

	def __len__(self):
		return self.connection.cquery("SELECT count(*) FROM pg_settings").first()

	def ife_snapshot_text(self):
		return "Settings"

	def __call__(self, **kw):
		# In the usual case:
		#   with connection.settings(search_path = 'public,pg_catalog'):
		# 
		# The expected effect would be that shared_buffers would be set,
		# so it's important that call prepend the settings as opposed to append.
		self._store.insert(0, kw)

	def __context__(self):
		return self

	def __enter__(self):
		# _store *usually* has one item. However, it is possible for
		# defaults to be pushed onto the stack.
		res = self.getset(self._store[0].keys())
		self.update(self._store[0])
		del self._store[0]
		self._restore.append(res)

	def __exit__(self, exc, val, tb):
		# If the transaction is open, restore the settings.
		self._restored.update(self._restore[-1])
		del self._restore[-1]
		if not self.connection.xact.failed:
			self.update(self._restored)
			self._restored.clear()

		return exc is None

	def path():
		def fget(self):
			return pg_str.split_ident(self["search_path"])
		def fset(self, value):
			self.settings['search_path'] = ','.join([
				'"%s"' %(x.replace('"', '""'),) for x in value
			])
		def fdel(self):
			if self.connection.connector.path is not None:
				self.path = self.connection.connector.path
			else:
				self.connection.query("RESET search_path")
		doc = 'structured search_path interface'
		return locals()
	path = property(**path())

	def get(self, k, alt = None):
		if k in self.cache:
			return self.cache[k]

		c = self.connection
		r = c.cquery(
			"SELECT setting FROM pg_settings WHERE name = $1",
			title = 'lookup_setting_by_name'
		)(k).read()
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
			r = self.connection.cquery(
				"SELECT name, setting FROM pg_settings WHERE name = ANY ($1)",
				title = 'lookup_settings_by_name'
			)(rkeys).read()
			self.cache.update(r)
			setmap.update(r)
			rem = set(rkeys) - set([x['name'] for x in r])
			if rem:
				raise KeyError(rem)
		return setmap

	def keys(self):
		for x, in self.connection.cquery(
			"SELECT name FROM pg_settings ORDER BY name",
			title = 'get_setting_names'
		):
			yield x
	__iter__ = keys

	def values(self):
		for x, in self.connection.cquery(
			"SELECT settings FROM pg_settings ORDER BY name",
			title = 'get_setting_values'
		):
			yield x

	def items(self):
		return self.connection.cquery(
			"SELECT name, settings FROM pg_settings ORDER BY name",
			title = 'get_settings'
		)

	def update(self, d):
		kvl = tuple(d.items())
		self.cache.update(self.connection.cquery(
			"SELECT ($1::text[][])[i][1] AS key, " \
			"set_config(($1::text[][])[i][1], $1[i][2], false) AS value " \
			"FROM generate_series(1, array_upper(($1::text[][]), 1)) g(i)",
			title = 'update_settings'
		)(kvl))

	def _notify(self, msg):
		subs = getattr(self, '_subscriptions', {})
		d = self.connection.typio._decode
		key = d(msg.name)[0]
		val = d(msg.value)[0]
		for x in subs.get(key, ()):
			x(self.connection, key, val)
		if None in subs:
			for x in subs[None]:
				x(self.connection, key, val)
		self.cache[key] = val

	def subscribe(self, key, callback):
		subs = self._subscriptions = getattr(self, '_subscriptions', {})
		callbacks = subs.setdefault(key, [])
		if callback not in callbacks:
			callbacks.append(callback)

	def unsubscribe(self, key, callback):
		subs = getattr(self, '_subscriptions', {})
		callbacks = subs.get(key, ())
		if callback in callbacks:
			callbacks.remove(callback)

class TransactionManager(pg_api.TransactionManager):
	ife_ancestor = property(attrgetter('connection'))
	level = property(attrgetter('_level'))
	connection = None

	def __init__(self, connection):
		self._level = 0
		self.connection = connection
		self.isolation = None
		self.mode = None
		self.gid = None
	
	def ife_snapshot_text(self):
		return "Transaction"

	@property
	def failed(self):
		s = self.connection._pq_state
		if s is None or s == b'I':
			return None
		elif s == b'E':
			return True
		else:
			return False

	@property
	def prepared(self):
		return tuple(
			self.connection.cquery(
				PreparedLookup,
				title = "usable_prepared_xacts"
			).first(self.connection.user)
		)

	def commit_prepared(self, gid):
		self.connection.execute(
			"COMMIT PREPARED '{1}'".format(
				gid.replace("'", "''")
			)
		)

	def rollback_prepared(self, gid):
		self.connection.execute(
			"ROLLBACK PREPARED '{1}'".format(
				gid.replace("'", "''")
			)
		)

	def _execute(self, qstring, adjustment):
		x = pq.Transaction((
			pq.element.Query(self.connection.typio._encode(qstring)[0]),
		))
		self.connection._pq_push(x)

		# The operation is going to happen. Adjust the level accordingly.
		if adjustment < 0 and self._level <= -adjustment:
			self.__init__(self.connection)
			self._level = 0
		else:
			self._level += adjustment
		self.connection._pq_complete()

	def _start_block_string(mode, isolation):
		return 'START TRANSACTION' + (
			isolation and ' ISOLATION LEVEL ' + isolation.replace(';', '') or \
			''
		) + (
			mode and ' ' + mode.replace(';', '') or ''
		)
	_start_block_string = staticmethod(_start_block_string)

	def reset(self):
		if self._level != 0:
			self._execute("ABORT", 0)
			self._level = 0

	def _start_string(self, level, isolation = None, mode = None):
		if level == 0:
			return self._start_block_string(
				mode or self.mode,
				isolation or self.isolation
			)
		else:
			return 'SAVEPOINT "xact(%d)"' %(level,)

	def start(self, isolation = None, mode = None):
		self._execute(
			self._start_string(
				self._level, isolation = isolation, mode = mode
			),
			1
		)
	begin = start

	def _commit_string(self, level):
		if level == 1:
			if self.gid is None:
				return 'COMMIT'
			else:
				return "PREPARE TRANSACTION '" + self.gid.replace("'", "''") + "'"
		else:
			return 'RELEASE "xact(%d)"' %(level - 1,)

	def commit(self):
		self._execute(self._commit_string(self._level), -1)

	def _rollback_string(self, level):
		if level == 1:
			return 'ABORT'
		else:
			return 'ROLLBACK TO "xact(%d)"' %(level - 1,)

	def rollback(self):
		if self._level == 0:
			raise TypeError("no transaction to rollback")
		self._execute(self._rollback_string(self._level), -1)
	abort = rollback

	def restart(self, isolation = None, mode = None):
		abort = self._rollback_string(self._level)
		start = self._start_string(
			self._level - 1, isolation = isolation, mode = mode
		)

		self.connection._pq_push(
			pq.Transaction((
				pq.element.Query(self.connection.typio._encode(abort)[0]),
				pq.element.Query(self.connection.typio._encode(start)[0]),
			))
		)
		self.connection._pq_complete()

	def checkpoint(self, isolation = None, mode = None):
		commit = self._commit_string(self._level)
		start = self._start_string(
			self._level - 1, isolation = isolation, mode = mode
		)

		self.connection._pq_push(
			pq.Transaction((
				pq.element.Query(self.connection.typio._encode(commit)[0]),
				pq.element.Query(self.connection.typio._encode(start)[0]),
			))
		)
		self.connection._pq_complete()

	# with statement interface
	__enter__ = start
	def __context__(self):
		return self
	def __exit__(self, type, value, tb):
		if not self.connection.closed:
			if type is None:
				self.commit()
			else:
				self.rollback()

		# Don't raise if it's an AbortTransaction
		return (type is None or type is pg_exc.AbortTransaction)

	def __call__(self, gid = None, mode = None, isolation = None):
		if self._level == 0:
			self.gid = gid
			self.mode = mode
			self.isolation = isolation
		return self

	def wrap(self, callable, *args, **kw):
		"""
		Execute the callable wrapped in a transaction.
		"""
		with self:
			try:
				xrob = callable(*args, **kw)
			except pg_exc.AbortTransaction as e:
				if self.connection.closed:
					raise
				xrob = getattr(e, 'args', (None,))[0]
		return xrob

class Connection(pg_api.Connection):
	ife_ancestor = property(attrgetter('connector'))
	connector = None

	type = None
	version_info = None
	version = None

	backend_id = None
	client_address = None
	client_port = None
	settings = Settings
	xact = TransactionManager

	def user():
		def fget(self):
			return self.cquery('SELECT current_user').first()
		def fset(self, val):
			return self.execute('SET ROLE "%s"' %(val.replace('"', '""'),))
		def fdel(self):
			self.execute("RESET ROLE")
		return locals()
	user = property(**user())

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
		if decoded['severity'].upper() == 'WARNING':
			mo = pg_exc.WarningLookup(c)(m, code = c, details = dmsg)
		else:
			mo = pg_api.Message(m, code = c, details = dmsg)
		mo.connection = self
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

	def _update_encoding(connection, key, value):
		'[internal] subscription method to client_encoding on settings'
		connection.typio.set_encoding(value)
	_update_encoding = staticmethod(_update_encoding)

	def _update_timezone(connection, key, value):
		'[internal] subscription method to TimeZone on settings'
		offset = connection.cquery(
			"SELECT EXTRACT(timezone FROM now())"
		).first()
		connection.typio.set_timezone(offset, value)
	_update_timezone = staticmethod(_update_timezone)

	def _update_server_version(connection, key, value):
		connection.version_info = pg_version.split(value)
	_update_server_version = staticmethod(_update_server_version)

	def cquery(self, *args, **kw):
		"""
		Create a query and store it in a dictionary that associates the
		created query with the string that defines the query. Passing the
		string back into the method will yield the same query object that was
		returned by prior calls. (Not consistent across disconnects)
		"""
		try:
			return self._query_cache[args[0]]
		except KeyError:
			return self._query_cache.setdefault(
				args[0], self.query(*args, **kw)
			)

	def __repr__(self):
		return '<%s.%s[%s] %s>' %(
			type(self).__module__,
			type(self).__name__,
			str(self.connector),
			self.closed and 'closed' or '%s.%d' %(
				self._pq_state, self.xact._level
			)
		)

	def ife_snapshot_text(self):
		lines = [
			('version', repr(self.version) if self.version else "<unknown>"),
			('backend_id',
				repr(self.backend_id) if self.backend_id else "<unknown>"),
			('client_address', str(self.client_address)),
			('client_port', str(self.client_port)),
		]
		# settings state.
		return "closed" if self.closed else (
			"" + os.linesep + '  ' + (
				(os.linesep + '  ').join([x[0] + ': ' + x[1] for x in lines])
			)
		)

	def __context__(self):
		return self

	def __exit__(self, type, value, tb):
		'Close the connection on exit.'
		self.close()
		return type is None

	def synchronize(self):
		"""
		Explicitly send a Synchronize message to the backend.
		Useful for forcing the completion of lazily processed transactions.
		[Avoids garbage collection not pushing; _complete, then set _xact.]
		"""
		if self._pq_xact is not None:
			self._pq_complete()
		x = pq.Transaction((pq.element.SynchronizeMessage,))
		self._pq_xact = x
		self._pq_complete()

	def interrupt(self, timeout = None):
		cq = pq.element.CancelQuery(
			self._pq_killinfo.pid, self._pq_killinfo.key
		).bytes()
		s = self._socketfactory(timeout = timeout)
		try:
			s.sendall(cq)
		finally:
			s.close()

	def execute(self, query : str) -> None:
		q = pq.Transaction((
			pq.element.Query(self.typio.encode(query)),
		))
		self.ife_descend(q)
		self._pq_push(q)
		self._pq_complete()

	def query(self, query_string : str, title = None) -> PreparedStatement:
		ps = PreparedStatement.from_query_string(query_string, self)
		self.ife_descend(ps)
		ps._init()
		return ps

	def statement(self, statement_id : str) -> PreparedStatement:
		ps = PreparedStatement(statement_id, self)
		self.ife_descend(ps)
		ps._init()
		return ps

	def proc(self, proc_id : (str, int)) -> StoredProcedure:
		sp =  StoredProcedure(proc_id, self)
		self.ife_descend(sp)
		return sp

	def cursor(self, cursor_id : str) -> Cursor:
		c = Cursor(cursor_id, self)
		self.ife_descend(c)
		c._init()
		return c

	def close(self):
		# Write out the disconnect message if the socket is around.
		# If the connection is known to be lost, don't bother. It will
		# generate an extra exception.
		try:
			if self.socket is not None and self._pq_state != 'LOST':
				self._write_messages((pq.element.DisconnectMessage,))
		finally:
			if self.socket is not None:
				self.socket.close()
			self._clear()
			self._reset()

	@property
	def closed(self) -> bool:
		return self._pq_xact is ClosedConnectionTransaction \
		or self._pq_state in ('LOST', None)

	def reset(self):
		"""
		restore original settings, reset the transaction, drop temporary
		objects.
		"""
		self.xact.reset()
		self.execute("RESET ALL")

	def connect(self, timeout = None):
		'Establish the connection to the server'
		# already connected?
		if self.closed is not True:
			return
		self._cid += 1
		self.typio.set_encoding('ascii')

		timeout = timeout or self.connector.connect_timeout
		sslmode = self.connector.sslmode

		# get the list of sockets to try
		socket_makers = self.connector.socket_factory_sequence()
		connection_failures = []

		# resolve when to do SSL.
		with_ssl = zip(repeat(True, len(socket_makers)), socket_makers)
		without_ssl = zip(repeat(False, len(socket_makers)), socket_makers)
		if sslmode == 'allow':
			# first, without ssl, then with. :)
			pair = (iter(with_ssl), iter(without_ssl))
			socket_makers = list((
				next(pair[x]) for x in cycle(range(2))
			))
		elif sslmode in ('prefer', 'require'):
			socket_makers = with_ssl
		else:
			socket_makers = without_ssl

		# for each potential socket
		for (dossl, socket_maker) in socket_makers:
			supported = None
			try:
				self.socket = socket_maker(timeout = timeout)
				if dossl:
					supported = self._negotiate_ssl()
					if supported is None:
						# probably not PQv3..
						raise pg_exc.ProtocolError(
							"server did not support SSL negotiation",
							source = 'DRIVER'
						)
					if not supported and sslmode == 'require':
						# ssl is required..
						raise pg_exc.InsecurityError(
							"sslmode required secure connection, " \
							"but was unsupported by server",
							source = 'DRIVER'
						)
					if supported:
						self.socket = self.connector.socket_secure(self.socket)
				# time to negotiate
				negxact = self._negotiation()
				self._pq_xact = negxact
				self._pq_complete()
				self._pq_killinfo = negxact.killinfo
				# Use this for `interrupt`.
				self._socketfactory = socket_maker
				self.ssl = supported is True
				# success!
				break
			except self.connector.fatal_exception as e:
				self.ssl = None
				self._pq_killinfo = None
				self._pq_xact = None
				if self.socket is not None:
					self.socket.close()
					self.socket = None
				connection_failures.append(
					(dossl, socket_maker, e)
				)
		else:
			# No servers available.
			err = pg_exc.ClientCannotConnectError(
				"failed to connect to server",
				details = {
					"severity" : "FATAL",
				},
				# It's really a collection of exceptions.
				source = 'DRIVER'
			)
			self.ife_descend(err)
			err.fatal = True
			err.connection = self
			err.set_connection_failures(connection_failures)
			err.raise_exception()
		self._init()
	__enter__ = connect

	def _negotiate_ssl(self) -> (bool, None):
		"""
		[internal] Negotiate SSL

		If SSL is available--received b'S'--return True.
		If SSL is unavailable--received b'N'--return False.
		Otherwise, return None. Indicates non-PQv3 endpoint.
		"""
		self._write_messages((pq.element.NegotiateSSLMessage,))
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
		'[internal] configure the connection after negotiation'
		# Use the version_info and integer_datetimes setting to identify
		# the necessary binary type i/o functions to use.
		self.backend_id = self._pq_killinfo.pid
		self.typio.select_time_io(
			self.version_info,
			self.settings.cache.get("integer_datetimes", "off").lower() in (
				't', 'true', 'on', 'yes',
			),
		)
		# Get the *full* version string.
		self.version = self.query("SELECT pg_catalog.version()").first()
		# First word from the version string.
		self.type = self.version.split()[0]

		try:
			r = self.query(
				"SELECT * FROM pg_catalog.pg_stat_activity " \
				"WHERE procpid = " + str(self.backend_id)
			).first()
			self.client_address = r.get('client_addr')
			self.client_port = r.get('client_port')
			self.backend_start = r.get('backend_start')
		except pg_exc.Error as e:
			w = pg_exc.Warning("failed to get pg_stat_activity data")
			e.ife_descend(w)
			w.emit()
			# Toss a warning instead of the error.
			# This is not vital information, but is useful in exceptions.

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
				msg = self.connector.fatal_error_message(e)
				if msg is not None:
					lost_connection_error = pg_exc.ConnectionFailureError(
						msg,
						details = {
							'severity' : 'FATAL',
							'detail' : 'Connector identified the exception as fatal.',
						},
						source = 'DRIVER'
					)
					lost_connection_error.connection = self
					lost_connection_error.fatal = True
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
						'detail' : "Zero-length string read from the connection's socket.",
					},
					source = 'DRIVER'
				)
				lost_connection_error.connection = self
				lost_connection_error.fatal = True
				self._pq_state = b'LOST'
				(self._pq_xact or self).ife_descend(lost_connection_error)
				lost_connection_error.raise_exception(raise_from = e)

			# Got data. Put it in the buffer and clear _read_data.
			self._pq_in_buffer.write(self._read_data)
			self._read_data = None

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
			msg = self.connector.fatal_error_message(e)
			if msg is not None:
				lost_connection_error = pg_exc.ConnectionFailureError(
					msg,
					details = {
						'severity' : 'FATAL',
					},
					source = 'DRIVER',
				)
				lost_connection_error.connection = self
				lost_connection_error.fatal = True
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
		x = pq.Transaction(xm)
		self._pq_xact = x
		del self._closeportals[:portals], self._closestatements[:statements]
		self._pq_complete()

	def _pq_push(self, xact : pq.ProtocolState):
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
		err.connection = self
		return err

	def _procasyncs(self):
		'[internal] process the async messages in self._asyncs'
		if self._n_procasyncs:
			return
		try:
			self._n_procasyncs = True
			while self._asyncs:
				for x in self._asyncs[0][1]:
					getattr(self, '_' + x.type.decode('ascii'))(x, self._asyncs[0])
				del self._asyncs[0]
		finally:
			self._n_procasyncs = False

	def _pq_step(self):
		'[internal] make a single transition on the transaction'
		try:
			dir, op = self._pq_xact.state
			if dir is pq.Sending:
				self._write_messages(self._pq_xact.messages)
				# The "op" callable will either switch the state, or
				# set the 'messages' attribute with a new sequence
				# of message objects for more writing.
				op()
			elif dir is pq.Receiving:
				self._read_messages()
				self._read = self._read[op(self._read):]
		except (socket.error, IOError) as e:
			# Unlike _complete, this catches at the outermost level
			# as there is no loop here for more transitioning.
			if e[0] == errno.EINTR:
				# Can't read or write, ATM? Consider it a transition. :(
				return errno.EINTR
			else:
				raise
		self._pq_state = getattr(self._pq_xact, 'last_ready', self._pq_state)
		if self._pq_xact.state is pq.Complete:
			self._pq_pop()

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

	def _pq_pop(self):
		'[internal] remove the transaction and raise the exception if any'
		# collect any asynchronous messages from the xact
		if self._pq_xact not in (x[0] for x in self._asyncs):
			self._asyncs.append((self._pq_xact, list(self._pq_xact.asyncs())))
			self._procasyncs()

		em = getattr(self._pq_xact, 'error_message', None)
		if em is not None:
			xact_error = self._postgres_error(em)
			self._pq_xact.ife_descend(xact_error)
			if self._pq_xact.fatal is not True:
				# only remove the transaction if it's *not* fatal
				xact_error.fatal = False
				self._pq_xact = None
			else:
				xact_error.fatal = True
			xact_error.raise_exception()
		# state is Complete, so remove it as the working transaction
		self._pq_xact = None

	def _clear(self):
		"""
		[internal] Clear container objects of data.
		"""
		del self._closestatements[:]
		del self._closeportals[:]

		self._pq_in_buffer.truncate()
		self.xact.__init__(self)
		self.settings._clear_cache()

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

		self._pq_killinfo = None
		self.backend_id = None
		self._pq_state = None

		self._pq_xact = ClosedConnectionTransaction

	def __init__(self, connector, *args, **kw):
		"""
		Create a connection based on the given connector.
		"""
		self._cid = 0
		self.connector = connector
		self._closestatements = []
		self._closeportals = []
		self._pq_in_buffer = pq_message_stream()
		self._readbytes = 2048
		# cquery
		self._query_cache = {}

		self.typio = TypeIO(self)
		self.settings = Settings(self)
		self.xact = TransactionManager(self)
		# Update the _encode and _decode attributes on the connection
		# when a client_encoding ShowOption message comes in.
		self.settings.subscribe('client_encoding', self._update_encoding)
		self.settings.subscribe('server_version', self._update_server_version)
		self.settings.subscribe('TimeZone', self._update_timezone)
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

	def __repr__(self):
		return type(self).__module__ + '.' + type(self).__name__ + '(%s)' %(
			',' + os.linesep + ' '.join([
				'%s = %r' %(k, getattr(self, k, None)) for k in self.words
				if getattr(self, k, None) is not None
			]),
		)

	@abstractmethod
	def socket_factory_sequence(self) -> [collections.Callable]:
		"""
		Generate a list of callables that will be used to attempt to make the
		connection to the server. It is assumed that each factory will produce an
		object with a socket interface that is ready for reading and writing data.

		The callables in the sequence must take a timeout parameter.
		"""

	@abstractmethod
	def socket_secure(self, socket):
		"""
		Given a socket produced using one of the callables created in the
		`socket_factory_sequence`, secure it using SSL.
		"""

	def __init__(self,
		connect_timeout : int = None,
		server_encoding : "server encoding hint for driver" = None,
		sslmode : ('allow', 'prefer', 'require', 'disable') = 'prefer',
		sslcrtfile : "filepath" = None,
		sslkeyfile : "filepath" = None,
		sslrootcrtfile : "filepath" = None,
		sslrootcrlfile : "filepath" = None,

		path : list = None,
		role : str = None,
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

		# Startup message parameters.
		tnkw = {}
		if self.settings:
			s = dict(self.settings)
			sp = s.get('search_path')
			if not isinstance(sp, str):
				s['search_path'] = ','.join(
					pg_str.quote_ident(x) for x in sp
				)
			tnkw.update(s)

		tnkw['user'] = self.user
		if self.database is not None:
			tnkw['database'] = self.database
		tnkw['standard_conforming_strings'] = True

		self._startup_parameters = tnkw
# class Connector

class SocketCreator(object):
	def __call__(self, timeout = None):
		s = socket.socket(*self.socket_create)
		s.settimeout(timeout)
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
		return pg_iri.serialize(self.__dict__, obscure_password = True)

	fatal_exception_messages = {
		errno.ECONNRESET : 'server explicitly closed the connection',
		errno.EPIPE : 'broken connection detected on send',
	}
	timeout_exception = socket.timeout
	fatal_exception = socket.error
	tryagain_exception = socket.error

	def tryagain(self, err) -> bool:
		# pretty easy; should the user try the operation again?
		return getattr(err, 'errno', 0) == errno.EINTR

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
			certfile = self.sslcertfile,
			ca_certs = self.sslrootcertfile,
		)
		# XXX: check revocation list?

class IP4(SocketConnector):
	'Connector for establishing IPv4 connections'
	ipv = 4

	def socket_factory_sequence(self):
		return self._socketcreators

	def __init__(self,
		ipv = 4,
		host = None,
		port = None,
		**kw
	):
		if ipv != self.ipv:
			raise TypeError("`ipv` keyword must be `4`")
		if host is None:
			raise TypeError("`host` is a required keyword and cannot be `None`")
		if port is None:
			raise TypeError("`port` is a required keyword and cannot be `None`")
		self.host = host
		self.port = port
		# constant socket connector
		self._socketcreator = SocketCreator(
			(socket.AF_INET4, socket.SOCK_STREAM), (self.host, self.port)
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

	def __init__(self, host = None, port = None, ipv = 6, **kw):
		if ipv != self.ipv:
			raise TypeError("`ipv` keyword must be `6`")
		if host is None:
			raise TypeError("`host` is a required keyword and cannot be `None`")
		if port is None:
			raise TypeError("`port` is a required keyword and cannot be `None`")
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
			raise TypeError("`unix` is a required keyword and cannot be `None`")
		self.unix = unix
		# constant socket connector
		self._socketcreator = SocketCreator((socket.AF_UNIX, socket.SOCK_STREAM), self.unix)
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
			raise TypeError("`host` is a required keyword")
		if port is None:
			raise TypeError("`port` is a required keyword")

		if address_family is not None and ipv is not None:
			raise TypeError("`ipv` and `address_family` on mutually exclusive")

		if ipv is None:
			self._address_family = address_family or socket.AF_UNSPEC
		elif ipv == 4:
			self._address_family = socket.AF_INET
		elif ipv == 6:
			self._address_family = socket.AF_INET6
		else:
			raise TypeError("unknown IP version selected: `ipv` = " + repr(ipv))
		self.host = host
		self.port = port
		super().__init__(**kw)

implementation = None
class Driver(pg_api.Driver):
	ife_ancestor = None
	# Connectors
	IP4 = IP4
	IP6 = IP6
	Host = Host
	Unix = Unix

	def select(self,
		unix = None,
		host = None,
		port = None,
	) -> Connector:
		"""
		Select the appropriate `Connector` based on the parameters.

		This also protects against mutually exclusive parameters.
		"""
		if unix is not None:
			if host is not None:
				raise TypeError("`unix` and `host` keywords are exclusive")
			if port is not None:
				raise TypeError("`unix` and `port` keywords are exclusive")
			return self.Unix
		else:
			if host is None or port is None:
				raise TypeError("`host` and `port`, or `unix` must be supplied")
			# We have a host and a port.
			# If it's an IP address, IP4 or IP6 should be selected.
			if ':' in host:
				# There's a ':' in host, good chance that it's IPv6.
				try:
					socket.inet_pton(socket.AF_INET6, host)
					return self.IP6
				except (socket.error, NameError):
					pass

			# Not IPv6, maybe IPv4...
			try:
				socket.inet_aton(host)
				# It's IP4
				return self.IP4
			except socket.error:
				pass

			# neither host, nor port are None, probably a hostname.
			return self.Host

	def create(self,
		unix = None,
		host = None,
		port = None,
		**kw
	) -> Connector:
		c = self.select(unix = unix, host = host, port = port)
		if c is self.Unix:
			return c(unix = unix, **kw)
		# Everything else uses host and port.
		return c(host = host, port = port, **kw)

	def connect(self,
		unix = None,
		host = None,
		port = None,
		**kw
	) -> Connection:
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
		cid = set(kw.items())
		if cid in self._connectors:
			c = self._connectors[cid]
		else:
			c = self._connectors[cid] = self.create(**kw)
			self.ife_descend(c)
		return c()

	def ife_snapshot_text(self):
		return 'postgresql.driver.pq3'

	def __init__(self):
		self._connectors = dict()
		self.ife_descend(
			self.Unix,
			self.IP4,
			self.IP6,
			self.Host,
		)

	def __new__(subtype):
		# There is only one instance of postgresql.driver.pq3.
		return implementation
implementation = pg_api.Driver.__new__(Driver)
implementation.__init__()
