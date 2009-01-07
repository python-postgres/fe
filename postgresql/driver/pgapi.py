##
# copyright 2008, pg/python project.
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
import warnings
import types

import postgresql.version as pg_version
import postgresql.iri as pg_iri
import postgresql.exceptions as pg_exc
import postgresql.strings as pg_str
import postgresql.encodings.aliases as pg_enc_aliases
import postgresql.api as pg_api

import postgresql.protocol.client3 as pq
from postgresql.protocol.buffer import pq_message_stream
import postgresql.protocol.typio as pg_typio

def secure_socket(
	connection,
	keyfile = None,
	certfile = None,
	rootcertfile = None,
):
	"take a would be connection socket and secure it with SSL"
	return ssl.wrap_socket(
		connection.socket,
		keyfile = keyfile,
		certfile = certfile,
		ca_certs = rootcertfile,
	)

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

IDNS = '%s(py:%s)'
def ID(s, title = None):
	'generate an id for a client statement or cursor'
	return IDNS %(title or 'untitled', hex(id(s)))

def idxiter(tupseq, idx):
	for x in tupseq:
		yield x[idx]

def subidxiter(seq, idx):
	for x in seq:
		for y in x[idx]:
			yield y

class ClosedConnection(object):
	asyncs = ()
	state = (pq.Complete, None)
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

def extract_count(rmsg):
	"""
	helper function to get the last set of digits in a command completion
	as an integer.
	"""
	if rmsg is not None:
		rms = rmsg.strip().split()
		if rms[0].lower() == 'copy':
			if len(rms) > 1:
				return int(rms[-1])
		elif rms[-1].isdigit():
			return int(rms[-1])

class ResultHandle(object):
	"""
	Object used to provide interfaces for copy and simple commands. Ties to
	a given transaction. These objects provide a way for transactions to be
	marked as defunct, so that no more time is wasted completing them.
	"""

	_result_types = (
		pq.element.CopyFromBegin.type,
		pq.element.CopyToBegin.type,
		pq.element.Complete.type,
		pq.element.Null.type,
	)
	_expect_types = _result_types + (pq.element.Tuple.type,)

	def __init__(self, connection, query, aformats, arguments):
		self.query = query
		self.connection = query.connection
		self.type = None
		self.complete = None

		self.xact = pq.Transaction((
			pq.element.Bind(
				'',
				self.query.statement_id,
				aformats, # formats
				arguments, # args
				(), # rformats; not expecting tuple data.
			),
			pq.element.Execute('', 1),
			pq.element.SynchronizeMessage,
		))
		self.connection._push(self.xact)

		typ = self._discover_type(self.xact)
		if typ.type == pq.element.Complete.type:
			self.complete = typ
			if self.xact.state[0] is not pq.Complete:
				self.connection._complete()
		elif typ.type == pq.element.CopyToBegin.type:
			self._state = (0, 0, [])
			self._last_readsize = 0
			self.buffersize = 100
		elif typ.type == pq.element.CopyFromBegin.type:
			self._lastcopy = None
		else:
			raise pq.ProtocolError(
				"%r expected COPY or utility statement" %(type(self).__name__,),
				typ, self._expect_types
			)

		self.type = typ

	def _discover_type(self, xact):
		"""
		helper method to step until the result handle's type message
		is received. This is a copy begin message or a complete message.
		"""
		typ = self.type
		while typ is None:
			for x in subidxiter(xact.completed, 1):
				if x.type in self._result_types:
					typ = x
					break
			else:
				# If the handle's transaction is not the current,
				# then it's already complete.
				if xact is self.connection._xact:
					self.connection._step()
				else:
					raise TypeError("unable to identify ResultHandle type")
				continue
			break
		else:
			raise pq.ProtocolError(
				"protocol transaction for %r finished before result " \
				"type could be identified" %(type(self).__name__,),
				None, self._result_types
			)
		return typ

	def command(self):
		"The completion message's command identifier"
		if self.complete is not None:
			return str.__str__(self.complete).strip().split()[0]

	def count(self):
		"The completion message's count number"
		if self.complete is not None:
			return extract_count(self.complete)

	def __iter__(self):
		return self

	def __next__(self):
		"""get the next copy line"""
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
		internal helper function to put more copy data onto the buffer for
		reading. This function will only append to the buffer and never
		set the offset.
		"""
		offset, oldbufsize, buffer = self._state
		x = self.xact

		# get more data while incomplete and within buffer needs/limits
		while self.connection._xact is x and len(x.completed) < 2:
			self.connection._step()

		if not x.completed:
			# completed list depleted, can't buffer more
			return False

		# check for completion message after xact is done
		if x.state[0] is pq.Complete and self.complete is None:
			for t in x.reverse():
				typ = getattr(t, 'type', None)
				# ignore the ready type
				if typ == pq.element.Ready.type:
					continue
				break
			if typ == pq.element.Complete.type:
				self.complete = t
			else:
				raise pq.ProtocolError(
					"complete message not in expected location"
				)

		# get copy data
		extension = [
			y for y in x.completed[0][1] if type(y) is str
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
		"reduce the buffer's size"
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
			if self.xact.state[0] is pq.Complete and not self.xact.completed:
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
		i.type.type == pq.element.CopyToBegin.type:
			if getattr(self, '_lastcopyseq', None) is None:
				self._lastcopyseq = i.read()
			while self._lastcopyseq:
				self.write(self._lastcopyseq)
				# XXX: interrupt gain
				self._lastcopyseq = None
				self._lastcopyseq = i.read()
		else:
			self._lastcopyseq = getattr(self, '_lastcopyseq', [])
			self._lastcopyseq_x = getattr(self, '_lastcopyseq_x', None)
			if self._lastcopyseq_x is not None:
				self._lastcopyseq.append(self._lastcopyseq_x)
				# XXX: interrupt gain
				self._lastcopyseq_x = None

			while self._lastcopyseq is not None:
				c = len(self._lastcopyseq)
				while self.xact.messages is not self.xact.CopyFailSequence:
					self.connection._step()
				for self._lastcopyseq_x in i:
					c += 1
					self._lastcopyseq.append(
						pq.element.CopyData(self._lastcopyseq_x)
					)
					# XXX: interrupt gain
					self._lastcopyseq_x = None
					if c > buffersize:
						self.xact.messages = self._lastcopyseq
						self._lastcopyseq = []
						c = 0
						break
				else:
					# iterator depleted.
					self.xact.messages = self._lastcopyseq
					self._lastcopyseq = None
					while self.xact.messages is not self.xact.CopyFailSequence:
						self.connection._step()

	def write(self, copylines):
		"""
		Write a sequence of COPY lines to the connection.
		"""
		if self._lastcopy is not None:
			self.xact.messages.append(
				pq.element.CopyData(self._lastcopy)
			)
			self._lastcopy = None
		# Step through the transaction until it's ready for more.
		while self.xact.messages is not self.xact.CopyFailSequence:
			self.connection._step()
		m = self.xact.messages = []

		for self._lastcopy in copylines:
			m.append(pq.element.CopyData(self._lastcopy))
			# XXX: interrupt gain
			self._lastcopy = None

		self.connection._step()

	closed = property(
		fget = lambda s: s.xact is not s.connection._xact,
		doc = 'whether the result handle is the current transaction'
	)
	def close(self):
		self.xact.messages = self.xact.CopyDoneSequence
		self.connection._complete()

class Portal(pg_api.Cursor):
	"""
	Class for controlling a Postgres protocol cursor. Provides buffering,
	seeking, and iterator interfaces
	"""
	whence_map = {
		0 : 'ABSOLUTE',
		1 : 'RELATIVE',
		2 : 'LAST',
	}

	def __init__(self, connection, portal_id, fetchcount = 30):
		self.connection = connection
		self.portal_id = portal_id
		self.fetchcount = fetchcount

	def __iter__(self):
		return self

	def _contract(self):
		"reduce the number of tuples in the buffer by removing past tuples"
		offset, bufsize, buffer, x = self._state
		trim = offset - self.fetchcount
		if trim > self.fetchcount:
			self._state = (
				offset - trim,
				bufsize - trim,
				buffer[trim:],
				x
			)

	def _xp_fetchmore(self, count = None):
		return pq.Transaction((
			pq.element.Execute(
				self.portal_id, count or self.fetchcount or 10
			),
			pq.element.SynchronizeMessage,
		))

	def _expand(self, count = None, next_xact = None):
		"""
		internal helper function to put more tuples onto the buffer for
		reading. This function will only append to the buffer and never
		set the offset.
		"""
		offset, oldbufsize, buffer, x = self._state
		# end of portal
		if x is None:
			return False

		# be sure to complete the transaction
		if x is self.connection._xact:
			self.connection._complete()
		elif x.state[0] is not pq.Complete:
			# push and complete if need be
			self.connection._push(x)
			self.connection._complete()
		elif hasattr(x, 'error_message'):
			x.reset()
			self.connection._push(x)
			return True

		extension = [
			self._mktuple(y)
			for y in subidxiter(x.completed, 1)
			if y.type == pq.element.Tuple.type
		]
		newbuffer = buffer + extension
		bufsize = oldbufsize + len(extension)
		# final message before the Ready message('Z')
		for final_msg in x.reverse():
			if final_msg.type == pq.element.Complete.type:
				# end of portal
				new_x = None
			elif final_msg.type == pq.element.Suspension.type:
				new_x = self._xp_fetchmore(count)
			elif final_msg.type == pq.element.Ready.type:
				continue
			else:
				raise pq.ProtocolError(
					"unexpected final message %r, expecting %r" %(
						final_msg.type, (
							pq.element.Complete.type,
							pq.element.Suspension.type,
						)
					), final_msg, expected = (
						pq.element.Complete.type,
						pq.element.Suspension.type,
					)
				)
			break
		self._state = (
			offset,
			bufsize,
			newbuffer,
			new_x
		)
		# push must occur after the state is set, otherwise it would
		# be possible for data loss from the portal
		if new_x is not None:
			self.connection._push(new_x)
		return not (bufsize == oldbufsize)

	def __getitem__(self, i):
		q = 'FETCH ABSOLUTE %d IN "%s"' %(i+1, self.portal_id)
		xact = self.connection._xp_query(q, rformats = self._rformats)
		self.connection._push(xact)
		self.connection._complete()
		for x in subidxiter(xact.completed, 1):
			if x.type == pq.element.Tuple.type:
				break
		if x.type != pq.element.Tuple.type:
			raise IndexError("portal index out of range(%d)" %(i,))
		return self._mktuple(x)

	def close(self):
		if self.closed is False:
			self.connection._closeportals.append(self.portal_id)
			self.closed = True

	def next(self):
		# reduce set size if need be
		if self._state[1] > (2 * self.fetchcount):
			self._contract()

		offset, bufsize, buffer, x = self._state
		while offset >= bufsize:
			if self._expand() is False:
				self._state = (bufsize, bufsize, buffer, x)
				raise StopIteration
			offset, bufsize, buffer, x = self._state
		t = buffer[offset]
		self._state = (
			offset + 1, bufsize, buffer, x
		)
		return t

	def read(self, quantity = 0xFFFFFFFF):
		# reduce set size if need be
		if self._state[1] > (2 * self.fetchcount):
			self._contract()

		# expand the buffer until there is enough for the request
		offset, bufsize, buffer, x = self._state
		while bufsize - offset < quantity and self._expand() is True:
			offset, bufsize, buffer, x = self._state
		offset, bufsize, buffer, x = self._state

		newoff = offset + quantity
		t = buffer[offset:newoff]
		self._state = (min(newoff, bufsize), bufsize, buffer, x)
		return t

	def _xp_move(self, whence, position, count = None):
		return (
			pq.element.Parse('',
				'MOVE %s %d IN "%s"' %(
					whence, position,
					self.portal_id.replace('"', '""')
				), ()
			),
			pq.element.Bind('', '', (), ()),
			pq.element.Execute('', 1),
			pq.element.Execute(self.portal_id, count or self.fetchcount),
			pq.element.SynchronizeMessage,
		)

	def scroll(self, quantity):
		offset, bufsize, buffer, x = self._state

		# Identify the relative server position, bufsize + ntups
		if x is not None:
			self.connection._push(x)
			if x is self.connection._xact:
				self.connection._complete()
			newtups = 0
			for y in subidxiter(x.completed, 1):
				if y.type is pq.element.Tuple.type:
					newtups += 1
			totalbufsize = newtups + bufsize
		else:
			totalbufsize = bufsize

		newpos = quantity + offset
		if newpos < 0:
			precede_by = self.fetchcount // 2
			scrollback = (totalbufsize - newpos)
			raise NotImplementedError("cannot scroll prior to buffer window")
		else:
			# in range of the existing buffer
			new_x = x
			offset = newpos
		self._state = (
			offset,
			bufsize,
			buffer,
			new_x
		)

	def move(self, location, count = None):
		if location < 0:
			x = pq.Transaction(
				(
					pq.element.Parse('',
						b'MOVE LAST IN "%s"' %(
							self.portal_id.replace(b'"', b'""'),
						), ()
					),
					pq.element.Bind(b'', b'', (), ()),
					pq.element.Execute(b'', 1),
				) + \
				self._xp_move(b'RELATIVE', location, count = count)
			)
		else:
			x = pq.Transaction(
				self._xp_move(b'ABSOLUTE', location, count = count)
			)
		# Just forget about the last transaction, there's nothing there for
		# the user after the move.
		self.connection._push(x)
		self._state = (0, 0, [], x)
		self._expand()

	def seek(self, location, whence = None, count = None):
		if whence is None:
			whence = b'ABSOLUTE'
		elif whence in whence_map or \
		whence in self.whence_map.values():
			whence = self.whence_map.get(whence, whence)
		else:
			raise TypeError(
				"unknown whence parameter, %r" %(whence,)
			)
		if whence == b'RELATIVE':
			return self.scroll(location)
		elif whence == b'ABSOLUTE':
			return self.move(location, count = count)
		else:
			return self.move(-location, count = count)

class Cursor(Portal):
	"Server declared cursor interfaces"
	def __init__(self, connection, cursor_name, **kw):
		self.closed = False
		Portal.__init__(self, connection, cursor_name, **kw)
		x = pq.Transaction((
			pq.element.DescribePortal(cursor_name),
			pq.element.SynchronizeMessage,
		))
		self.connection._push(x)
		self.connection._complete()
		xi = x.reverse()
		xi.next()
		self.output = xi.next()
		self._output_io = []
		self._rformats = []
		for x in self.output:
			fmt = x[6]
			self._output_io.append(
				fmt == pq.element.BinaryFormat and
				(connection.typio.lookup(x[3]) or (None,None))[1] or
				None
			)
			self._rformats.append(fmt)
		self._output_attmap = attmap_from_pqdesc(self.output)

		prime = self._xp_fetchmore(self.fetchcount)
		self._state = (
			# position in buffer, buffer length
			0, 0,
			# available buffers associated with the id of the
			# transaction who produced the tuples.
			[],
			# transaction in progress
			prime,
		)
		self.connection._push(prime)

class ClientStatementCursor(Portal):
	"Client statement created cursors"
	def __init__(self, query, params, output, output_io, rformats, **kw):
		Portal.__init__(self,
			query.connection,
			ID(self, title = b'#'),
			**kw
		)
		self.closed = True
		self.query = query
		self.output = output
		self._output_io = output_io
		self._rformats = rformats
		self._output_attmap = attmap_from_pqdesc(self.output)

		# Finish any outstanding transactions to identify
		# the current transaction state. If it's not in a
		# transaction block, fetch them all as the Portal
		# won't exist after the next synchronize message.
		if self.connection._xact is not None:
			self.connection._complete()
		if self.connection.state == b'I':
			self.fetchcount = 0xFFFFFFFF
		x = pq.Transaction((
			pq.element.Bind(
				self.portal_id, query.statement_id, params, rformats,
			),
			pq.element.Execute(self.portal_id, self.fetchcount),
			pq.element.SynchronizeMessage
		))

		self._state = (
			# position in buffer, buffer length, buffer, nextdata
			0, 0, [], x
		)

		if self.connection.state not in b'EI':
			self.closed = False
		self.connection._push(x)
		# Check for bind completion or let _step() throw the exception.
		y = None
		while y is not pq.element.BindCompleteMessage:
			self.connection._step()
			for y in subidxiter(x.completed, 1):
				break
	__del__ = Portal.close

class PreparedStatement(pg_api.PreparedStatement):
	def __init__(self, connection, statement_id, title = None):
		# Assume that the statement is open; it's normally a
		# statement prepared on the server in this context.
		self.closed = False
		self.statement_id = statement_id
		self.connection = connection
		self.title = title and title or statement_id

	def __repr__(self):
		return '<%s.%s[%s]%s>' %(
			type(self).__module__,
			type(self).__name__,
			self.connection.connector.iri,
			self.closed and ' closed' or ''
		)

	def close(self):
		if getattr(self, 'closed', True) is False:
			self.connection._closestatements.append(self.statement_id)
			self.closed = True

	def prepare(self):
		self.prime()
		self.finish()

	def prime(self):
		"""
		Push initialization messages to the server, but don't wait for
		the return as there may be things that can be done while waiting
		for the return. Use the finish() to complete.
		"""
		self._init_xact = pq.Transaction((
			pq.element.DescribeStatement(statement_id),
			pq.element.SynchronizeMessage,
		))
		self.connection._push(self._init_xact)

	def finish(self):
		"""
		complete initialization that the prime() method started.
		"""
		# assume that the transaction has been primed.
		if self._init_xact is self.connection._xact:
			self.connection._complete()

		r = list(subidxiter(self._init_xact.completed, 1))
		argtypes = r[-3]
		tupdesc = r[-2]

		if tupdesc is None or tupdesc is pq.element.NoDataMessage:
			self.portal = self.connection.ResultHandle
			self.output = None
			self._output_io = ()
			self._rformats = ()
		else:
			self.portal = self.connection.ClientStatementCursor
			self.output = tupdesc
			self._output_attmap = pg_typio.attmap_from_pqdesc(tupdesc)
			# tuple output
			self._output_io = self.connection.typio.resolve_descriptor(
				tupdesc, 1
			)
			self._rformats = [
				x is None and pq.element.StringFormat or pq.element.BinaryFormat
				for x in self._output_io
			]
		self.input = argtypes
		self._input_io = [
			(self.connection.typio.lookup(x) or (None,None))[0]
			for x in argtypes
	 	]
		del self._init_xact

	def __iter__(self):
		return self(*self.defaults)

	def __call__(self, *args, **kw):
		if self._input_io:
			params = list(pg_typio.row_pack(
				args, self._input_io, self.connection.typio.encode
			))
		else:
			params = ()

		if self.output:
			portal = self.portal(
				self, params, self.output, self._output_io, self._rformats, **kw
			)
		else:
			# non-tuple output(copy, ddl, non-returning dml)
			portal = self.portal(self, params, **kw)
			if portal.type.type == pq.element.CopyFromBegin.type:
				# Run COPY FROM if given an iterator
				if params is () and args:
					portal(*args, **kw)
					portal.close()
			elif portal.type.type == pq.element.Null.type:
				return None
		return portal

	def first(self, *args):
		# Parameters? Build em'.
		c = self.connection

		if self._input_io:
			params = list(
				pg_typio.row_pack(
					args or self.defaults,
					self._input_io,
					c.typio.encode
				)
			)
		else:
			params = ()

		# Run the statement
		x = pq.Transaction((
			pq.element.Bind(
				b'', self.statement_id, params, self._rformats,
			),
			pq.element.Execute(b'', 1),
			pq.element.SynchronizeMessage
		))
		c._push(x)
		c._complete()

		if self._output_io:
			# Look for the first tuple.
			for xt in subidxiter(x.completed, 1):
				if xt.type is pq.element.Tuple.type:
					break
			else:
				return None

			if len(self._output_io) > 1:
				return c.typio.row_unpack(
					xt, self._output_io, self._output_attmap
				)
			else:
				if xt[0] is None:
					return None
				io = self._output_io[0]
				if io:
					return io(xt[0])
				return c.typio.decode(xt[0])
		else:
			# It doesn't return rows, so return a count.
			for cm in subidxiter(x.completed, 1):
				if cm.type == pq.element.Complete.type:
					break
			else:
				return None
			return extract_count(cm)
	__invert__ = first

	def load(self, tupleseq, tps = 40):
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
						pq.element.Bind(b'', self.statement_id, params, (),),
						pq.element.Execute(b'', 1),
					))
					if c == tps:
						break
					c += 1
				else:
					last = pq.element.SynchronizeMessage
				xm.append(last)
				self.connection._push(pq.Transaction(xm))
			self.connection._complete()
		except:
			if self.connection._xact is not None:
				self.connection._complete()
			self.connection.synchronize()
			raise
	__lshift__ = load

class ClientPreparedStatement(PreparedStatement):
	'A prepared statement generated by the connection user'

	def __init__(self, connection, src, *defaults, **kw):
		self.string = src
		self.connection = connection
		self.defaults = defaults
		self.title = kw.get('title')
		self.statement_id = ID(self, title = self.title)
		self.closed = True

	# Always close CPSs as the way the statement_id is generated
	# might cause a conflict if Python were to reuse the previously
	# used id()[it can and has happened]. - jwp 2007
	__del__ = PreparedStatement.close

	def prime(self):
		'Push out initialization messages for query preparation'
		qstr = self.connection._encode(self.string)[0]
		if self.closed is True:
			closestatement = ()
		else:
			closestatement = (
				pq.element.CloseStatement(self.statement_id),
			)
		self._init_xact = pq.Transaction(
			closestatement + (
			pq.element.Parse(self.statement_id, qstr, ()),
			pq.element.DescribeStatement(self.statement_id),
			pq.element.SynchronizeMessage,
		))
		self.closed = False
		self.connection._push(self._init_xact)

class StoredProcedure(pg_api.StoredProcedure):
	'Interface to functions kept in the backend'

	def __repr__(self):
		return '<%s:%s>' %(
			self.name, self.query.string
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
			word_idx.sort(cmp = lambda x, y: cmp(x[1], y[1]))
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
				return idxiter(r, 0)
		else:
			if self.composite is True:
				return r.read(1)[0]
			else:
				return r.read(1)[0][0]

	def __init__(self, connection, ident, description = ()):
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

		self.procid = self.oid = proctup[0]
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
		self.srf = bool(proctup.get("proretset"))
		self.composite = proctup["composite"]
		self.input = self.query.input
		self.output = self.query.output

class ServerSettings(pg_api.Settings):
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
		setas = ~(c.cquery(
			"SELECT set_config($1, $2, false)",
			title = 'set_setting',
		)(i, v))
		self.cache[i] = setas

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
			self['search_path'] = ','.join([
				'"%s"' %(x.replace('"', '""'),) for x in value
			])
		def fdel(self):
			self.path = self.connection.connector.path
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
		d = self.connection._decode
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

class TransactionManager(pg_api.Transaction):
	def __init__(self, connection):
		self._level = 0
		self.connection = connection
		self.isolation = None
		self.mode = None
		self.gid = None

	def failed():
		def fget(self):
			s = self.connection.state
			if s is None or s == 'I':
				return None
			elif s == 'E':
				return True
			else:
				return False
		return locals()
	failed = property(**failed())

	def closed():
		def fget(self):
			return self.connection.state in (None, 'I')
		return locals()
	closed = property(**closed())

	def prepared():
		def fget(self):
			return tuple(
				self.connection.cquery(
					PreparedLookup,
					title = "usable_prepared_xacts"
				).first(self.connection.user)
			)
		return locals()
	prepared = property(**prepared())

	def commit_prepared(self, gid):
		self.connection.execute(
			"COMMIT PREPARED '%s'" %(
				self.connection.escape_string(gid),
			)
		)

	def rollback_prepared(self, gid):
		self.connection.execute(
			"ROLLBACK PREPARED '%s'" %(
				self.connection.escape_string(gid),
			)
		)

	def _execute(self, qstring, adjustment):
		x = pq.Transaction((
			pq.element.Query(self.connection._encode(qstring)[0]),
		))
		self.connection._push(x)

		# The operation is going to happen. Adjust the level accordingly.
		if adjustment < 0 and self._level <= -adjustment:
			self.__init__(self.connection)
			self._level = 0
		else:
			self._level += adjustment
		self.connection._complete()

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
				return "PREPARE TRANSACTION '%s'" %(
					self.connection.escape_string(self.gid),
				)
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

		self.connection._push(
			pq.Transaction((
				pq.element.Query(self.connection._encode(abort)[0]),
				pq.element.Query(self.connection._encode(start)[0]),
			))
		)
		self.connection._complete()

	def checkpoint(self, isolation = None, mode = None):
		commit = self._commit_string(self._level)
		start = self._start_string(
			self._level - 1, isolation = isolation, mode = mode
		)

		self.connection._push(
			pq.Transaction((
				pq.element.Query(self.connection._encode(commit)[0]),
				pq.element.Query(self.connection._encode(start)[0]),
			))
		)
		self.connection._complete()

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
		self.start()
		try:
			xrob = callable(*args, **kw)
		except self.AbortTransaction as e:
			if not self.connection.closed:
				self.abort()
			xrob = getattr(e, 'returning', None)
		except:
			if not self.connection.closed:
				self.abort()
			raise
		else:
			self.commit()
		return xrob

class Connection(pg_api.Connection):
	"""
	Connection(connector) -> Connection

	PG-API <- Driver -> PQ Protocol <- Socket -> PostgreSQL
	"""
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

	type = None
	closed = None
	user = None
	version_info = None
	version = None

	def _N(self, msg):
		'[internal] print a notice message using the notifier attribute'
		self.notifier(msg)

	def _A(self, msg):
		'[internal] Send notification to any listeners; NOTIFY messages'
		subs = getattr(self, '_subscriptions', {})
		for x in subs.get(msg.relation, ()):
			x(self, msg)
		if None in subs:
			subs[None](self, msg)

	def _S(self, msg):
		'[internal] Receive ShowOption message'
		self.settings._notify(msg)

	def _update_encoding(connection, key, value):
		'[internal] subscription method to client_encoding on settings'
		connection.typio.set_encoding(value)
	_update_encoding = staticmethod(_update_encoding)

	def _update_timezone(connection, key, value):
		'[internal] subscription method to TimeZone on settings'
		connection.typio.set_timezone(value)
	_update_timezone = staticmethod(_update_timezone)

	def escape_literal(self, strobj):
		"""
		Replace "'" with "''". Iff standard_conforming_strings = no, then
		also replace '\' with '\\'.
		"""
		# Presume that if standard_conforming_strings is available,
		# that it's in the future and been depecrated. Chances are that
		# older versions of PostgreSQL that may not have this are not
		# supported anyways.
		#
		# And yes, the setting must resolved everytime in order to
		# produce the correct results for the context that it is executed in.
		if self.settings.get('standard_conforming_strings') == 'no':
			strobj = strobj.replace('\\', '\\\\')
		return strobj.replace("'", "''")

	def quote_literal(self, strobj):
		"""
		Wrap the results of `escape_literal` with single quotes.
		"""
		return "'%s'" %(self.escape_literal(strobj),)

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

	def __nonzero__(self):
		'connection can issue a query without an existing impediment'
		return not self.closed and self.state not in ('LOST', 'E', None)

	def __repr__(self):
		return '<%s.%s[%s] %s>' %(
			type(self).__module__,
			type(self).__name__,
			self.connector.iri,
			self.closed and 'closed' or '%s.%d' %(
				self.state, self.xact._level
			)
		)

	def __enter__(self):
		'Connect on entrance.'
		self.connect()

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
		if self._xact is not None:
			self._complete()
		x = pq.Transaction((pq.element.SynchronizeMessage,))
		self._xact = x
		self._complete()

	def interrupt(self):
		'Send a CancelQuery message to the backend'
		cq = pq.element.CancelQuery(self._killinfo.pid, self._killinfo.key)
		s = self.connector.socket_connect()
		try:
			s.sendall(str(cq))
		finally:
			s.close()

	def execute(self, query):
		'Execute an arbitrary block of SQL'
		q = pq.Transaction((
			pq.element.Query(self.typio.encode(query)),
		))
		self._push(q)
		self._complete()

	def query(*args, **kw):
		'Create a query object using the given query string'
		q = ClientPreparedStatement(*args, **kw)
		q.prime()
		q.finish()
		return q

	def statement(*args, **kw):
		'Create a query object using the statement identifier'
		q = PreparedStatement(*args, **kw)
		q.prime()
		q.finish()
		return q

	def proc(*args, **kw):
		'Create a StoredProcedure object using the given procedure identity'
		return StoredProcedure(*args, **kw)

	def cursor(*args, **kw):
		'Create a Portal object that references an already existing cursor'
		return Cursor(*args, **kw)

	def ClientStatementCursor(self, *args, **kw):
		'Create a Portal using the given client prepared statement'
		return ClientStatementCursor(*args, **kw)

	def ResultHandle(*args, **kw):
		'Execute a query with the given arguments that does not return rows'
		return ResultHandle(*args, **kw)

	def _xp_query(self, string, args = (), rformats = ()):
		aformats = ()
		if args:
			args = [self.typio.encode(x) for x in args]
			aformats = (pq.element.StringFormat,) * len(args)

		return pq.Transaction(
			pq.element.Parse('', self.typio.encode(string), argtypes),
			pq.element.Bind('', '', aformats, args, rformats),
			pq.element.Execute('', 0xFFFFFFFF),
			pq.element.SynchronizeMessage,
		)

	def close(self):
		'Close the connection and reinitialize the state and information'
		# Write out the disconnect message if the socket is around.
		# If the connection is known to be lost, don't bother. It will
		# generate an extra exception.
		try:
			if self.socket is not None and self.state != 'LOST':
				self._write_messages((pq.element.DisconnectMessage,))
		finally:
			if self.socket is not None:
				self.socket.close()
			self._clear()
			self._reset()

	def closed():
		def fget(self):
			return self._xact is ClosedConnectionTransaction
		def fset(self, value):
			if value is True:
				self.connect()
			elif value is False:
				self.close()
			else:
				raise ValueError("closed can only be set to True or False")
		doc = 'Whether the connection is closed or not'
		return locals()
	closed = property(**closed())

	def reconnect(self, *args, **kw):
		'close() and connect()'
		self.close()
		self.connect(*args, **kw)

	def reset(self):
		'restore original settings, reset the transaction, drop temporary objects'
		self.xact.reset()
		self.execute("RESET ALL")

	def _negotiate(self):
		"""
		Initialize the connection between the client and the server.
		This requires that a socket interface has been set on the
		'socket' attribute.
		"""
		self._write_messages((
			pq.element.Startup(self.connector._parameters),
		))
		self._read_messages()

		first = self._read[0]
		messages = list(self._read[1:])
		self._read = ()
		user = str(self.connector._parameters['user'])
		password = str(self.connector.password)

		# Read the authentication message and respond accordingly.
		if first[0] == pq.element.Authentication.type:
			auth = pq.element.Authentication.parse(first[1])
			req = auth.request
			if req != pq.element.AuthRequest_OK:
				if req == pq.element.AuthRequest_Cleartext:
					pw = self.connector.password
				elif req == pq.element.AuthRequest_Crypt:
					pw = crypt.crypt(password, auth.salt)
				elif req == pq.element.AuthRequest_MD5:
					pw = md5(password + user).hexdigest()
					pw = 'md5' + md5(pw + auth.salt).hexdigest()
				else:
					# Not going to work. Sorry :(
					raise NotImplementedError(
						"unsupported authentication request %r(%d)" %(
						pq.element.AuthNameMap.get(req, '<unknown>'), req,
					))
				self._write_messages((pq.element.Password(pw),))
				sent_password = True
				self._read_messages()
				authres = self._read[0]
				messages.extend(self._read[1:])
				self._read = ()

				# Authentication has taken place, require a zero R message.
				if authres[0] == pq.element.Error.type:
					return pq.element.Error.parse(authres[1])
				if authres[0] != pq.element.Authentication.type:
					raise pq.ProtocolError(
						"expected an authentication message of type %r, " \
						"but received %r instead" %(
							pq.element.Authentication.type,
							authres[0],
						),
						authres
					)
				if authres[1][0:4] != b'\x00\x00\x00\x00':
					raise pq.ProtocolError(
						"expected an OK from the authentication " \
						"message, but received %r instead" %(msg[1][0:4],),
						authres
					)
		elif first[0] == pq.element.Error.type:
			return pq.element.Error.parse(first[1])

		if not messages:
			self._read_messages()
			messages = self._read
			self._read = ()

		fget = pq.Transaction.asynchook.get
		while True is True:
			for x in messages:
				if x[0] is pq.element.Ready.type:
					self.state = x[1]
				elif x[0] == pq.element.KillInformation.type:
					self._killinfo = pq.element.KillInformation.parse(x[1])
					self.backend_id = self._killinfo.pid
				elif x[0] == pq.element.Error.type:
					return pq.element.Error.parse(x[1])
				else:
					f = fget(x[0])
					if f is not None:
						m = f(x[1])
						getattr(self, '_' + m.type)(m)
					else:
						raise pq.ProtocolError(
							"expected messages of types %r, " \
							"but received %r instead" %(
								(pq.element.Ready.type,),
								x[0],
							),
							x, list(pq.Transaction.asynchook.keys()) + [
								pq.element.Ready.type
							],
						)
			if self.state is not None:
				break
			self._read_messages()
			messages = self._read
			self._read = ()

	def connect(self):
		'Establish the connection to the server'
		if self.closed is not True:
			# already connected
			return
		c = self.connector
		error_message = None
		dossl = c.sslmode in ('require', 'prefer')
		fatal = None

		# Loop over the connection code until
		# a connection is achieved using the selected SSL mode
		while True:
			self.socket = c.socket_connect()
			try:
				if dossl is True:
					self._write_messages((pq.element.NegotiateSSLMessage,))
					status = self.socket.recv(1)
					if status == 'S':
						self.socket = secure_socket(
							self,
							keyfile = c.sslkeyfile,
							certfile = c.sslcrtfile,
							rootcertfile = c.sslrootcrtfile,
						)
						# XXX: Check Revocation List.
					elif c.sslmode == 'require':
						raise pg_exc.UnavailableSSL(c.iri)

				# Authenticate and read initial data.
				error_message = self._negotiate()
				if error_message is not None:
					fatal = error_message['severity'].upper() in ('FATAL','PANIC')

				##
				# Under some conditions, it will try again:
				#  If it did not try ssl, and ssl is allowed; try with ssl.
				#  If it did try ssl, and ssl is preferred; try without ssl.
				if fatal is True and \
				((c.sslmode == 'allow' and dossl is False) or \
				 (c.sslmode == 'prefer' and dossl is True)):
					# It failed without SSL, so try with.
					self.socket.close()
					dossl = not dossl

					if error_message['code'] != '28000':
						# If it's not 28000, it's not due to an hba failure,
						# so break out of the loop.
						break
				else:
					break
			except:
				self.socket.close()
				raise

		if error_message is not None and fatal is True:
			connect_error = self._postgres_error(error_message)
			connect_error.fatal = True
			raise connect_error
		##
		# Connection established, complete initialization.
		#
		# Ready for [protocol] transactions.
		try:
			self._xact = None
			self.version_info = pg_version.split(
				self.settings["server_version"]
			)

			# Resolve time IO routines
			# (double vs. int64 and day interval vs. noday interval)
			intdt = self.settings.get('integer_datetimes', '').lower() in ('on', 'true', 't')
			self.typio.config(
				version = self.version_info,
				integer_datetimes = intdt,
				timestamp = self.settings.get('timezone')
			)

			self.type = self.query("SELECT version()").first().split()[0]
		except:
			self.close()
			raise

	def _read_into(self):
		'[internal] protocol message reader. internal use only'
		while not self._pq_in_buffer.has_message():
			if self._read_data is not None:
				self._pq_in_buffer.write(self._read_data)
				self._read_data = None
				continue

			try:
				self._read_data = self.socket.recv(self.readbytes)
			except socket.error as e:
				enum, emsg = e
				if enum == errno.ECONNRESET:
					lost_connection_error = pg_exc.ConnectionFailureError(
						emsg,
						details = {
							'severity' : 'FATAL',
							'detail' : 'The server explicitly closed the connection.',
							'errno' : enum,
						}
					)
					lost_connection_error.connection = self
					lost_connection_error.fatal = True
					self.state = 'LOST'
					raise lost_connection_error
				# It's probably a non-fatal error.
				# XXX: Need a better way to identify fatalities. :(
				raise
			# nothing read from a blocking socket? it's over.
			if not self._read_data:
				lost_connection_error = pg_exc.ConnectionFailureError(
					"unexpected EOF from server",
					details = {
						'severity' : 'FATAL',
						'detail' : 'Zero-length string read from the connection.'
					}
				)
				lost_connection_error.connection = self
				lost_connection_error.fatal = True
				self.state = 'LOST'
				raise lost_connection_error
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
				self._message_data = self._message_data[
					self.socket.send(self._message_data):
				]
		except (IOError, socket.error,) as e:
			if e[0] == errno.EPIPE:
				lost_connection_error = pg_exc.ConnectionFailureError(
					"broken connection detected on send",
					details = {
						'severity' : 'FATAL',
						'detail' : '%s (errno.EPIPE=%r).' %(e[1], e[0]),
					}
				)
				lost_connection_error.connection = self
				lost_connection_error.fatal = True
				self.state = 'LOST'
				raise lost_connection_error
			raise

	def _standard_write_messages(self, messages):
		'[internal] protocol message writer'
		if self._writing is not self._written:
			self._message_data += ''.join([str(x) for x in self._writing])
			self._written = self._writing

		if messages is not self._writing:
			self._writing = messages
			self._message_data += ''.join([str(x) for x in self._writing])
			self._written = self._writing
		self._send_message_data()
	_write_messages = _standard_write_messages

	def _traced_write_messages(self, messages):
		'[internal] _message_writer used when tracing'
		for msg in messages:
			t = getattr(msg, 'type', None)
			if t is not None:
				body = msg.serialize()
				self._tracer('↑ %r(%d): %r%s' %(
					t, len(body), body, os.linesep
				))
			else:
				# It's not a message instance, so assume raw data.
				self._tracer('↑    (%d): %r%s' %(
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
		close portals and statements slated for closure.
		NOTE: Assumes no running transaction.
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
		self._xact = x
		del self._closeportals[:portals], self._closestatements[:statements]
		self._complete()

	def _push(self, xact):
		'[internal] setup the given transaction to be processed'
		# Push any queued closures onto the transaction or a new transaction.
		#
		# This is interrupt safe as the auxiliaries are deleted
		# after the auxiliaries are appended, and considering
		# that completion of a transaction is final, there is no
		# need to be concerned with processing an already completed
		# transaction(ie, processing a xact twice is not an issue).
		if self._xact is not None:
			self._complete()
		if self._closestatements or self._closeportals:
			self._backend_gc()
		if xact.state[0] == pq.Complete:
			# Push it, but don't step; and note it as the "last_xact"
			# as the asyncs have already been processed if a push
			# is occurring on a completed transaction.
			self._xact = xact
			self._last_xact = xact
		else:
			self._xact = xact
			# Prime it.
			self._step()

	def _postgres_error(self, em):
		'[internal] lookup a PostgreSQL error and instantiate it'
		err = pg_exc.ErrorLookup(em["code"])
		err = err(
			em['message'],
			code = em.get('code'),
			details = em
		)
		err.error_message = err
		err.connection = self
		return err

	def _step(self):
		'[internal] make a single transition on the transaction'
		try:
			dir, op = self._xact.state
			if dir is pq.Sending:
				self._write_messages(self._xact.messages)
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
		self.state = getattr(self._xact, 'last_ready', self.state)
		if self._xact.state[0] is pq.Complete:
			self._pop()

	def _complete(self):
		'[internal] complete the current transaction'
		# Continue to transition until all transactions have been
		# completed, or an exception occurs that is not EINTR.
		while self._xact.state[0] is not pq.Complete:
			try:
				while self._xact.state[0] is pq.Receiving:
					self._read_messages()
					self._read = self._read[self._xact.state[1](self._read):]
				# _push() always takes one step, so it is likely that
				# the transaction is done sending out data by the time
				# _complete() is called.
				while self._xact.state[0] is pq.Sending:
					self._write_messages(self._xact.messages)
					# Multiple calls to get() without signaling
					# completion *should* yield the same set over
					# and over again.
					self._xact.state[1]()
			except (socket.error, IOError) as e:
				if e[0] != errno.EINTR:
					raise
		self.state = getattr(self._xact, 'last_ready', self.state)
		self._pop()

	def _pop(self):
		'[internal] remove the transaction and raise the exception if any'
		# process any asynchronous messages in the xact
		if self._xact is not self._last_xact:
			# If it hasn't been processed, run through the messages.
			if self._asynciter is None:
				self._asynciter = iter(list(subidxiter(self._xact.asyncs, 1)))

			# If this is not None, an interrupt or exception
			# occurred while processing the message.
			if self._amsg is not None:
				getattr(self, '_' + self._amsg.type)(self._amsg)
				self._amsg = None
			for self._amsg in self._asynciter:
				getattr(self, '_' + self._amsg.type)(self._amsg)
				self._amsg = None
			self._asynciter = None
			self._last_xact = self._xact

		em = getattr(self._xact, 'error_message', None)
		if em is not None:
			xact_error = self._postgres_error(em)
			if self._xact.fatal is not True:
				# only remove the transaction if it's *not* fatal
				xact_error.fatal = False
				self._xact = None
			else:
				xact_error.fatal = True
			raise xact_error
		# state is Complete, so remove it as the working transaction
		self._xact = None

	def _clear(self):
		"""
		[internal] Clear container objects of data.
		"""
		del self._closestatements[:]
		del self._closeportals[:]

		self._query_cache.clear()
		self.typio = None
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
		self._message_data = ''
		self._writing = None
		self._written = None
		self._asynciter = None
		self._amsg = None

		self._killinfo = None
		self.backend_id = None
		self.state = None

		# So the initial ShowOptions can be decoded without conditions.
		self.typio = pg_typio.TypeIO()
		self.typio.config(
			encoding = 'ascii',
			timezone = 'utc',
		)

		self._last_xact = None
		self._xact = ClosedConnectionTransaction

	def user():
		def fget(self):
			return ~self.cquery('SELECT current_user')
		def fset(self, val):
			return self.execute('SET ROLE "%s"' %(val.replace('"', '""'),))
		def fdel(self):
			self.execute("RESET ROLE")
		return locals()
	user = property(**user())

	def __init__(self, connector, *args, **kw):
		"""
		Create a connection based on the given connector.
		"""
		self.connector = connector
		self.notifier = connector.notifier
		self._closestatements = []
		self._closeportals = []
		self._pq_in_buffer = pq_message_stream()
		self.readbytes = 2048
		self._query_cache = {}

		self.settings = ServerSettings(self)
		# Update the _encode and _decode attributes on the connection
		# when a client_encoding ShowOption message comes in.
		self.settings.subscribe('client_encoding', self._update_encoding)
		self.settings.subscribe('TimeZone', self._update_timezone)
		self.xact = TransactionManager(self)
		self._reset()
# class Connection

def format_message(msg):
	loc = [
		msg.get('file'),
		msg.get('line'),
		msg.get('function')
	]
	# If there are any location details, make the locstr.
	if loc.count(None) < 3:
		locstr = 'LOCATION: File %r, line %s, in %s' %(
			loc[0] or '?',
			loc[1] or '?',
			loc[2] or '?',
		)
	else:
		locstr = ''
	detstr = os.linesep.join([
		'%s: %s' %(k.upper(), v) for k, v in msg.items()
		if k not in ("severity", "code", "message", "file", "line", "function")
	]) 

	return '%s(%s): %s%s%s%s%s%s' %(
		msg["severity"].upper(), msg["code"], msg["message"], os.linesep,
		locstr, locstr and os.linesep or '',
		detstr, detstr and os.linesep or '',
	)

try:
	# termstyle not available? no colors for you.
	def stderr_notifier(c, msg, termstyle = termstyle):
		sys.stderr.write(
			message_style.get(msg['severity'], '') + \
			format_message(msg) + termstyle.NORMAL
		)
except NameError:
	def stderr_notifier(c, msg):
		sys.stderr.write(format_message(msg))

class Connector(object):
	"""Connector(**config) -> Connector

	All arguments to Connector are keywords. At the very least, user,
	and socket, may be provided. If socket, unix, or process is not
	provided, host and port must be.
	"""

	connection_class = Connection
	sslmode = 'prefer'
	words = (
		'user',
		# Piping
		'host', 'port', 'ipv',
		'process',
		'pipe',
		'socket',
		'unix',
		'sslmode',
		'sslcrtfile',
		'sslkeyfile',
		'sslrootcrtfile',
		'sslrootcrlfile',
		'tracer',
		# PostgreSQL
		'database', 'options', 'settings', 'password',
		# Config
		'path', 'role'
	)
	notifier = stderr_notifier
	iri = property(
		fget = lambda x: pg_iri.serialize(x.__dict__)
	)

	def __repr__(self):
		return type(self).__module__ + '.' + type(self).__name__ + '(%s)' %(
			', '.join([
				'%s = %r' %(k, getattr(self, k, None)) for k in self.words
				if getattr(self, k, None) is not None
			]),
		)

	def __getitem__(self, k):
		return getattr(self, k)

	def get(self, k, otherwise = None):
		return getattr(self, k, otherwise)

	def socket_create(self):
		return socket.socket(*self._socket_data[0])	

	def socket_connect(self):
		"create and connect a socket using the configured _socket_data"
		s = socket.socket(*self._socket_data[0])
		try:
			s.connect(self._socket_data[1])
		except:
			s.close()
			raise
		return s

	def __call__(self, *args, **kw):
		c = self.connection_class(self, *args, **kw)
		c.connect()
		return c

	def create(self, *args, **kw):
		# connector is the first arguments(self)
		return self.connection_class(self, *args, **kw)

	def __init__(self, **kw):
		for k in kw:
			if k not in self.words:
				raise TypeError(
					"%s() got an unexpected keyword argument '%s'" %(
						type(self).__name__, k,
					)
				)

		if not kw.has_key('user'):
			raise TypeError(
				"missing required key 'user' for Connection class"
			)

		count = (
			(int('pipe' in kw)) + \
			(int('socket' in kw)) + \
			(int('unix' in kw)) + \
			(int('host' in kw)) + \
			(int('process' in kw))
		)
		if count != 1:
			raise TypeError(
				"only one of the keyword arguments " \
				"'host', 'socket', 'unix', 'process', 'pipe' may be supplied"
			)

		if kw.has_key('host') and not kw.has_key('port'):
			raise TypeError(
				"missing keyword 'port' required by 'host' connections"
			)

		self.pipe = kw.get('pipe')
		self.socket = kw.get('socket')
		self.unix = kw.get('unix')
		self.host = kw.get('host')
		self.port = int(kw.get('port'))
		self.process = kw.get('process')
		self.sslmode = kw.get('sslmode') or self.sslmode
		self.database = kw.get('database')
		self.sslkeyfile = kw.get('sslkeyfile')
		self.sslcrtfile = kw.get('sslcrtfile')
		self.sslrootcrtfile = kw.get('sslrootcrtfile')
		self.sslrootcrlfile = kw.get('sslrootcrlfile')
		self.ipv = kw.get('ipv')

		# Initialize the socket arguments.
		if self.socket is not None:
			# Nothing to do with user specified socket.
			self._socket_data = self.socket
		elif self.unix is not None:
			self._socket_data = (
				(socket.AF_UNIX, socket.SOCK_STREAM), self.unix
			)
		elif self.process is not None:
			raise NotImplementedError("'process' method is currently unsupported")
		elif self.ipv == 4:
			self._socket_data = (
				(socket.AF_INET, socket.SOCK_STREAM),
				(self.host, port)
			)
		elif self.ipv == 6:
			self._socket_data = (
				(socket.AF_INET6, socket.SOCK_STREAM),
				(self.host, port)
			)
		else:
			ai = socket.getaddrinfo(
				self.host, self.port, socket.AF_UNSPEC, socket.SOCK_STREAM
			)
			af, socktype, proto, cname, sa, = ai[0]
			self._socket_data = ((af, socktype, proto), (self.host, self.port))
			if af == socket.AF_INET:
				self.ipv = 4
			elif af == getattr(socket, 'AF_INET6', 0):
				self.ipv = 6

		self.user = kw['user']
		self.password = kw.get('password') or ''
		self.options = kw.get('options')
		self.settings = kw.get('settings') or {}

		self.notifier = kw.get('notifier', self.notifier)

		# Startup message parameters.
		tnkw = {}
		if self.settings:
			tnkw.update(self.settings)

		tnkw['user'] = self.user
		if self.database is not None:
			tnkw['database'] = self.database
		if self.options is not None:
			tnkw['options'] = self.options

		self._parameters = tnkw
# class Connector
