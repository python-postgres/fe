##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
PG-API interface for PostgreSQL using PQ version 3.0.
"""
import os
import weakref
import socket
from traceback import format_exception
from operator import itemgetter
get0 = itemgetter(0)
get1 = itemgetter(1)
from itertools import repeat, chain
from abc import abstractmethod

from .. import lib as pg_lib

from .. import versionstring as pg_version
from .. import iri as pg_iri
from .. import exceptions as pg_exc
from .. import string as pg_str
from .. import api as pg_api
from ..encodings import aliases as pg_enc_aliases

from ..python.itertools import interlace, chunk
from ..python.socket import SocketFactory

from ..protocol import xact3 as xact
from ..protocol import element3 as element
from ..protocol import client3 as client
from ..protocol import typio as pg_typio

from .. import types as pg_types

IDNS = 'py:%s'
def ID(s, title = None):
	'generate an id for a client statement or cursor'
	return IDNS %(hex(id(s)),)

def declare_statement_string(
	cursor_id,
	statement_string,
	insensitive = True,
	scroll = True,
	hold = True
):
	s = 'DECLARE ' + cursor_id
	if insensitive is True:
		s += ' INSENSITIVE'
	if scroll is True:
		s += ' SCROLL'
	s += ' CURSOR'
	if hold is True:
		s += ' WITH HOLD'
	else:
		s += ' WITHOUT HOLD'
	return s + ' FOR ' + statement_string

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
		return self.database.sys.lookup_type(typid)

	def lookup_composite_type_info(self, typid):
		return self.database.sys.lookup_composite(typid)

class Output(object):
	_output = None
	_output_io = None
	_output_formats = None
	_output_attmap = None

	closed = False
	cursor_id = None
	statement = None
	parameters = None

	_complete_message = None

	@abstractmethod
	def _init(self):
		"""
		Bind a cursor based on the configured parameters.
		"""

	def __init__(self, cursor_id):
		self.cursor_id = cursor_id
		if self.statement is not None:
			self._output = self.statement._output
			self._output_io = self.statement._output_io
			self._output_formats = self.statement._output_formats or ()
			self._output_attmap = self.statement._output_attmap

		if self.cursor_id == ID(self):
			addgarbage = self.database.pq.garbage_cursors.append
			typio = self.database.typio
			self._del = weakref.ref(
				self, lambda x: addgarbage(typio.encode(cursor_id))
			)
		self._quoted_cursor_id = '"' + self.cursor_id.replace('"', '""') + '"'
		self._pq_cursor_id = self.database.typio.encode(self.cursor_id)
		self._init()

	def __iter__(self):
		return self

	def close(self):
		if self.closed is False:
			self.database.pq.garbage_cursors.append(
				self.database.typio.encode(self.cursor_id)
			)
		self.closed = True
		if hasattr(self, '_del'):
			del self._del

	def _ins(self, *args):
		return xact.Instruction(*args, asynchook = self.database._receive_async)

	def _pq_xp_describe(self):
		return (element.DescribePortal(self._pq_cursor_id),)

	def _pq_xp_bind(self):
		return (
			element.Bind(
				self._pq_cursor_id,
				self.statement._pq_statement_id,
				self.statement._input_formats,
				self.statement._pq_parameters(self.parameters),
				self._output_formats,
			),
		)

	def _pq_xp_fetchall(self):
		return (
			element.Bind(
				b'',
				self.statement._pq_statement_id,
				self.statement._input_formats,
				self.statement._pq_parameters(self.parameters),
				self._output_formats,
			),
			element.Execute(b'', 0xFFFFFFFF),
		)

	def _pq_xp_declare(self):
		return (
			element.Parse(b'', self.database.typio.encode(
					declare_statement_string(
						str(self._quoted_cursor_id),
						str(self.statement.string)
					)
				), ()
			),
			element.Bind(
				b'', b'', self.statement._input_formats,
				self.statement._pq_parameters(self.parameters), ()
			),
			element.Execute(b'', 1),
		)

	def _pq_xp_execute(self, quantity):
		return (
			element.Execute(self._pq_cursor_id, quantity),
		)

	def _pq_xp_fetch(self, direction, quantity):
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

	def _process_copy_chunk(self, x):
		if x:
			if x[0].__class__ is not bytes or x[-1].__class__ is not bytes:
				return [
					y for y in x if y.__class__ is bytes
				]
		return x

	def _process_tuple_chunk_Row(self, x):
		"""
		Process the Tuple messages in `x`.
		"""
		return [
			pg_types.Row.from_sequence(self._output_attmap, y)
			for y in pg_typio.process_chunk(
				self._output_io, x, self._raise_column_tuple_error
			)
		]

	def _process_tuple_chunk(self, x):
		"""
		Process the Tuple messages in `x`.
		"""
		return pg_typio.process_chunk(
			self._output_io, x, self._raise_column_tuple_error
		)

	def _raise_column_tuple_error(self, procs, tup, itemnum):
		'for column processing'
		# The element traceback will include the full list of parameters.
		data = repr(tup[itemnum])
		if len(data) > 80:
			# Be sure not to fill screen with noise.
			data = data[:75] + ' ...'

		em = element.ClientError(
			code = "--CIO",
			message = "failed to unpack column %r, %s::%s, from wire data" %(
				itemnum,
				self.column_names[itemnum],
				self.database.typio.sql_type_from_oid(
					self.statement.pg_column_types[itemnum]
				) or '<unknown>',
			),
			detail = data,
			hint = "Try casting the column to 'text'.",
			position = str(itemnum),
		)
		self.database._raise_a_pq_error(em, controller = self)
		# "can't happen"
		raise RuntimeError("failed to raise client error")

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

	def command(self):
		"The completion message's command identifier"
		if self._complete_message is not None:
			return self._complete_message.extract_command().decode('ascii')

	def count(self):
		"The completion message's count number"
		if self._complete_message is not None:
			return self._complete_message.extract_count()

class Chunks(Output, pg_api.Chunks):
	pass

class FetchAll(Chunks):
	_e_factors = ('statement', 'parameters',)
	def _e_metas(self):
		yield ('type', type(self).__name__)

	def __init__(self, statement, parameters):
		self.statement = statement
		self.parameters = parameters
		self.database = statement.database
		Output.__init__(self, '')

	def _init(self,
		null = element.Null.type,
		complete = element.Complete.type,
	):
		expect = self._expect
		self._xact = self._ins(
			self._pq_xp_fetchall() + (element.SynchronizeMessage,)
		)
		self.database._pq_push(self._xact, self)
		while self._xact.state != xact.Complete:
			self.database._pq_step()
			for x in self._xact.messages_received():
				if x.type == null:
					self.database._pq_complete()
					self._xact = None
					return
				elif x.type == complete:
					self._complete_message = x
					self.database._pq_complete()
					# If this was a select/copy cursor,
					# the data messages would have caused an earlier
					# return.
					self._xact = None
					return
				elif x.type == expect:
					# no need to step once this is seen
					return
				elif x.type in (
					element.BindComplete.type, element.ParseComplete.type
				):
					pass
				else:
					self.database._pq_complete()
					if self._xact.fatal is None:
						self._xact.fatal = False
						self._xact.error_message = element.ClientError(
							code = "--000",
							message = "unexpected message type " + repr(x.type)
						)
						self.database._raise_pq_error(self._xact, controller = self)
					return

	def __next__(self):
		x = self._xact
		if x is None:
			raise StopIteration

		while x.state is not xact.Complete and not x.completed:
			self.database._pq_step()
		if x.fatal is not None:
			self.database._raise_pq_error(x, controller = self)

		if not x.completed:
			# Transaction has been cleaned out of completed? iterator is done.
			self._xact = None
			raise StopIteration

		chunk = x.completed[0][1]
		r = self._process_chunk(chunk)
		del x.completed[0]
		return r

class SingleXactCopy(FetchAll):
	_expect = element.CopyToBegin.type
	_process_chunk = FetchAll._process_copy_chunk

class SingleXactFetch(FetchAll):
	_expect = element.Tuple.type
	_process_chunk_ = FetchAll._process_tuple_chunk_Row
	def _process_chunk(self, x,
		tuple_type = element.Tuple.type
	):
		return self._process_chunk_((
			y for y in x if y.type == tuple_type
		))

class MultiXactStream(Chunks):
	chunksize = 512
	# only tuple streams
	_process_chunk = Output._process_tuple_chunk_Row

	def _e_metas(self):
		yield ('chunksize', self.chunksize)
		yield ('type', type(self).__name__)

	def __init__(self, statement, parameters, cursor_id):
		self.statement = statement
		self.parameters = parameters
		self.database = statement.database
		Output.__init__(self, cursor_id or ID(self))

	@abstractmethod
	def _bind(self):
		"""
		Generate the commands needed to bind the cursor.
		"""

	@abstractmethod
	def _fetch(self):
		"""
		Generate the commands needed to bind the cursor.
		"""

	def _init(self):
		self._command = self._fetch()
		self._xact = self._ins(self._bind() + self._command)
		self.database._pq_push(self._xact, self)

	def __next__(self):
		x = self._xact
		if x is None:
			raise StopIteration

		if self.database.pq.xact is x:
			self.database._pq_complete()

		chunk = [
			y for y in x.messages_received() if y.type == element.Tuple.type
		]
		if len(chunk) == self.chunksize:
			# there may be more, dispatch the request for the next chunk
			self._xact = self._ins(self._command)
			self.database._pq_push(self._xact, self)
		else:
			# it's done.
			self._xact = None
			if not chunk:
				raise StopIteration
		chunk = self._process_chunk(chunk)
		return chunk

class MultiXactInsideBlock(MultiXactStream):
	_bind = MultiXactStream._pq_xp_bind
	def _fetch(self):
		return self._pq_xp_execute(self.chunksize) + \
			(element.SynchronizeMessage,)

class MultiXactOutsideBlock(MultiXactStream):
	_bind = MultiXactStream._pq_xp_declare
	def _fetch(self):
		return self._pq_xp_fetch(True, self.chunksize) + \
			(element.SynchronizeMessage,)
##
# Base Cursor class and cursor creation entry points.
class Cursor(Output, pg_api.Cursor):
	_process_tuple = Output._process_tuple_chunk_Row
	def _e_metas(self):
		yield ('direction', 'FORWARD' if self.direction else 'BACKWORD')
		yield ('type', 'Cursor')

	def clone(self):
		return type(self)(self.statement, self.parameters, self.database, None)

	def __init__(self, statement, parameters, database, cursor_id):
		self.database = database or statement.database
		self.statement = statement
		self.parameters = parameters
		self.__dict__['direction'] = True
		if self.statement is None:
			self._e_factors = ('database', 'cursor_id')
		Output.__init__(self, cursor_id or ID(self))

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

	def _init(self,
		tupledesc = element.TupleDescriptor.type,
	):
		"""
		Based on the cursor parameters and the current transaction state,
		select a cursor strategy for managing the response from the server.
		"""
		if self.statement is not None:
			x = self._ins(self._pq_xp_declare() + (element.SynchronizeMessage,))
			self.database._pq_push(x, self)
			self.database._pq_complete()
		else:
			x = self._ins(self._pq_xp_describe() + (element.SynchronizeMessage,))
			self.database._pq_push(x, self)
			self.database._pq_complete()
			for m in x.messages_received():
				if m.type == tupledesc:
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

	def __next__(self):
		return self._fetch(self.direction, 1)

	def read(self, quantity = None, direction = None):
		if quantity == 0:
			return []
		dir = self._which_way(direction)
		return self._fetch(dir, quantity)

	def _fetch(self, direction, quantity):
		x = self._ins(
			self._pq_xp_fetch(direction, quantity) + \
			(element.SynchronizeMessage,)
		)
		self.database._pq_push(x, self)
		self.database._pq_complete()
		return self._process_tuple((
			y for y in x.messages_received() if y.type == element.Tuple.type
		))

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

		x = self._ins(cmd + (element.SynchronizeMessage,),)
		self.database._pq_push(x, self)
		self.database._pq_complete()

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
		yield (None, '[' + self.state + ']')
		if hasattr(self._xact, 'error_message'):
			# be very careful not to trigger an exception.
			# even in the cases of effective protocol errors, 
			# it is important not to bomb out.
			pos = self._xact.error_message.get('position')
			if pos is not None and pos.isdigit():
				try:
					pos = int(pos)
					# get the statement source
					q = str(self.string)
					# normalize position..
					pos = len('\n'.join(q[:pos].splitlines()))
					# normalize newlines
					q = '\n'.join(q.splitlines())
					line_no = q.count('\n', 0, pos) + 1
					# replace tabs with spaces because there is no way to identify
					# the tab size of the final display. (ie, marker will be wrong)
					q = q.replace('\t', ' ')
					# grab the relevant part of the query string.
					# the full source will be printed elsewhere.
					# beginning of string or the newline before the position
					bov = q.rfind('\n', 0, pos) + 1
					# end of string or the newline after the position
					eov = q.find('\n', pos)
					if eov == -1:
						eov = len(q)
					view = q[bov:eov]
					# position relative to the beginning of the view
					pos = pos-bov
					# analyze lines prior to position
					dlines = view.splitlines()
					marker = ((pos-1) * ' ') + '^' + (
						' [line %d, character %d] ' %(line_no, pos)
					)
					# insert marker
					dlines.append(marker)
					yield ('LINE', os.linesep.join(dlines))
				except:
					import traceback
					yield ('LINE', traceback.format_exc(chain=False))
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

	def clone(self):
		ps = type(self)(self.database, None, self.string)
		ps._init()
		ps._fini()
		return ps

	def __init__(self, database, statement_id, string):
		self.database = database
		self.statement_id = statement_id or ID(self)
		self.string = string
		self._xact = None
		self._pq_statement_id = None
		self.closed = None

		if not statement_id:
			addgarbage = database.pq.garbage_statements.append
			typio = database.typio
			sid = self.statement_id
			self._del = weakref.ref(
				self, lambda x: addgarbage(typio.encode(sid))
			)

	def __repr__(self):
		return '<{mod}.{name}[{ci}] {state}>'.format(
			mod = type(self).__module__,
			name = type(self).__name__,
			ci = self.database.connector._pq_iri,
			state = self.state,
		)

	def _pq_parameters(self, parameters):
		return pg_typio.process_tuple(
			self._input_io, parameters,
			self._raise_parameter_tuple_error
		)

	def _raise_parameter_tuple_error(self, procs, tup, itemnum):
		typ = self.database.typio.sql_type_from_oid(
			self.pg_parameter_types[itemnum]
		) or '<unknown>'

		data = repr(tup[itemnum])
		if len(data) > 80:
			# Be sure not to fill screen with noise.
			data = data[:75] + ' ...'
		em = element.ClientError(
			message = "failed to pack parameter %s::%s for transfer" %(
				('$' + str(itemnum + 1)), typ,
			),
			code = '--PIO',
			detail = data,
			hint = "Try casting the parameter to 'text', then to the target type.",
			position = str(itemnum)
		)
		self.database._raise_a_pq_error(em, controller = self)
		raise RuntimeError("failed to raise client error")

	def _raise_column_tuple_error(self, procs, tup, itemnum):
		typ = self.database.typio.sql_type_from_oid(
			self.pg_column_types(itemnum)
		) or '<unknown>'

		data = repr(tup[itemnum])
		if len(data) > 80:
			# Be sure not to fill screen with noise.
			data = data[:75] + ' ...'

		em = element.ClientError(
			message = "failed to unpack column %r, %s::%s, from wire data" %(
				itemnum, self.column_names[itemnum], typ
			),
			code = "--CIO",
			detail = data,
			hint = "Try casting the column to 'text'.",
			position = str(itemnum),
		)
		self.database._raise_a_pq_error(em, controller = self)
		# "can't happen"
		raise RuntimeError("failed to raise client error")

	@property
	def state(self) -> str:
		if self.closed:
			if self._xact is not None:
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
		if self.closed is False:
			self.database.pq.garbage_statements.append(self._pq_statement_id)
		self.closed = True
		if hasattr(self, '_del'):
			del self._del

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
		self._xact = xact.Instruction(cmd, asynchook = self.database._receive_async)
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
			except Exception:
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

	def __call__(self, *parameters):
		if self._input is not None:
			if len(parameters) != len(self._input):
				raise TypeError("statement requires %d parameters, given %d" %(
					len(self._input), len(parameters)
				))
		# get em' all!
		if self._output is None:
			# might be a copy.
			c = SingleXactCopy(self, parameters)
		else:
			c = SingleXactFetch(self, parameters)

		# iff output is None, it's not a tuple returning query.
		# however, if it's a copy, detect that fact by SingleXactCopy's
		# immediate return after finding the copy begin message(no complete).
		if self._output is None and c.command() is not None:
			return (c.command(), c.count())
		else:
			r = []
			for x in c:
				r.extend(x)
			return r

	def declare(self, *parameters):
		if self.closed is None:
			self._fini()
		if self._input is not None:
			if len(parameters) != len(self._input):
				raise TypeError("statement requires %d parameters, given %d" %(
					len(self._input), len(parameters)
				))
		return Cursor(self, parameters, self.database, None)

	def rows(self, *parameters, **kw):
		return chain.from_iterable(self.chunks(*parameters, **kw))
	__iter__ = rows

	def chunks(self, *parameters):
		if self.closed is None:
			self._fini()
		if self._input is not None:
			if len(parameters) != len(self._input):
				raise TypeError("statement requires %d parameters, given %d" %(
					len(self._input), len(parameters)
				))
		if self._output is None:
			return SingleXactCopy(self, parameters)
		if self.database.pq.state == b'I':
			if self.string is not None:
				return MultiXactOutsideBlock(self, parameters, None)
			else:
				# statement source unknown, so it can't be DECLARE'd.
				return SingleXactFetch(self, parameters)
		else:
			return MultiXactInsideBlock(self, parameters, None)

	def first(self, *parameters):
		if self.closed is None:
			self._fini()
		if self._input is not None:
			if len(parameters) != len(self._input):
				raise TypeError("statement requires %d parameters, given %d" %(
					len(self._input), len(parameters)
				))
		# Parameters? Build em'.
		db = self.database

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
				# Get all
				element.Execute(b'', 0xFFFFFFFF),
				element.SynchronizeMessage
			),
			asynchook = db._receive_async
		)
		db._pq_push(x, self)
		db._pq_complete()

		if self._output_io:
			##
			# Look for the first tuple.
			tuple_type = element.Tuple.type
			for xt in x.messages_received():
				if xt.type == tuple_type:
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
			##
			# This loop searches through the received messages
			# for the Complete message which contains the count.
			complete = element.Complete.type
			for cm in x.messages_received():
				# Use getattr because COPY doesn't produce
				# element.Message instances.
				if getattr(cm, 'type', None) == complete:
					break
			else:
				# Probably a Null command.
				return None
			return cm.extract_count() or cm.extract_command()

	def _load_copy_chunks(self, chunks):
		"""
		Given an chunks of COPY lines, execute the COPY ... FROM STDIN
		statement and send the copy lines produced by the iterable to
		the remote end.
		"""
		x = xact.Instruction((
				element.Bind(
					b'',
					self._pq_statement_id,
					(), (), (),
				),
				element.Execute(b'', 1),
				element.FlushMessage,
			),
			asynchook = self.database._receive_async
		)
		self.database._pq_push(x, self)

		# Get the COPY started.
		while x.state is not xact.Complete:
			self.database._pq_step()
			if hasattr(x, 'CopyFailSequence') and x.messages is x.CopyFailSequence:
				break
		else:
			# Oh, it's not a COPY at all.
			x.error_message = element.ClientError(
				# OperationError
				code = '--OPE',
				message = "_load_copy_chunks() used on a non-COPY FROM STDIN query",
			)
			x.fatal = False
			self.database._raise_pq_error(x, controller = self)
			raise RuntimeError("failed to raise client error")

		for chunk in chunks:
			x.messages = list(chunk)
			while x.messages is not x.CopyFailSequence:
				self.database._pq_step()
		x.messages = x.CopyDoneSequence
		self.database._pq_complete()
		self.database.pq.synchronize()

	def _load_tuple_chunks(self, chunks):
		pte = self._raise_parameter_tuple_error
		last = (element.SynchronizeMessage,)
		try:
			for chunk in chunks:
				bindings = [
					(
						element.Bind(
							b'',
							self._pq_statement_id,
							self._input_formats,
							pg_typio.process_tuple(
								self._input_io, tuple(t), pte
							),
							(),
						),
						element.Execute(b'', 1),
					)
					for t in chunk
				]
				bindings.append(last)
				self.database._pq_push(
					xact.Instruction(
						chain.from_iterable(bindings),
						asynchook = self.database._receive_async
					),
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
			self.database.pq.synchronize()
			raise

	def load(self, iterable, tps = None):
		"""
		WARNING: Deprecated, use load_chunks and load_rows instead.
		"""
		if self.closed is None:
			self._fini()
		if isinstance(iterable, Chunks):
			l = self.load_chunks
		else:
			l = self.load_rows
		return l(iterable)

	def load_chunks(self, chunks):
		"""
		Execute the query for each row-parameter set in `iterable`.

		In cases of ``COPY ... FROM STDIN``, iterable must be an iterable of
		sequences of `bytes`.
		"""
		if self.closed is None:
			self._fini()
		if not self._input:
			return self._load_copy_chunks(chunks)
		else:
			return self._load_tuple_chunks(chunks)

	def load_rows(self, rows, chunksize = 256):
		return self.load_chunks(chunk(rows, chunksize))

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
			proctup = database.sys.lookup_procedure_oid(int(ident))
		else:
			proctup = database.sys.lookup_procedure_rp(str(ident))
		if proctup is None:
			raise LookupError("no function with identifier %s" %(str(ident),))

		self.procedure_id = ident
		self.oid = proctup[0]
		self.name = proctup["proname"]

		self._input_attmap = {}
		argnames = proctup.get('proargnames') or ()
		for x in range(len(argnames)):
			an = argnames[x]
			if an is not None:
				self._input_attmap[an] = x

		proargs = proctup['proargtypes']
		for x in proargs:
			# get metadata filled out.
			database.typio.resolve(x)

		self.statement = database.prepare(
			"SELECT * FROM %s(%s) AS func%s" %(
				proctup['_proid'],
				# ($1::type, $2::type, ... $n::type)
				', '.join([
					 '$%d::%s' %(x + 1, database.typio.sql_type_from_oid(proargs[x]))
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
			r = self.database.sys.setting_get(i)

			if r:
				v = r[0][0]
			else:
				raise KeyError(i)
		return v

	def __setitem__(self, i, v):
		cv = self.cache.get(i)
		if cv == v:
			return
		setas = self.database.sys.setting_set(i, v)
		self.cache[i] = setas

	def __delitem__(self, k):
		self.database.execute(
			'RESET "' + k.replace('"', '""') + '"'
		)
		self.cache.pop(k, None)

	def __len__(self):
		return self.database.sys.setting_len()

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
		r = self.database.sys.setting_get(k)
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
			r = self.database.sys.setting_mget(rkeys)
			self.cache.update(r)
			setmap.update(r)
			rem = set(rkeys) - set([x['name'] for x in r])
			if rem:
				raise KeyError(rem)
		return setmap

	def keys(self):
		return map(
			get0, self.database.sys.setting_keys()
		)
	__iter__ = keys

	def values(self):
		return map(
			get0, self.database.sys.setting_values()
		)

	def items(self):
		return self.database.sys.setting_items()

	def update(self, d):
		kvl = [list(x) for x in dict(d).items()]
		self.cache.update(
			self.database.sys.setting_update(kvl)
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

	def __init__(self, database, isolation = None, mode = None, gid = None):
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
				if not self.database.closed:
					self.rollback()
				# pg_exc.InFailedTransactionError
				em = element.ClientError(
					code = '25P02',
					message = 'invalid transaction block exit detected',
					hint = "Database was in an error-state, but no exception was raised."
				)
				self.database._raise_a_pq_error(em, self)
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
		return 'SAVEPOINT "xact(' + id.replace('"', '""') + ')";'

	def start(self):
		if self.state == 'open':
			return
		if self.state != 'initialized':
			em = element.ClientError(
				code = '--OPE',
				message = "transactions cannot be restarted",
				hint = \
					'Create a new transaction object instead of re-using an old one.'
			)
			self.database._raise_a_pq_error(em, self)

		if self.database.pq.state == b'I':
			self.type = 'block'
			q = self._start_xact_string(
				isolation = self.isolation,
				mode = self.mode,
			)
		else:
			self.type = 'savepoint'
			if (self.gid, self.isolation, self.mode) != (None,None,None):
				em = element.ClientError(
					code = '--OPE',
					message = "configured transaction used inside a transaction block",
					hint = 'A transaction block was already started.',
				)
				self.database._raise_a_pq_error(em, self)
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
			em = element.ClientError(
				code = '--OPE',
				message = "transaction state must be 'open' in order to prepare",
			)
			self.database._raise_a_pq_error(em, self)
		if self.type != 'block':
			em = element.ClientError(
				code = '--OPE',
				message = "improper transaction type to prepare",
			)
			self.database._raise_a_pq_error(em, self)
		q = self._prepare_string(self.gid)
		self.database.execute(q)
		self.state = 'prepared'

	def recover(self):
		if self.state != 'initialized':
			em = element.ClientError(
				code = '--OPE',
				message = "improper state for prepared transaction recovery",
			)
			self.database._raise_a_pq_error(em, self)
		if self.database.sys.xact_is_prepared(self.gid):
			self.state = 'prepared'
			self.type = 'block'
		else:
			em = element.ClientError(
				code = '42704', # UndefinedObjectError
				message = "prepared transaction does not exist",
			)
			self.database._raise_a_pq_error(em, self)

	def commit(self):
		if self.state == 'committed':
			return
		if self.state not in ('prepared', 'open'):
			em = element.ClientError(
				code = '--OPE',
				message = "commit attempted on transaction with unexpected state, " + repr(self.state),
			)
			self.database._raise_a_pq_error(em, self)

		if self.type == 'block':
			if self.gid is not None:
				# User better have prepared it.
				q = "COMMIT PREPARED '" + self.gid.replace("'", "''") + "';"
			else:
				q = 'COMMIT'
		else:
			if self.gid is not None:
				em = element.ClientError(
					code = '--OPE',
					message = "savepoint configured with global identifier",
				)
				self.database._raise_a_pq_error(em, self)
			q = self._release_string(hex(id(self)))
		self.database.execute(q)
		self.state = 'committed'

	@staticmethod
	def _rollback_to_string(id):
		return 'ROLLBACK TO "xact(' + id.replace('"', '""') + ')";'

	def rollback(self):
		if self.state == 'aborted':
			return
		if self.state not in ('prepared', 'open'):
			em = element.ClientError(
				code = '--OPE',
				message = "ABORT attempted on transaction with unexpected state, " \
					+ repr(self.state),
			)
			self.database._raise_a_pq_error(em, self)

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
		ps = PreparedStatement(self, statement_id, sql_statement_string)
		ps._init()
		ps._fini()
		return ps

	def statement_from_id(self, statement_id : str) -> PreparedStatement:
		ps = PreparedStatement(self, statement_id, None)
		ps._init()
		ps._fini()
		return ps

	def proc(self, proc_id : (str, int)) -> StoredProcedure:
		sp = StoredProcedure(proc_id, self)
		return sp

	def cursor_from_id(self, cursor_id : str) -> Cursor:
		c = Cursor(None, None, self, cursor_id)
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
			if self.pq.state == b'E':
				return 'failed block'
			return 'idle' + (' in block' if self.pq.state != b'I' else '')
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
		# When ssl is False: SSL negotiation will occur but it may fail(NOSSL).
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
			# Grab the negotiation transaction before
			# connecting as it will be needed later if successful.
			neg = pq.xact
			pq.connect(ssl = ssl, timeout = timeout)

			didssl = getattr(pq, 'ssl_negotiation', -1)

			# It successfully connected if pq.xact is None.
			if pq.xact is None:
				self.pq = pq
				self.security = 'ssl' if didssl is True else None
				showoption_type = element.ShowOption.type
				for x in neg.asyncs:
					if x.type == showoption_type:
						self._receive_async(x)
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
		# manual binding
		self.sys = pg_lib.Binding(self, pg_lib.sys)

		# Get the *full* version string.
		self.version = self.prepare("SELECT pg_catalog.version()").first()
		# First word from the version string.
		self.type = self.version.split()[0]

		r = self.sys.activity_for(self.backend_id)
		if r is not None:
			# conditional initialization of client_address.
			# pythons without ipaddr will likely give strings.
			ca = r.get('client_addr')
			if ca is not None:
				self.client_address = ca.split('/')[0]
			self.client_port = r.get('client_port')
			self.backend_start = r.get('backend_start')
		##
		# Set standard_conforming_strings
		scstr = self.settings.get('standard_conforming_strings')
		if scstr is None or (self.version_info[0] == 8 and self.version_info[1] == 1):
			# There used to be a warning emitted here.
			# It was noisy, and had little added value
			# over a nice WARNING at the top of the driver documentation.
			pass
		elif scstr.lower() not in ('on','true','yes'):
			self.settings['standard_conforming_strings'] = 'on'
		super().connect()

	def _pq_push(self, xact, controller = None):
		x = self.pq.xact
		if x is not None:
			self.pq.complete()
			self._raise_pq_error(x)
		if controller is not None:
			self._controller = controller
		self.pq.push(xact)

	def _pq_complete(self):
		x = self.pq.xact
		if self.pq.xact is not None:
			self.pq.complete()
			self._raise_pq_error(x)
			del self._controller

	def _pq_step(self):
		x = self.pq.xact
		if x is not None:
			self.pq.step()
			if x.state is xact.Complete:
				self._raise_pq_error(x)
				del self._controller

	def _raise_pq_error(self, xact = None, controller = None):
		if xact is not None:
			x = xact
		else:
			x = self.pq.xact
		if x.fatal is None:
			# No error occurred..
			return
		err = self._error_lookup(x.error_message)
		fromexc = getattr(x, 'exception', None)
		if controller is None:
			fromcontroller = getattr(self, '_controller', self)
		err.creator = fromcontroller
		if fromexc is not None:
			err.__cause__ = fromexc
		raise err

	def _raise_a_pq_error(self, em, controller):
		err = self._error_lookup(em)
		err.creator = controller
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

	def _convert_pq_message(self, msg, source = None):
		'create a message, warning or error'
		dmsg = self._decode_pq_message(msg)
		m = dmsg.pop('message')
		c = dmsg.pop('code')
		sev = dmsg['severity'].upper()
		if source is None:
			if type(msg) in (element.ClientNotice, element.ClientError):
				source = 'CLIENT'
			else:
				source = 'SERVER'
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

	def _receive_async(self, msg, controller = None,
		showoption = element.ShowOption.type,
		notice = element.Notice.type,
		notify = element.Notify.type,
	):
		c = controller or getattr(self, '_controller', self)
		if msg.type == showoption:
			if msg.name == b'client_encoding':
				self.typio.set_encoding(msg.value.decode('ascii'))
			self.settings._notify(msg)
		elif msg.type == notice:
			src = 'SERVER'
			if type(msg) is element.ClientNotice:
				src = 'CLIENT'
			m = self._convert_pq_message(msg, source = src)
			m.creator = c
			m.raise_message()
		elif msg.type == notify:
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

	def clone(self, *args, **kw):
		c = type(self)(self.connector, *args, **kw)
		c.connect()
		return c

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
				if v is not None and not k.startswith('_') and k not in (
					'driver', 'category'
				)
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
		driver = None,
		**kw
	):
		super().__init__(**kw)
		self.driver = driver

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
			SocketFactory(x[0:3], x[4][:2], self._socket_secure)
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
	def _e_metas(self):
		yield (None, type(self).__module__ + '.' + type(self).__name__)

	def ip4(self, **kw):
		return IP4(driver = self, **kw)

	def ip6(self, **kw):
		return IP6(driver = self, **kw)

	def host(self, **kw):
		return Host(driver = self, **kw)

	def unix(self, **kw):
		return Unix(driver = self, **kw)

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
