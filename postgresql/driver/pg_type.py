##
# .driver.pg_type - Standard Database Type I/O
##
from codecs import lookup as lookup_codecs
from abc import ABCMeta, abstractmethod
from operator import itemgetter
get0 = itemgetter(0)
get1 = itemgetter(1)
from itertools import count
from ..encodings.aliases import get_python_name
from ..python.functools import Composition as compose
from ..string import quote_ident
from .. import types as pg_types
from ..types.io import resolve
from ..types import Row, Array, oid_to_sql_name, oid_to_name
from ..python.functools import process_tuple
from .. import exceptions as pg_exc
from ..message import Message
from ..types.io import lib
from ..protocol.element3 import ClientError, ClientNotice
from ..protocol.message_types import message_types

# Map element3.Notice field identifiers
# to names used by message.Message.
notice_field_to_name = {
	message_types[b'S'[0]] : 'severity',
	message_types[b'C'[0]] : 'code',
	message_types[b'M'[0]] : 'message',
	message_types[b'D'[0]] : 'detail',
	message_types[b'H'[0]] : 'hint',
	message_types[b'W'[0]] : 'context',
	message_types[b'P'[0]] : 'position',
	message_types[b'p'[0]] : 'internal_position',
	message_types[b'q'[0]] : 'internal_query',
	message_types[b'F'[0]] : 'file',
	message_types[b'L'[0]] : 'line',
	message_types[b'R'[0]] : 'function',
}

notice_field_from_name = dict(
	(v, k) for (k, v) in notice_field_to_name.items()
)

class TypeIO(object, metaclass = ABCMeta):
	"""
	A class that manages I/O for a given configuration. Normally, a connection
	would create an instance, and configure it based upon the version and
	configuration of PostgreSQL that it is connected to.
	"""
	strio = (None, None, str)

	@abstractmethod
	def lookup_type_info(self, typid):
		"""
		"""

	@abstractmethod
	def lookup_composite_type_info(self, typid):
		"""
		"""

	def set_encoding(self, value):
		"""
		Set a new client encoding.
		"""
		self.encoding = value.lower().strip()
		enc = get_python_name(self.encoding)
		ci = lookup_codecs(enc or self.encoding)
		self._encode, self._decode, *_ = ci

	def encode(self, string_data):
		return self._encode(string_data)[0]

	def decode(self, bytes_data):
		return self._decode(bytes_data)[0]

	def encodes(self, iter, get0 = get0):
		"""
		Encode the items in the iterable in the configured encoding.
		"""
		return map(compose((self._encode, get0)), iter)

	def decodes(self, iter, get0 = get0):
		"""
		Decode the items in the iterable from the configured encoding.
		"""
		return map(compose((self._decode, get0)), iter)

	def resolve_pack(self, typid):
		return self.resolve(typid)[0] or self.encode

	def resolve_unpack(self, typid):
		return self.resolve(typid)[1] or self.decode

	def attribute_map(self, pq_descriptor):
		return zip(self.decodes(pq_descriptor.keys()), count())

	def __init__(self):
		strio = self.strio
		self.encoding = None
		self._cache = {
			# Encoded character strings
			pg_types.ACLITEMOID : strio, # No binary functions.
			pg_types.NAMEOID : strio,
			pg_types.BPCHAROID : strio,
			pg_types.VARCHAROID : strio,
			pg_types.CSTRINGOID : strio,
			pg_types.TEXTOID : strio,
			pg_types.REGTYPEOID : strio,
			pg_types.REGPROCOID : strio,
			pg_types.REGPROCEDUREOID : strio,
			pg_types.REGOPEROID : strio,
			pg_types.REGOPERATOROID : strio,
			pg_types.REGCLASSOID : strio,
		}
		self.typinfo = {}

	def sql_type_from_oid(self, oid, qi = quote_ident):
		if oid in oid_to_sql_name:
			return oid_to_sql_name[oid]
		if oid in self.typinfo:
			nsp, name, *_ = self.typinfo[oid]
			return qi(nsp) + '.' + qi(name)
		return 'pg_catalog.' + pg_types.oid_to_name.get(oid)

	def type_from_oid(self, oid):
		if oid in self._cache:
			typ = self._cache[oid][2]
		return typ

	def resolve_descriptor(self, desc, index):
		'create a sequence of I/O routines from a pq descriptor'
		return [
			(self.resolve(x[3]) or (None, None))[index] for x in desc
		]

	# lookup a type's IO routines from a given typid
	def resolve(self,
		typid : "The Oid of the type to resolve pack and unpack routines for.",
		from_resolution_of : \
		"Sequence of typid's used to identify infinite recursion" = (),
		builtins : "types.io.resolve" = resolve,
		quote_ident = quote_ident
	):
		if from_resolution_of and typid in from_resolution_of:
			raise TypeError(
				"type, %d, is already being looked up: %r" %(
					typid, from_resolution_of
				)
			)
		typid = int(typid)
		typio = None

		if typid in self._cache:
			typio = self._cache[typid]
		else:
			typio = builtins(typid)
			if typio is not None:
				if typio.__class__ is not tuple:
					typio = typio(typid, self)
				self._cache[typid] = typio

		if typio is None:
			# Lookup the type information for the typid as it's not cached.
			##
			ti = self.lookup_type_info(typid)
			if ti is not None:
				typnamespace, typname, typtype, typlen, typelem, typrelid, \
					ae_typid, ae_hasbin_input, ae_hasbin_output = ti
				self.typinfo[typid] = (
					typnamespace, typname, typrelid, int(typelem) if ae_typid else None
				)
				if typrelid:
					# Row type
					#
					# The attribute name map,
					#  column I/O,
					#  column type Oids
					# are needed to build the packing pair.
					attmap = {}
					cio = []
					typids = []
					attnames = []
					i = 0
					for x in self.lookup_composite_type_info(typrelid):
						attmap[x[1]] = i
						attnames.append(x[1])
						typids.append(x[0])
						pack, unpack, typ = self.resolve(
							x[0], list(from_resolution_of) + [typid]
						)
						cio.append((pack or self.encode, unpack or self.decode))
						i += 1
					self._cache[typid] = typio = self.record_io_factory(
						cio, typids, attmap, list(
							map(self.sql_type_from_oid, typids)
						), attnames,
						quote_ident(typnamespace) + '.' + \
						quote_ident(typname),
					)
				elif ae_typid is not None:
					# resolve the element type and I/O pair
					te = self.resolve(
						int(typelem),
						from_resolution_of = list(from_resolution_of) + [typid]
					) or (None, None)
					typio = self.array_io_factory(
						te[0] or self.encode,
						te[1] or self.decode,
						typelem,
						ae_hasbin_input,
						ae_hasbin_output
					)
					self._cache[typid] = typio
				else:
					self._cache[typid] = typio = self.strio
			else:
				# Throw warning about type without entry in pg_type?
				typio = self.strio
		return typio

	def identify(self, **identity_mappings):
		"""
		Explicitly designate the I/O handler for the specified type.

		Primarily used in cases involving UDTs.
		"""
		# get them ordered; we process separately, then recombine.
		id = list(identity_mappings.items())
		ios = [resolve(x[0]) for x in id]
		oids = list(self.database.sys.regtypes([x[1] for x in id]))

		self._cache.update([
			(oid, io if io.__class__ is tuple else io(oid, self))
			for oid, io in zip(oids, ios)
		])

	def array_parts(self, array, ArrayType = Array):
		if array.__class__ is not ArrayType:
			# Assume the data is a nested list.
			array = ArrayType(array)
		return (
			array.elements(),
			array.dimensions,
			array.lowerbounds
		)

	def array_from_parts(self, parts, ArrayType = Array):
		elements, dimensions, lowerbounds = parts
		return ArrayType.from_elements(
			elements,
			lowerbounds = lowerbounds,
			upperbounds = [x + lb - 1 for x, lb in zip(dimensions, lowerbounds)]
		)

	##
	# array_io_factory - build I/O pair for ARRAYs
	##
	def array_io_factory(
		self,
		pack_element, unpack_element,
		typoid, # array element id
		hasbin_input, hasbin_output,
		array_pack = lib.array_pack,
		array_unpack = lib.array_unpack,
	):
		packed_typoid = lib.ulong_pack(typoid)
		if hasbin_input:
			def pack_an_array(data, get_parts = self.array_parts):
				elements, dimensions, lowerbounds = get_parts(data)
				return array_pack((
					0, # unused flags
					typoid, dimensions, lowerbounds,
					(x if x is None else pack_element(x) for x in elements),
				))
		else:
			# signals string formatting
			pack_an_array = None

		if hasbin_output:
			def unpack_an_array(data, array_from_parts = self.array_from_parts):
				flags, typoid, dims, lbs, elements = array_unpack(data)
				return array_from_parts((map(unpack_element, elements), dims, lbs))
		else:
			# signals string formatting
			unpack_an_array = None

		return (pack_an_array, unpack_an_array, Array)

	##
	# record_io_factory - Build an I/O pair for RECORDs
	##
	def record_io_factory(self,
		column_io : "sequence (pack,unpack) tuples corresponding to the columns",
		typids : "sequence of type Oids; index must correspond to the composite's",
		attmap : "mapping of column name to index number",
		typnames : "sequence of sql type names in order",
		attnames : "sequence of attribute names in order",
		composite_name : "the name of the composite type",
		get0 = get0,
		get1 = get1,
		fmt_errmsg = "failed to {0} attribute {1}, {2}::{3}, of composite {4} from wire data".format
	):
		fpack = tuple(map(get0, column_io))
		funpack = tuple(map(get1, column_io))

		def raise_pack_tuple_error(procs, tup, itemnum):
			data = repr(tup[itemnum])
			if len(data) > 80:
				# Be sure not to fill screen with noise.
				data = data[:75] + ' ...'
			self.raise_client_error(ClientError((
				(b'C', '--cIO',),
				(b'S', 'ERROR',),
				(b'M', fmt_errmsg('pack', itemnum, attnames[itemnum], typnames[itemnum], composite_name),),
				(b'W', data,),
				(b'P', str(itemnum),)
			)))

		def raise_unpack_tuple_error(procs, tup, itemnum):
			data = repr(tup[itemnum])
			if len(data) > 80:
				# Be sure not to fill screen with noise.
				data = data[:75] + ' ...'
			self.raise_client_error(ClientError((
				(b'C', '--cIO',),
				(b'S', 'ERROR',),
				(b'M', fmt_errmsg('unpack', itemnum, attnames[itemnum], typnames[itemnum], composite_name),),
				(b'W', data,),
				(b'P', str(itemnum),),
			)))

		def unpack_a_record(data,
			unpack = lib.record_unpack,
			process_tuple = process_tuple,
			row_from_seq = Row.from_sequence
		):
			data = tuple([x[1] for x in unpack(data)])
			return row_from_seq(
				attmap, process_tuple(funpack, data, raise_unpack_tuple_error),
			)

		sorted_atts = sorted(attmap.items(), key = get1)
		def pack_a_record(data,
			pack = lib.record_pack,
			process_tuple = process_tuple,
		):
			if isinstance(data, dict):
				data = [data.get(k) for k,_ in sorted_atts]
			return pack(
				tuple(zip(
					typids,
					process_tuple(fpack, tuple(data), raise_pack_tuple_error)
				))
			)
		return (pack_a_record, unpack_a_record, Row)

	def raise_client_error(self, error_message, cause = None, creator = None):
		m = {
			notice_field_to_name[k] : v
			for k, v in error_message.items()
			# don't include unknown messages in this list.
			if k in notice_field_to_name
		}
		c = m.pop('code')
		ms = m.pop('message')
		client_error = self.lookup_exception(c)
		client_error = client_error(ms, code = c, details = m, source = 'CLIENT', creator = creator or self.database)
		client_error.database = self.database
		if cause is not None:
			raise client_error from cause
		else:
			raise client_error

	def lookup_exception(self, code, errorlookup = pg_exc.ErrorLookup,):
		return errorlookup(code)

	def lookup_warning(self, code, warninglookup = pg_exc.WarningLookup,):
		return warninglookup(code)

	def raise_server_error(self, error_message, cause = None, creator = None):
		m = dict(self.decode_notice(error_message))
		c = m.pop('code')
		ms = m.pop('message')
		server_error = self.lookup_exception(c)
		server_error = server_error(ms, code = c, details = m, source = 'SERVER', creator = creator or self.database)
		server_error.database = self.database
		if cause is not None:
			raise server_error from cause
		else:
			raise server_error

	def raise_error(self, error_message, ClientError = ClientError, **kw):
		if 'creator' not in kw:
			kw['creator'] = getattr(self.database, '_controller', self.database) or self.database

		if error_message.__class__ is ClientError:
			self.raise_client_error(error_message, **kw)
		else:
			self.raise_server_error(error_message, **kw)

	##
	# Used by decode_notice()
	def _decode_failsafe(self, data):
		decode = self._decode
		i = iter(data)
		for x in i:
			try:
				# prematurely optimized for your viewing displeasure.
				v = x[1]
				yield (x[0], decode(v)[0])
				for x in i:
					v = x[1]
					yield (x[0], decode(v)[0])
			except UnicodeDecodeError:
				# Fallback to the bytes representation.
				# This should be sufficiently informative in most cases,
				# and in the cases where it isn't, an element traceback should
				# ultimately yield the pertinent information
				yield (x[0], repr(data[1])[2:-1])

	def decode_notice(self, notice):
		notice = self._decode_failsafe(notice.items())
		return {
			notice_field_to_name[k] : v
			for k, v in notice
			# don't include unknown messages in this list.
			if k in notice_field_to_name
		}

	def emit_server_message(self, message, creator = None,
		MessageType = Message
	):
		fields = self.decode_notice(message)
		m = fields.pop('message')
		c = fields.pop('code')

		if fields['severity'].upper() == 'WARNING':
			MessageType = self.lookup_warning(c)

		message = Message(m, code = c, details = fields,
			creator = creator, source = 'SERVER')
		message.database = self.database
		message.emit()
		return message

	def emit_client_message(self, message, creator = None,
		MessageType = Message
	):
		fields = {
			notice_field_to_name[k] : v
			for k, v in message.items()
			# don't include unknown messages in this list.
			if k in notice_field_to_name
		}
		m = fields.pop('message')
		c = fields.pop('code')

		if fields['severity'].upper() == 'WARNING':
			MessageType = self.lookup_warning(c)

		message = MessageType(m, code = c, details = fields,
			creator = creator, source = 'CLIENT')
		message.database = self.database
		message.emit()
		return message

	def emit_message(self, message, ClientNotice = ClientNotice, **kw):
		if message.__class__ is ClientNotice:
			return self.emit_client_message(message, **kw)
		else:
			return self.emit_server_message(message, **kw)
