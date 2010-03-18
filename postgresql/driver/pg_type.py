##
# .driver.pg_type - Standard Database Type I/O
##
from codecs import lookup as lookup_codecs
from abc import ABCMeta, abstractmethod
from operator import itemgetter
from itertools import count
from ..encodings.aliases import get_python_name
from ..python.functools import Composition as compose
from ..string import quote_ident
from .. import types as pg_types
from ..types.io import resolve
from ..types import Row, Array, oid_to_sql_name, oid_to_name
from ..python.functools import process_tuple
from .. import exceptions as pg_exc
from ..types.io import lib

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

	def encodes(self, iter, get0 = itemgetter(0)):
		"""
		Encode the items in the iterable in the configured encoding.
		"""
		return map(compose((self._encode, get0)), iter)

	def decodes(self, iter, get0 = itemgetter(0)):
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

	def row_type_factory(self, column_names):
		pass

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
		get0 = itemgetter(0),
		get1 = itemgetter(1),
	):
		fpack = tuple(map(get0, column_io))
		funpack = tuple(map(get1, column_io))

		def raise_pack_tuple_error(procs, tup, itemnum):
			data = repr(tup[itemnum])
			if len(data) > 80:
				# Be sure not to fill screen with noise.
				data = data[:75] + ' ...'
			raise pg_exc.ColumnError(
				"failed to pack attribute %d, %s::%s, of composite %s for transfer" %(
					itemnum,
					attnames[itemnum],
					typnames[itemnum],
					composite_name,
				),
				details = {
					'context': data,
					'position' : str(itemnum)
				},
			)

		def raise_unpack_tuple_error(procs, tup, itemnum):
			data = repr(tup[itemnum])
			if len(data) > 80:
				# Be sure not to fill screen with noise.
				data = data[:75] + ' ...'
			raise pg_exc.ColumnError(
				"failed to unpack attribute %d, %s::%s, of composite %s from wire data" %(
					itemnum,
					attnames[itemnum],
					typnames[itemnum],
					composite_name,
				),
				details = {
					'context': data,
					'position' : str(itemnum),
				},
			)

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
