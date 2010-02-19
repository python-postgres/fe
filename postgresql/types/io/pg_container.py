##
# types.io.pg_container - construct I/O pairs for container types
##
from . import lib
from .. import Row, Array, ANYARRAYOID, RECORDOID
from ... import exceptions as pg_exc
from ...python.functools import process_tuple
from ...python.itertools import interlace
from operator import itemgetter

##
# array_io_factory - build I/O pair for ARRAYs
##
def array_io_factory(
	pack_element, unpack_element,
	typoid, hasbin_input, hasbin_output,
	array_pack = lib.array_pack,
	array_unpack = lib.array_unpack,
	ArrayType = Array,
	interlace = interlace
):
	if hasbin_input:
		def pack_an_array(data):
			if not data.__class__ is ArrayType:
				# Assume the data is a nested list.
				data = ArrayType(data)
			return array_pack((
				0, # unused flags
				typoid, tuple(interlace(data.dimensions, data.lowerbounds)),
				(x if x is None else pack_element(x) for x in data.elements()),
			))
	else:
		# signals string formatting
		pack_an_array = None

	if hasbin_output:
		def unpack_an_array(data):
			flags, typoid, dlb, elements = array_unpack(data)
			upper = []
			lower = []
			for x in range(0, len(dlb), 2):
				lb = dlb[x+1]
				lower.append(lb)
				upper.append(dlb[x] + lb - 1)
			return Array.from_elements(
				(x if x is None else unpack_element(x) for x in elements),
				lowerbounds = lower, upperbounds = upper,
			)
	else:
		# signals string formatting
		unpack_an_array = None

	return (pack_an_array, unpack_an_array, Array)

##
# record_io_factory - Build an I/O pair for RECORDs
##
def record_io_factory(
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

oid_to_io = {
	ANYARRAYOID : array_io_factory,
	RECORDOID : record_io_factory,
}
