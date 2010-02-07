##
# copyright 2009, James William Pye.
# http://python.projects.postgresql.org
##
"""
PostgreSQL types and identifiers.
"""
# XXX: Would be nicer to generate these from a header file...
InvalidOid = 0

RECORDOID = 2249
BOOLOID = 16
BITOID = 1560
VARBITOID = 1562
ACLITEMOID = 1033

CHAROID = 18
NAMEOID = 19
TEXTOID = 25
BYTEAOID = 17
BPCHAROID = 1042
VARCHAROID = 1043
CSTRINGOID = 2275
UNKNOWNOID = 705
REFCURSOROID = 1790
UUIDOID = 2950

TSVECTOROID = 3614
GTSVECTOROID = 3642
TSQUERYOID = 3615
REGCONFIGOID = 3734
REGDICTIONARYOID = 3769

XMLOID = 142

MACADDROID = 829
INETOID = 869
CIDROID = 650

TYPEOID = 71
PROCOID = 81
CLASSOID = 83
ATTRIBUTEOID = 75

DATEOID = 1082
TIMEOID = 1083
TIMESTAMPOID = 1114
TIMESTAMPTZOID = 1184
INTERVALOID = 1186
TIMETZOID = 1266
ABSTIMEOID = 702
RELTIMEOID = 703
TINTERVALOID = 704

INT8OID = 20
INT2OID = 21
INT4OID = 23
OIDOID = 26
TIDOID = 27
XIDOID = 28
CIDOID = 29
CASHOID = 790
FLOAT4OID = 700
FLOAT8OID = 701
NUMERICOID = 1700

POINTOID = 600
LINEOID = 628
LSEGOID = 601
PATHOID = 602
BOXOID = 603
POLYGONOID = 604
CIRCLEOID = 718

OIDVECTOROID = 30
INT2VECTOROID = 22
INT4ARRAYOID = 1007

REGPROCOID = 24
REGPROCEDUREOID = 2202
REGOPEROID = 2203
REGOPERATOROID = 2204
REGCLASSOID = 2205
REGTYPEOID = 2206
REGTYPEARRAYOID = 2211

TRIGGEROID = 2279
LANGUAGE_HANDLEROID = 2280
INTERNALOID = 2281
OPAQUEOID = 2282
VOIDOID = 2278
ANYARRAYOID = 2277
ANYELEMENTOID = 2283
ANYOID = 2276
ANYNONARRAYOID = 2776
ANYENUMOID = 3500

oid_to_sql_name = {
	BPCHAROID : 'CHARACTER',
	VARCHAROID : 'CHARACTER VARYING',
	# *OID : 'CHARACTER LARGE OBJECT',

	# SELECT X'0F' -> bit. XXX: Does bytea have any play here?
	#BITOID : 'BINARY',
	#BYTEAOID : 'BINARY VARYING',
	# *OID : 'BINARY LARGE OBJECT',

	BOOLOID : 'BOOLEAN',

# exact numeric types
	INT2OID : 'SMALLINT',
	INT4OID : 'INTEGER',
	INT8OID : 'BIGINT',
	NUMERICOID : 'NUMERIC',

# approximate numeric types
	FLOAT4OID : 'REAL',
	FLOAT8OID : 'DOUBLE PRECISION',

# datetime types
	TIMEOID : 'TIME WITHOUT TIME ZONE',
	TIMETZOID : 'TIME WITH TIME ZONE',
	TIMESTAMPOID : 'TIMESTAMP WITHOUT TIME ZONE',
	TIMESTAMPTZOID : 'TIMESTAMP WITH TIME ZONE',
	DATEOID : 'DATE',

# interval types
	INTERVALOID : 'INTERVAL',

	XMLOID : 'XML',
}

oid_to_name = {
	RECORDOID : 'record',
	BOOLOID : 'bool',
	BITOID : 'bit',
	VARBITOID : 'varbit',
	ACLITEMOID : 'aclitem',

	CHAROID : 'char',
	NAMEOID : 'name',
	TEXTOID : 'text',
	BYTEAOID : 'bytea',
	BPCHAROID : 'bpchar',
	VARCHAROID : 'varchar',
	CSTRINGOID : 'cstring',
	UNKNOWNOID : 'unknown',
	REFCURSOROID : 'refcursor',
	UUIDOID : 'uuid',

	TSVECTOROID : 'tsvector',
	GTSVECTOROID : 'gtsvector',
	TSQUERYOID : 'tsquery',
	REGCONFIGOID : 'regconfig',
	REGDICTIONARYOID : 'regdictionary',

	XMLOID : 'xml',

	MACADDROID : 'macaddr',
	INETOID : 'inet',
	CIDROID : 'cidr',

	TYPEOID : 'type',
	PROCOID : 'proc',
	CLASSOID : 'class',
	ATTRIBUTEOID : 'attribute',

	DATEOID : 'date',
	TIMEOID : 'time',
	TIMESTAMPOID : 'timestamp',
	TIMESTAMPTZOID : 'timestamptz',
	INTERVALOID : 'interval',
	TIMETZOID : 'timetz',
	ABSTIMEOID : 'abstime',
	RELTIMEOID : 'reltime',
	TINTERVALOID : 'tinterval',

	INT8OID : 'int8',
	INT2OID : 'int2',
	INT4OID : 'int4',
	OIDOID : 'oid',
	TIDOID : 'tid',
	XIDOID : 'xid',
	CIDOID : 'cid',
	CASHOID : 'cash',
	FLOAT4OID : 'float4',
	FLOAT8OID : 'float8',
	NUMERICOID : 'numeric',

	POINTOID : 'point',
	LINEOID : 'line',
	LSEGOID : 'lseg',
	PATHOID : 'path',
	BOXOID : 'box',
	POLYGONOID : 'polygon',
	CIRCLEOID : 'circle',

	OIDVECTOROID : 'oidvector',
	INT2VECTOROID : 'int2vector',
	INT4ARRAYOID : 'int4array',

	REGPROCOID : 'regproc',
	REGPROCEDUREOID : 'regprocedure',
	REGOPEROID : 'regoper',
	REGOPERATOROID : 'regoperator',
	REGCLASSOID : 'regclass',
	REGTYPEOID : 'regtype',
	REGTYPEARRAYOID : 'regtypearray',

	TRIGGEROID : 'trigger',
	LANGUAGE_HANDLEROID : 'language_handler',
	INTERNALOID : 'internal',
	OPAQUEOID : 'opaque',
	VOIDOID : 'void',
	ANYARRAYOID : 'anyarray',
	ANYELEMENTOID : 'anyelement',
	ANYOID : 'any',
	ANYNONARRAYOID : 'anynonarray',
	ANYENUMOID : 'anyenum',
}

name_to_oid = dict(
	[(v,k) for k,v in oid_to_name.items()]
)

class Array(object):
	"""
	Type used to mimic PostgreSQL arrays.

	It primarily implements the Python sequence interfaces for treating a
	PostgreSQL array like nested Python lists.
	"""

	# return an iterator over the absolute elements of a nested sequence
	@staticmethod
	def unroll_nest(hier, dimensions):
		weight = []
		elc = 1
		dims = list(dimensions[:-1])
		dims.reverse()
		for x in dims:
			weight.insert(0, elc)
			elc *= x

		for x in range(elc):
			v = hier
			for w in weight:
				d, r = divmod(x, w)
				v = v[d]
				x = r
			for i in v:
				yield i

	# Detect the dimensions of a nested sequence
	@staticmethod
	def detect_dimensions(hier, len = len):
		while hier.__class__ is list:
			l = len(hier)
			if l > 0:
				# boundary consistency checks come later.
				hier = hier[0]
			else:
				break
			yield l

	@classmethod
	def from_nest(typ, nest, offset = None):
		'Create an array from a nested sequence'
		dims = tuple(typ.detect_dimensions(nest))
		if offset:
			dims = dims[:len(dims)-offset]
		return typ.from_elements(list(typ.unroll_nest(nest, dims)), dims)

	@classmethod
	def from_elements(subtype,
		elements : "iterable of elements in the array",
		dimensions : "size of each axis" = (),
		lowerbounds : "beginning of each axis" = (),
		len = len,
	):
		# resolve iterable
		elements = list(elements)

		if dimensions:
			# dimensions were given, so check.
			elcount = 1
			for x in dimensions:
				elcount = x * elcount
			if len(elements) != elcount:
				raise ValueError("array element count inconsistent with dimensions")
			dimensions = tuple(dimensions)
		else:
			# fill in default
			elcount = len(elements)
			dimensions = (elcount,)
		d1 = dimensions[0]
		ndims = len(dimensions)

		if not lowerbounds:
			lowerbounds = (1,) * ndims

		# lowerbounds can be just about anything, so no checks here.
		if len(lowerbounds) != ndims:
			raise ValueError("number of bounds inconsistent with number of dimensions")
		# XXX: class is not ready for this check just yet
		#elif not lowerbounds <= dimensions:
		#	raise ValueError("lowerbounds exceeds upperbounds")

		rob = super().__new__(subtype)
		rob.elements = elements
		rob.dimensions = dimensions
		rob.lowerbounds = lowerbounds
		rob.position = ()
		rob.slice = slice(0, d1)

		weight = []
		cw = 1
		rdim = list(dimensions)
		rdim.reverse()
		for x in rdim[:-1]:
			cw *= x
			weight.insert(0, cw)
		rob.weight = tuple(weight)

		return rob

	def __new__(subtype, elements, *args, **kw):
		if elements.__class__ is Array:
			return elements
		return subtype.from_nest(list(elements), **kw)

	def arrayslice(self, subpos):
		rob = object.__new__(type(self))
		rob.elements = self.elements
		rob.dimensions = self.dimensions
		rob.position = self.position
		rob.weight = self.weight
		d = self.dimensions[len(self.position)]
		newslice = slice(
			self.slice.start + (subpos.start or 0),
			# Can't extend the slice, so grab whatever is the least.
			min((
				self.slice.start + (subpos.stop or d),
				self.slice.stop or d
			))
		)
		rob.slice = newslice
		return rob

	def subarray(self, idx):
		rob = object.__new__(type(self))
		rob.elements = self.elements
		rob.dimensions = self.dimensions
		rob.weight = self.weight
		idx = self.slice.start + idx
		rob.position = self.position + (slice(idx, self.slice.stop),)
		rob.slice = slice(0, rob.dimensions[len(rob.position)])
		return rob

	def nest(self, seqtype = list):
		'Transform the array into a nested sequence'
		rl = []
		for x in self:
			if type(self) is type(x):
				rl.append(x.nest())
			else:
				rl.append(x)
		return seqtype(rl)

	def __repr__(self):
		return '%s.%s(%r)' %(
			type(self).__module__,
			type(self).__name__,
			self.nest()
		)

	def __len__(self):
		sl = self.slice
		return sl.stop - sl.start

	def __eq__(self, ob):
		return list(self) == ob

	def __ne__(self, ob):
		return list(self) != ob

	def __gt__(self, ob):
		return list(self) > ob

	def __lt__(self, ob):
		return list(self) < ob

	def __le__(self, ob):
		return list(self) <= ob

	def __ge__(self, ob):
		return list(self) >= ob

	def __getitem__(self, item):
		if isinstance(item, slice):
			return self.arrayslice(item)
		else:
			if item < 0:
				item = item + (self.slice.stop - self.slice.start)

			npos = len(self.position)
			ndim = len(self.dimensions)
			if npos == ndim - 1:
				# get the actual element
				idx = self.slice.start + item
				if not (0 <= idx < self.dimensions[-1]):
					return None
				for x in range(npos):
					pos = self.position[x]
					dim = self.dimensions[x]
					# Out of bounds position
					if not (0 <= pos.start < dim) or pos.start >= pos.stop:
						return None
					idx += pos.start * self.weight[x]

				return self.elements[idx]
			else:
				# get a new array at that level
				return self.subarray(item)

	def __iter__(self):
		for x in range(len(self)):
			yield self[x]

from operator import itemgetter
get0 = itemgetter(0)
get1 = itemgetter(1)
del itemgetter

class Row(tuple):
	"Name addressable items tuple; mapping and sequence"
	@classmethod
	def from_mapping(typ, keymap, map, get1 = get1):
		iter = [
			map.get(k) for k,_ in sorted(keymap.items(), key = get1)
		]
		r = typ(iter)
		r.keymap = keymap
		return r

	@classmethod
	def from_sequence(typ, keymap, seq):
		r = typ(seq)
		r.keymap = keymap
		return r

	def __getitem__(self, i, gi = tuple.__getitem__):
		if isinstance(i, (int, slice)):
			return gi(self, i)
		idx = self.keymap[i]
		return gi(self, idx)

	def get(self, i, gi = tuple.__getitem__, len = len):
		if type(i) is int:
			l = len(self)
			if -l < i < l:
				return gi(self, i)
		else:
			idx = self.keymap.get(i)
			if idx is not None:
				return gi(self, idx)
		return None

	def keys(self):
		return self.keymap.keys()

	def values(self):
		return iter(self)

	def items(self):
		return zip(iter(self.column_names), iter(self))

	def index_from_key(self, key):
		return self.keymap.get(key)

	def key_from_index(self, index):
		for k,v in self.keymap.items():
			if v == index:
				return k
		return None

	@property
	def column_names(self, get0 = get0, get1 = get1):
		l=list(self.keymap.items())
		l.sort(key=get1)
		return tuple(map(get0, l))

	def transform(self, *args, **kw):
		"""
		Make a new Row after processing the values with the callables associated
		with the values either by index, \*args, or my column name, \*\*kw.

			>>> r=Row.from_sequence({'col1':0,'col2':1}, (1,'two'))
			>>> r.transform(str)
			('1','two')
			>>> r.transform(col2 = str.upper)
			(1,'TWO')
			>>> r.transform(str, col2 = str.upper)
			('1','TWO')

		Combine with methodcaller and map to transform lots of rows:

			>>> rowseq = [r]
			>>> xf = operator.methodcaller('transform', col2 = str.upper)
			>>> list(map(xf, rowseq))
			[(1,'TWO')]

		"""
		r = list(self)
		i = 0
		for x in args:
			if x is not None:
				r[i] = x(tuple.__getitem__(self, i))
			i = i + 1
		for k,v in kw.items():
			if v is not None:
				i = self.index_from_key(k)
				if i is None:
					raise KeyError("row has no such key, " + repr(k))
				r[i] = v(self[k])
		return type(self).from_sequence(self.keymap, r)
