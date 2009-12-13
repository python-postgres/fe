##
# copyright 2009, James William Pye.
# http://python.projects.postgresql.org
##
"""
PostgreSQL types and identifiers.
"""
import math
import decimal
import datetime
import operator
get0 = operator.itemgetter(0)
get1 = operator.itemgetter(1)
try:
	import xml.etree.cElementTree as etree
except ImportError:
	import xml.etree.ElementTree as etree

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

def int2(ob):
	'overflow check for int2'
	return -0x8000 <= ob < 0x8000

def int4(ob):
	'overflow check for int4'
	return -0x80000000 <= ob < 0x80000000

def int8(ob):
	'overflow check for int8'
	return -0x8000000000000000 <= rob < 0x8000000000000000

def oid(ob):
	'overflow check for oid'
	return 0 <= ob <= 0xFFFFFFFF

class varbit(object):
	__slots__ = ('data', 'bits')

	def from_bits(subtype, bits, data):
		if bits == 1:
			return (data[0] & (1 << 7)) and OneBit or ZeroBit
		else:
			rob = object.__new__(subtype)
			rob.bits = bits
			rob.data = data
			return rob
	from_bits = classmethod(from_bits)

	def __new__(typ, data):
		if isinstance(data, varbit):
			return data
		if isinstance(data, bytes):
			return typ.from_bits(len(data) * 8, data)
		# str(), eg '00101100'
		bits = len(data)
		nbytes, remain = divmod(bits, 8)
		bdata = [bytes((int(data[x:x+8], 2),)) for x in range(0, bits - remain, 8)]
		if remain != 0:
			bdata.append(bytes((int(data[nbytes*8:].ljust(8,'0'), 2),)))
		return typ.from_bits(bits, b''.join(bdata))

	def __str__(self):
		if self.bits:
			# cut off the remainder from the bits
			blocks = [bin(x)[2:].rjust(8, '0') for x in self.data]
			blocks[-1] = blocks[-1][0:(self.bits % 8) or 8]
			return ''.join(blocks)
		else:
			return ''

	def __repr__(self):
		return '%s.%s(%r)' %(
			type(self).__module__,
			type(self).__name__,
			str(self)
		)

	def __eq__(self, ob):
		if not isinstance(ob, type(self)):
			ob = type(self)(ob)
		return ob.bits == self.bits and ob.data == self.data

	def __len__(self):
		return self.bits

	def __add__(self, ob):
		return varbit(str(self) + str(ob))

	def __mul__(self, ob):
		return varbit(str(self) * ob)

	def getbit(self, bitoffset):
		if bitoffset < 0:
			idx = self.bits + bitoffset
		else:
			idx = bitoffset
		if not 0 <= idx < self.bits:
			raise IndexError("bit index %d out of range" %(bitoffset,))

		byte, bitofbyte = divmod(idx, 8)
		if ord(self.data[byte]) & (1 << (7 - bitofbyte)):
			return OneBit
		else:
			return ZeroBit

	def __getitem__(self, item):
		if isinstance(item, slice):
			return type(self)(str(self)[item])
		else:
			return self.getbit(item)

	def __nonzero__(self):
		for x in self.data:
			if x != 0:
				return True
		return False

class bit(varbit):
	def __new__(subtype, ob):
		if ob is ZeroBit or ob is False or ob == '0':
			return ZeroBit
		elif ob is OneBit or ob is True or ob == '1':
			return OneBit

		raise ValueError('unknown bit value %r, 0 or 1' %(ob,))

	def __nonzero__(self):
		return self is OneBit

	def __str__(self):
		return self is OneBit and '1' or '0'

ZeroBit = object.__new__(bit)
ZeroBit.data = b'\x00'
ZeroBit.bits = 1
OneBit = object.__new__(bit)
OneBit.data = b'\x80'
OneBit.bits = 1

# Geometric types

class point(tuple):
	"""
	A point; a pair of floating point numbers.
	"""
	x = property(fget = lambda s: s[0])
	y = property(fget = lambda s: s[1])

	def __new__(subtype, pair):
		return tuple.__new__(subtype, (float(pair[0]), float(pair[1])))

	def __repr__(self):
		return '%s.%s(%s)' %(
			type(self).__module__,
			type(self).__name__,
			tuple.__repr__(self),
		)

	def __str__(self):
		return tuple.__repr__(self)

	def __add__(self, ob):
		wx, wy = ob
		return type(self)((self[0] + wx, self[1] + wy))

	def __sub__(self, ob):
		wx, wy = ob
		return type(self)((self[0] - wx, self[1] - wy))

	def __mul__(self, ob):
		wx, wy = ob
		rx = (self[0] * wx) - (self[1] * wy)
		ry = (self[0] * wy) + (self[1] * wx)
		return type(self)((rx, ry))

	def __div__(self, ob):
		sx, sy = self
		wx, wy = ob
		div = (wx * wx) + (wy * wy)
		rx = ((sx * wx) + (sy * wy)) / div
		ry = ((wx * sy) + (wy * sx)) / div
		return type(self)((rx, ry))

	def distance(self, ob):
		wx, wy = ob
		dx = self[0] - float(wx)
		dy = self[1] - float(wy)
		return math.sqrt(dx**2 + dy**2)

class lseg(tuple):
	one = property(fget = lambda s: s[0])
	two = property(fget = lambda s: s[1])

	length = property(fget = lambda s: s[0].distance(s[1]))
	vertical = property(fget = lambda s: s[0][0] == s[1][0])
	horizontal = property(fget = lambda s: s[0][1] == s[1][1])
	slope = property(
		fget = lambda s: (s[1][1] - s[0][1]) / (s[1][0] - s[0][0])
	)
	center = property(
		fget = lambda s: point((
			(s[0][0] + s[1][0]) / 2.0,
			(s[0][1] + s[1][1]) / 2.0,
		))
	)

	def __new__(subtype, pair):
		p1, p2 = pair
		return tuple.__new__(subtype, (point(p1), point(p2)))

	def __repr__(self):
		# Avoid the point representation
		return '%s.%s(%s, %s)' %(
			type(self).__module__,
			type(self).__name__,
			tuple.__repr__(self[0]),
			tuple.__repr__(self[1]),
		)

	def __str__(self):
		return '[(%s,%s),(%s,%s)]' %(
			self[0][0],
			self[0][1],
			self[1][0],
			self[1][1],
		)

	def parallel(self, ob):
		return self.slope == type(self)(ob).slope

	def intersect(self, ob):
		raise NotImplementedError

	def perpendicular(self, ob):
		return (self.slope / type(self)(ob).slope) == -1.0

class box(tuple):
	"""
	A pair of points. One specifying the top-right point of the box; the other
	specifying the bottom-left. `high` being top-right; `low` being bottom-left.

	http://www.postgresql.org/docs/current/static/datatype-geometric.html

		>>> box(( (0,0), (-2, -2) ))
		postgresql.types.box(((0.0, 0.0), (-2.0, -2.0)))

	It will also relocate values to enforce the high-low expectation:

		>>> t.box(((-4,0),(-2,-3)))
		postgresql.types.box(((-2.0, 0.0), (-4.0, -3.0)))

	::
		
		                (-2, 0) `high`
		                   |
		                   |
		    (-4,-3) -------+-x
		     `low`         y

	This happens because ``-4`` is less than ``-2``; therefore the ``-4``
	belongs on the low point. This is consistent with what PostgreSQL does
	with its ``box`` type.
	"""
	high = property(fget = get0, doc = "high point of the box")
	low = property(fget = get1, doc = "low point of the box")
	center = property(
		fget = lambda s: point((
			(s[0][0] + s[1][0]) / 2.0,
			(s[0][1] + s[1][1]) / 2.0
		)),
		doc = "center of the box as a point"
	)

	def __new__(subtype, hl):
		if isinstance(hl, box):
			return hl
		one, two = hl
		if one[0] > two[0]:
			hx = one[0]
			lx = two[0]
		else:
			hx = two[0]
			lx = one[0]
		if one[1] > two[1]:
			hy = one[1]
			ly = two[1]
		else:
			hy = two[1]
			ly = one[1]
		return tuple.__new__(subtype, (point((hx, hy)), point((lx, ly))))

	def __repr__(self):
		return '%s.%s((%s, %s))' %(
			type(self).__module__,
			type(self).__name__,
			tuple.__repr__(self[0]),
			tuple.__repr__(self[1]),
		)

	def __str__(self):
		'Comma separate the box\'s points'
		return '%s,%s' %(self[0], self[1])

class circle(tuple):
	'type for PostgreSQL circles'
	center = property(fget = get0, doc = "center of the circle (point)")
	radius = property(fget = get1, doc = "radius of the circle (radius >= 0)")

	def __new__(subtype, pair):
		center, radius = pair
		if radius < 0:
			raise ValueError("radius is subzero")
		return tuple.__new__(subtype, (point(center), float(radius)))

	def __repr__(self):
		return '%s.%s((%s, %s))' %(
			type(self).__module__,
			type(self).__name__,
			tuple.__repr__(self[0]),
			repr(self[1])
		)

	def __str__(self):
		return '<%s,%s>' %(self[0], self[1])

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
	def detect_dimensions(hier):
		while type(hier) is list or type(hier) is Array:
			if type(hier) is Array:
				for x in hier.dimensions:
					yield x
				break
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
		dims = list(typ.detect_dimensions(nest,))
		if offset:
			dims = dims[:len(dims)-offset]
		return typ(list(typ.unroll_nest(nest, dims)), dims)

	def __new__(subtype, elements, dimensions = None, **kw):
		if dimensions is None:
			# there has to be at least one dimension, so
			# normalize it into a list.
			return subtype.from_nest(list(elements), **kw)
		rob = object.__new__(subtype)
		rob.elements = elements
		rob.dimensions = dimensions
		if dimensions:
			d1 = dimensions[0]
			elcount = 1
			for x in rob.dimensions:
				elcount *= x
		else:
			d1 = 0
			elcount = 0

		if len(rob.elements) != elcount:
			raise TypeError(
				"array element count inconsistent with dimensions"
			)
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

class Row(tuple):
	"Name addressable items tuple; mapping and sequence"
	@classmethod
	def from_mapping(typ, keymap, map):
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

	def __getitem__(self, i):
		if isinstance(i, (int, slice)):
			return tuple.__getitem__(self, i)
		idx = self.keymap[i]
		return tuple.__getitem__(self, idx)

	def get(self, i):
		if type(i) is int:
			l = len(self)
			if -l < i < l:
				return tuple.__getitem__(self, i)
		else:
			idx = self.keymap.get(i)
			if idx is not None:
				return tuple.__getitem__(self, idx)
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
	def column_names(self):
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

# Python Representations of PostgreSQL Types
oid_to_type = {
	VARBITOID: varbit,
	BITOID: bit,

	POINTOID: point,
	BOXOID: box,
	LSEGOID: lseg,
	CIRCLEOID: circle,

	BOOLOID: bool,

	VARCHAROID: str,
	TEXTOID: str,
	BPCHAROID: str,
	NAMEOID: str,

	XMLOID: etree.ElementTree,

	# This is *not* bpchar, the SQL CHARACTER type.
	CHAROID: bytes,
	BYTEAOID: bytes,

	INT2OID: int,
	INT4OID: int,
	INT8OID: int,
	NUMERICOID: decimal.Decimal,

	FLOAT4OID: float,
	FLOAT8OID: float,

	DATEOID: datetime.date,
	TIMESTAMPOID: datetime.datetime,
	TIMESTAMPTZOID: datetime.datetime,
	TIMEOID: datetime.time,
	TIMETZOID: datetime.time,

	# XXX: doesn't support months.
	INTERVALOID: datetime.timedelta,

	RECORDOID : tuple,
	ANYARRAYOID : list,
	ANYELEMENTOID : str,
	ANYENUMOID : int,
}
