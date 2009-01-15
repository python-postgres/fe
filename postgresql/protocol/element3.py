##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
'PQ version 3.0 elements'
import sys
import os
from struct import pack, unpack, Struct

StringFormat = b'\x00\x00'
BinaryFormat = b'\x00\x01'

byte = Struct("!B")
ushort = Struct("!H")
ulong = Struct("!L")

class Message(object):
	bytes_struct = Struct("!cL")
	__slots__ = ()
	def __repr__(self):
		return '%s.%s(%s)' %(
			type(self).__module__,
			type(self).__name__,
			', '.join([repr(getattr(self, x)) for x in self.__slots__])
		)

	def __eq__(self, ob):
		return isinstance(ob, type(self)) and self.type == ob.type and \
		not False in (
			getattr(self, x) == getattr(ob, x)
			for x in self.__slots__
		)

	def bytes(self):
		data = self.serialize()
		return self.bytes_struct.pack(self.type, len(data) + 4) + data

	def serialization(self, writer):
		writer(self.serialize())

	def parse(self, data):
		return self(data)
	parse = classmethod(parse)

class StringMessage(Message):
	"""
	A message based on a single string component.
	"""
	type = b''
	__slots__ = ('data')

	def __repr__(self):
		return '%s.%s(%s)' %(
			type(self).__module__,
			type(self).__name__,
			repr(self.data),
		)

	def __getitem__(self, i):
		return self.data.__getitem__(i)

	def __init__(self, data):
		self.data = data

	def serialize(self):
		return bytes(self.data) + b'\x00'

class TupleMessage(Message):
	"""
	A message who's data is based on a tuple structure.
	"""
	type = b''
	__slots__ = ('data',)

	def __repr__(self):
		return '%s.%s(%s)' %(
			type(self).__module__,
			type(self).__name__,
			repr(self.data)
		)

	def __iter__(self):
		return self.data.__iter__()

	def __len__(self):
		return self.data.__len__()

	def __getitem__(self, i):
		return self.data.__getitem__(i)

	def __init__(self, data):
		self.data = tuple(data)

class Void(Message):
	"""
	An absolutely empty message. When serialized, it always yields an empty
	string.
	"""
	type = b''
	__slots__ = ()

	def bytes(self):
		return b''

	def serialize(self):
		return b''
	
	def __new__(self, *args, **kw):
		return VoidMessage
VoidMessage = Message.__new__(Void)

def dict_message_repr(self):
	return '%s.%s(**%s)' %(
		type(self).__module__,
		type(self).__name__,
		dict.__repr__(self)
	)

class WireMessage(Message):
	def __init__(self, typ_data):
		type = bytes(type)[0]
		self.type = typ_data[0]
		self.data = typ_data[1]

	def serialize(self):
		return self[1]

	def parse(self, data):
		if ulong.unpack(data[1:5])[0] != len(data) - 1:
			raise ValueError(
				"invalid wire message where data is %d bytes and " \
				"internal size stamp is %d bytes" %(
					len(data), ulong.unpack(data[1:5])[0] + 1
				)
			)
		return self((data[0], data[5:]))
	parse = classmethod(parse)

class EmptyMessage(Message):
	'An abstract message that is always empty'
	__slots__ = ()
	type = b''

	def serialize(self):
		return b''

	def parse(self, data):
		if data != b'':
			raise ValueError("empty message(%r) had data" %(self.type,))
		return self.SingleInstance
	parse = classmethod(parse)

class Notify(Message):
	'Asynchronous notification message'
	type = b'A'
	__slots__ = ('pid', 'relation', 'parameter')

	def __init__(self, pid, relation, parameter = b''):
		self.pid = pid
		self.relation = relation
		self.parameter = parameter

	def serialize(self):
		return ulong.pack(self.pid) + \
			self.relation + b'\x00' + \
			self.parameter + b'\x00'

	def parse(self, data):
		pid = ulong.unpack(data[0:4])[0]
		relname, param, nothing = data[4:].split(b'\x00', 2)
		return self(pid, relname, param)
	parse = classmethod(parse)

class ShowOption(Message):
	"""ShowOption(name, value)
	GUC variable information from backend"""
	type = b'S'
	__slots__ = ('name', 'value')

	def __init__(self, name, value):
		self.name = name
		self.value = value

	def serialize(self):
		return self.name + b'\x00' + self.value + b'\x00'

	def parse(self, data):
		return self(*(data.split(b'\x00', 2)[0:2]))
	parse = classmethod(parse)

class Complete(StringMessage):
	'Complete command'
	type = b'C'
	__slots__ = ()

	def parse(self, data):
		return self(data[:-1])
	parse = classmethod(parse)

class Null(EmptyMessage):
	'Null command'
	type = b'I'
	__slots__ = ()
	def __new__(subtype):
		return NullMessage
NullMessage = EmptyMessage.__new__(Null)
Null.SingleInstance = NullMessage

class NoData(EmptyMessage):
	'Null command'
	type = b'n'
	__slots__ = ()
	def __new__(subtype):
		return NoDataMessage
NoDataMessage = EmptyMessage.__new__(NoData)
NoData.SingleInstance = NoDataMessage

class ParseComplete(EmptyMessage):
	'Parse reaction'
	type = b'1'
	__slots__ = ()
	def __new__(subtype):
		return ParseCompleteMessage
ParseCompleteMessage = EmptyMessage.__new__(ParseComplete)
ParseComplete.SingleInstance = ParseCompleteMessage

class BindComplete(EmptyMessage):
	'Bind reaction'
	type = b'2'
	__slots__ = ()
	def __new__(subtype):
		return BindCompleteMessage
BindCompleteMessage = EmptyMessage.__new__(BindComplete)
BindComplete.SingleInstance = BindCompleteMessage

class CloseComplete(EmptyMessage):
	'Close statement or Portal'
	type = b'3'
	__slots__ = ()
	def __new__(subtype):
		return CloseCompleteMessage
CloseCompleteMessage = EmptyMessage.__new__(CloseComplete)
CloseComplete.SingleInstance = CloseCompleteMessage

class Suspension(EmptyMessage):
	'Portal was suspended, more tuples for reading'
	type = b's'
	__slots__ = ()
	def __new__(subtype):
		return SuspensionMessage
SuspensionMessage = EmptyMessage.__new__(Suspension)
Suspension.SingleInstance = SuspensionMessage

class Ready(Message):
	'Ready for new query'
	type = b'Z'
	__slots__ = ('xact_state',)

	def __init__(self, data):
		self.xact_state = data

	def serialize(self):
		return self.xact_state

class Notice(Message, dict):
	"""Notification message"""
	type = b'N'
	_dtm = {
		b'S' : 'severity',
		b'C' : 'code',
		b'M' : 'message',
		b'D' : 'detail',
		b'H' : 'hint',
		b'W' : 'context',
		b'P' : 'position',
		b'p' : 'internal_position',
		b'q' : 'internal_query',
		b'F' : 'file',
		b'L' : 'line',
		b'R' : 'function',
	}
	__slots__ = ()

	def __init__(self,
		severity = None,
		message = None,
		code = None,
		detail = None,
		hint = None,
		position = None,
		internal_position = None,
		internal_query = None,
		context = None,
		file = None,
		line = None,
		function = None
	):
		for (k, v) in locals().items():
			if v not in (self, None):
				self[k] = v
	__repr__ = dict_message_repr

	def serialize(self):
		return b''.join([
			k + self[v] + b'\x00'
			for k, v in self._dtm.items()
			if self.get(v) is not None
		]) + b'\x00'

	def parse(self, data):
		kw = {}
		for frag in data.split(b'\x00'):
			if frag:
				kw[self._dtm[frag[0:1]]] = frag[1:]
		return self(**kw)
	parse = classmethod(parse)

class Error(Notice):
	"""Incoming error"""
	type = b'E'
	__slots__ = ()

class FunctionResult(Message):
	"""Function result value"""
	type = b'V'
	__slots__ = ('result',)

	def __init__(self, datum):
		self.result = datum

	def serialize(self):
		return self.result is None and b'\xff\xff\xff\xff' or \
			ulong.pack(len(self.result)) + self.result
	
	def parse(self, data):
		if data == b'\xff\xff\xff\xff':
			return self(None)
		size = ulong.unpack(data[0:4])[0]
		data = data[4:]
		if size != len(data):
			raise ValueError(
				"data length(%d) is not equal to the specified message size(%d)" %(
					len(data), size
				)
			)
		return self(data)
	parse = classmethod(parse)

class AttributeTypes(TupleMessage):
	"""Tuple attribute types"""
	type = b't'
	__slots__ = ()

	def serialize(self):
		return ushort.pack(len(self)) + b''.join([ulong.pack(x) for x in self])

	def parse(self, data):
		ac = ushort.unpack(data[0:2])[0]
		args = data[2:]
		if len(args) != ac * 4:
			raise ValueError("invalid argument type data size")
		return self(unpack('!%dL'%(ac,), args))
	parse = classmethod(parse)

class TupleDescriptor(TupleMessage):
	"""Tuple description"""
	type = b'T'
	struct = Struct("!LhLhlh")
	__slots__ = ()

	@property
	def attribute_map(self):
		"""
		create a dictionary from a pq desc that maps attribute names
		to their index
		"""
		return {
			self[x][0] : x for x in range(len(self))
		}

	def keys(self):
		return [x[0] for x in self]

	def serialize(self):
		return ushort.pack(len(self)) + b''.join([
			x[0] + b'\x00' + self.struct.pack(*x[1:])
			for x in self
		])

	def parse(self, data):
		ac = ushort.unpack(data[0:2])[0]
		atts = []
		data = data[2:]
		ca = 0
		while ca < ac:
			# End Of Attribute Name
			eoan = data.index(b'\x00')
			name = data[0:eoan]
			data = data[eoan+1:]
			# name, relationId, columnNumber, typeId, typlen, typmod, format
			atts.append((name,) + self.struct.unpack(data[0:18]))
			data = data[18:]
			ca += 1
		return self(atts)
	parse = classmethod(parse)

class Tuple(TupleMessage):
	"""Incoming tuple"""
	type = b'D'
	__slots__ = ()

	def serialize(self):
		return ushort.pack(len(self)) + b''.join([
			x is None and b'\xff\xff\xff\xff' or ulong.pack(len(x)) + bytes(x)
			for x in self
		])

	def parse(self, data):
		natts = ushort.unpack(data[0:2])[0]
		atts = list()
		offset = 2

		while natts > 0:
			alo = offset
			offset += 4
			size = data[alo:offset]
			if size == b'\xff\xff\xff\xff':
				att = None
			else:
				al = ulong.unpack(size)[0]
				ao = offset
				offset = ao + al
				att = data[ao:offset]
			atts.append(att)
			natts -= 1
		return self(atts)
	parse = classmethod(parse)

class KillInformation(Message):
	'Backend cancellation information'
	type = b'K'
	struct = Struct("!LL")
	__slots__ = ('pid', 'key')

	def __init__(self, pid, key):
		self.pid = pid
		self.key = key

	def serialize(self):
		return self.struct.pack(self.pid, self.key)

	def parse(self, data):
		return self(*self.struct.unpack(data))
	parse = classmethod(parse)

class CancelQuery(KillInformation):
	'Abort the query in the specified backend'
	type = b''
	from .version import CancelRequestCode as version
	packed_version = version.bytes()
	__slots__ = ('pid', 'key')

	def serialize(self):
		return pack("!HHLL",
			self.version[0], self.version[1],
			self.pid, self.key
		)

	def bytes(self):
		data = self.serialize()
		return ulong.pack(len(data)) + data

	def parse(self, data):
		if data[0:4] != self.packed_version:
			raise ValueError("invalid cancel query code")
		return self(*unpack("!xxxxLL", data))
	parse = classmethod(parse)

class NegotiateSSL(Message):
	"Discover backend's SSL support"
	type = b''
	from .version import NegotiateSSLCode as version
	packed_version = version.bytes()
	__slots__ = ()

	def __new__(subtype):
		return NegotiateSSLMessage

	def bytes(self):
		data = self.serialize()
		return ulong.pack(len(data) + 4) + data

	def serialize(self):
		return self.packed_version

	def parse(self, data):
		if data != self.packed_version:
			raise ValueError("invalid SSL Negotiation code")
		return NegotiateSSLMessage
	parse = classmethod(parse)
NegotiateSSLMessage = Message.__new__(NegotiateSSL)

class Startup(Message, dict):
	"""
	Initiate a connection using the given keywords.
	"""
	type = b''
	from postgresql.protocol.version import V3_0 as version
	packed_version = version.bytes()
	__slots__ = ()
	__repr__ = dict_message_repr

	def serialize(self):
		return self.packed_version + b''.join([
			k + b'\x00' + v + b'\x00'
			for k, v in self.items()
			if v is not None
		]) + b'\x00'

	def bytes(self):
		data = self.serialize()
		return ulong.pack(len(data) + 4) + data

	def parse(self, data):
		if data[0:4] != self.version.bytes():
			raise ValueError("invalid version code {1}".format(repr(data[0:4])))
		kw = dict()
		key = None
		for value in data[4:].split(b'\x00')[:-2]:
			if key is None:
				key = value
				continue
			kw[key] = value
			key = None
		return self(kw)
	parse = classmethod(parse)


AuthRequest_OK = 0
AuthRequest_Cleartext = 3
AuthRequest_Password = AuthRequest_Cleartext
AuthRequest_Crypt = 4
AuthRequest_MD5 = 5

# Unsupported by pg_protocol.
AuthRequest_KRB4 = 1
AuthRequest_KRB5 = 2
AuthRequest_SCMC = 6
AuthRequest_SSPI = 9
AuthRequest_GSS = 7
AuthRequest_GSSContinue = 8

AuthNameMap = {
	AuthRequest_Password : 'Cleartext',
	AuthRequest_Crypt : 'Crypt',
	AuthRequest_MD5 : 'MD5',

	AuthRequest_KRB4 : 'Kerberos4',
	AuthRequest_KRB5 : 'Kerberos5',
	AuthRequest_SCMC : 'SCM Credential',
	AuthRequest_SSPI : 'SSPI',
	AuthRequest_GSS : 'GSS',
	AuthRequest_GSSContinue : 'GSSContinue',
}

class Authentication(Message):
	"""Authentication(request, salt)"""
	type = b'R'
	__slots__ = ('request', 'salt')

	def __init__(self, request, salt):
		self.request = request
		self.salt = salt

	def serialize(self):
		return ulong.pack(self.request) + self.salt

	def parse(subtype, data):
		return subtype(ulong.unpack(data[0:4])[0], data[4:])
	parse = classmethod(parse)

class Password(StringMessage):
	'Password supplement'
	type = b'p'
	__slots__ = ()

	def serialize(self):
		return self.data

class Disconnect(EmptyMessage):
	'Close the connection'
	type = b'X'
	__slots__ = ()
	def __new__(subtype):
		return DisconnectMessage
DisconnectMessage = EmptyMessage.__new__(Disconnect)
Disconnect.SingleInstance = DisconnectMessage

class Flush(EmptyMessage):
	'Flush'
	type = b'H'
	__slots__ = ()
	def __new__(subtype):
		return FlushMessage
FlushMessage = EmptyMessage.__new__(Flush)
Flush.SingleInstance = FlushMessage

class Synchronize(EmptyMessage):
	'Synchronize'
	type = b'S'
	__slots__ = ()
	def __new__(subtype):
		return SynchronizeMessage
SynchronizeMessage = EmptyMessage.__new__(Synchronize)
Synchronize.SingleInstance = SynchronizeMessage

class Query(StringMessage):
	"""Execute the query with the given arguments"""
	type = b'Q'
	__slots__ = ()

	def parse(self, data):
		return self(data[0:-1])
	parse = classmethod(parse)

class Parse(Message):
	"""Parse a query with the specified argument types"""
	type = b'P'
	__slots__ = ('name', 'statement', 'argtypes')

	def __init__(self, name, statement, argtypes):
		self.name = name
		self.statement = statement
		self.argtypes = argtypes

	def parse(self, data):
		name, statement, args = data.split(b'\x00', 2)
		ac = ushort.unpack(args[0:2])[0]
		args = args[2:]
		if len(args) != ac * 4:
			raise ValueError("invalid argument type data")
		at = unpack('!%dL'%(ac,), args)
		return self(name, statement, at)
	parse = classmethod(parse)

	def serialize(self):
		ac = ushort.pack(len(self.argtypes))
		return self.name + b'\x00' + self.statement + b'\x00' + ac + b''.join([
			ulong.pack(x) for x in self.argtypes
		])

class Bind(Message):
	"""
	Bind a parsed statement with the given arguments to a Portal

	Bind(
		name,      # Portal/Cursor identifier
		statement, # Prepared Statement name/identifier
		aformats,  # Argument formats; Sequence of BinaryFormat or StringFormat.
		arguments, # Argument data; Sequence of None or argument data(str).
		rformats,  # Result formats; Sequence of BinaryFormat or StringFormat.
	)
	"""
	type = b'B'
	__slots__ = ('name', 'statement', 'aformats', 'arguments', 'rformats')

	def __init__(self, name, statement, aformats, arguments, rformats):
		self.name = name
		self.statement = statement
		self.aformats = aformats
		self.arguments = arguments
		self.rformats = rformats

	def serialize(self):
		args = self.arguments
		ac = ushort.pack(len(args))
		ad = b''.join([
			b'\xff\xff\xff\xff'
			if x is None
			else (ulong.pack(len(x)) + x)
			for x in args
		])
		rfc = ushort.pack(len(self.rformats))
		return \
			self.name + b'\x00' + self.statement + b'\x00' + \
			ac + b''.join(self.aformats) + ac + ad + rfc + \
			b''.join(self.rformats)

	def parse(subtype, message_data):
		name, statement, data = message_data.split(b'\x00', 2)
		ac = ushort.unpack(data[:2])[0]
		offset = 2 + (2 * ac)
		aformats = unpack(("2s" * ac), data[2:offset])

		natts = ushort.unpack(data[offset:offset+2])[0]
		args = list()
		offset += 2

		while natts > 0:
			alo = offset
			offset += 4
			size = data[alo:offset]
			if size == b'\xff\xff\xff\xff':
				att = None
			else:
				al = ulong.unpack(size)[0]
				ao = offset
				offset = ao + al
				att = data[ao:offset]
			args.append(att)
			natts -= 1

		rfc = ushort.unpack(data[offset:offset+2])[0]
		ao = offset + 2
		offset = ao + (2 * rfc)
		rformats = unpack(("2s" * rfc), data[ao:offset])

		return subtype(name, statement, aformats, args, rformats)
	parse = classmethod(parse)

class Execute(Message):
	"""Fetch results from the specified Portal"""
	type = b'E'
	__slots__ = ('name', 'max')

	def __init__(self, name, max = 0):
		self.name = name
		self.max = max

	def serialize(self):
		return self.name + pack("!BL", 0, self.max)
	
	def parse(self, data):
		name, max = data.split(b'\x00', 1)
		return self(name, ulong.unpack(max)[0])
	parse = classmethod(parse)

class Describe(StringMessage):
	"""Describe a Portal or Prepared Statement"""
	type = b'D'
	__slots__ = ()

	def serialize(self):
		return self.subtype + self.data + b'\x00'

	def parse(subtype, data):
		if data[0] != subtype.subtype:
			raise ValueError(
				"invalid Describe message subtype, %r; expected %r" %(
					subtype.subtype, data[0]
				)
			)
		return subtype(data[1:-1])
	parse = classmethod(parse)

class DescribeStatement(Describe):
	subtype = b'S'
	__slots__ = ()

class DescribePortal(Describe):
	subtype = b'P'
	__slots__ = ()

class Close(StringMessage):
	"""Generic Close"""
	type = b'C'
	__slots__ = ()

	def serialize(self):
		return self.subtype + self.data + b'\x00'

	def parse(subtype, data):
		if data[0] != subtype.subtype:
			raise ValueError(
				"invalid Close message subtype, %r; expected %r" %(
					subtype.subtype, data[0]
				)
			)
		return subtype(data[1:-1])
	parse = classmethod(parse)

class CloseStatement(Close):
	"""Close the specified Statement"""
	subtype = b'S'
	__slots__ = ()

class ClosePortal(Close):
	"""Close the specified Portal"""
	subtype = b'P'
	__slots__ = ()

class Function(Message):
	"""Execute the specified function with the given arguments"""
	type = b'F'
	__slots__ = ('oid', 'aformats', 'arguments', 'rformat')

	def __init__(self, oid, aformats, args, rformat = StringFormat):
		self.oid = oid
		self.aformats = aformats
		self.arguments = args
		self.rformat = rformat

	def serialize(self):
		ac = ushort.pack(len(self.arguments))
		return ulong.pack(self.oid) + \
			ac + b''.join(self.aformats) + \
			ac + b''.join([
				(x is None) and b'\xff\xff\xff\xff' or ulong.pack(len(x)) + x
				for x in self.arguments
			]) + self.rformat

	def parse(self, data):
		oid = ulong.unpack(data[0:4])[0]

		ac = ushort.unpack(data[4:6])[0]
		offset = 6 + (2 * ac)
		aformats = unpack(("2s" * ac), data[6:offset])

		natts = ushort.unpack(data[offset:offset+2])[0]
		args = list()
		offset += 2

		while natts > 0:
			alo = offset
			offset += 4
			size = data[alo:offset]
			if size == b'\xff\xff\xff\xff':
				att = None
			else:
				al = ulong.unpack(size)[0]
				ao = offset
				offset = ao + al
				att = data[ao:offset]
			args.append(att)
			natts -= 1

		return self(oid, aformats, args, data[offset:])
	parse = classmethod(parse)

class CopyBegin(Message):
	type = None
	struct = Struct("!BH")
	__slots__ = ('format', 'formats')

	def __init__(self, format, formats):
		self.format = format
		self.formats = formats

	def serialize(self):
		return self.struct.pack(self.format, len(self.formats)) + b''.join([
			ushort.pack(x) for x in self.formats
		])

	def parse(subtype, data):
		format, natts = self.struct.unpack(data[:3])
		formats_str = data[3:]
		if len(formats_str) != natts * 2:
			raise ValueError("number of formats and data do not match up")
		return subtype(format, [
			ushort.unpack(formats_str[x:x+2])[0] for x in range(0, natts * 2, 2)
		])
	parse = classmethod(parse)

class CopyToBegin(CopyBegin):
	"""Begin copying to"""
	type = b'H'
	__slots__ = ('format', 'formats')

class CopyFromBegin(CopyBegin):
	"""Begin copying from"""
	type = b'G'
	__slots__ = ('format', 'formats')

class CopyData(Message):
	type = b'd'
	__slots__ = ('data',)

	def __init__(self, data):
		self.data = bytes(data)

	def serialize(self):
		return self.data

	def parse(subtype, data):
		return subtype(data)
	parse = classmethod(parse)

class CopyFail(StringMessage):
	type = b'f'
	__slots__ = ()

	def parse(self, data):
		return self(data[:-1])
	parse = classmethod(parse)

class CopyDone(EmptyMessage):
	type = b'c'
	__slots__ = ()
	def __new__(subtype):
		return CopyDoneMessage
CopyDoneMessage = EmptyMessage.__new__(CopyDone)
