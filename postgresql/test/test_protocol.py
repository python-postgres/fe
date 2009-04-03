##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import unittest
import struct
import decimal
from ..protocol import element3 as e3
from ..protocol import xact3 as x3
from ..protocol import pbuffer as p_buffer_module
from ..protocol import typstruct as pg_typstruct
from ..protocol import typio as pg_typio
from .. import types as pg_types

try:
	from ..protocol import cbuffer as c_buffer_module
except ImportError:
	c_buffer_module = None

class buffer_test(object):
	def setUp(self):
		self.buffer = self.bufferclass()

	def testMultiByteMessage(self):
		b = self.buffer
		b.write(b's')
		self.failUnless(b.next_message() is None)
		b.write(b'\x00\x00')
		self.failUnless(b.next_message() is None)
		b.write(b'\x00\x10')
		self.failUnless(b.next_message() is None)
		data = b'twelve_chars'
		b.write(data)
		self.failUnless(b.next_message() == (b's', data))

	def testSingleByteMessage(self):
		b = self.buffer
		b.write(b's')
		self.failUnless(b.next_message() is None)
		b.write(b'\x00')
		self.failUnless(b.next_message() is None)
		b.write(b'\x00\x00\x05')
		self.failUnless(b.next_message() is None)
		b.write(b'b')
		self.failUnless(b.next_message() == (b's', b'b'))

	def testEmptyMessage(self):
		b = self.buffer
		b.write(b'x')
		self.failUnless(b.next_message() is None)
		b.write(b'\x00\x00\x00')
		self.failUnless(b.next_message() is None)
		b.write(b'\x04')
		self.failUnless(b.next_message() == (b'x', b''))

	def testInvalidLength(self):
		b = self.buffer
		b.write(b'y\x00\x00\x00\x03')
		self.failUnlessRaises(ValueError, b.next_message,)

	def testRemainder(self):
		b = self.buffer
		b.write(b'r\x00\x00\x00\x05Aremainder')
		self.failUnless(b.next_message() == (b'r', b'A'))

	def testLarge(self):
		b = self.buffer
		factor = 1024
		r = 10000
		b.write(b'X' + struct.pack("!L", factor * r + 4))
		segment = b'\x00' * factor
		for x in range(r-1):
			b.write(segment)
		b.write(segment)
		msg = b.next_message()
		self.failUnless(msg is not None)
		self.failUnless(msg[0] == b'X')

if c_buffer_module is not None:
	class c_buffer(buffer_test, unittest.TestCase):
		bufferclass = c_buffer_module.pq_message_stream

class p_buffer(buffer_test, unittest.TestCase):
	bufferclass = p_buffer_module.pq_message_stream

##
# element3 tests
##

message_samples = [
	e3.VoidMessage,
	e3.Startup(**{
		b'user' : b'jwp',
		b'database' : b'template1',
		b'options' : b'-f',
	}),
	e3.Notice(
		severity = b'FATAL',
		message = b'a descriptive message',
		code = b'FIVEC',
		detail = b'bleh',
		hint = b'dont spit into the fan',
	),
	e3.Notify(123, b'wood_table'),
	e3.KillInformation(19320, 589483),
	e3.ShowOption(b'foo', b'bar'),
	e3.Authentication(4, b'salt'),
	e3.Complete(b'SELECT'),
	e3.Ready(b'I'),
	e3.CancelRequest(4123, 14252),
	e3.NegotiateSSL(),
	e3.Password(b'ckr4t'),
	e3.AttributeTypes(()),
	e3.AttributeTypes(
		(123,) * 1
	),
	e3.AttributeTypes(
		(123,0) * 1
	),
	e3.AttributeTypes(
		(123,0) * 2
	),
	e3.AttributeTypes(
		(123,0) * 4
	),
	e3.TupleDescriptor(()),
	e3.TupleDescriptor((
		(b'name', 123, 1, 1, 0, 0, 1,),
	)),
	e3.TupleDescriptor((
		(b'name', 123, 1, 2, 0, 0, 1,),
	) * 2),
	e3.TupleDescriptor((
		(b'name', 123, 1, 2, 1, 0, 1,),
	) * 3),
	e3.TupleDescriptor((
		(b'name', 123, 1, 1, 0, 0, 1,),
	) * 1000),
	e3.Tuple([]),
	e3.Tuple([b'foo',]),
	e3.Tuple([None]),
	e3.Tuple([b'foo',b'bar']),
	e3.Tuple([None, None]),
	e3.Tuple([None, b'foo', None]),
	e3.Tuple([b'bar', None, b'foo', None, b'bleh']),
	e3.Tuple([b'foo', b'bar'] * 100),
	e3.Tuple([None] * 100),
	e3.Query(b'select * from u'),
	e3.Parse(b'statement_id', b'query', (123, 0)),
	e3.Parse(b'statement_id', b'query', (123,)),
	e3.Parse(b'statement_id', b'query', ()),
	e3.Bind(b'portal_id', b'statement_id',
		(b'tt',b'\x00\x00'),
		[b'data',None], (b'ff',b'xx')),
	e3.Bind(b'portal_id', b'statement_id', (b'tt',), [None], (b'xx',)),
	e3.Bind(b'portal_id', b'statement_id', (b'ff',), [b'data'], ()),
	e3.Bind(b'portal_id', b'statement_id', (), [], (b'xx',)),
	e3.Bind(b'portal_id', b'statement_id', (), [], ()),
	e3.Execute(b'portal_id', 500),
	e3.Execute(b'portal_id', 0),
	e3.DescribeStatement(b'statement_id'),
	e3.DescribePortal(b'portal_id'),
	e3.CloseStatement(b'statement_id'),
	e3.ClosePortal(b'portal_id'),
	e3.Function(123, (), [], b'xx'),
	e3.Function(321, (b'tt',), [b'foo'], b'xx'),
	e3.Function(321, (b'tt',), [None], b'xx'),
	e3.Function(321, (b'aa', b'aa'), [None,b'a' * 200], b'xx'),
	e3.FunctionResult(b''),
	e3.FunctionResult(b'foobar'),
	e3.FunctionResult(None),
	e3.CopyToBegin(123, [321,123]),
	e3.CopyToBegin(0, [10,]),
	e3.CopyToBegin(123, []),
	e3.CopyFromBegin(123, [321,123]),
	e3.CopyFromBegin(0, [10]),
	e3.CopyFromBegin(123, []),
	e3.CopyData(b''),
	e3.CopyData(b'foo'),
	e3.CopyData(b'a' * 2048),
	e3.CopyFail(b''),
	e3.CopyFail(b'iiieeeeee!'),
]

class test_element3(unittest.TestCase):
	def testSerializeParseConsistency(self):
		for msg in message_samples:
			smsg = msg.serialize()
			self.failUnlessEqual(msg, msg.parse(smsg))

	def testEmptyMessages(self):
		for x in e3.__dict__.values():
			if isinstance(x, e3.EmptyMessage):
				xtype = type(x)
				self.failUnless(x is xtype())

##
# xact3 tests
##

xact_samples = [
	# Simple contrived exchange.
	(
		(
			e3.Query(b"COMPLETE"),
		), (
			e3.Complete(b'COMPLETE'),
			e3.Ready(b'I'),
		)
	),
	(
		(
			e3.Query(b"ROW DATA"),
		), (
			e3.TupleDescriptor((
				(b'foo', 1, 1, 1, 1, 1, 1),
				(b'bar', 1, 2, 1, 1, 1, 1),
			)),
			e3.Tuple((b'lame', b'lame')),
			e3.Complete(b'COMPLETE'),
			e3.Ready(b'I'),
		)
	),
	(
		(
			e3.Query(b"ROW DATA"),
		), (
			e3.TupleDescriptor((
				(b'foo', 1, 1, 1, 1, 1, 1),
				(b'bar', 1, 2, 1, 1, 1, 1),
			)),
			e3.Tuple((b'lame', b'lame')),
			e3.Tuple((b'lame', b'lame')),
			e3.Tuple((b'lame', b'lame')),
			e3.Tuple((b'lame', b'lame')),
			e3.Ready(b'I'),
		)
	),
	(
		(
			e3.Query(b"NULL"),
		), (
			e3.Null(),
			e3.Ready(b'I'),
		)
	),
	(
		(
			e3.Query(b"COPY TO"),
		), (
			e3.CopyToBegin(1, [1,2]),
			e3.CopyData(b'row1'),
			e3.CopyData(b'row2'),
			e3.CopyDone(),
			e3.Complete(b'COPY TO'),
			e3.Ready(b'I'),
		)
	),
	(
		(
			e3.Function(1, [b''], [b''], 1),
		), (
			e3.FunctionResult(b'foo'),
			e3.Ready(b'I'),
		)
	),
	(
		(
			e3.Parse(b"NAME", b"SQL", ()),
		), (
			e3.ParseComplete(),
		)
	),
	(
		(
			e3.Bind(b"NAME", b"STATEMENT_ID", (), (), ()),
		), (
			e3.BindComplete(),
		)
	),
	(
		(
			e3.Parse(b"NAME", b"SQL", ()),
			e3.Bind(b"NAME", b"STATEMENT_ID", (), (), ()),
		), (
			e3.ParseComplete(),
			e3.BindComplete(),
		)
	),
	(
		(
			e3.Describe(b"STATEMENT_ID"),
		), (
			e3.AttributeTypes(()),
			e3.NoData(),
		)
	),
	(
		(
			e3.Describe(b"STATEMENT_ID"),
		), (
			e3.AttributeTypes(()),
			e3.TupleDescriptor(()),
		)
	),
	(
		(
			e3.CloseStatement(b"foo"),
		), (
			e3.CloseComplete(),
		),
	),
	(
		(
			e3.ClosePortal(b"foo"),
		), (
			e3.CloseComplete(),
		),
	),
	(
		(
			e3.Synchronize(),
		), (
			e3.Ready(b'I'),
		),
	),
]

class test_xact3(unittest.TestCase):
	def testTransactionSamplesAll(self):
		for xcmd, xres in xact_samples:
			x = x3.Instruction(xcmd)
			r = tuple([(y.type, y.serialize()) for y in xres])
			x.state[1]()
			self.failUnlessEqual(x.messages, ())
			x.state[1](r)
			self.failUnlessEqual(x.state, x3.Complete)
			rec = []
			for y in x.completed:
				for z in y[1]:
					if type(z) is type(b''):
						z = e3.CopyData(z)
					rec.append(z)
			self.failUnlessEqual(xres, tuple(rec))


# this must pack to that, and
# that must unpack to this
expectation_samples = {
	pg_types.BOOLOID : [
		(True, b'\x01'),
		(False, b'\x00')
	],

	pg_types.INT2OID : [
		(0, b'\x00\x00'),
		(1, b'\x00\x01'),
		(2, b'\x00\x02'),
		(0x0f, b'\x00\x0f'),
		(0xf00, b'\x0f\x00'),
		(0x7fff, b'\x7f\xff'),
		(-0x8000, b'\x80\x00'),
		(-1, b'\xff\xff'),
		(-2, b'\xff\xfe'),
		(-3, b'\xff\xfd'),
	],

	pg_types.INT4OID : [
		(0, b'\x00\x00\x00\x00'),
		(1, b'\x00\x00\x00\x01'),
		(2, b'\x00\x00\x00\x02'),
		(0x0f, b'\x00\x00\x00\x0f'),
		(0x7fff, b'\x00\x00\x7f\xff'),
		(-0x8000, b'\xff\xff\x80\x00'),
		(0x7fffffff, b'\x7f\xff\xff\xff'),
		(-0x80000000, b'\x80\x00\x00\x00'),
		(-1, b'\xff\xff\xff\xff'),
		(-2, b'\xff\xff\xff\xfe'),
		(-3, b'\xff\xff\xff\xfd'),
	],

	pg_types.INT8OID : [
		(0, b'\x00\x00\x00\x00\x00\x00\x00\x00'),
		(1, b'\x00\x00\x00\x00\x00\x00\x00\x01'),
		(2, b'\x00\x00\x00\x00\x00\x00\x00\x02'),
		(0x0f, b'\x00\x00\x00\x00\x00\x00\x00\x0f'),
		(0x7fffffff, b'\x00\x00\x00\x00\x7f\xff\xff\xff'),
		(0x80000000, b'\x00\x00\x00\x00\x80\x00\x00\x00'),
		(-0x80000000, b'\xff\xff\xff\xff\x80\x00\x00\x00'),
		(-1, b'\xff\xff\xff\xff\xff\xff\xff\xff'),
		(-2, b'\xff\xff\xff\xff\xff\xff\xff\xfe'),
		(-3, b'\xff\xff\xff\xff\xff\xff\xff\xfd'),
	],

	pg_types.NUMERICOID : [
		(((0,0,0,0),[]), b'\x00'*2*4),
		(((0,0,0,0),[1]), b'\x00'*2*4 + b'\x00\x01'),
		(((1,0,0,0),[1]), b'\x00\x01' + b'\x00'*2*3 + b'\x00\x01'),
		(((1,1,1,1),[1]), b'\x00\x01'*4 + b'\x00\x01'),
		(((1,1,1,1),[1,2]), b'\x00\x01'*4 + b'\x00\x01\x00\x02'),
		(((1,1,1,1),[1,2,3]), b'\x00\x01'*4 + b'\x00\x01\x00\x02\x00\x03'),
	],

	pg_types.BITOID : [
		(False, b'\x00\x00\x00\x01\x00'),
		(True, b'\x00\x00\x00\x01\x01'),
	],

	pg_types.VARBITOID : [
		((0, b'\x00'), b'\x00\x00\x00\x00\x00'),
		((1, b'\x01'), b'\x00\x00\x00\x01\x01'),
		((1, b'\x00'), b'\x00\x00\x00\x01\x00'),
		((2, b'\x00'), b'\x00\x00\x00\x02\x00'),
		((3, b'\x00'), b'\x00\x00\x00\x03\x00'),
		((9, b'\x00\x00'), b'\x00\x00\x00\x09\x00\x00'),
		# More data than necessary, we allow this.
		# Let the user do the necessary check if the cost is worth the benefit.
		((9, b'\x00\x00\x00'), b'\x00\x00\x00\x09\x00\x00\x00'),
	],

	pg_types.BYTEAOID : [
		(b'foo', b'foo'),
		(b'bar', b'bar'),
		(b'\x00', b'\x00'),
		(b'\x01', b'\x01'),
	],

	pg_types.CHAROID : [
		(b'a', b'a'),
		(b'b', b'b'),
		(b'\x00', b'\x00'),
	],

	pg_types.POINTOID : [
		((1.0, 1.0), b'?\xf0\x00\x00\x00\x00\x00\x00?\xf0\x00\x00\x00\x00\x00\x00'),
		((2.0, 2.0), b'@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00'),
		((-1.0, -1.0),
			b'\xbf\xf0\x00\x00\x00\x00\x00\x00\xbf\xf0\x00\x00\x00\x00\x00\x00'),
	],

	pg_types.CIRCLEOID : [
		((1.0, 1.0, 1.0),
			b'?\xf0\x00\x00\x00\x00\x00\x00?\xf0\x00\x00' \
			b'\x00\x00\x00\x00?\xf0\x00\x00\x00\x00\x00\x00'),
		((2.0, 2.0, 2.0),
			b'@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00' \
			b'\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00'),
	],

	pg_types.RECORDOID : [
		([], b'\x00\x00\x00\x00'),
		([(0,b'foo')], b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x03foo'),
		([(0,None)], b'\x00\x00\x00\x01\x00\x00\x00\x00\xff\xff\xff\xff'),
		([(15,None)], b'\x00\x00\x00\x01\x00\x00\x00\x0f\xff\xff\xff\xff'),
		([(0xffffffff,None)], b'\x00\x00\x00\x01\xff\xff\xff\xff\xff\xff\xff\xff'),
		([(0,None), (1,b'some')],
		 b'\x00\x00\x00\x02\x00\x00\x00\x00\xff\xff\xff\xff' \
		 b'\x00\x00\x00\x01\x00\x00\x00\x04some'),
	],

	pg_types.ANYARRAYOID : [
		([0, 0xf, (1, 0), (b'foo',)],
			b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x01' \
			b'\x00\x00\x00\x00\x00\x00\x00\x03foo'
		),
		([0, 0xf, (1, 0), (None,)],
			b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x01' \
			b'\x00\x00\x00\x00\xff\xff\xff\xff'
		)
	],
}
expectation_samples[pg_types.BOXOID] = \
	expectation_samples[pg_types.LSEGOID] = [
		((1.0, 1.0, 1.0, 1.0),
			b'?\xf0\x00\x00\x00\x00\x00\x00?\xf0' \
			b'\x00\x00\x00\x00\x00\x00?\xf0\x00\x00' \
			b'\x00\x00\x00\x00?\xf0\x00\x00\x00\x00\x00\x00'),
		((2.0, 2.0, 1.0, 1.0),
			b'@\x00\x00\x00\x00\x00\x00\x00@\x00\x00' \
			b'\x00\x00\x00\x00\x00?\xf0\x00\x00\x00\x00' \
			b'\x00\x00?\xf0\x00\x00\x00\x00\x00\x00'),
		((-1.0, -1.0, 1.0, 1.0),
			b'\xbf\xf0\x00\x00\x00\x00\x00\x00\xbf\xf0' \
			b'\x00\x00\x00\x00\x00\x00?\xf0\x00\x00\x00' \
			b'\x00\x00\x00?\xf0\x00\x00\x00\x00\x00\x00'),
	]

expectation_samples[pg_types.OIDOID] = \
	expectation_samples[pg_types.CIDOID] = \
	expectation_samples[pg_types.XIDOID] = [
		(0, b'\x00\x00\x00\x00'),
		(1, b'\x00\x00\x00\x01'),
		(2, b'\x00\x00\x00\x02'),
		(0xf, b'\x00\x00\x00\x0f'),
		(0xffffffff, b'\xff\xff\xff\xff'),
		(0x7fffffff, b'\x7f\xff\xff\xff'),
	]

# this must pack and then unpack back into this
consistency_samples = {
	pg_types.BOOLOID : [True, False],

	pg_types.RECORDOID : [
		[],
		[(0,b'foo')],
		[(0,None)],
		[(15,None)],
		[(0xffffffff,None)],
		[(0,None), (1,b'some')],
		[(0,None), (1,b'some'), (0xffff, b"something_else\x00")],
		[(0,None), (1,b"s\x00me"), (0xffff, b"\x00something_else\x00")],
	],

	pg_types.ANYARRAYOID : [
		[0, 0xf, (), ()],
		[0, 0xf, (0, 0), ()],
		[0, 0xf, (1, 0), (b'foo',)],
		[0, 0xf, (1, 0), (None,)],
		[0, 0xf, (2, 0), (None,None)],
		[0, 0xf, (2, 0), (b'foo',None)],
		[0, 0xff, (2, 0), (None,b'foo',)],
		[0, 0xffffffff, (3, 0), (None,b'foo',None)],
		[1, 0xffffffff, (3, 0), (None,b'foo',None)],
		[1, 0xffffffff, (3, 0, 1, 0), (None,b'foo',None)],
		[1, 0xffffffff, (3, 0, 2, 0), (None,b'one',b'foo',b'two',None,b'three')],
	],

	# Just some random data; it's just an integer, so nothing fancy.
	pg_types.DATEOID : [
		123,
		321,
		0x7FFFFFF,
		-0x8000000,
	],

	pg_types.TIMETZOID : [
		((0, 0), 0),
		((123, 123), 123),
		((0xFFFFFFFF, 999999), -123),
	],

	pg_types.POINTOID : [
		(0, 0),
		(2, 2),
		(-1, -1),
		(-1.5, -1.2),
		(1.5, 1.2),
	],

	pg_types.CIRCLEOID : [
		(0, 0, 0),
		(2, 2, 2),
		(-1, -1, -1),
		(-1.5, -1.2, -1.8),
	],

	pg_types.TIDOID : [
		(0, 0),
		(1, 1),
		(0xffffffff, 0xffff),
		(0, 0xffff),
		(0xffffffff, 0),
		(0xffffffff // 2, 0xffff // 2),
	],

	pg_types.CIDROID : [
		(0, 0, b"\x00\x00\x00\x00"),
		(2, 0, b"\x00" * 4),
		(2, 0, b"\xFF" * 4),
		(2, 32, b"\xFF" * 4),
		(3, 0, b"\x00\x00" * 16),
	],

	pg_types.INETOID : [
		(2, 32, b"\x00\x00\x00\x00"),
		(2, 16, b"\x7f\x00\x00\x01"),
		(2, 8, b"\xff\x00\xff\x01"),
		(3, 128, b"\x7f\x00" * 16),
		(3, 64, b"\xff\xff" * 16),
		(3, 32, b"\x00\x00" * 16),
	],
}

consistency_samples[pg_types.TIMEOID] = \
consistency_samples[pg_types.TIMESTAMPOID] = \
consistency_samples[pg_types.TIMESTAMPTZOID] = [
	(0, 0),
	(123, 123),
	(0xFFFFFFFF, 999999),
]

# months, days, (seconds, microseconds)
consistency_samples[pg_types.INTERVALOID] = [
	(0, 0, (0, 0)),
	(1, 0, (0, 0)),
	(0, 1, (0, 0)),
	(1, 1, (0, 0)),
	(0, 0, (0, 10000)),
	(0, 0, (1, 0)),
	(0, 0, (1, 10000)),
	(1, 1, (1, 10000)),
	(100, 50, (1423, 29313))
]

consistency_samples[pg_types.OIDOID] = \
	consistency_samples[pg_types.CIDOID] = \
	consistency_samples[pg_types.XIDOID] = [
	0, 0xffffffff, 0xffffffff // 2, 123, 321, 1, 2, 3
]

consistency_samples[pg_types.LSEGOID] = \
	consistency_samples[pg_types.BOXOID] = [
	(1,2,3,4),
	(4,3,2,1),
	(0,0,0,0),
	(-1,-1,-1,-1),
	(-1.2,-1.5,-2.0,4.0)
]

consistency_samples[pg_types.PATHOID] = \
	consistency_samples[pg_types.POLYGONOID] = [
	(1,2,3,4),
	(4,3,2,1),
	(0,0,0,0),
	(-1,-1,-1,-1),
	(-1.2,-1.5,-2.0,4.0)
]

from types import GeneratorType
def resolve(ob):
	'make sure generators get "tuplified"'
	if type(ob) not in (list, tuple, GeneratorType):
		return ob
	return [resolve(x) for x in ob]

def testExpectIO(self, map, samples):
	for oid, sample in samples.items():
		for (sample_unpacked, sample_packed) in sample:
			pack, unpack = map[oid]

			pack_trial = pack(sample_unpacked)
			self.failUnless(
				pack_trial == sample_packed,
				"%s sample: unpacked sample, %r, did not match " \
				"%r when packed, rather, %r" %(
					pg_types.oid_to_name[oid], sample_unpacked,
					sample_packed, pack_trial
				)
			)

			sample_unpacked = resolve(sample_unpacked)
			unpack_trial = resolve(unpack(sample_packed))
			self.failUnless(
				unpack_trial == sample_unpacked,
				"%s sample: packed sample, %r, did not match " \
				"%r when unpacked, rather, %r" %(
					pg_types.oid_to_name[oid], sample_packed,
					sample_unpacked, unpack_trial
				)
			)

class test_typio(unittest.TestCase):
	def test_process_tuple(self):
		pt = pg_typio.process_tuple
		def funpass(procs, tup, col):
			pass
		self.failUnlessEqual(tuple(pt((),(), funpass)), ())
		self.failUnlessEqual(tuple(pt((int,),("100",), funpass)), (100,))
		self.failUnlessEqual(tuple(pt((int,int),("100","200"), funpass)), (100,200))
		self.failUnlessEqual(tuple(pt((int,int),(None,"200"), funpass)), (None,200))
		self.failUnlessEqual(tuple(pt((int,int,int),(None,None,"200"), funpass)), (None,None,200))
		# The exception handler must raise.
		self.failUnlessRaises(RuntimeError, pt, (int,), ("foo",), funpass)

		class ThisError(Exception):
			pass
		data = []
		def funraise(procs, tup, col):
			data.append((procs, tup, col))
			raise ThisError
		self.failUnlessRaises(ThisError, pt, (int,), ("foo",), funraise)
		self.failUnlessEqual(data[0], ((int,), ("foo",), 0))
		del data[0]
		self.failUnlessRaises(ThisError, pt, (int,int), ("100","bar"), funraise)
		self.failUnlessEqual(data[0], ((int,int), ("100","bar"), 1))

	def testExpectations(self):
		'IO tests where the pre-made expected serialized form is compared'
		testExpectIO(self, pg_typstruct.oid_to_io, expectation_samples)

	def testConsistency(self):
		'IO tests where the unpacked source is compared to re-unpacked result'
		for oid, sample in consistency_samples.items():
			pack, unpack = pg_typstruct.oid_to_io.get(oid, (None, None))
			if pack is not None:
				for x in sample:
					packed = pack(x)
					unpacked = resolve(unpack(packed))
					x = resolve(x)
					self.failUnless(x == unpacked,
						"inconsistency with %s, %r -> %r -> %r" %(
							pg_types.oid_to_name[oid],
							x, packed, unpacked
						)
					)
		for oid, (pack, unpack) in pg_typstruct.time_io.items():
			sample = consistency_samples.get(oid, [])
			for x in sample:
				packed = pack(x)
				unpacked = resolve(unpack(packed))
				x = resolve(x)
				self.failUnless(x == unpacked,
					"inconsistency with %s, %r -> %r -> %r" %(
						pg_types.oid_to_name[oid],
						x, packed, unpacked
					)
				)

		for oid, (pack, unpack) in pg_typstruct.time64_io.items():
			sample = consistency_samples.get(oid, [])
			for x in sample:
				packed = pack(x)
				unpacked = resolve(unpack(packed))
				x = resolve(x)
				self.failUnless(x == unpacked,
					"inconsistency with %s, %r -> %r -> %r" %(
						pg_types.oid_to_name[oid],
						x, packed, unpacked
					)
				)

try:
	from ..protocol import optimized as protocol_optimized

	class test_optimized(unittest.TestCase):
		def test_parse_tuple_message(self):
			ptm = protocol_optimized.parse_tuple_message
			self.failUnlessRaises(TypeError, ptm, tuple, "stringzor")
			self.failUnlessRaises(TypeError, ptm, tuple, 123)
			self.failUnlessRaises(ValueError, ptm, tuple, b'')
			self.failUnlessRaises(ValueError, ptm, tuple, b'0')

			notenoughdata = struct.pack('!H', 2)
			self.failUnlessRaises(ValueError, ptm, tuple, notenoughdata)

			wraparound = struct.pack('!HL', 2, 10) + (b'0' * 10) + struct.pack('!L', 0xFFFFFFFE)
			self.failUnlessRaises(ValueError, ptm, tuple, wraparound)

			oneatt_notenough = struct.pack('!HL', 2, 10) + (b'0' * 10) + struct.pack('!L', 15)
			self.failUnlessRaises(ValueError, ptm, tuple, oneatt_notenough)

			toomuchdata = struct.pack('!HL', 1, 3) + (b'0' * 10)
			self.failUnlessRaises(ValueError, ptm, tuple, toomuchdata)

			class faketup(tuple):
				def __new__(subtype, geeze):
					r = tuple.__new__(subtype, ())
					r.foo = geeze
					return r
			zerodata = struct.pack('!H', 0)
			r = ptm(tuple, zerodata)
			self.failUnlessRaises(AttributeError, getattr, r, 'foo')
			self.failUnlessRaises(AttributeError, setattr, r, 'foo', 'bar')
			self.failUnlessEqual(len(r), 0)

		def test_process_tuple(self):
			def funpass(procs, tup, col):
				pass
			pt = protocol_optimized.process_tuple
			# tuple() requirements
			self.failUnlessRaises(TypeError, pt, "foo", "bar", funpass)
			self.failUnlessRaises(TypeError, pt, (), "bar", funpass)
			self.failUnlessRaises(TypeError, pt, "foo", (), funpass)
			self.failUnlessRaises(ValueError, pt, (), ("foo",), funpass)
except ImportError:
	pass

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
