##
# copyright 2007, pg/python project.
# http://python.projects.postgresql.org
##
import unittest
import postgresql.iri as pg_iri

def cmbx(t):
	'Yield a list of combinations using a mask'
	tl = len(t)
	return [
		[x & m and t[x-1] or '' for x in range(1, tl + 1)]
		for m in range(1, (2 ** tl) + 1)
	]

iri_samples = (
	'host/dbname/path?param=val#frag',
	'{ssh remotehost nc "$(cat%_somehost%_)" 5432}',
	'user:pass@{ssh remotehost nc somehost 5432}/dbname',
	'#frag',
	'?param=val',
	'?param=val#frag',
	'user@',
	':pass@',
	'u:p@h',
	'u:p@h:1',
)

iri_abnormal_normal_samples = {
	'pq://fæm.com:123/õéf/á?param=val' :
		'pq://xn--fm-1ia.com:123/õéf/á?param=val',
	'pq://l»»@fæm.com:123/õéf/á?param=val' :
		'pq://l»»@xn--fm-1ia.com:123/õéf/á?param=val',
	'pq://fæᎱᏋm.com/õéf/á?param=val' :
		'pq://xn--fm-1ia853qzd.com/õéf/á?param=val',
}

# Split RI bases
iri_split_samples = [
	('host:1111', 'dbname', 'path', 'param=val&param2=val', 'frag'),
	('host:1111', 'dbname', 'paths:p2', 'param=val&param2=val', 'frag'),
	('host', 'dbname', 'paths:p2', 'p2=val', 'frag()'),
# IPvN
	('[::1]:9876', 'dbname', 'paths:p2', 'p2=val', 'frag()'),
# Some unicode lovin'
	('fæm.com', 'dátabÁse', 'pôths:p2', 'p2=vïl', 'frag(ëì)'),
	('fæm.com:5432', 'dátabÁse', 'pôths:p2', 'p2=vïl', 'frag(ëì)'),
	('{ssh h nc localhost 5432}', 'dbname', 'paths:p2', 'p2=val', 'frag()'),
	('{::1}:9876', 'dbname', 'paths:p2', 'p2=val', 'frag()'),
#	('%xfa.:5432', 'dátabÁse', 'pôths:p2-postfix:p3', 'p2=vïl', 'frag(ëì)'),
]

sample_structured_parameters = [
	{
		'host' : 'hostname',
		'port' : 1234,
		'database' : 'foo_db',
	},
	{
		'process' : ['ssh', 'u@foo.com', 'nc localhost 8976'],
		'database' : 'foo_db',
	},
	{
		'user' : 'username',
		'database' : 'database_name',
		'settings' : {'foo':'bar','feh':'bl%,23'},
	},
	{
		'user' : 'username',
		'database' : 'database_name',
	},
	{
		'database' : 'database_name',
	},
	{
		'user' : 'user_name',
	},
	{
		'password' : 'passwd',
	},
	{
		'host' : 'hostname',
	},
	{
		'user' : 'username',
		'password' : 'pass',
		'port' : 4321,
		'database' : 'database_name',
		'path' : ['path'],
		'fragment' : 'random_data',
	}
]

escaped_unescaped = [
	('%_', ' '),
	('%N', '\n'),
	('%T', '\t'),
	('%%', '%'),
	# Doesn't match. (invalid percent escape)
	('foo%bzr', 'foo%bzr'),
	('foo%bar', 'foo\xbar'),
	('foo%00bar', 'foo\0bar'),
	('foo%_bar', 'foo bar'),
	('fooæbar', 'fooæbar'),
	('fooæbar', 'fooæbar'),
	('', ''),
]

percent_values = {
	0x0 : '%00',
	0x1 : '%01',
	0x10 : '%10',
	0x20 : '%20',
	0x23 : '%23',
	0x93 : '%93',
}

sample_split_paths = (
	['foo', 'bar'],
	[':'],
	[':', '/'],
	['colon:', '/slash'],
	['/'],
	[':/'],
	['/:'],
	['foo'],
	['foo:bar', 'feh'],
	['foo/bar'],
	['::foo/bar'],
	['p'],
	['\x00'],
	[':\x00:'],
	[':\x00:', '!@#$%^&'],
)

sample_paths = (
	'foobar',
	'%3A',
	'%2F',
	'%3A%2F',
	'%2F%3A',
	'foo',
	'foo%3Abar',
	'foo%2Fbar',
	'%3A%3Afoo%2Fbar',
	'p',
	'%00',
	'%3A%00%3A',
)

sample_netlocs = (
	'user:pass@host:1234',
	'us%3Aer:p%40ss@host:1234',
	'user@host',
	':pass@host',
	':pass@:4321',
	':pass@host:1234',
	'user:pass@host:1234',
	'{cmd}',
	'user@{cmd}',
	'user@{cmd @foo}',
	'user:pass@{cmd @foo}',
	':pass@{cmd @foo}',
	':pass@{cmd }',
)

sample_split_netlocs = (
	('user', 'pass', 'host', '1234'),
	('us@er', 'p@ss', 'host', '1234'),
	('îíus@:er', 'p@:ss', 'æçèêíãøú.com'.encode('idna'), '5432'),
	('us@:er', 'íáp@:ss', 'æçèêíãøú.com'.encode('idna'), '5432'),
)

class test_ri(unittest.TestCase):
	def testEscapes(self):
		for e, u in escaped_unescaped:
			self.failUnless(pg_iri.unescape(e) == u,
				'escape incongruity, %r(%r) != %r' %(
					e, pg_iri.unescape(e), u
				)
			)

	def testPercentValue(self):
		for k, v in percent_values.iteritems():
			self.failUnless(
				v == pg_iri.percent_value(k),
				'percent_value %x did not make %r, rather %r' %(
					k, v, pg_iri.percent_value(k),
				)
			)
	
	def testSplitPath(self):
		for x in sample_paths:
			y = pg_iri.split_path(x)
			xy = pg_iri.unsplit_path(y)
			self.failUnless(
				xy == x,
				'path split-unsplit incongruity %r -> %r -> %r' %(
					x, y, xy
				)
			)

	def testUnsplitPath(self):
		for x in sample_split_paths:
			y = pg_iri.unsplit_path(x)
			xy = pg_iri.split_path(y)
			self.failUnless(
				xy == x,
				'path unsplit-split incongruity %r -> %r -> %r' %(
					x, y, xy
				)
			)

	def testSplitUnsplitNetloc(self):
		for x in sample_netlocs:
			y = pg_iri.split_netloc(x)
			xy = pg_iri.unsplit_netloc(y)
			self.failUnless(
				xy == x,
				'netloc split-unsplit incongruity %r -> %r -> %r' %(
					x, y, xy
				)
			)

	def testUnsplitSplitNetloc(self):
		for t in sample_split_netlocs:
			for x in cmbx(t):
				x = tuple(x)
				y = pg_iri.unsplit_netloc(x)
				xy = pg_iri.split_netloc(y)
				self.failUnless(
					xy == x,
					'netloc unsplit-split incongruity %r -> %r -> %r' %(
						x, y, xy
					)
				)

	def testSplitUnsplit(self):
		scheme = 'pq://'
		for x in iri_samples:
			xs = pg_iri.split(x)
			uxs = pg_iri.unsplit(xs)
			self.failUnless(
				x.strip('/ ') == uxs[len(scheme):].rstrip('/ '),
				"split-unsplit incongruity, %r -> %r -> %r" %(
					x, xs, uxs
				)
			)

	def testUnsplitSplit(self):
		for scheme in ('pq', 'pqs',):
			for b in iri_split_samples:
				for x in cmbx(b):
					x.insert(0, scheme)
					x = tuple(x)
					xs = pg_iri.unsplit(x)
					uxs = pg_iri.split(xs)
					self.failUnless(
						uxs == x,
						"unsplit-split incongruity, %r -> %r -> %r" %(
							x, xs, uxs
						)
					)

	def testParseSerialize(self):
		scheme = 'pq://'
		for x in iri_samples:
			xs = pg_iri.parse(x)
			uxs = pg_iri.serialize(xs)
			c1 = pg_iri.unescape(x.strip('/ '))
			c2 = pg_iri.unescape(uxs[len(scheme):].strip('/ '))
			self.failUnless(
				c1 == c2,
				"parse-serialize incongruity, %r -> %r -> %r : %r != %r" %(
					x, xs, uxs, c1, c2
				)
			)
	
	def testSerializeParse(self):
		for x in sample_structured_parameters:
			xs = pg_iri.serialize(x)
			uxs = pg_iri.parse(xs)
			self.failUnless(
				x == uxs,
				"serialize-parse incongruity, %r -> %r -> %r" %(
					x, xs, uxs,
				)
			)

	def testConstructStructure(self):
		for x in sample_structured_parameters:
			y = pg_iri.construct(x)
			xy = pg_iri.structure(y)
			self.failUnless(
				x == xy,
				"construct-structre incongruity, %r -> %r -> %r" %(
					x, y, xy
				)
			)

	def testConstructStructure(self):
		for x in sample_structured_parameters:
			y = pg_iri.construct(x)
			xy = pg_iri.structure(y)
			self.failUnless(
				x == xy,
				"construct-structre incongruity, %r -> %r -> %r" %(
					x, y, xy
				)
			)

	def testNormalize(self):
		for abn, norm in iri_abnormal_normal_samples.iteritems():
			normalized = pg_iri.normalize(abn)
			self.failUnless(
				normalized == norm,
				"normalization failure, {%r: %r} != %r" %(
					abn, norm, normalized
				)
			)

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
