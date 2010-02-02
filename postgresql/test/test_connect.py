##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import sys
import os
import unittest

from .. import exceptions as pg_exc
from .. import unittest as pg_unittest

from ..driver import dbapi20 as dbapi20
from .. import driver as pg_driver
from .. import open as pg_open

msw = sys.platform in ('win32', 'win64')

class test_connect(pg_unittest.TestCaseWithCluster):
	"""
	postgresql.driver connectivity tests
	"""
	ip6 = '::1'
	ip4 = '127.0.0.1'
	host = 'localhost'
	params = {}
	cluster_path_suffix = '_test_connect'

	def __init__(self, *args, **kw):
		super().__init__(*args,**kw)
		# 8.4 nixed this.
		self.do_crypt = self.cluster.installation.version_info < (8,4)
		self.do_unix = sys.platform != msw

	def configure_cluster(self):
		super().configure_cluster()
		self.cluster.settings.update({
			'log_min_messages' : 'log',
			'unix_socket_directory' : self.cluster.data_directory,
		})

		# Configure the hba file with the supported methods.
		with open(self.cluster.hba_file, 'w') as hba:
			hosts = ['0.0.0.0/0',]
			if not msw:
				hosts.append('0::0/0')
			methods = ['md5', 'password'] + (['crypt'] if self.do_crypt else [])
			for h in hosts:
				for m in methods:
					# user and method are the same name.
					hba.writelines(['host test {m} {h} {m}\n'.format(
						h = h,
						m = m
					)])
			# trusted
			hba.writelines(["local all all trust\n"])
			hba.writelines(["host test trusted 0.0.0.0/0 trust\n"])
			if not msw:
				hba.writelines(["host test trusted 0::0/0 trust\n"])
			# admin lines
			hba.writelines(["host all test 0.0.0.0/0 trust\n"])
			if not msw:
				hba.writelines(["host all test 0::0/0 trust\n"])

	def initialize_database(self):
		super().initialize_database()
		with self.cluster.connection(user = 'test') as db:
			db.execute(
				"""
CREATE USER md5 WITH
	ENCRYPTED PASSWORD 'md5_password'
;

-- crypt doesn't work with encrypted passwords:
-- http://www.postgresql.org/docs/8.2/interactive/auth-methods.html#AUTH-PASSWORD
CREATE USER crypt WITH
	UNENCRYPTED PASSWORD 'crypt_password'
;

CREATE USER password WITH
	ENCRYPTED PASSWORD 'password_password'
;

CREATE USER trusted;
				"""
			)

	def test_pg_open_SQL_ASCII(self):
		# postgresql.open
		host, port = self.cluster.address()
		# test simple locators..
		with pg_open(
			'pq://' + 'md5:' + 'md5_password@' + host + ':' + str(port) \
			+ '/test?client_encoding=SQL_ASCII'
		) as db:
			self.failUnlessEqual(db.prepare('select 1')(), [(1,)])
			self.failUnlessEqual(db.settings['client_encoding'], 'SQL_ASCII')
		self.failUnless(db.closed)

	def test_pg_open_keywords(self):
		host, port = self.cluster.address()
		# straight test, no IRI
		with pg_open(
			user = 'md5',
			password = 'md5_password',
			host = host,
			port = port,
			database = 'test'
		) as db:
			self.failUnlessEqual(db.prepare('select 1')(), [(1,)])
		self.failUnless(db.closed)
		# composite test
		with pg_open(
			"pq://md5:md5_password@",
			host = host,
			port = port,
			database = 'test'
		) as db:
			self.failUnlessEqual(db.prepare('select 1')(), [(1,)])
		# override test
		with pg_open(
			"pq://md5:foobar@",
			password = 'md5_password',
			host = host,
			port = port,
			database = 'test'
		) as db:
			self.failUnlessEqual(db.prepare('select 1')(), [(1,)])
		# and, one with some settings
		with pg_open(
			"pq://md5:foobar@?search_path=ieeee",
			password = 'md5_password',
			host = host,
			port = port,
			database = 'test',
			settings = {'search_path' : 'public'}
		) as db:
			self.failUnlessEqual(db.prepare('select 1')(), [(1,)])
			self.failUnlessEqual(db.settings['search_path'], 'public')

	def test_pg_open(self):
		# postgresql.open
		host, port = self.cluster.address()
		# test simple locators..
		with pg_open(
			'pq://' + 'md5:' + 'md5_password@' + host + ':' + str(port) \
			+ '/test'
		) as db:
			self.failUnlessEqual(db.prepare('select 1')(), [(1,)])
		self.failUnless(db.closed)

		with pg_open(
			'pq://' + 'password:' + 'password_password@' + host + ':' + str(port) \
			+ '/test'
		) as db:
			self.failUnlessEqual(db.prepare('select 1')(), [(1,)])
		self.failUnless(db.closed)

		with pg_open(
			'pq://' + 'trusted@' + host + ':' + str(port) + '/test'
		) as db:
			self.failUnlessEqual(db.prepare('select 1')(), [(1,)])
		self.failUnless(db.closed)

		# test environment collection
		pgenv = ('PGUSER', 'PGPORT', 'PGHOST', 'PGSERVICE', 'PGPASSWORD', 'PGDATABASE')
		stored = list(map(os.environ.get, pgenv))
		try:
			os.environ.pop('PGSERVICE', None)
			os.environ['PGUSER'] = 'md5'
			os.environ['PGPASSWORD'] = 'md5_password'
			os.environ['PGHOST'] = host
			os.environ['PGPORT'] = str(port)
			os.environ['PGDATABASE'] = 'test'
			# No arguments, the environment provided everything.
			with pg_open() as db:
				self.failUnlessEqual(db.prepare('select 1')(), [(1,)])
				self.failUnlessEqual(db.prepare('select current_user').first(), 'md5')
			self.failUnless(db.closed)
		finally:
			i = 0
			for x in stored:
				env = pgenv[i]
				if x is None:
					os.environ.pop(env, None)
				else:
					os.environ[env] = x

		oldservice = os.environ.get('PGSERVICE')
		oldsysconfdir = os.environ.get('PGSYSCONFDIR')
		try:
			with open('pg_service.conf', 'w') as sf:
				sf.write('''
[myserv]
user = password
password = password_password
host = {host}
port = {port}
dbname = test
search_path = public
'''.format(host = host, port = port))
				sf.flush()
				try:
					os.environ['PGSERVICE'] = 'myserv'
					os.environ['PGSYSCONFDIR'] = os.getcwd()
					with pg_open() as db:
						self.failUnlessEqual(db.prepare('select 1')(), [(1,)])
						self.failUnlessEqual(db.prepare('select current_user').first(), 'password')
						self.failUnlessEqual(db.settings['search_path'], 'public')
				finally:
					if oldservice is None:
						os.environ.pop('PGSERVICE', None)
					else:
						os.environ['PGSERVICE'] = oldservice
					if oldsysconfdir is None:
						os.environ.pop('PGSYSCONFDIR', None)
					else:
						os.environ['PGSYSCONFDIR'] = oldsysconfdir
		finally:
			if os.path.exists('pg_service.conf'):
				os.remove('pg_service.conf')

	def test_dbapi_connect(self):
		host, port = self.cluster.address()
		MD5 = dbapi20.connect(
			user = 'md5',
			database = 'test',
			password = 'md5_password',
			host = host, port = port,
			**self.params
		)
		self.failUnlessEqual(MD5.cursor().execute('select 1').fetchone()[0], 1)
		MD5.close()
		self.failUnlessRaises(pg_exc.ConnectionDoesNotExistError,
			MD5.cursor().execute, 'select 1'
		)

		if self.do_crypt:
			CRYPT = dbapi20.connect(
				user = 'crypt',
				database = 'test',
				password = 'crypt_password',
				host = host, port = port,
				**self.params
			)
			self.failUnlessEqual(CRYPT.cursor().execute('select 1').fetchone()[0], 1)
			CRYPT.close()
			self.failUnlessRaises(pg_exc.ConnectionDoesNotExistError,
				CRYPT.cursor().execute, 'select 1'
			)

		PASSWORD = dbapi20.connect(
			user = 'password',
			database = 'test',
			password = 'password_password',
			host = host, port = port,
			**self.params
		)
		self.failUnlessEqual(PASSWORD.cursor().execute('select 1').fetchone()[0], 1)
		PASSWORD.close()
		self.failUnlessRaises(pg_exc.ConnectionDoesNotExistError,
			PASSWORD.cursor().execute, 'select 1'
		)

		TRUST = dbapi20.connect(
			user = 'trusted',
			database = 'test',
			password = '',
			host = host, port = port,
			**self.params
		)
		self.failUnlessEqual(TRUST.cursor().execute('select 1').fetchone()[0], 1)
		TRUST.close()
		self.failUnlessRaises(pg_exc.ConnectionDoesNotExistError,
			TRUST.cursor().execute, 'select 1'
		)

	def test_IP4_connect(self):
		C = pg_driver.default.ip4(
			user = 'test',
			host = '127.0.0.1',
			database = 'test',
			port = self.cluster.address()[1],
			**self.params
		)
		with C() as c:
			self.failUnlessEqual(c.prepare('select 1').first(), 1)

	if not msw:
		# win32 binaries don't appear to be built with ipv6
		# so filter this test on windows.
		def test_IP6_connect(self):
			C = pg_driver.default.ip6(
				user = 'test',
				host = '::1',
				database = 'test',
				port = self.cluster.address()[1],
				**self.params
			)
			with C() as c:
				self.failUnlessEqual(c.prepare('select 1').first(), 1)

	def test_Host_connect(self):
		C = pg_driver.default.host(
			user = 'test',
			host = 'localhost',
			database = 'test',
			port = self.cluster.address()[1],
			**self.params
		)
		with C() as c:
			self.failUnlessEqual(c.prepare('select 1').first(), 1)

	def test_md5_connect(self):
		c = self.cluster.connection(
			user = 'md5',
			password = 'md5_password',
			database = 'test',
			**self.params
		)
		with c:
			self.failUnlessEqual(c.prepare('select current_user').first(), 'md5')

	def test_crypt_connect(self):
		if self.do_crypt:
			c = self.cluster.connection(
				user = 'crypt',
				password = 'crypt_password',
				database = 'test',
				**self.params
			)
			with c:
				self.failUnlessEqual(c.prepare('select current_user').first(), 'crypt')

	def test_password_connect(self):
		c = self.cluster.connection(
			user = 'password',
			password = 'password_password',
			database = 'test',
		)
		with c:
			self.failUnlessEqual(c.prepare('select current_user').first(), 'password')

	def test_trusted_connect(self):
		c = self.cluster.connection(
			user = 'trusted',
			password = '',
			database = 'test',
			**self.params
		)
		with c:
			self.failUnlessEqual(c.prepare('select current_user').first(), 'trusted')

	def test_Unix_connect(self):
		if msw:
			return
		unix_domain_socket = os.path.join(
			self.cluster.data_directory,
			'.s.PGSQL.' + self.cluster.settings['port']
		)
		C = pg_driver.default.unix(
			user = 'test',
			unix = unix_domain_socket,
		)
		with C() as c:
			self.failUnlessEqual(c.prepare('select 1').first(), 1)
			self.failUnlessEqual(c.client_address, None)

	def test_pg_open_unix(self):
		if msw:
			return
		unix_domain_socket = os.path.join(
			self.cluster.data_directory,
			'.s.PGSQL.' + self.cluster.settings['port']
		)
		with pg_open(unix = unix_domain_socket, user = 'test') as c:
			self.failUnlessEqual(c.prepare('select 1').first(), 1)
			self.failUnlessEqual(c.client_address, None)
		with pg_open('pq://test@[unix:' + unix_domain_socket.replace('/',':') + ']') as c:
			self.failUnlessEqual(c.prepare('select 1').first(), 1)
			self.failUnlessEqual(c.client_address, None)

if __name__ == '__main__':
	unittest.main()
