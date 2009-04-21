##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import sys
import time
import unittest
import tempfile
from ..installation import Installation
from ..cluster import Cluster

default_install = Installation.default()
if default_install is None:
	sys.stderr.write("ERROR: cannot find 'default' pg_config\n")
	sys.stderr.write("HINT: set the PGINSTALLATION environment variable to the `pg_config` path\n")
	sys.exit(1)

class test_cluster(unittest.TestCase):
	def setUp(self):
		self.cluster = Cluster('test_cluster', default_install)

	def tearDown(self):
		self.cluster.drop()
		self.cluster = None

	def start_cluster(self):
		self.cluster.start(logfile = None)
		self.cluster.wait_until_started(timeout = 10)

	def init(self, *args, **kw):
		self.cluster.init(*args, **kw)
		self.cluster.settings.update({
			'max_connections' : '8',
			'listen_addresses' : 'localhost',
			'port' : '6543',
			'silent_mode' : 'off',
		})

	def testSilentMode(self):
		self.init(logfile = None)
		self.cluster.settings['silent_mode'] = 'on'
		# if it fails to start(ClusterError), silent_mode is not working properly.
		self.start_cluster()

	def testSuperPassword(self):
		self.init(
			user = 'test',
			password = 'secret',
			logfile = sys.stdout,
		)
		self.start_cluster()
		c = self.cluster.connection(
			user='test',
			password='secret',
			database='template1',
		)
		with c:
			self.failUnless(c.prepare('select 1').first() == 1)

	def testNoParameters(self):
		'simple init and drop'
		self.init()
		self.start_cluster()

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
